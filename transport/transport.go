package transport

import (
	"context"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/domain"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/raft"
	"github.com/ColdToo/Cold2DB/transport/transport"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"go.etcd.io/etcd/etcdserver/api/snap"

	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Raft app_node实现该接口
type RaftTransport interface {
	Process(ctx context.Context, m *pb.Message) error
	IsIDRemoved(id uint64) bool
	ReportUnreachable(id uint64)
	ReportSnapshotStatus(id uint64, status raft.SnapshotStatus)
}

type Transporter interface {
	Initialize() error

	Handler() http.Handler

	// Send 应用层通过该接口发送消息给peer,如果在transport中没有找到该peer那么忽略该消息
	Send(m []pb.Message)

	SendSnapshot(m snap.Message)

	AddRemote(id types.ID, urls []string)

	AddPeer(id types.ID, urls []string)

	RemovePeer(id types.ID)

	RemoveAllPeers()

	UpdatePeer(id types.ID, urls []string)

	ActiveSince(id types.ID) time.Time

	ActivePeers() int

	Stop()
}

type Transport struct {
	//DialTimeout是请求超时时间，而DialRetryFrequency定义了每个对等节点的重试频率限制，即每秒最多重试10次。
	DialTimeout time.Duration

	DialRetryFrequency time.Duration

	TLSInfo transport.TLSInfo // TLS information used when creating connection

	LocalID   types.ID   // local member ID
	URLs      types.URLs // local peer URLs
	ClusterID types.ID   // raft cluster ID for request validation
	Raft      Raft       // raft state machine, to which the Transport forwards received messages and reports status

	//todo 实现一个快照管理器 方便传输快照
	Snapshotter *db.SnapShotter

	ErrorC chan error

	streamRt   http.RoundTripper // roundTripper used by streams
	pipelineRt http.RoundTripper // roundTripper used by pipelines

	mu sync.RWMutex // protect the remote and peer map

	//帮助其追赶集群的进度，但在追赶完成后就不再使用。而t.peers是一个map[types.ID]Peer类型的变量，用于维护集群中的所有节点。
	//其中，Peer是一个接口类型，定义了send、stop等方法，用于发送消息和停止节点
	remotes map[types.ID]*remote
	peers   map[types.ID]Peer
}

func (t *Transport) Initialize() error {
	var err error
	t.streamRt, err = newStreamRoundTripper(t.TLSInfo, t.DialTimeout)
	if err != nil {
		return err
	}

	//创建Pipeline消息通道用的http.RoundTripper实例与streamRt不同的是，读写请求的起时时间设置成了永不过期
	t.pipelineRt, err = NewPipeLineRoundTripper(t.TLSInfo, t.DialTimeout)
	if err != nil {
		return err
	}

	t.remotes = make(map[types.ID]*remote)
	t.peers = make(map[types.ID]Peer)

	if t.DialRetryFrequency == 0 {
		t.DialRetryFrequency = 100 * time.Millisecond
	}
	return nil
}

func (t *Transport) Handler() http.Handler {
	pipelineHandler := newPipelineHandler(t, t.Raft, t.ClusterID)
	streamHandler := newStreamHandler(t, t, t.Raft, t.LocalID, t.ClusterID)
	snapHandler := newSnapshotHandler(t, t.Raft, t.Snapshotter, t.ClusterID) //应该是v3版本使用，之前的版本使用pipelineHandler处理收到的快照)。

	//路由
	mux := http.NewServeMux()
	mux.Handle(RaftPrefix, pipelineHandler)
	mux.Handle(RaftStreamPrefix+"/", streamHandler)
	mux.Handle(RaftSnapshotPrefix, snapHandler)
	return mux
}

func (t *Transport) Get(id types.ID) Peer {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.peers[id]
}

func (t *Transport) Send(msgs []pb.Message) {
	for _, m := range msgs {
		if m.To == 0 {
			continue
		}
		to := types.ID(m.To)

		t.mu.RLock()
		p, pok := t.peers[to]
		g, rok := t.remotes[to]
		t.mu.RUnlock()

		if pok {
			if m.MsgType == pb.MessageType_MsgAppend {
				t.ServerStats.SendAppendReq(int(m.Size()))
			}
			p.send(m)
			continue
		}

		if rok {
			g.send(m)
			continue
		}

		domain.Log.Warn()
		t.Logger.Debug(
			"ignored message send request; unknown remote peer target",
			zap.String("type", m.Type.String()),
			zap.String("unknown-target-peer-id", to.String()),
		)
	}
}

func (t *Transport) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, r := range t.remotes {
		r.stop()
	}
	for _, p := range t.peers {
		p.stop()
	}

	if tr, ok := t.streamRt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	if tr, ok := t.pipelineRt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	t.peers = nil
	t.remotes = nil
}

// CutPeer drops messages to the specified peer.
func (t *Transport) CutPeer(id types.ID) {
	t.mu.RLock()
	p, pok := t.peers[id]
	g, gok := t.remotes[id]
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Pause()
	}
	if gok {
		g.Pause()
	}
}

// MendPeer recovers the message dropping behavior of the given peer.
func (t *Transport) MendPeer(id types.ID) {
	t.mu.RLock()
	p, pok := t.peers[id]
	g, gok := t.remotes[id]
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Resume()
	}
	if gok {
		g.Resume()
	}
}

func (t *Transport) AddRemote(id types.ID, urlList []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.remotes == nil {
		// there's no clean way to shutdown the golang http server
		// (see: https://github.com/golang/go/issues/4674) before
		// stopping the transport; ignore any new connections.
		return
	}
	if _, ok := t.peers[id]; ok {
		return
	}
	if _, ok := t.remotes[id]; ok {
		return
	}
	urls, err := types.NewURLs(urlList)
	if err != nil {
		t.Logger.Panic("failed NewURLs", zap.Strings("urls", urlList), zap.Error(err))
	}
	t.remotes[id] = startRemote(t, urls, id)

	t.Logger.Info(
		"added new remote peer",
		zap.String("local-member-id", t.LocalID.String()),
		zap.String("remote-peer-id", id.String()),
		zap.Strings("remote-peer-urls", urlList),
	)
}

// AddPeer peer 相当于是其他节点在本地的代言人，本地节点发送消息给其他节点实质就是传入参数给其他节点在本地的代言人peer
func (t *Transport) AddPeer(id types.ID, urlList []string) {
	urls, err := types.NewURLs(urlList)
	if err != nil {
		log.Panic("failed NewURLs").Err("urls", err)
	}

	t.peers[id] = startPeer(t, urls, id)
	log.Info("added remote peer").Str(code.LocalMemberId, t.LocalID.Str()).Str(code.RemotePeerId, id.Str()).Record()
}

func (t *Transport) RemovePeer(id types.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.removePeer(id)
}

func (t *Transport) RemoveAllPeers() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id := range t.peers {
		t.removePeer(id)
	}
}

// the caller of this function must have the peers mutex.
func (t *Transport) removePeer(id types.ID) {
	if peer, ok := t.peers[id]; ok {
		peer.stop()
	} else {
		t.Logger.Panic("unexpected removal of unknown remote peer", zap.String("remote-peer-id", id.String()))
	}
	delete(t.peers, id)
	delete(t.LeaderStats.Followers, id.String())

	t.Logger.Info(
		"removed remote peer",
		zap.String("local-member-id", t.ID.String()),
		zap.String("removed-remote-peer-id", id.String()),
	)
}

func (t *Transport) UpdatePeer(id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// TODO: return error or just panic?
	if _, ok := t.peers[id]; !ok {
		return
	}
	urls, err := types.NewURLs(us)
	if err != nil {
		t.Logger.Panic("failed NewURLs", zap.Strings("urls", us), zap.Error(err))
	}
	t.peers[id].update(urls)

	t.Logger.Info(
		"updated remote peer",
		zap.String("local-member-id", t.ID.String()),
		zap.String("updated-remote-peer-id", id.String()),
		zap.Strings("updated-remote-peer-urls", us),
	)
}

func (t *Transport) SendSnapshot(m snap.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.peers[types.ID(m.To)]
	if p == nil {
		m.CloseWithError(errMemberNotFound)
		return
	}

	p.sendSnap(m)
}

// Pausable is a testing interface for pausing transport traffic.
type Pausable interface {
	Pause()
	Resume()
}

func (t *Transport) Pause() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.peers {
		p.(Pausable).Pause()
	}
}

func (t *Transport) Resume() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.peers {
		p.(Pausable).Resume()
	}
}

func (t *Transport) ActiveSince(id types.ID) time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if p, ok := t.peers[id]; ok {
		return p.activeSince()
	}
	return time.Time{}
}

func (t *Transport) ActivePeers() (cnt int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.peers {
		if !p.activeSince().IsZero() {
			cnt++
		}
	}
	return cnt
}
