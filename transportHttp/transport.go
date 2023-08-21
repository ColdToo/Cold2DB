package transportHttp

import (
	"context"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/raft"
	types "github.com/ColdToo/Cold2DB/transportHttp/types"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"net/http"
	"sync"
	"time"
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
	Send(m []*pb.Message)

	SendSnapshot(m snap.Message)

	AddPeer(id types.ID, url string)

	RemovePeer(id types.ID)

	RemoveAllPeers()

	UpdatePeer(id types.ID, urls []string)

	ActiveSince(id types.ID) time.Time

	ActivePeers() int

	Stop()
}

type Transport struct {
	//DialTimeout是请求超时时间，而DialRetryFrequency定义了每个对等节点的重试频率限制
	DialTimeout        time.Duration
	DialRetryFrequency time.Duration

	TLSInfo TLSInfo // TLS information used when creating connection

	LocalID   types.ID      // 本地节点的ID
	URLs      types.URLs    // peers URLs
	ClusterID types.ID      // raft cluster ID for request validation
	Raft      RaftTransport // raft state machine, to which the Transport forwards received messages and reports status

	//todo 实现一个快照管理器 方便传输快照
	Snapshotter *db.SnapShotter

	ErrorC chan error

	streamRt   http.RoundTripper // roundTripper used by streams
	pipelineRt http.RoundTripper // roundTripper used by pipelines

	mu sync.RWMutex // protect the remote and peer map

	//帮助其追赶集群的进度，但在追赶完成后就不再使用。而t.peers是一个map[types.ID]Peer类型的变量，用于维护集群中的所有节点。
	//其中，Peer是一个接口类型，定义了send、stop等方法，用于发送消息和停止节点
	peers map[types.ID]Peer
}

// AddPeer peer 相当于是其他节点在本地的代言人，本地节点发送消息给其他节点实质就是传入参数给其他节点在本地的代言人peer
func (t *Transport) AddPeer(peerID types.ID, url string) {
	peerStatus := newPeerStatus(t.LocalID, peerID)
	recvC := make(chan *pb.Message, recvBufSize)
	propC := make(chan *pb.Message, maxPendingProposals)
	ctx, cancel := context.WithCancel(context.Background())

	streamWriter := startStreamWriter(t.LocalID, peerID, peerStatus, t.Raft)
	streamReader := startStreamReader(t.LocalID, peerID, peerStatus, cancel, t, recvC, propC, t.ErrorC)
	p := &peer{
		localID:      t.LocalID,
		remoteID:     peerID,
		raft:         t.Raft,
		status:       peerStatus,
		streamWriter: streamWriter,
		streamReader: streamReader,
		recvC:        recvC,
		propC:        propC,
		stopc:        make(chan struct{}),
		cancel:       cancel,
	}
	p.handleReceiveCAndPropC(t.Raft, ctx)
	t.peers[peerID] = p
	log.Info("added remote peer").Str(code.LocalId, t.LocalID.Str()).Str(code.RemoteId, peerID.Str()).Record()
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

	t.peers = make(map[types.ID]Peer)
	t.DialRetryFrequency = 100 * time.Millisecond

	return nil
}

func (t *Transport) Handler() http.Handler {
	streamHandler := newStreamHandler(t, t, t.Raft, t.LocalID, t.ClusterID)
	mux := http.NewServeMux()
	mux.Handle(RaftStream, streamHandler)
	return mux
}

func (t *Transport) Get(id types.ID) Peer {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.peers[id]
}

func (t *Transport) Send(msgs []*pb.Message) {
	for _, m := range msgs {
		if m.To == 0 {
			continue
		}
		to := types.ID(m.To)

		t.mu.RLock()
		p, pok := t.peers[to]
		t.mu.RUnlock()

		if pok {
			p.send(m)
			continue
		}
	}
}

func (t *Transport) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
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
}

func (t *Transport) CutPeer(id types.ID) {
	t.mu.RLock()
	p, pok := t.peers[id]
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Pause()
	}
}

func (t *Transport) MendPeer(id types.ID) {
	t.mu.RLock()
	p, pok := t.peers[id]
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Resume()
	}
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

func (t *Transport) removePeer(id types.ID) {
	if peer, ok := t.peers[id]; ok {
		peer.stop()
	} else {
		log.Panic("unexpected removal of unknown remote peer").Str("remote-peer-id", id.Str()).Record()
	}
	delete(t.peers, id)

	log.Info("removed remote peer").Str("local-member-id", t.LocalID.Str()).Str("removed-remote-peer-id", id.Str())
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
	}

	log.Info("updated remote peer").Str("local-member-id", t.LocalID.Str()).
		Str("updated-remote-peer-id", id.Str()).
		Str("updated-remote-peer-urls", urls.String())
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

// Pausable is a testing interface for pausing transportHttp traffic.
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
