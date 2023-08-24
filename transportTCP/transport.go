package transportTCP

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/raft"
	types "github.com/ColdToo/Cold2DB/transportHttp/types"
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
	URLs      types.URLs    // Peers URLs
	ClusterID types.ID      // raft cluster ID for request validation
	Raft      RaftTransport // raft state machine, to which the Transport forwards received messages and reports status

	Snapshotter *db.SnapShotter

	ErrorC chan error

	streamRt   http.RoundTripper // roundTripper used by streams
	pipelineRt http.RoundTripper // roundTripper used by pipelines

	mu sync.RWMutex // protect the remote and peer map

	//帮助其追赶集群的进度，但在追赶完成后就不再使用。而t.Peers是一个map[types.ID]Peer类型的变量，用于维护集群中的所有节点。
	//其中，Peer是一个接口类型，定义了send、stop等方法，用于发送消息和停止节点
	Peers map[types.ID]Peer
}

// AddPeer peer 相当于是其他节点在本地的代言人，本地节点发送消息给其他节点实质是将消息递给peer由peer发送给对端节点
func (t *Transport) AddPeer(peerID types.ID, u string) {
	recvC := make(chan *pb.Message, recvBufSize)
	propC := make(chan *pb.Message, maxPendingProposals)
	ctx, cancel := context.WithCancel(context.Background())
	Peerstatus := newPeerStatus(t.LocalID, peerID)
	streamReader := startStreamReader(t.LocalID, peerID, Peerstatus, cancel, t, recvC, propC, t.ErrorC, u)
	streamWriter := startStreamWriter(t.LocalID, peerID, Peerstatus, t.Raft)
	p := &peer{
		localID:      t.LocalID,
		remoteID:     peerID,
		url:          u,
		raft:         t.Raft,
		status:       Peerstatus,
		streamWriter: streamWriter,
		streamReader: streamReader,
		recvC:        recvC,
		propC:        propC,
		stopc:        make(chan struct{}),
		cancel:       cancel,
	}

	p.handleReceiveCAndPropC(t.Raft, ctx)
	t.Peers[peerID] = p
	log.Info("added remote peer").Str(code.LocalId, t.LocalID.Str()).Str(code.RemoteId, peerID.Str()).Record()
}

func (t *Transport) ListenPeer(localIp string) {
	log.Debugf("start app server node id: &s", t.LocalID, "listening...")
	listener, err := NewStoppableListener(localIp, make(chan struct{}))
	if err != nil {
		return
	}
	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Panic("Accept tcp err").Record()
		}

		remoteAddr := conn.RemoteAddr().String()
		log.Debugf("Get conn remote addr = ", remoteAddr)

		for _, v := range t.Peers {
			p := v.(*peer)
			if strings.Contains(p.url, remoteAddr) {
				v.attachConn(conn)
			}
		}
		log.Errorf("get wrong conn the remote ip addr:%s close it", remoteAddr)
		conn.Close()
	}
}

func (t *Transport) Get(id types.ID) Peer {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Peers[id]
}

func (t *Transport) Send(msgs []*pb.Message) {
	for _, m := range msgs {
		if m.To == 0 {
			continue
		}
		to := types.ID(m.To)

		t.mu.RLock()
		p, pok := t.Peers[to]
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
	for _, p := range t.Peers {
		p.stop()
	}

	if tr, ok := t.streamRt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	if tr, ok := t.pipelineRt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	t.Peers = nil
}

func (t *Transport) CutPeer(id types.ID) {
	t.mu.RLock()
	p, pok := t.Peers[id]
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Pause()
	}
}

func (t *Transport) MendPeer(id types.ID) {
	t.mu.RLock()
	p, pok := t.Peers[id]
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
	for id := range t.Peers {
		t.removePeer(id)
	}
}

func (t *Transport) removePeer(id types.ID) {
	if peer, ok := t.Peers[id]; ok {
		peer.stop()
	} else {
		log.Panic("unexpected removal of unknown remote peer").Str("remote-peer-id", id.Str()).Record()
	}
	delete(t.Peers, id)

	log.Info("removed remote peer").Str("local-member-id", t.LocalID.Str()).Str("removed-remote-peer-id", id.Str())
}

func (t *Transport) UpdatePeer(id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// TODO: return error or just panic?
	if _, ok := t.Peers[id]; !ok {
		return
	}
	urls, err := types.NewURLs(us)
	if err != nil {
	}

	log.Info("updated remote peer").Str("local-member-id", t.LocalID.Str()).
		Str("updated-remote-peer-id", id.Str()).
		Str("updated-remote-peer-urls", urls.String())
}

//todo
//func (t *Transport) SendSnapshot(m snap.Message) {
//}

// Pausable is a testing interface for pausing transportHttp traffic.
type Pausable interface {
	Pause()
	Resume()
}

func (t *Transport) Pause() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.Peers {
		p.(Pausable).Pause()
	}
}

func (t *Transport) Resume() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.Peers {
		p.(Pausable).Resume()
	}
}

func (t *Transport) ActiveSince(id types.ID) time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if p, ok := t.Peers[id]; ok {
		return p.activeSince()
	}
	return time.Time{}
}

func (t *Transport) ActivePeers() (cnt int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.Peers {
		if !p.activeSince().IsZero() {
			cnt++
		}
	}
	return cnt
}