package transport

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/raft"
	types "github.com/ColdToo/Cold2DB/transport/types"
)

type RaftTransport interface {
	Process(ctx context.Context, m *pb.Message) error
	IsIDRemoved(id uint64) bool
	ReportUnreachable(id uint64)
	ReportSnapshotStatus(id uint64, status raft.SnapshotStatus)
}

type Transporter interface {
	ListenPeerConn(localIp string)

	// Send 应用层通过该接口发送消息给peer,如果在transport中没有找到该peer那么忽略该消息
	Send(m []*pb.Message)

	AddPeer(id types.ID, url string)

	RemovePeer(id types.ID)

	RemoveAllPeers()

	UpdatePeer(id types.ID, url string)

	ActiveSince(id types.ID) time.Time

	ActivePeers() int

	Stop()
}

type Transport struct {
	LocalID   types.ID   // 本地节点的ID
	ClusterID types.ID   // raft cluster ID for request validation
	URLs      types.URLs // Peers URLs

	TLSInfo     TLSInfo       // TLS information used when creating connection
	Raft        RaftTransport // raft state machine, to which the Transport forwards received messages and reports status
	Snapshotter *db.SnapShotter

	// ErrorC is used to report detected critical errors, e.g.,
	// the member has been permanently removed from the cluster
	// When an error is received from ErrorC, user should stop raft state
	// machine and thus stop the Transport.
	ErrorC chan error
	StopC  chan struct{}

	mu sync.RWMutex // protect the remote and peer map
	//其中，Peer是一个接口类型，定义了send、stop等方法，用于发送消息和停止节点
	Peers map[types.ID]Peer
}

// AddPeer peer 相当于是其他节点在本地的代言人，本地节点发送消息给其他节点实质是将消息递给peer由peer发送给对端节点
func (t *Transport) AddPeer(peerID types.ID, u string) {
	recvC := make(chan *pb.Message, recvBufSize)
	propC := make(chan *pb.Message, maxPendingProposals)
	ctx, cancel := context.WithCancel(context.Background())
	Peerstatus := newPeerStatus(peerID)
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

	p.handleReceiveCAndPropC(ctx)
	t.Peers[peerID] = p
	log.Info("added remote peer success").Str(code.LocalId, t.LocalID.Str()).Str(code.RemoteId, peerID.Str()).Record()
}

func (t *Transport) ListenPeerConn(localIp string) {
	log.Debugf("start app server node id: &s", t.LocalID, "listening...")
	listener, err := NewStoppableListener(localIp, t.StopC)
	if err != nil {
		return
	}
	for {
	flag:
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Panic("Accept tcp err").Record()
		}

		remoteAddr := strings.Split(conn.RemoteAddr().String(), ":")
		log.Debugf("Get conn remote addr = ", remoteAddr)

		for _, v := range t.Peers {
			p := v.(*peer)
			if len(remoteAddr) == 2 && strings.Contains(p.url, remoteAddr[0]) {
				v.attachConn(conn)
				goto flag
			}
		}
		log.Errorf("get wrong conn the remote ip addr:%s drop it", remoteAddr)
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
	t.Peers = nil
	// 停止listen
	close(t.StopC)
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

func (t *Transport) UpdatePeer(id types.ID, us string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// todo 修改url并重新dial
	log.Info("updated remote peer").Str("local-member-id", t.LocalID.Str()).
		Str("updated-remote-peer-id", id.Str()).
		Str("updated-remote-peer-url", us)
}

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
