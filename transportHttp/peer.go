package transportHttp

import (
	"context"
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/raft"
	types "github.com/ColdToo/Cold2DB/transportHttp/types"
	"net/url"
	"sync"
	"time"
)

const (
	// ConnReadTimeout 这段代码定义了两个常量，DefaultConnReadTimeout 和 DefaultConnWriteTimeout，它们分别表示每个连接的读取和写入超时时间。在 rafthttp 包中创建连接时，会设置这两个超时时间。
	ConnReadTimeout  = 5 * time.Second
	ConnWriteTimeout = 5 * time.Second

	recvBufSize = 4096

	//这段注释是关于在一次 leader 选举过程中，最多可以容纳多少个 proposal 的说明。一般来说，一次 leader 选举最多需要 1 秒钟，可能会有 0-2 次选举冲突，每次冲突需要 0.5 秒钟。
	//我们假设并发 proposer 的数量小于 4096，因为一个 client 的 proposal 至少需要阻塞 1 秒钟，所以 4096 足以容纳所有的 proposals。
	maxPendingProposals = 4096

	streamMsg   = "streamMsg"
	pipelineMsg = "pipeline"
	sendSnap    = "sendMsgSnap"
)

type Peer interface {
	send(m *pb.Message)

	attachOutgoingConn(conn *outgoingConn)

	activeSince() time.Time

	stop()
}

type peer struct {
	localID  types.ID
	remoteID types.ID
	url      *url.URL

	raft   RaftTransport
	status *peerStatus

	streamWriter *streamWriter
	streamReader *streamReader

	recvC chan *pb.Message //从Stream消息通道中读取到消息之后，会通过该通道将消息交给Raft接口，然后由它返回给底层etcd-raft模块进行处理
	propC chan *pb.Message //从Stream消息通道中读取到MsgProp类型的消息之后，会通过该通道将MsgApp消息交给Raft接口，然后由它返回给底层的etcd-raft模块进行处理

	mu     sync.Mutex
	paused bool
	cancel context.CancelFunc // cancel pending works in go routine created by peer.
	stopc  chan struct{}
}

func (p *peer) handleReceiveCAndPropC(r RaftTransport, ctx context.Context) {
	go func() {
		for {
			select {
			case mm := <-p.recvC:
				if err := r.Process(ctx, mm); err != nil {
					log.Warn("failed to process Raft message").Err(code.MessageProcErr, err)
				}
			case <-p.stopc:
				return
			}
		}
	}()

	// 当没有主节点的时候proposal信息有可能会阻塞，所以需要一个单独的协程来处理投票信息
	go func() {
		for {
			select {
			case mm := <-p.propC:
				if err := r.Process(ctx, mm); err != nil {
					log.Warn("failed to process Raft message").Err(code.MessageProcErr, err)
				}
			case <-p.stopc:
				return
			}
		}
	}()
}

func (p *peer) send(m *pb.Message) {
	p.mu.Lock()
	paused := p.paused
	p.mu.Unlock()

	if paused {
		return
	}

	writeC, _ := p.streamWriter.writeC()
	select {
	case writeC <- m:
	default:
		p.raft.ReportUnreachable(m.To)
		if isMsgSnap(m) {
			p.raft.ReportSnapshotStatus(m.To, raft.SnapshotFailure)
		}
		if p.status.isActive() {
			log.Warn(
				"dropped internal Raft message since sending buffer is full (overloaded network)").
				Str("message-type", m.Type.String()).
				Str("local-member-id", p.localID.Str()).
				Str("from", types.ID(m.From).Str()).
				Str("remote-peer-id", p.remoteID.Str()).
				Bool("remote-peer-active", p.status.isActive()).Record()
		}
	}
}

//todo
//func (p *peer) sendSnap(m snap.Message) {
//}

func (p *peer) attachOutgoingConn(conn *outgoingConn) {
	ok := p.streamWriter.attach(conn)
	if !ok {
		conn.Close()
	}
}

func (p *peer) activeSince() time.Time { return p.status.activeSince() }

func (p *peer) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	//p.streamWriter.pause()
	p.streamReader.pause()
}

func (p *peer) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
	//p.streamWriter.resume()
	p.streamReader.resume()
}

func (p *peer) stop() {
	defer func() {
		log.Info("stopped remote peer").Str("remote-peer-id", p.remoteID.Str())
	}()

	close(p.stopc)
	p.cancel()
	p.streamWriter.stop()
	p.streamReader.stop()
}

type failureType struct {
	source string
	action string
}

type peerStatus struct {
	localId types.ID
	peerId  types.ID
	mu      sync.Mutex // protect variables below
	active  bool
	since   time.Time
}

func newPeerStatus(local, id types.ID) *peerStatus {
	return &peerStatus{localId: local, peerId: id}
}

// 变更为在线状态
func (s *peerStatus) activate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		log.Info("peer became active").Str("peer-id", s.peerId.Str())
		s.active = true
		s.since = time.Now()
	}
}

// 变更为离线状态
func (s *peerStatus) deactivate(failure failureType, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := fmt.Sprintf("failed to %s %s on %s (%s)", failure.action, s.peerId, failure.source, reason)
	if s.active {
		log.Warn("peer became inactive (message send to peer failed)").Str("peer-id", s.peerId.Str()).Err("", errors.New(msg)).Record()
		s.active = false
		s.since = time.Time{}
		return
	}
}

func (s *peerStatus) isActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}

func (s *peerStatus) activeSince() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.since
}

func isMsgApp(m *pb.Message) bool { return m.Type == pb.MsgApp }

func isMsgSnap(m *pb.Message) bool { return m.Type == pb.MsgSnap }
