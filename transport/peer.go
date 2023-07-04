package transport

import (
	"context"
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"sync"
	"time"

	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft"
	"go.uber.org/zap"
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
	// send sends the message to the remote peer. The function is non-blocking
	// and has no promise that the message will be received by the remote.
	// When it fails to send message out, it will report the status to underlying
	// raft.
	send(m pb.Message)

	// sendSnap sends the merged snapshot message to the remote peer. Its behavior
	// is similar to send.
	sendSnap(m snap.Message)

	// update updates the urls of remote peer.
	update(urls types.URLs)

	// attachOutgoingConn attaches the outgoing connection to the peer for
	// stream usage. After the call, the ownership of the outgoing
	// connection hands over to the peer. The peer will close the connection
	// when it is no longer used.
	attachOutgoingConn(conn *outgoingConn)
	// activeSince returns the time that the connection with the
	// peer becomes active.
	activeSince() time.Time
	// stop performs any necessary finalization and terminates the peer
	// elegantly.
	stop()
}

// peer 代表了远程节点，本地节点发送消息通过该结构体发送
// 本地Raft节点通过peer向远程节点发送消息。每个peer都有两种发送消息的机制：stream和pipeline。
// stream是一个初始化的长轮询连接，它始终打开以传输消息。除了一般的stream之外，peer还有一个优化的stream用于发送msgApp，因为msgApp占所有消息的大部分。
// 只有Raft leader使用优化的stream将msgApp发送到远程follower节点。pipeline是一系列向远程发送HTTP请求的HTTP客户端。仅在未建立stream时使用。
type peer struct {
	localID types.ID
	// id of the remote raft peer node
	remoteID types.ID

	raft RaftTransport

	status *peerStatus

	/*
		每个节点可能提供了多个URL供其他节点正常访问，当其中一个访问失败时，我们应该可以尝试访问另一个。
		urlPicker提供的主要功能就是在这些URL之间进行切换
	*/
	picker *urlPicker

	writer       *streamWriter
	msgAppReader *streamReader

	pipeline   *pipeline
	snapSender *snapshotSender // snapshot sender to send v3 snapshot messages

	recvc chan *pb.Message //从Stream消息通道中读取到消息之后，会通过该通道将消息交给Raft接口，然后由它返回给底层etcd-raft模块进行处理
	propc chan *pb.Message //从Stream消息通道中读取到MsgProp类型的消息之后，会通过该通道将MsgApp消息交给Raft接口，然后由它返回给底层的etcd-raft模块进行处理

	mu     sync.Mutex
	paused bool

	cancel context.CancelFunc // cancel pending works in go routine created by peer.
	stopc  chan struct{}
}

func startPeer(t *Transport, urls types.URLs, peerID types.ID) *peer {
	peerStatus := newPeerStatus(t.LocalID, peerID)
	picker := newURLPicker(urls)
	errorC := t.ErrorC
	r := t.Raft

	// pipeline 用于将快照数据发送到远端本体
	pipeline := &pipeline{
		peerID:     peerID,
		tr:         t,
		picker:     picker,
		peerStatus: peerStatus,
		raft:       r,
		errorC:     errorC,
	}

	pipeline.start()

	// 发送message到远端本体
	streamWriter := startStreamWriter(t.LocalID, peerID, peerStatus, r)

	// 读出recvc和propc的数据交给raft层进行处理
	p := &peer{
		localID:    t.LocalID,
		remoteID:   peerID,
		raft:       r,
		status:     peerStatus,
		picker:     picker,
		writer:     streamWriter,
		pipeline:   pipeline,
		snapSender: newSnapshotSender(t, picker, peerID, peerStatus),

		recvc: make(chan *pb.Message, recvBufSize),
		propc: make(chan *pb.Message, maxPendingProposals),
		stopc: make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	go func() {
		for {
			select {
			case mm := <-p.recvc:
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
			case mm := <-p.propc:
				if err := r.Process(ctx, mm); err != nil {
					log.Warn("failed to process Raft message").Err(code.MessageProcErr, err)
				}
			case <-p.stopc:
				return
			}
		}
	}()

	// 用于接收其他节点发送过来的数据传递给recvc和propc通道
	p.msgAppReader = &streamReader{
		peerID:     peerID,
		streamType: streamTypeMessage,
		tr:         t,
		picker:     picker,
		peerStatus: peerStatus,
		recvc:      p.recvc,
		propc:      p.propc,
	}

	p.msgAppReader.start()

	return p
}

func (p *peer) send(m pb.Message) {
	p.mu.Lock()
	paused := p.paused
	p.mu.Unlock()

	if paused {
		return
	}

	//获取
	writec, name := p.pick(m)
	select {
	case writec <- m:
	default:
		p.r.ReportUnreachable(m.To)
		if isMsgSnap(m) {
			p.r.ReportSnapshot(m.To, raft.SnapshotFailure)
		}
		if p.status.isActive() {
			if p.lg != nil {
				p.lg.Warn(
					"dropped internal Raft message since sending buffer is full (overloaded network)",
					zap.String("message-type", m.Type.String()),
					zap.String("local-member-id", p.localID.String()),
					zap.String("from", types.ID(m.From).String()),
					zap.String("remote-peer-id", p.id.String()),
					zap.Bool("remote-peer-active", p.status.isActive()),
				)
			} else {
				p.lg.Warn(
					"dropped internal Raft message since sending buffer is full (overloaded network)",
					zap.String("message-type", m.Type.String()),
					zap.String("local-member-id", p.localID.String()),
					zap.String("from", types.ID(m.From).String()),
					zap.String("remote-peer-id", p.id.String()),
					zap.Bool("remote-peer-active", p.status.isActive()),
				)
			}
		}
	}
}

func (p *peer) sendSnap(m snap.Message) {
	go p.snapSender.send(m)
}

func (p *peer) update(urls types.URLs) {
	p.picker.update(urls)
}

func (p *peer) attachOutgoingConn(conn *outgoingConn) {
	var ok bool
	switch conn.t {
	case streamTypeMessage:
		ok = p.writer.attach(conn)
	default:
	}
	if !ok {
		conn.Close()
	}
}

func (p *peer) activeSince() time.Time { return p.status.activeSince() }

// Pause pauses the peer. The peer will simply drops all incoming
// messages without returning an error.
func (p *peer) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	p.msgAppReader.pause()
}

// Resume resumes a paused peer.
func (p *peer) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
	p.msgAppReader.resume()
}

func (p *peer) stop() {
	defer func() {
		p.lg.Info("stopped remote peer", zap.String("remote-peer-id", p.id.String()))
	}()

	close(p.stopc)
	p.cancel()
	p.writer.stop()
	p.pipeline.stop()
	p.snapSender.stop()
	p.msgAppReader.stop()
}

// pick picks a chan for sending the given message. The picked chan and the picked chan
// string name are returned.
// 根据消息类型选取可以发送的消息信道
func (p *peer) pick(m *pb.Message) (writec chan<- *pb.Message, picked string) {
	var ok bool
	// Considering MsgSnap may have a big size, e.g., 1G, and will block
	// stream for a long time, only use one of the N pipelines to send MsgSnap.
	if isMsgSnap(m) {
		return p.pipeline.msgc, pipelineMsg
	} else if writec, ok = p.writer.writec(); ok {
		return writec, streamMsg
	}
	return p.pipeline.msgc, pipelineMsg
}

type failureType struct {
	source string
	action string
}

type peerStatus struct {
	localId types.ID
	id      types.ID
	mu      sync.Mutex // protect variables below
	active  bool
	since   time.Time
}

func newPeerStatus(local, id types.ID) *peerStatus {
	return &peerStatus{localId: local, id: id}
}

// 变更为在线状态
func (s *peerStatus) activate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		s.lg.Info("peer became active", zap.String("peer-id", s.id.String()))
		s.active = true
		s.since = time.Now()
	}
}

// 变更为离线状态
func (s *peerStatus) deactivate(failure failureType, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := fmt.Sprintf("failed to %s %s on %s (%s)", failure.action, s.id, failure.source, reason)
	if s.active {
		s.lg.Warn("peer became inactive (message send to peer failed)", zap.String("peer-id", s.id.String()), zap.Error(errors.New(msg)))
		s.active = false
		s.since = time.Time{}
		return
	}
	s.lg.Debug("peer deactivated again", zap.String("peer-id", s.id.String()), zap.Error(errors.New(msg)))
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

func isMsgApp(m *pb.Message) bool { return m.MsgType == pb.MessageType_MsgAppend }

func isMsgSnap(m *pb.Message) bool { return m.MsgType == pb.MessageType_MsgSnapshot }
