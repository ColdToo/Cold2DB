package raftTransport

import (
	"context"
	"errors"
	"fmt"
	stats "github.com/ColdToo/Cold2DB/raftTransport/stats"
	types "github.com/ColdToo/Cold2DB/raftTransport/types"
	"github.com/ColdToo/Cold2DB/raftproto"
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
	// maxPendingProposals holds the proposals during one leader election process.
	// Generally one leader election takes at most 1 sec. It should have
	// 0-2 election conflicts, and each one takes 0.5 sec.
	// We assume the number of concurrent proposers is smaller than 4096.
	// One client blocks on its proposal for at least 1 sec, so 4096 is enough
	// to hold all proposals.
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
	send(m raftproto.Message)

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

// peer is the representative of a remote raft node. Local raft node sends
// messages to the remote through peer.
// Each peer has two underlying mechanisms to send out a message: stream and
// pipeline.
// A stream is a receiver initialized long-polling connection, which
// is always open to transfer messages. Besides general stream, peer also has
// a optimized stream for sending msgApp since msgApp accounts for large part
// of all messages. Only raft leader uses the optimized stream to send msgApp
// to the remote follower node.
// A pipeline is a series of http clients that send http requests to the remote.
// It is only used when the stream has not been established.
type peer struct {
	lg *zap.Logger

	localID types.ID
	// id of the remote raft peer node
	remoteID types.ID

	r Raft

	status *peerStatus

	picker *urlPicker

	writer       *streamWriter
	msgAppReader *streamReader

	pipeline   *pipeline
	snapSender *snapshotSender // snapshot sender to send v3 snapshot messages

	recvc chan *raftproto.Message
	propc chan *raftproto.Message

	mu     sync.Mutex
	paused bool

	cancel context.CancelFunc // cancel pending works in go routine created by peer.
	stopc  chan struct{}
}

func startPeer(t *Transport, urls types.URLs, peerID types.ID, fs *stats.FollowerStats) *peer {
	status := newPeerStatus(t.Logger, t.ID, peerID)
	picker := newURLPicker(urls)
	errorc := t.ErrorC
	r := t.Raft

	pipeline := &pipeline{
		peerID:        peerID,
		tr:            t,
		picker:        picker,
		status:        status,
		followerStats: fs,
		raft:          r,
		errorc:        errorc,
	}
	pipeline.start()

	p := &peer{
		lg:         t.Logger,
		localID:    t.ID,
		remoteID:   peerID,
		r:          r,
		status:     status,
		picker:     picker,
		writer:     startStreamWriter(t.Logger, t.ID, peerID, status, fs, r),
		pipeline:   pipeline,
		snapSender: newSnapshotSender(t, picker, peerID, status),

		recvc: make(chan *raftproto.Message, recvBufSize),
		propc: make(chan *raftproto.Message, maxPendingProposals),
		stopc: make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	//处理别的节点发送的Message
	go func() {
		for {
			select {
			case mm := <-p.recvc:
				if err := r.Process(ctx, mm); err != nil {
					t.Logger.Warn("failed to process Raft message", zap.Error(err))
				}
			case <-p.stopc:
				return
			}
		}
	}()

	// r.Process might block for processing proposal when there is no leader.
	// Thus propc must be put into a separate routine with recvc to avoid blocking
	// processing other raft messages.
	go func() {
		for {
			select {
			case mm := <-p.propc:
				if err := r.Process(ctx, mm); err != nil {
				}
			case <-p.stopc:
				return
			}
		}
	}()

	p.msgAppReader = &streamReader{
		lg:         t.Logger,
		peerID:     peerID,
		streamType: streamTypeMessage,
		tr:         t,
		picker:     picker,
		status:     status,
		recvc:      p.recvc,
		propc:      p.propc,
	}

	p.msgAppReader.start()

	return p
}

func (p *peer) send(m *raftproto.Message) {
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
	case streamTypeMsgAppV2:
		ok = p.msgAppV2Writer.attach(conn)
	case streamTypeMessage:
		ok = p.writer.attach(conn)
	default:
		if p.lg != nil {
			p.lg.Panic("unknown stream type", zap.String("type", conn.t.String()))
		} else {
			plog.Panicf("unhandled stream type %s", conn.t)
		}
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
// 根据消息类型选取可以发送的
func (p *peer) pick(m *raftproto.Message) (writec chan<- *raftproto.Message, picked string) {
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

func isMsgApp(m *raftproto.Message) bool { return m.MsgType == raftproto.MessageType_MsgAppend }

func isMsgSnap(m *raftproto.Message) bool { return m.MsgType == raftproto.MessageType_MsgSnapshot }

type failureType struct {
	source string
	action string
}

type peerStatus struct {
	lg     *zap.Logger
	local  types.ID
	id     types.ID
	mu     sync.Mutex // protect variables below
	active bool
	since  time.Time
}

func newPeerStatus(lg *zap.Logger, local, id types.ID) *peerStatus {
	return &peerStatus{lg: lg, local: local, id: id}
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
