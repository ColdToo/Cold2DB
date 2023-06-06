package raftTransport

import (
	"context"
	"fmt"
	types "github.com/ColdToo/Cold2DB/raftTransport/types"
	"github.com/ColdToo/Cold2DB/raftproto"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/pkg/httputil"
	"go.etcd.io/etcd/pkg/transport"
	"go.etcd.io/etcd/version"

	"go.uber.org/zap"
)

const (
	streamTypeMessage streamType = "message"

	streamBufSize = 4096
)

var (
	errUnsupportedStreamType = fmt.Errorf("unsupported stream type")
)

type streamType string

func (t streamType) endpoint() string {
	switch t {
	case streamTypeMessage:
		return path.Join(RaftStreamPrefix, "message")
	default:
		return ""
	}
}

func (t streamType) String() string {
	switch t {
	case streamTypeMessage:
		return "stream Message"
	default:
		return "unknown stream"
	}
}

var (
	// 理解为线路的心跳信息
	linkHeartbeatMessage = raftproto.Message{MsgType: raftproto.MessageType_MsgHeartbeat}
)

func isLinkHeartbeatMessage(m *raftproto.Message) bool {
	return m.MsgType == raftproto.MessageType_MsgHeartbeat && m.From == 0 && m.To == 0
}

type outgoingConn struct {
	t streamType
	io.Writer
	http.Flusher
	io.Closer

	localID types.ID
	peerID  types.ID
}

// streamWriter writes messages to the attached outgoingConn.
type streamWriter struct {
	lg *zap.Logger

	localID types.ID
	peerID  types.ID

	status *peerStatus
	r      Raft

	mu      sync.Mutex // guard field working and closer
	closer  io.Closer
	working bool

	msgc  chan *raftproto.Message //Peer会将待发送的消息写入到该通道，streamWriter则从该通道中读取消息并发送出去
	connc chan *outgoingConn      //通过该通道获取当前streamWriter实例关联的底层网络连接，  outgoingConn其实是对网络连接的一层封装，其中记录了当前连接使用的协议版本，以及用于关闭连接的Flusher和Closer等信息。
	stopc chan struct{}
	done  chan struct{}
}

// startStreamWriter creates a streamWrite and starts a long running go-routine that accepts
// messages and writes to the attached outgoing connection.
func startStreamWriter(lg *zap.Logger, local, id types.ID, status *peerStatus, r Raft) *streamWriter {
	w := &streamWriter{
		lg:      lg,
		localID: local,
		peerID:  id,
		status:  status,
		r:       r,
		msgc:    make(chan *raftproto.Message, streamBufSize),
		connc:   make(chan *outgoingConn),
		stopc:   make(chan struct{}),
		done:    make(chan struct{}),
	}
	go w.run()
	return w
}

// 从管道中获取msg
func (cw *streamWriter) run() {
	var (
		msgc       chan *raftproto.Message
		heartbeatc <-chan time.Time
		t          streamType
		enc        encoder
		flusher    http.Flusher
		batched    int
	)
	tickc := time.NewTicker(ConnReadTimeout / 3)
	defer tickc.Stop()
	unflushed := 0

	cw.lg.Info(
		"started stream writer with remote peer",
		zap.String("local-member-id", cw.localID.String()),
		zap.String("remote-peer-id", cw.peerID.String()),
	)

	for {
		select {
		case <-heartbeatc:
			err := enc.encode(&linkHeartbeatMessage)
			unflushed += linkHeartbeatMessage.Size()
			if err == nil {
				flusher.Flush()
				batched = 0
				unflushed = 0
				continue
			}

			cw.status.deactivate(failureType{source: t.String(), action: "heartbeat"}, err.Error())

			cw.close()
			cw.lg.Warn(
				"lost TCP streaming connection with remote peer",
				zap.String("stream-writer-type", t.String()),
				zap.String("local-member-id", cw.localID.String()),
				zap.String("remote-peer-id", cw.peerID.String()),
			)
			heartbeatc, msgc = nil, nil

		case m := <-msgc:
			err := enc.encode(&m)
			if err == nil {
				unflushed += m.Size()

				if len(msgc) == 0 || batched > streamBufSize/2 {
					flusher.Flush()
					sentBytes.WithLabelValues(cw.peerID.String()).Add(float64(unflushed))
					unflushed = 0
					batched = 0
				} else {
					batched++
				}

				continue
			}

			cw.status.deactivate(peer.failureType{source: t.String(), action: "write"}, err.Error())
			cw.close()
			cw.lg.Warn(
				"lost TCP streaming connection with remote peer",
				zap.String("stream-writer-type", t.String()),
				zap.String("local-member-id", cw.localID.String()),
				zap.String("remote-peer-id", cw.peerID.String()),
			)
			heartbeatc, msgc = nil, nil
			cw.r.ReportUnreachable(m.To)
			sentFailures.WithLabelValues(cw.peerID.String()).Inc()

		case conn := <-cw.connc:
			cw.mu.Lock()
			closed := cw.closeUnlocked()
			t = conn.t
			switch conn.t {
			case streamTypeMessage:
				enc = &messageEncoder{w: conn.Writer}
			default:
			}

			cw.lg.Info(
				"set message encoder",
				zap.String("from", conn.localID.String()),
				zap.String("to", conn.peerID.String()),
				zap.String("stream-type", t.String()),
			)

			flusher = conn.Flusher
			unflushed = 0
			cw.status.activate()
			cw.closer = conn.Closer
			cw.working = true
			cw.mu.Unlock()

			if closed {
				cw.lg.Warn(
					"closed TCP streaming connection with remote peer",
					zap.String("stream-writer-type", t.String()),
					zap.String("local-member-id", cw.localID.String()),
					zap.String("remote-peer-id", cw.peerID.String()),
				)
			}
			cw.lg.Warn(
				"established TCP streaming connection with remote peer",
				zap.String("stream-writer-type", t.String()),
				zap.String("local-member-id", cw.localID.String()),
				zap.String("remote-peer-id", cw.peerID.String()),
			)
			heartbeatc, msgc = tickc.C, cw.msgc

		case <-cw.stopc:
			if cw.close() {
				cw.lg.Warn(
					"closed TCP streaming connection with remote peer",
					zap.String("stream-writer-type", t.String()),
					zap.String("remote-peer-id", cw.peerID.String()),
				)
			}
			cw.lg.Warn(
				"stopped TCP streaming connection with remote peer",
				zap.String("stream-writer-type", t.String()),
				zap.String("remote-peer-id", cw.peerID.String()),
			)
			close(cw.done)
			return
		}
	}
}

func (cw *streamWriter) writec() (chan<- *raftproto.Message, bool) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.msgc, cw.working
}

func (cw *streamWriter) close() bool {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.closeUnlocked()
}

func (cw *streamWriter) closeUnlocked() bool {
	if !cw.working {
		return false
	}
	if err := cw.closer.Close(); err != nil {
		cw.lg.Warn(
			"failed to close connection with remote peer",
			zap.String("remote-peer-id", cw.peerID.String()),
			zap.Error(err),
		)
	}
	if len(cw.msgc) > 0 {
		cw.r.ReportUnreachable(uint64(cw.peerID))
	}
	cw.msgc = make(chan *raftproto.Message, streamBufSize)
	cw.working = false
	return true
}

func (cw *streamWriter) attach(conn *outgoingConn) bool {
	select {
	case cw.connc <- conn:
		return true
	case <-cw.done:
		return false
	}
}

func (cw *streamWriter) stop() {
	close(cw.stopc)
	<-cw.done
}

// streamReader is a long-running go-routine that dials to the remote stream
// endpoint and reads messages from the response body returned.
type streamReader struct {
	lg *zap.Logger

	peerID     types.ID
	streamType streamType

	tr     *Transport
	picker *urlPicker
	status *peerStatus
	recvc  chan<- *raftproto.Message
	propc  chan<- *raftproto.Message

	errorc chan<- error

	mu     sync.Mutex
	paused bool
	closer io.Closer

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func (cr *streamReader) start() {
	cr.done = make(chan struct{})
	cr.errorc = cr.tr.ErrorC
	cr.ctx, cr.cancel = context.WithCancel(context.Background())
	go cr.run()
}

func (cr *streamReader) run() {
	t := cr.streamType

	cr.lg.Info(
		"started stream reader with remote peer",
		zap.String("stream-reader-type", t.String()),
		zap.String("local-member-id", cr.tr.LocalID.String()),
		zap.String("remote-peer-id", cr.peerID.String()),
	)

	for {
		rc, err := cr.dial(t)
		if err != nil {
			cr.status.deactivate(failureType{source: t.String(), action: "dial"}, err.Error())
		} else {
			cr.status.activate()
			cr.lg.Info(
				"established TCP streaming connection with remote peer",
				zap.String("local-member-id", cr.tr.ID.String()),
				zap.String("remote-peer-id", cr.peerID.String()),
			)
			err = cr.decodeLoop(rc, t)
			cr.lg.Warn(
				"lost TCP streaming connection with remote peer",
				zap.String("stream-reader-type", cr.typ.String()),
				zap.String("local-member-id", cr.tr.ID.String()),
				zap.String("remote-peer-id", cr.peerID.String()),
				zap.Error(err),
			)
			switch {
			// all data is read out
			case err == io.EOF:
			// connection is closed by the remote
			case transport.IsClosedConnError(err):
			default:
				cr.status.deactivate(failureType{source: t.String(), action: "read"}, err.Error())
			}
		}

		if cr.ctx.Err() != nil {
			cr.lg.Info(
				"stopped stream reader with remote peer",
				zap.String("stream-reader-type", t.String()),
				zap.String("local-member-id", cr.tr.LocalID.String()),
				zap.String("remote-peer-id", cr.peerID.String()),
			)
			close(cr.done)
			return
		}
		if err != nil {
			cr.lg.Warn(
				"rate limit on stream reader with remote peer",
				zap.String("stream-reader-type", t.String()),
				zap.String("local-member-id", cr.tr.LocalID.String()),
				zap.String("remote-peer-id", cr.peerID.String()),
				zap.Error(err),
			)
		}
	}
}

func (cr *streamReader) decodeLoop(rc io.ReadCloser, t streamType) error {
	var dec decoder
	cr.mu.Lock()
	switch t {
	case streamTypeMessage:
		dec = &messageDecoder{r: rc}
	default:
		cr.lg.Panic("unknown stream type", zap.String("type", t.String()))
	}
	select {
	case <-cr.ctx.Done():
		cr.mu.Unlock()
		if err := rc.Close(); err != nil {
			return err
		}
		return io.EOF
	default:
		cr.closer = rc
	}
	cr.mu.Unlock()

	// gofail: labelRaftDropHeartbeat:
	for {
		m, err := dec.decode()
		if err != nil {
			cr.mu.Lock()
			cr.close()
			cr.mu.Unlock()
			return err
		}

		cr.mu.Lock()
		paused := cr.paused
		cr.mu.Unlock()

		if paused {
			continue
		}

		if isLinkHeartbeatMessage(&m) {
			// raft is not interested in link layer
			// heartbeat message, so we should ignore
			// it.
			continue
		}

		recvc := cr.recvc
		if m.Type == raftproto.MsgProp {
			recvc = cr.propc
		}

		select {
		case recvc <- m:
		default:
			if cr.status.isActive() {
				cr.lg.Warn(
					"dropped internal Raft message since receiving buffer is full (overloaded network)",
					zap.String("message-type", m.Type.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("from", types.ID(m.From).String()),
					zap.String("remote-peer-id", types.ID(m.To).String()),
					zap.Bool("remote-peer-active", cr.status.isActive()),
				)
			} else {
				cr.lg.Warn(
					"dropped Raft message since receiving buffer is full (overloaded network)",
					zap.String("message-type", m.Type.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("from", types.ID(m.From).String()),
					zap.String("remote-peer-id", types.ID(m.To).String()),
					zap.Bool("remote-peer-active", cr.status.isActive()),
				)
			}
		}
	}
}

func (cr *streamReader) stop() {
	cr.mu.Lock()
	cr.cancel()
	cr.close()
	cr.mu.Unlock()
	<-cr.done
}

func (cr *streamReader) dial(t streamType) (io.ReadCloser, error) {
	u := cr.picker.pick()
	uu := u
	uu.Path = path.Join(t.endpoint(), cr.tr.LocalID.String())

	cr.lg.Debug(
		"dial stream reader",
		zap.String("from", cr.tr.LocalID.String()),
		zap.String("to", cr.peerID.String()),
		zap.String("address", uu.String()),
	)

	req, err := http.NewRequest("GET", uu.String(), nil)
	if err != nil {
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("failed to make http request to %v (%v)", u, err)
	}

	req.Header.Set("X-Server-From", cr.tr.LocalID.String())
	req.Header.Set("X-Server-Version", version.Version)
	req.Header.Set("X-Min-Cluster-Version", version.MinClusterVersion)
	req.Header.Set("X-Etcd-Cluster-ID", cr.tr.ClusterID.String())
	req.Header.Set("X-Raft-To", cr.peerID.String())
	req = req.WithContext(cr.ctx)
	setPeerURLsHeader(req, cr.tr.URLs)

	cr.mu.Lock()
	select {
	case <-cr.ctx.Done():
		cr.mu.Unlock()
		return nil, fmt.Errorf("stream reader is stopped")
	default:
	}
	cr.mu.Unlock()

	resp, err := cr.tr.streamRt.RoundTrip(req)
	if err != nil {
		cr.picker.unreachable(u)
		return nil, err
	}

	switch resp.StatusCode {
	case http.StatusGone:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		reportCriticalError(errMemberRemoved, cr.errorc)
		return nil, errMemberRemoved

	case http.StatusOK:
		return resp.Body, nil

	case http.StatusNotFound:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("peer %s failed to find local node %s", cr.peerID, cr.tr.LocalID)

	case http.StatusPreconditionFailed:
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			cr.picker.unreachable(u)
			return nil, err
		}
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)

		switch strings.TrimSuffix(string(b), "\n") {
		case errIncompatibleVersion.Error():
			cr.lg.Warn(
				"request sent was ignored by remote peer due to server version incompatibility",
				zap.String("local-member-id", cr.tr.LocalID.String()),
				zap.String("remote-peer-id", cr.peerID.String()),
				zap.Error(errIncompatibleVersion),
			)
			return nil, errIncompatibleVersion

		case errClusterIDMismatch.Error():
			cr.lg.Warn(
				"request sent was ignored by remote peer due to cluster ID mismatch",
				zap.String("remote-peer-id", cr.peerID.String()),
				zap.String("remote-peer-cluster-id", resp.Header.Get("X-Etcd-Cluster-ID")),
				zap.String("local-member-id", cr.tr.LocalID.String()),
				zap.String("local-member-cluster-id", cr.tr.ClusterID.String()),
				zap.Error(errClusterIDMismatch),
			)
			return nil, errClusterIDMismatch

		default:
			return nil, fmt.Errorf("unhandled error %q when precondition failed", string(b))
		}

	default:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("unhandled http status %d", resp.StatusCode)
	}
}

func (cr *streamReader) close() {
	if cr.closer != nil {
		if err := cr.closer.Close(); err != nil {
			if cr.lg != nil {
				cr.lg.Warn(
					"failed to close remote peer connection",
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
					zap.Error(err),
				)
			} else {
				plog.Errorf("peer %s (reader) connection close error: %v", cr.peerID, err)
			}
		}
	}
	cr.closer = nil
}

func (cr *streamReader) pause() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = true
}

func (cr *streamReader) resume() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = false
}
