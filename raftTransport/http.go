package raftTransport

import (
	"context"
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/db"
	types "github.com/ColdToo/Cold2DB/raftTransport/types"
	"github.com/ColdToo/Cold2DB/raftproto"
	pioutil "go.etcd.io/etcd/pkg/ioutil"
	"io/ioutil"
	"net/http"
	"path"

	"go.uber.org/zap"
)

const (
	// 限制单次读取的字节数，64KB的限制既不会导致吞吐量瓶颈，也不会导致读取超时。
	connReadLimitByte = 64 * 1024
)

var (
	RaftPrefix         = "/raft"
	RaftStreamPrefix   = path.Join(RaftPrefix, "stream")
	RaftSnapshotPrefix = path.Join(RaftPrefix, "snapshot")

	errIncompatibleVersion = errors.New("incompatible version")
	errClusterIDMismatch   = errors.New("cluster ID mismatch")
)

type peerGetter interface {
	Get(id types.ID) Peer
}

type writerToResponse interface {
	WriteTo(w http.ResponseWriter)
}

// pipeline
type pipelineHandler struct {
	lg        *zap.Logger
	localID   types.ID
	trans     Transporter
	raft      Raft
	clusterId types.ID
}

func newPipelineHandler(t *Transport, r Raft, clusterId types.ID) http.Handler {
	return &pipelineHandler{
		lg:        t.Logger,
		localID:   t.LocalID,
		trans:     t,
		raft:      r,
		clusterId: clusterId,
	}
}

func (h *pipelineHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Etcd-Cluster-ID", h.clusterId.String())

	addRemoteFromRequest(h.trans, r)

	//限制从请求体中读取的数据大小，这可以确保由于底层实现中可能的阻塞而导致的连接读取不会意外超时。
	limitedr := pioutil.NewLimitedBufferReader(r.Body, connReadLimitByte)
	b, err := ioutil.ReadAll(limitedr)
	if err != nil {
		h.lg.Warn(
			"failed to read Raft message",
			zap.String("local-member-id", h.localID.String()),
			zap.Error(err),
		)
		http.Error(w, "error reading raft message", http.StatusBadRequest)
		return
	}

	var m *raftproto.Message
	if err := m.Unmarshal(b); err != nil {
		h.lg.Warn(
			"failed to unmarshal Raft message",
			zap.String("local-member-id", h.localID.String()),
			zap.Error(err),
		)
		http.Error(w, "error unmarshalling raft message", http.StatusBadRequest)
		return
	}

	if err := h.raft.Process(context.TODO(), m); err != nil {
		switch v := err.(type) {
		case writerToResponse:
			v.WriteTo(w)
		default:
			h.lg.Warn(
				"failed to process Raft message",
				zap.String("local-member-id", h.localID.String()),
				zap.Error(err),
			)
			http.Error(w, "error processing raft message", http.StatusInternalServerError)
			w.(http.Flusher).Flush()
			// disconnect the http stream
			panic(err)
		}
		return
	}

	// Write StatusNoContent header after the message has been processed by
	// raft, which facilitates the client to report MsgSnap status.
	w.WriteHeader(http.StatusNoContent)
}

// snapshot
type snapshotHandler struct {
	lg          *zap.Logger
	trans       Transporter
	raft        Raft
	snapshotter *db.SnapShotter

	localID   types.ID
	clusterId types.ID
}

func newSnapshotHandler(t *Transport, r Raft, snapshotter *db.SnapShotter, clusterId types.ID) http.Handler {
	return &snapshotHandler{
		lg:          t.Logger,
		trans:       t,
		raft:        r,
		snapshotter: snapshotter,
		localID:     t.ID,
		clusterId:   clusterId,
	}
}

func (h *snapshotHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Etcd-Cluster-ID", h.clusterId.String())

	addRemoteFromRequest(h.trans, r)

	dec := &messageDecoder{r: r.Body}
	// let snapshots be very large since they can exceed 512MB for large installations
	m, err := dec.decodeLimit(uint64(1 << 63))
	from := types.ID(m.From).String()
	if err != nil {
		msg := fmt.Sprintf("failed to decode raft message (%v)", err)
		h.lg.Warn(
			"failed to decode Raft message",
			zap.String("local-member-id", h.localID.String()),
			zap.String("remote-snapshot-sender-id", from),
			zap.Error(err),
		)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	msgSize := m.Size()

	if m.MsgType != raftproto.MessageType_MsgSnapshot {
		h.lg.Warn(
			"unexpected Raft message type",
			zap.String("local-member-id", h.localID.String()),
			zap.String("remote-snapshot-sender-id", from),
			zap.String("message-type", m.MsgType.String()),
		)
		http.Error(w, "wrong raft message type", http.StatusBadRequest)
		return
	}

	h.lg.Info(
		"receiving database snapshot",
		zap.String("local-member-id", h.localID.String()),
		zap.String("remote-snapshot-sender-id", from),
		zap.Uint64("incoming-snapshot-index", m.Snapshot.Metadata.Index),
		zap.Int("incoming-snapshot-message-size-bytes", int(msgSize)),
	)

	// save incoming database snapshot.
	n, err := h.snapshotter.SaveToDB()
	if err != nil {
		h.lg.Warn(
			"failed to save incoming database snapshot",
			zap.String("local-member-id", h.localID.String()),
			zap.String("remote-snapshot-sender-id", from),
			zap.Uint64("incoming-snapshot-index", m.Snapshot.Metadata.Index),
			zap.Error(err),
		)
		msg := fmt.Sprintf("failed to save KV snapshot (%v)", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	h.lg.Info(
		"received and saved database snapshot",
		zap.String("local-member-id", h.localID.String()),
		zap.String("remote-snapshot-sender-id", from),
		zap.Uint64("incoming-snapshot-index", m.Snapshot.Metadata.Index),
		zap.Int64("incoming-snapshot-size-bytes", int64(n)),
	)

	if err := h.raft.Process(context.TODO(), &m); err != nil {
		switch v := err.(type) {
		// Process may return writerToResponse error when doing some
		// additional checks before calling raft.Node.Step.
		case writerToResponse:
			v.WriteTo(w)
		default:
			msg := fmt.Sprintf("failed to process raft message (%v)", err)
			h.lg.Warn(
				"failed to process Raft message",
				zap.String("local-member-id", h.localID.String()),
				zap.String("remote-snapshot-sender-id", from),
				zap.Error(err),
			)
			http.Error(w, msg, http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

//stream
type streamHandler struct {
	lg         *zap.Logger
	tr         *Transport
	peerGetter peerGetter
	r          Raft
	id         types.ID
	clusterId  types.ID
}

func newStreamHandler(t *Transport, pg peerGetter, r Raft, id, clusterId types.ID) http.Handler {
	return &streamHandler{
		lg:         t.Logger,
		tr:         t,
		peerGetter: pg,
		r:          r,
		id:         id,
		clusterId:  clusterId,
	}
}

func (h *streamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.Header().Set("Allow", "GET")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Etcd-Cluster-ID", h.clusterId.String())

	var t streamType
	switch path.Dir(r.URL.Path) {
	case streamTypeMessage.endpoint():
		t = streamTypeMessage
	default:
		h.lg.Debug(
			"ignored unexpected streaming request path",
			zap.String("local-member-id", h.tr.LocalID.String()),
			zap.String("remote-peer-id-stream-handler", h.id.String()),
			zap.String("path", r.URL.Path),
		)
		http.Error(w, "invalid path", http.StatusNotFound)
		return
	}

	fromStr := path.Base(r.URL.Path)
	from, err := types.IDFromString(fromStr)
	if err != nil {
		h.lg.Warn(
			"failed to parse path into ID",
			zap.String("local-member-id", h.tr.ID.String()),
			zap.String("remote-peer-id-stream-handler", h.id.String()),
			zap.String("path", fromStr),
			zap.Error(err),
		)

		http.Error(w, "invalid from", http.StatusNotFound)
		return
	}

	if h.r.IsIDRemoved(uint64(from)) {

		h.lg.Warn(
			"rejected stream from remote peer because it was removed",
			zap.String("local-member-id", h.tr.LocalID.String()),
			zap.String("remote-peer-id-stream-handler", h.id.String()),
			zap.String("remote-peer-id-from", from.String()),
		)

		http.Error(w, "removed member", http.StatusGone)
		return
	}
	p := h.peerGetter.Get(from)

	wto := h.id.String()
	if gto := r.Header.Get("X-Raft-To"); gto != wto {
		h.lg.Warn(
			"ignored streaming request; ID mismatch",
			zap.String("local-member-id", h.tr.LocalID.String()),
			zap.String("remote-peer-id-stream-handler", h.id.String()),
			zap.String("remote-peer-id-header", gto),
			zap.String("remote-peer-id-from", from.String()),
			zap.String("cluster-id", h.clusterId.String()),
		)
		http.Error(w, "to field mismatch", http.StatusPreconditionFailed)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()

	c := newCloseNotifier()
	conn := &outgoingConn{
		t:       t,
		Writer:  w,
		Flusher: w.(http.Flusher),
		Closer:  c,
		localID: h.tr.LocalID,
		peerID:  h.id,
	}
	p.attachOutgoingConn(conn)
	<-c.closeNotify()
}

// closeNotifier
type closeNotifier struct {
	done chan struct{}
}

func newCloseNotifier() *closeNotifier {
	return &closeNotifier{
		done: make(chan struct{}),
	}
}

func (n *closeNotifier) Close() error {
	close(n.done)
	return nil
}

func (n *closeNotifier) closeNotify() <-chan struct{} { return n.done }
