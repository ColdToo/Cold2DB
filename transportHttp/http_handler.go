package transportHttp

import (
	"context"
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/db"
	log "github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transportHttp/types"
	"net/http"
	"path"

	"go.uber.org/zap"
)

const (
	// 限制单次读取的字节数，64KB的限制既不会导致吞吐量瓶颈，也不会导致读取超时。
	connReadLimitByte = 64 * 1024
)

var (
	RaftPrefix             = "/raft"
	RaftStream             = path.Join(RaftPrefix, "stream")
	RaftSnapshot           = path.Join(RaftPrefix, "snapshot")
	errIncompatibleVersion = errors.New("incompatible version")
	errClusterIDMismatch   = errors.New("cluster ID mismatch")
)

type peerGetter interface {
	Get(id types.ID) Peer
}

type writerToResponse interface {
	WriteTo(w http.ResponseWriter)
}

//stream Handler的主要作用就是建立一个长链接
type streamHandler struct {
	tr         *Transport
	peerGetter peerGetter
	r          RaftTransport
	id         types.ID
	clusterId  types.ID
}

func newStreamHandler(t *Transport, pg peerGetter, r RaftTransport, id, clusterId types.ID) http.Handler {
	return &streamHandler{
		tr:         t,
		peerGetter: pg,
		r:          r,
		id:         id,
		clusterId:  clusterId,
	}
}

func (h *streamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// precheck
	if r.Method != "GET" {
		w.Header().Set("Allow", "GET")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	fromStr := path.Base(r.URL.Path)
	from, err := types.IDFromString(fromStr)
	if err != nil {
		log.Warn("failed to parse path into ID").Str(code.LocalId, h.tr.LocalID.Str()).Record()
		http.Error(w, "invalid from", http.StatusNotFound)
		return
	}

	if h.r.IsIDRemoved(uint64(from)) {
		log.Info("rejected stream from remote peer because it was removed").Str(code.RemoteId, fromStr).Record()
		http.Error(w, "removed member", http.StatusGone)
		return
	}

	wto := h.id.Str()
	if gto := r.Header.Get("X-Raft-To"); gto != wto {
		log.Warn("ignored streaming request; ID mismatch").Str(code.LocalId, h.tr.LocalID.Str()).
			Str(code.RemoteReqId, gto).Str(code.ClusterId, h.clusterId.Str()).Record()
		http.Error(w, "to field mismatch", http.StatusPreconditionFailed)
		return
	}

	w.Header().Set("X-Etcd-Cluster-ID", h.clusterId.Str())
	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()

	c := newCloseNotifier()
	conn := &outgoingConn{
		Writer:  w,
		Flusher: w.(http.Flusher),
		Closer:  c,
		localID: h.tr.LocalID,
		peerID:  h.id,
	}

	p := h.peerGetter.Get(from)
	//将链接传递给streamWriter
	p.attachOutgoingConn(conn)
	<-c.closeNotify()
}

// v3 版本使用新接口处理snapshot
type snapshotHandler struct {
	lg          *zap.Logger
	trans       Transporter
	raft        RaftTransport
	snapshotter *db.SnapShotter

	localID   types.ID
	clusterId types.ID
}

func newSnapshotHandler(t *Transport, r RaftTransport, snapshotter *db.SnapShotter, clusterId types.ID) http.Handler {
	return &snapshotHandler{
		trans:       t,
		raft:        r,
		snapshotter: snapshotter,
		localID:     t.LocalID,
		clusterId:   clusterId,
	}
}

func (h *snapshotHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Etcd-Cluster-ID", h.clusterId.Str())

	dec := &messageDecoder{r: r.Body}
	// let snapshots be very large since they can exceed 512MB for large installations
	m, err := dec.decodeLimit(uint64(1 << 63))
	from := types.ID(m.From).Str()
	if err != nil {
		msg := fmt.Sprintf("failed to decode raft message (%v)", err)
		h.lg.Warn(
			"failed to decode Raft message",
			zap.String("local-member-id", h.localID.Str()),
			zap.String("remote-snapshot-sender-id", from),
			zap.Error(err),
		)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	msgSize := m.Size()

	if m.Type != pb.MsgSnap {
		h.lg.Warn(
			"unexpected Raft message type",
			zap.String("local-member-id", h.localID.Str()),
			zap.String("remote-snapshot-sender-id", from),
			zap.String("message-type", m.Type.String()),
		)
		http.Error(w, "wrong raft message type", http.StatusBadRequest)
		return
	}

	h.lg.Info(
		"receiving database snapshot",
		zap.String("local-member-id", h.localID.Str()),
		zap.String("remote-snapshot-sender-id", from),
		zap.Uint64("incoming-snapshot-index", m.Snapshot.Metadata.Index),
		zap.Int("incoming-snapshot-message-size-bytes", int(msgSize)),
	)

	// save incoming database snapshot.
	n, err := h.snapshotter.SaveToDB()
	if err != nil {
		h.lg.Warn(
			"failed to save incoming database snapshot",
			zap.String("local-member-id", h.localID.Str()),
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
		zap.String("local-member-id", h.localID.Str()),
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
				zap.String("local-member-id", h.localID.Str()),
				zap.String("remote-snapshot-sender-id", from),
				zap.Error(err),
			)
			http.Error(w, msg, http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
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
