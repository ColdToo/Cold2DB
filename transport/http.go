package transport

import (
	"context"
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/db"
	log "github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transport/types"
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

type pipelineHandler struct {
	lg        *zap.Logger
	localID   types.ID
	trans     Transporter
	raft      RaftTransport
	clusterId types.ID
}

func newPipelineHandler(t *Transport, r RaftTransport, clusterId types.ID) http.Handler {
	return &pipelineHandler{
		localID:   t.LocalID,
		trans:     t,
		raft:      r,
		clusterId: clusterId,
	}
}

// pipeline 主要用于传输快照数据
//1.读取对端发送过来的数据，
//读取数据的时候，限制每次从底层连接读取的字节数上线，默认是64KB，因为快照数据可能非常大，为了防止读取超时，只能每次读取一部分数据到缓冲区中，最后将全部数据拼接起来，得到完整的快照数据。
//2.将读取到的消息发送给底层的raft模块
//3.返回对端节点状态码
func (h *pipelineHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// pipeline只允许post请求
	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Etcd-Cluster-ID", h.clusterId.Str())

	//why pipeline add remote
	addRemoteFromRequest(h.trans, r)

	//限制从请求体中读取的数据大小，这可以确保由于底层实现中可能的阻塞而导致的连接读取不会意外超时。
	limitedr := pioutil.NewLimitedBufferReader(r.Body, connReadLimitByte)
	b, err := ioutil.ReadAll(limitedr)
	if err != nil {
		log.Warn("failed to read Raft message").Str(code.LocalMemberId, h.localID.Str()).Err(code.FailedReadMessage, err).Record()
		http.Error(w, "error reading raft message", http.StatusBadRequest)
		return
	}

	var m *pb.Message
	if err := m.Unmarshal(b); err != nil {
		h.lg.Warn(
			"failed to unmarshal Raft message",
			zap.String("local-member-id", h.localID.Str()),
			zap.Error(err),
		)
		http.Error(w, "error unmarshalling raft message", http.StatusBadRequest)
		return
	}

	//调用 raft层处理消息
	if err := h.raft.Process(context.TODO(), m); err != nil {
		switch v := err.(type) {
		case writerToResponse:
			v.WriteTo(w)
		default:
			h.lg.Warn(
				"failed to process Raft message",
				zap.String("local-member-id", h.localID.Str()),
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
	if r.Method != "GET" {
		w.Header().Set("Allow", "GET")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Etcd-Cluster-ID", h.clusterId.Str())

	var t streamType
	switch path.Dir(r.URL.Path) {
	case streamTypeMessage.endpoint():
		t = streamTypeMessage
	default:
		http.Error(w, "invalid path", http.StatusNotFound)
		return
	}

	fromStr := path.Base(r.URL.Path)
	from, err := types.IDFromString(fromStr)
	if err != nil {
		log.Warn("failed to parse path into ID").Str(code.LocalMemberId, h.tr.LocalID.Str()).Record()

		http.Error(w, "invalid from", http.StatusNotFound)
		return
	}

	if h.r.IsIDRemoved(uint64(from)) {
		log.Info("rejected stream from remote peer because it was removed").Str(code.RemotePeerId, fromStr).Record()
		http.Error(w, "removed member", http.StatusGone)
		return
	}
	p := h.peerGetter.Get(from)

	wto := h.id.Str()
	if gto := r.Header.Get("X-Raft-To"); gto != wto {
		log.Warn("ignored streaming request; ID mismatch").Str(code.LocalMemberId, h.tr.LocalID.Str()).
			Str(code.RemotePeerReqId, gto).Str(code.ClusterId, h.clusterId.Str()).Record()
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

	addRemoteFromRequest(h.trans, r)

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
