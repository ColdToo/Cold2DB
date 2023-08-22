package transportHttp

import (
	"errors"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/db"
	log "github.com/ColdToo/Cold2DB/log"
	types "github.com/ColdToo/Cold2DB/transportHttp/types"
	"net/http"
	"path"
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

// stream Handler的主要作用就是建立一个长链接
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
	trans       Transporter
	raft        RaftTransport
	snapshotter *db.SnapShotter

	localID   types.ID
	clusterId types.ID
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
