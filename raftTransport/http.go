// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raftTransport

import (
	"context"
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/db"
	types "github.com/ColdToo/Cold2DB/raftTransport/types"
	"github.com/dustin/go-humanize"
	pioutil "go.etcd.io/etcd/pkg/ioutil"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/version"
	"io/ioutil"
	"net/http"
	"path"
	"strings"

	"go.uber.org/zap"
)

const (
	// 限制单次读取的字节数，64KB的限制既不会导致吞吐量瓶颈，也不会导致读取超时。
	connReadLimitByte = 64 * 1024
)

var (
	RaftPrefix         = "/raft"
	ProbingPrefix      = path.Join(RaftPrefix, "probing")
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
	tr        Transporter
	r         Raft
	clusterId types.ID
}

func newPipelineHandler(t *Transport, r Raft, clusterId types.ID) http.Handler {
	return &pipelineHandler{
		lg:        t.Logger,
		localID:   t.ID,
		tr:        t,
		r:         r,
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

	if err := checkClusterCompatibilityFromHeader(h.lg, h.localID, r.Header, h.clusterId); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}

	addRemoteFromRequest(h.tr, r)

	//这段代码的注释是在限制从请求体中读取的数据大小，这可以确保由于底层实现中可能的阻塞而导致的连接读取不会意外超时。
	limitedr := pioutil.NewLimitedBufferReader(r.Body, connReadLimitByte)
	b, err := ioutil.ReadAll(limitedr)
	if err != nil {
		if h.lg != nil {
			h.lg.Warn(
				"failed to read Raft message",
				zap.String("local-member-id", h.localID.String()),
				zap.Error(err),
			)
		} else {
			plog.Errorf("failed to read raft message (%v)", err)
		}
		http.Error(w, "error reading raft message", http.StatusBadRequest)
		recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		return
	}

	var m raftpb.Message
	if err := m.Unmarshal(b); err != nil {
		h.lg.Warn(
			"failed to unmarshal Raft message",
			zap.String("local-member-id", h.localID.String()),
			zap.Error(err),
		)
		http.Error(w, "error unmarshalling raft message", http.StatusBadRequest)
		recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		return
	}

	receivedBytes.WithLabelValues(types.ID(m.From).String()).Add(float64(len(b)))

	if err := h.r.Process(context.TODO(), m); err != nil {
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
	tr          Transporter
	r           Raft
	snapshotter *db.SnapShotter

	localID   types.ID
	clusterId types.ID
}

func newSnapshotHandler(t *Transport, r Raft, snapshotter *db.SnapShotter, clusterId types.ID) http.Handler {
	return &snapshotHandler{
		lg:          t.Logger,
		tr:          t,
		r:           r,
		snapshotter: snapshotter,
		localID:     t.ID,
		clusterId:   clusterId,
	}
}

const unknownSnapshotSender = "UNKNOWN_SNAPSHOT_SENDER"

func (h *snapshotHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Etcd-Cluster-ID", h.clusterId.String())

	addRemoteFromRequest(h.tr, r)

	dec := &messageDecoder{r: r.Body}
	// let snapshots be very large since they can exceed 512MB for large installations
	m, err := dec.decodeLimit(uint64(1 << 63))
	from := types.ID(m.From).String()
	if err != nil {
		msg := fmt.Sprintf("failed to decode raft message (%v)", err)
		if h.lg != nil {
			h.lg.Warn(
				"failed to decode Raft message",
				zap.String("local-member-id", h.localID.String()),
				zap.String("remote-snapshot-sender-id", from),
				zap.Error(err),
			)
		} else {
			plog.Error(msg)
		}
		http.Error(w, msg, http.StatusBadRequest)
		recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		snapshotReceiveFailures.WithLabelValues(from).Inc()
		return
	}

	msgSize := m.Size()
	receivedBytes.WithLabelValues(from).Add(float64(msgSize))

	if m.Type != raftpb.MsgSnap {
		if h.lg != nil {
			h.lg.Warn(
				"unexpected Raft message type",
				zap.String("local-member-id", h.localID.String()),
				zap.String("remote-snapshot-sender-id", from),
				zap.String("message-type", m.Type.String()),
			)
		} else {
			plog.Errorf("unexpected raft message type %s on snapshot path", m.Type)
		}
		http.Error(w, "wrong raft message type", http.StatusBadRequest)
		snapshotReceiveFailures.WithLabelValues(from).Inc()
		return
	}

	snapshotReceiveInflights.WithLabelValues(from).Inc()
	defer func() {
		snapshotReceiveInflights.WithLabelValues(from).Dec()
	}()

	if h.lg != nil {
		h.lg.Info(
			"receiving database snapshot",
			zap.String("local-member-id", h.localID.String()),
			zap.String("remote-snapshot-sender-id", from),
			zap.Uint64("incoming-snapshot-index", m.Snapshot.Metadata.Index),
			zap.Int("incoming-snapshot-message-size-bytes", msgSize),
			zap.String("incoming-snapshot-message-size", humanize.Bytes(uint64(msgSize))),
		)
	} else {
		plog.Infof("receiving database snapshot [index:%d, from %s] ...", m.Snapshot.Metadata.Index, types.ID(m.From))
	}

	// save incoming database snapshot.
	n, err := h.snapshotter.SaveDBFrom(r.Body, m.Snapshot.Metadata.Index)
	if err != nil {
		msg := fmt.Sprintf("failed to save KV snapshot (%v)", err)
		if h.lg != nil {
			h.lg.Warn(
				"failed to save incoming database snapshot",
				zap.String("local-member-id", h.localID.String()),
				zap.String("remote-snapshot-sender-id", from),
				zap.Uint64("incoming-snapshot-index", m.Snapshot.Metadata.Index),
				zap.Error(err),
			)
		} else {
			plog.Error(msg)
		}
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	if h.lg != nil {
		h.lg.Info(
			"received and saved database snapshot",
			zap.String("local-member-id", h.localID.String()),
			zap.String("remote-snapshot-sender-id", from),
			zap.Uint64("incoming-snapshot-index", m.Snapshot.Metadata.Index),
			zap.Int64("incoming-snapshot-size-bytes", n),
			zap.String("incoming-snapshot-size", humanize.Bytes(uint64(n))),
		)
	} else {
		plog.Infof("received and saved database snapshot [index: %d, from: %s] successfully", m.Snapshot.Metadata.Index, types.ID(m.From))
	}

	if err := h.r.Process(context.TODO(), m); err != nil {
		switch v := err.(type) {
		// Process may return writerToResponse error when doing some
		// additional checks before calling raft.Node.Step.
		case writerToResponse:
			v.WriteTo(w)
		default:
			msg := fmt.Sprintf("failed to process raft message (%v)", err)
			if h.lg != nil {
				h.lg.Warn(
					"failed to process Raft message",
					zap.String("local-member-id", h.localID.String()),
					zap.String("remote-snapshot-sender-id", from),
					zap.Error(err),
				)
			} else {
				plog.Error(msg)
			}
			http.Error(w, msg, http.StatusInternalServerError)
		}
		return
	}

	// Write StatusNoContent header after the message has been processed by
	// raft, which facilitates the client to report MsgSnap status.
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

	w.Header().Set("X-Server-Version", version.Version)
	w.Header().Set("X-Etcd-Cluster-ID", h.clusterId.String())

	if err := checkClusterCompatibilityFromHeader(h.lg, h.tr.ID, r.Header, h.clusterId); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}

	var t streamType
	switch path.Dir(r.URL.Path) {
	case streamTypeMsgAppV2.endpoint():
		t = streamTypeMsgAppV2
	case streamTypeMessage.endpoint():
		t = streamTypeMessage
	default:
		if h.lg != nil {
			h.lg.Debug(
				"ignored unexpected streaming request path",
				zap.String("local-member-id", h.tr.ID.String()),
				zap.String("remote-peer-id-stream-handler", h.id.String()),
				zap.String("path", r.URL.Path),
			)
		} else {
			plog.Debugf("ignored unexpected streaming request path %s", r.URL.Path)
		}
		http.Error(w, "invalid path", http.StatusNotFound)
		return
	}

	fromStr := path.Base(r.URL.Path)
	from, err := types.IDFromString(fromStr)
	if err != nil {
		if h.lg != nil {
			h.lg.Warn(
				"failed to parse path into ID",
				zap.String("local-member-id", h.tr.ID.String()),
				zap.String("remote-peer-id-stream-handler", h.id.String()),
				zap.String("path", fromStr),
				zap.Error(err),
			)
		} else {
			plog.Errorf("failed to parse from %s into ID (%v)", fromStr, err)
		}
		http.Error(w, "invalid from", http.StatusNotFound)
		return
	}
	if h.r.IsIDRemoved(uint64(from)) {
		if h.lg != nil {
			h.lg.Warn(
				"rejected stream from remote peer because it was removed",
				zap.String("local-member-id", h.tr.ID.String()),
				zap.String("remote-peer-id-stream-handler", h.id.String()),
				zap.String("remote-peer-id-from", from.String()),
			)
		} else {
			plog.Warningf("rejected the stream from peer %s since it was removed", from)
		}
		http.Error(w, "removed member", http.StatusGone)
		return
	}
	p := h.peerGetter.Get(from)
	if p == nil {
		// This may happen in following cases:
		// 1. user starts a remote peer that belongs to a different cluster
		// with the same cluster ID.
		// 2. local etcd falls behind of the cluster, and cannot recognize
		// the members that joined after its current progress.
		if urls := r.Header.Get("X-PeerURLs"); urls != "" {
			h.tr.AddRemote(from, strings.Split(urls, ","))
		}
		if h.lg != nil {
			h.lg.Warn(
				"failed to find remote peer in cluster",
				zap.String("local-member-id", h.tr.ID.String()),
				zap.String("remote-peer-id-stream-handler", h.id.String()),
				zap.String("remote-peer-id-from", from.String()),
				zap.String("cluster-id", h.clusterId.String()),
			)
		} else {
			plog.Errorf("failed to find member %s in cluster %s", from, h.clusterId)
		}
		http.Error(w, "error sender not found", http.StatusNotFound)
		return
	}

	wto := h.id.String()
	if gto := r.Header.Get("X-Raft-To"); gto != wto {
		if h.lg != nil {
			h.lg.Warn(
				"ignored streaming request; ID mismatch",
				zap.String("local-member-id", h.tr.ID.String()),
				zap.String("remote-peer-id-stream-handler", h.id.String()),
				zap.String("remote-peer-id-header", gto),
				zap.String("remote-peer-id-from", from.String()),
				zap.String("cluster-id", h.clusterId.String()),
			)
		} else {
			plog.Errorf("streaming request ignored (ID mismatch got %s want %s)", gto, wto)
		}
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
		localID: h.tr.ID,
		peerID:  h.id,
	}
	p.attachOutgoingConn(conn)
	<-c.closeNotify()
}

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
