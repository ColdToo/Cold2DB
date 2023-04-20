package main

import (
	"Cold2DB/raft"
	"Cold2DB/raft/raftproto"
	"go.uber.org/zap"
)

func NewRaftNode() {

}

// A key-value stream backed by raft
type raftNode struct {
	proposeC    <-chan string               // proposed messages (k,v)
	confChangeC <-chan raftproto.ConfChange // proposed cluster config changes
	commitC     chan<- *commit              // entries committed to log (k,v)
	errorC      chan<- error                // errors from raft session

	id          int      // client ID for raft session
	peers       []string // raft peer URLs
	join        bool     // node is joining an existing cluster
	waldir      string   // path to WAL directory
	snapdir     string   // path to snapshot directory
	getSnapshot func() ([]byte, error)

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready

	snapCount uint64
	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete

	logger *zap.Logger
}
