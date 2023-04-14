package raftlog

import (
	"Cold2DB/raft"
	"go.uber.org/zap"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	storage raft.Storage

	unstable unstable

	committed uint64

	applied uint64

	logger zap.Logger
}

func NewRaftLog() {

}
