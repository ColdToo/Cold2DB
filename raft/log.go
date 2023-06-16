package raft

import (
	"errors"
	"github.com/ColdToo/Cold2DB/pb"
	"go.uber.org/zap"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries

type RaftLog struct {
	// 持久化从first到stabled这一块的日志
	storage Storage

	first uint64

	last uint64

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	LastApplied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// 所有还未压缩的日志
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	pendingSnapshot *pb.Snapshot

	logger zap.Logger
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	firstIndex, err := storage.FirstIndex()
	if err != nil {

		return nil
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		return nil
	}

	return &RaftLog{storage: storage, first: firstIndex, last: lastIndex}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	if len(l.entries[:l.LastApplied+1]) > 1000 {
		// todo compact
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	return l.entries[:l.stabled+1]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	return l.entries[l.committed:]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	return uint64(len(l.entries) - 1)
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if uint64(len(l.entries)) < i {
		return 0, errors.New("not find the log entry")
	}
	return l.entries[i].Term, nil
}
