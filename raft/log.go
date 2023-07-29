package raft

import (
	"errors"
	"github.com/ColdToo/Cold2DB/pb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries

type RaftLog struct {
	first uint64

	applied uint64

	committed uint64

	stabled uint64

	last uint64

	// raftlog中暂时保存的日志
	entries []*pb.Entry

	// db 持久化保存的日志
	storage Storage
}

func newRaftLog(storage Storage) (*RaftLog, error) {
	firstIndex, err := storage.FirstIndex()
	if err != nil {

		return nil, err
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		return nil, err
	}
	allEntrys, err := storage.Entries(firstIndex, lastIndex+1)

	return &RaftLog{storage: storage, first: firstIndex, last: lastIndex, entries: allEntrys}, nil
}

func (l *RaftLog) getAllEntries() []pb.Entry {
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	return l.entries[l.first : l.stabled+1]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextApplyEnts() (ents []pb.Entry) {
	return l.entries[l.applied : l.committed+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	return l.last
}

func (l *RaftLog) Term(i uint64) (uint64, error) {
	if uint64(len(l.entries)) < i {
		return 0, errors.New("not find the log entry")
	}
	return l.entries[i].Term, nil
}

func (l *RaftLog) AppendEntries(ents []*pb.Entry) {
	l.entries = append(l.entries, ents...)
}
