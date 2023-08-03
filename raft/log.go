package raft

import (
	"github.com/ColdToo/Cold2DB/pb"
)

//leader log structure
//
//	snapshot/first.................. applied............ committed...............
//	--------|--------mem-table----------|------------------memory---------------|
//	                              log entries

//follower log structure
//
//	snapshot/first.................. applied.....................................
//	--------|--------mem-table----------|------------------memory---------------|
//

type RaftLog struct {
	first uint64

	applied uint64

	committed uint64

	last uint64

	// 未被committed的日志
	// todo entries存在并发访问的可能性吗
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

// Term 根据index返回term,如果raftlog中没有那么就从memtable中获取,如果memtble也获取不到那么说明已经compact了
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i > l.committed {
		//todo 从raftlog中获取term
	} else {
		term, err := l.storage.Term(i)
		if err != nil {
			return term, err
		}
	}
	return
}

func (l *RaftLog) AppendEntries(ents []*pb.Entry) (committed uint64) {
	l.entries = append(l.entries, ents...)
}

func (l *RaftLog) ApplyEntries(ents []*pb.Entry) (applied uint64, err error) {
	l.entries = append(l.entries, ents...)
}

func (l *RaftLog) Entries(low, high uint64) (ents []*pb.Entry, err error) {
	return
}
