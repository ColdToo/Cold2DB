package raft

import (
	"github.com/ColdToo/Cold2DB/pb"
)

// log structure
//
//	snapshot/first.................. applied............ committed...............
//	--------|--------mem-table----------|--------------memory entries-----------|
//	                                                      entries

type RaftLog struct {
	first uint64

	applied uint64

	committed uint64

	last uint64

	entries []*pb.Entry

	// db 持久化保存的日志
	storage Storage
}

func newRaftLog(storage Storage) (*RaftLog, error) {
	firstIndex, err := storage.FirstIndex()
	if err != nil {

		return nil, err
	}
	appliedIndex, err := storage.AppliedIndex()
	if err != nil {
		return nil, err
	}

	emptyEntsS := make([]*pb.Entry, 0)

	return &RaftLog{storage: storage, first: firstIndex, applied: appliedIndex, entries: emptyEntsS}, nil
}

func (l *RaftLog) nextApplyEnts() (ents []*pb.Entry) {
	return l.entries[l.applied+1 : l.committed+1]
}

func (l *RaftLog) LastIndex() uint64 {
	return l.last
}

// Term 根据index返回term,如果raft log中没有那么就从mem table中获取,如果mem table也获取不到那么说明这条日志已经被compact了
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i > l.applied {
		return l.entries[i-l.applied].Index, nil
	} else {
		term, err := l.storage.Term(i)
		if err != nil {
			return term, err
		}
	}
	//todo 不应该走到这个分支
	return 0, ErrUnavailable
}

func (l *RaftLog) AppendEntries(ents []*pb.Entry) {
	l.entries = append(l.entries, ents...)
}

func (l *RaftLog) Entries(low, high uint64) (ents []*pb.Entry, err error) {
	return
}
