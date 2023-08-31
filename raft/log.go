package raft

import (
	"errors"
	"github.com/ColdToo/Cold2DB/pb"
)

//  log structure
//
//	snapshot/first.................. applied............ committed.............last
//	--------|--------mem-table----------|--------------memory-entries-----------|
//	                   wal

type RaftLog struct {
	first uint64

	applied uint64

	committed uint64

	last uint64

	entries []*pb.Entry

	storage Storage
}

func newRaftLog(storage Storage) (*RaftLog, error) {
	firstIndex := storage.FirstIndex()
	appliedIndex := storage.AppliedIndex()
	emptyEntsS := make([]*pb.Entry, 0)
	return &RaftLog{storage: storage, first: firstIndex, applied: appliedIndex, entries: emptyEntsS}, nil
}

//todo entries 逻辑需要重新判断

func (l *RaftLog) nextApplyEnts() (ents []*pb.Entry) {
	if len(l.entries) > 0 && l.committed > l.applied {
		return l.entries[0 : l.committed-l.applied]
	}
	return nil
}

func (l *RaftLog) hasNextApplyEnts() bool {
	return l.committed > l.applied
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

	return 0, ErrUnavailable
}

func (l *RaftLog) AppendEntries(ents []pb.Entry) {
	for _, e := range ents {
		l.entries = append(l.entries, &e)
	}
}

func (l *RaftLog) Entries(low, high uint64) (ents []*pb.Entry, err error) {
	if low > high {
		return nil, errors.New("low should not > high")
	}

	if low < l.first {
		//这条entries已经被压缩了无法返回
		return nil, errors.New("the low is < first log index")
	}

	if low > l.applied {
		return l.entries[low-l.applied : high-l.applied+1], nil
	}

	if high < l.applied {
		return l.storage.Entries(low, high)
	}

	if low < l.applied && high > l.applied {
		entries, _ := l.storage.Entries(low, l.applied)
		ents = append(ents, entries...)
		entries = l.entries[0 : high-l.applied+1]
		ents = append(ents, entries...)
	}

	return
}

func (l *RaftLog) RefreshFirstAndAppliedIndex() {
	l.first = l.storage.FirstIndex()
	l.applied = l.storage.AppliedIndex()
}
