package raft

import (
	"errors"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
)

//  log structure
//
//	snapshot/first.................. applied......................committed.............last
//	--------|--------mem-table----------|-----------------------memory-entries-----------|
//	                                   wal

type Log interface {
	Term(i uint64) (uint64, error)
	LastIndex() uint64
	FirstIndex() uint64
	AppliedIndex() uint64
	SetCommittedIndex(i uint64)
	CommittedIndex() uint64
	RefreshFirstAndAppliedIndex()

	NextApplyEnts() (ents []*pb.Entry)
	HasNextApplyEnts() bool
	AppendEntries(ents []pb.Entry)
	Entries(low, high uint64) (ents []*pb.Entry, err error)
}

type RaftLog struct {
	first uint64

	applied uint64

	committed uint64

	last uint64

	stabled uint64

	//第i条entries数组数据在raft日志中的索引为i + unstable.offset。
	offset uint64

	entries []*pb.Entry

	storage db.Storage
}

func newRaftLog(storage db.Storage) Log {
	firstIndex := storage.FirstIndex()
	appliedIndex := storage.AppliedIndex()
	emptyEntS := make([]*pb.Entry, 0)
	return &RaftLog{storage: storage, first: firstIndex, applied: appliedIndex, entries: emptyEntS}
}

func (l *RaftLog) Term(i uint64) (uint64, error) {
	//如果i已经stable那么通过storage获取
	if i > l.applied {
		return l.entries[i-l.applied].Index, nil
	} else {
		term, err := l.storage.Term(i)
		if err != nil {
			return term, err
		}
	}

	return 0, errors.New("is compacted")
}

func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}

	return l.storage.LastIndex()
}

func (l *RaftLog) FirstIndex() uint64 {
	return l.storage.FirstIndex()
}

func (l *RaftLog) AppliedIndex() uint64 {
	return l.applied
}

func (l *RaftLog) CommittedIndex() uint64 {
	return l.committed
}

func (l *RaftLog) SetCommittedIndex(i uint64) {
	l.committed = i
}

func (l *RaftLog) RefreshFirstAndAppliedIndex() {
	// todo 使用锁来保证first
	l.first = l.storage.FirstIndex()
	l.applied = l.storage.AppliedIndex()
}

func (l *RaftLog) NextApplyEnts() (ents []*pb.Entry) {
	return nil
}

func (l *RaftLog) HasNextApplyEnts() bool {
	return true
}

func (l *RaftLog) Entries(low, high uint64) (ents []*pb.Entry, err error) {
	err = l.mustCheckOutOfBounds(low, high)
	if err != nil {
		return
	}

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

func (l *RaftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		log.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.FirstIndex()
	if lo < fi {
		return errors.New("is compacted")
	}

	length := l.LastIndex() + 1 - fi
	if lo < fi || hi > fi+length {
		log.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.LastIndex())
	}
	return nil
}

func (l *RaftLog) StableTo(i uint64) {
	l.stabled = i
}

// AppendEntries leader append
func (l *RaftLog) AppendEntries(ents []pb.Entry) {
	for _, e := range ents {
		l.entries = append(l.entries, &e)
	}
	l.last = l.last + uint64(len(ents))
}

// TruncateAndAppend follower append maybe conflict log so should truncate
func (l *RaftLog) TruncateAndAppend() {
	if l.stabled > l.committed {
	} else {
		l.storage.Append(l.entries[l.committed-l.stabled : l.committed])
		l.entries = l.entries[l.committed-l.stabled : l.committed]
		l.offset = l.committed
	}
}
