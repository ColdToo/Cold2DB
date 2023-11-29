package raft

import (
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
)

//  log structure
//
//	snapshot/first.................. applied.........committed.....stabled.............last
//	--------|--------mem-table----------|-----------------------memory entries-----------|
//	--------|------------------------------wal------------------------|------------------|

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

	stabled uint64

	// 这个偏移量（u.offset）表示当前不稳定日志中的第一个条目在整个日志中的位置。举个例子，
	// 如果 u.offset 为 10，那么不稳定日志中的第一个条目在整个日志中的位置就是第 10 个位置。
	// 这个字段通常在日志条目被写入存储或者日志被截断并追加新条目时进行更新。
	// raftLog在创建时，会将unstable的offset置为storage的last index + 1，
	offset uint64

	// maxNextEntsSize is the maximum number aggregate byte size for per ready
	maxNextEntsSize uint64

	unstableEnts []*pb.Entry

	storage db.Storage
}

func newRaftLog(storage db.Storage, maxNextEntsSize uint64) Log {
	firstIndex := storage.FirstIndex()
	appliedIndex := storage.AppliedIndex()
	emptyEntS := make([]*pb.Entry, 0)
	return &RaftLog{storage: storage, first: firstIndex, applied: appliedIndex, unstableEnts: emptyEntS, maxNextEntsSize: maxNextEntsSize}
}

// FirstIndex 返回未压缩日志的索引
func (l *RaftLog) firstIndex() uint64 {
	//在raft初始化时storage此时还未有已经持久化的raftlog日志此时应该返回unstable中的最后一条日志
	return l.storage.FirstIndex()
}

func (l *RaftLog) lastIndex() uint64 {
	if len(l.unstableEnts) > 0 {
		return l.unstableEnts[len(l.unstableEnts)-1].Index
	}

	return l.storage.LastIndex()
}

func (l *RaftLog) Term(i uint64) (uint64, error) {
	//如果i已经stable那么通过storage获取
	if i > l.stabled {
		return l.unstableEnts[i-l.applied].Index, nil
	}

	return l.storage.Term(i)
}

func (l *RaftLog) StableTo(i uint64) {
	//裁剪掉unstable中已经stable的日志，可以减少内存中所占用的空间
	l.stabled = i
}

// Entries 获取指定范围内的日志切片
func (l *RaftLog) Entries(lo, hi uint64, maxSize uint64) (ents []pb.Entry, err error) {
	if lo > l.lastIndex() || lo == hi {
		return nil, nil
	}
	err = l.mustCheckOutOfBounds(lo, hi)

	if lo < l.offset {
		storedEnts, err := l.storage.Entries(lo, min(hi, l.offset), maxSize)
		if err == code.ErrCompacted {
			return nil, err
		} else if err == code.ErrUnavailable {
			log.Panicf("entries[%d:%d) is unavailable from storage", lo, min(hi, l.offset))
		} else if err != nil {
			panic(err) // TODO(bdarnell)
		}

		// check if ents has reached the size limitation
		if uint64(len(storedEnts)) < min(hi, l.offset)-lo {
			return storedEnts, nil
		}

		ents = storedEnts
	}

	if hi > l.offset {
		unstable := l.unstableEnts[lo-l.offset : hi-l.offset]
		if len(ents) > 0 {
			combined := make([]pb.Entry, len(ents)+len(unstable))
			n := copy(combined, ents)
			copy(combined[n:], unstable)
			ents = combined
		} else {
			ents = unstable
		}
	}
	return limitSize(ents, maxSize), nil
}

// l.firstIndex <= lo <= hi <= total raft log length
func (l *RaftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		log.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi {
		return code.ErrCompacted
	}

	//计算整个raft日志的长度
	length := l.lastIndex() + 1 - fi
	if lo < fi || hi > fi+length {
		log.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.lastIndex())
	}
	return nil
}

func (l *RaftLog) unstableEntries() []*pb.Entry {
	if len(l.unstableEnts) == 0 {
		return nil
	}
	return l.unstableEnts
}

// AppendEntries leader append
func (l *RaftLog) AppendEnts(ents ...pb.Entry) uint64 {
	after := ents[0].Index - 1
	if after < l.committed {
		log.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}

	//todo leader的日志不应该出现截断日志的情况
	if after != l.offset+uint64(len(l.unstableEnts)) {
		log.Panicf("leader after should directly append ")
	}

	l.unstableEnts = append(l.unstableEnts, &ents...)

	return l.lastIndex()
}

// TruncateAndAppend follower append maybe conflict log so should truncate
func (l *RaftLog) TruncateAndAppend() {
	if l.stabled > l.committed {
	} else {
		//如果发现需要裁剪的日志已经被stable了那么就需要将其从storage中删除
		l.storage.Append(l.entries[l.committed-l.stabled : l.committed])
		l.entries = l.entries[l.committed-l.stabled : l.committed]
		l.offset = l.committed
		l.storage.Truncate()
	}

}

// NextApplyEnts 返回可以应用到状态机的日志索引，若无返回index0
// 以及持久化的日志实质上已经存储在storage中通过raft判断哪些日志可以apply到状态机中
func (l *RaftLog) NextApplyEnts() (ents []*pb.Entry) {
	return nil
}

func (l *RaftLog) SetCommittedIndex(i uint64) {
	l.committed = i
}

// 每次处理完一轮ready后都需要刷新firstIndex
func (l *RaftLog) RefreshFirstAndAppliedIndex() {
	// todo 使用锁来保证first
	l.first = l.storage.FirstIndex()
	l.applied = l.storage.AppliedIndex()
}
