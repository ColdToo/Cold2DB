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
	lastTerm() uint64
}

type raftLog struct {
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

	unstableEnts []pb.Entry

	storage db.Storage
}

func newRaftLog(storage db.Storage, maxNextEntsSize uint64) (r *raftLog) {
	r = &raftLog{
		storage:         storage,
		maxNextEntsSize: maxNextEntsSize,
	}
	firstIndex := storage.FirstIndex()
	lastIndex := storage.LastIndex()
	appliedIndex := storage.AppliedIndex()
	r.offset = lastIndex + 1
	// Initialize our committed and applied pointers to the time of the last compaction.
	r.committed = firstIndex - 1
	if appliedIndex == 0 {
		r.applied = firstIndex - 1
	}
	return
}

// FirstIndex 返回未压缩日志的索引
func (l *raftLog) firstIndex() uint64 {
	//在raft初始化时storage此时还未有已经持久化的raftlog日志此时应该返回unstable中的最后一条日志
	return l.storage.FirstIndex()
}

func (l *raftLog) lastIndex() uint64 {
	if lenth := len(l.unstableEnts); lenth != 0 {
		return l.offset + uint64(lenth)
	}
	if i := l.storage.LastIndex(); i != 0 {
		return i
	}
	//index	 默认从1开始
	return 1
}

func (l *raftLog) lastTerm() uint64 {
	if lenth := len(l.unstableEnts); lenth != 0 {
		return l.offset + uint64(lenth)
	}

	if i := l.storage.LastIndex(); i != 0 {
		return i
	}
	//index	 默认从1开始
	return 1
}

func (l *raftLog) Term(i uint64) (uint64, error) {
	//如果i已经stable那么通过storage获取
	if i > l.stabled {
		return l.unstableEnts[i-l.applied].Index, nil
	}

	return l.storage.Term(i)
}

func (l *raftLog) StableTo(i uint64) {
	//裁剪掉unstable中已经stable的日志，可以减少内存中所占用的空间
	l.stabled = i
}

// Entries 获取指定范围内的日志切片
func (l *raftLog) Entries(lo, hi uint64, maxSize uint64) (ents []pb.Entry, err error) {
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
func (l *raftLog) mustCheckOutOfBounds(lo, hi uint64) error {
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

func (l *raftLog) unstableEntries() []*pb.Entry {
	if len(l.unstableEnts) == 0 {
		return nil
	}
	return l.unstableEnts
}

// TruncateAndAppend follower append maybe conflict log so should truncate
func (l *raftLog) TruncateAndAppend() {
	if l.stabled > l.committed {
	} else {
		//如果发现需要裁剪的日志已经被stable了那么就需要将其从storage中删除
		l.storage.Append(l.entries[l.committed-l.stabled : l.committed])
		l.entries = l.entries[l.committed-l.stabled : l.committed]
		l.offset = l.committed
		l.storage.Truncate()
	}
}

func (l *raftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	if appendLast := ents[0].Index - 1; appendLast < l.committed {
		log.Panicf("after(%d) is out of range [committed(%d)]", appendLast, l.committed)
	}
	l.unstable.truncateAndAppend(ents)
	return l.lastIndex()
}

// NextApplyEnts 返回可以应用到状态机的日志索引，若无返回index0
// 以及持久化的日志实质上已经存储在storage中通过raft判断哪些日志可以apply到状态机中
func (l *raftLog) NextApplyEnts() (ents []*pb.Entry) {
	return nil
}

func (l *raftLog) SetCommittedIndex(i uint64) {
	l.committed = i
}

// 每次处理完一轮ready后都需要刷新firstIndex
func (l *raftLog) RefreshFirstAndAppliedIndex() {
	// todo 使用锁来保证first
	l.first = l.storage.FirstIndex()
	l.applied = l.storage.AppliedIndex()
}

func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		log.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

// findConflictByTerm takes an (index, term) pair (indicating a conflicting log
// entry on a leader/follower during an append) and finds the largest index in
// log l with a term <= `term` and an index <= `index`. If no such index exists
// in the log, the log's first index is returned.
//
// The index provided MUST be equal to or less than l.lastIndex(). Invalid
// inputs log a warning and the input index is returned.
func (l *raftLog) findConflictByTerm(index uint64, term uint64) uint64 {
	if li := l.lastIndex(); index > li {
		// NB: such calls should not exist, but since there is a straightfoward
		// way to recover, do it.
		//
		// It is tempting to also check something about the first index, but
		// there is odd behavior with peers that have no log, in which case
		// lastIndex will return zero and firstIndex will return one, which
		// leads to calls with an index of zero into this method.
		l.logger.Warningf("index(%d) is out of range [0, lastIndex(%d)] in findConflictByTerm",
			index, li)
		return index
	}
	for {
		logTerm, err := l.term(index)
		if logTerm <= term || err != nil {
			break
		}
		index--
	}
	return index
}

// isUpToDate determines if the given (lastIndex,term) log is more up-to-date
// by comparing the index and term of the last entries in the existing logs.
// If the logs have last entries with different terms, then the log with the
// later term is more up-to-date. If the logs end with the same term, then
// whichever log has the larger lastIndex is more up-to-date. If the logs are
// the same, the given log is up-to-date.
func (l *raftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.lastIndex())
}

func (l *raftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	l.logger.Panicf("unexpected error (%v)", err)
	return 0
}

func (l *raftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {
		if l.lastIndex() < tocommit {
			l.logger.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.lastIndex())
		}
		l.committed = tocommit
	}
}
