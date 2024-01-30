package raft

import (
	"errors"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
)

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
// snapshot is temporarily unavailable.
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

//  log structure
//
//		 persist................ applied/first.........committed......................last/stabled
//	--------|--------mem-table----------|-------------memory entries-----------------------|
//	--------|--------------------------wal-------------------------------------------------|

type raftLog struct {
	applied uint64

	committed uint64

	stabled uint64 //等于稳定存储的last index

	// 这个偏移量（u.offset）表示当前不稳定日志中的第一个条目在整个日志中的位置。举个例子，
	// 如果 u.offset 为 10，那么不稳定日志中的第一个条目在整个日志中的位置就是第 10 个位置。
	// raftLog在创建时，会将unstable的offset置为storage的last index + 1，
	offset uint64

	// maxNextEntsSize is the maximum number aggregate byte size for per ready
	maxNextEntsSize uint64

	unstableEnts []pb.Entry

	storage db.Storage
}

func newRaftLog(storage db.Storage, maxNextEntsSize uint64) (r *raftLog) {
	if storage == nil {
		log.Panicf("storage must not be nil")
	}
	r = &raftLog{
		storage:         storage,
		maxNextEntsSize: maxNextEntsSize,
	}
	r.stabled = storage.LastIndex()
	r.applied = storage.FirstIndex() - 1
	r.offset = r.stabled + 1
	return
}

// FirstIndex 返回未压缩日志的索引
func (l *raftLog) firstIndex() uint64 {
	firstIndex := l.storage.FirstIndex()
	if firstIndex == 0 {
		if len(l.unstableEnts) != 0 {
			return l.unstableEnts[0].Index
		}
		return 0
	} else {
		return firstIndex
	}
}

func (l *raftLog) lastIndex() uint64 {
	if length := len(l.unstableEnts); length != 0 {
		return l.offset + uint64(length) - 1
	}
	return l.storage.LastIndex()
}

func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		log.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
}

func (l *raftLog) term(i uint64) (uint64, error) {
	// the valid term range is [index of dummy entry, last index]
	if i < l.firstIndex() || i > l.lastIndex() {
		return 0, nil
	}

	if i > l.stabled {
		return l.unstableEnts[i-l.offset].Term, nil
	}

	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic("should not happen")
}

// allEntries returns all entries in the log.
func (l *raftLog) allEntries() []pb.Entry {
	ents, err := l.entries(l.firstIndex(), noLimit)
	if err == nil {
		return ents
	}
	if err == ErrCompacted {
		// try again if there was a racing compaction
		return l.allEntries()
	}
	panic(err)
}

// Entries 获取指定范围内的日志切片
func (l *raftLog) entries(i, maxSize uint64) (ents []pb.Entry, err error) {
	if i > l.lastIndex() {
		return nil, nil
	}
	return l.slice(i, l.lastIndex()+1, maxSize)
}

// slice returns a slice of log entries from lo through hi-1, inclusive.
func (l *raftLog) slice(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}

	var ents []pb.Entry
	if lo < l.offset {
		storedEnts, err := l.storage.Entries(lo, min(hi, l.offset), maxSize)
		if err == ErrCompacted {
			return nil, err
		}

		if uint64(len(storedEnts)) < min(hi, l.offset)-lo {
			return storedEnts, nil
		}

		ents = storedEnts
	}
	if hi > l.offset {
		unstableEnts := l.unstableEnts[lo-l.offset : hi-l.offset]
		if len(ents) > 0 {
			combined := make([]pb.Entry, len(ents)+len(unstableEnts))
			n := copy(combined, ents)
			copy(combined[n:], unstableEnts)
			ents = combined
		} else {
			ents = unstableEnts
		}
	}
	return limitSize(ents, maxSize), nil
}

// 是否满足lo < hi。（slice获取的是左闭右开区间$[lo,hi)$的日志切片。）
// 是否满足lo > firstIndex，否则该范围中部分日志已被压缩，无法获取。
// 是否满足hi > lastIndex+1，否则该范围中部分日志还没被追加到当前节点的日志中，无法获取。
// l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l *raftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		log.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}

	if hi > l.lastIndex() {
		log.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.lastIndex())
	}
	return nil
}

// for ready

func (l *raftLog) unstableEntries() []pb.Entry {
	if len(l.unstableEnts) == 0 {
		return nil
	}
	return l.unstableEnts
}

func (l *raftLog) nextCommittedEnts() (ents []pb.Entry) {
	off := max(l.applied+1, l.firstIndex())
	if l.committed+1 > off {
		ents, err := l.slice(off, l.committed+1, l.maxNextEntsSize)
		if err != nil {
			log.Panicf("unexpected error when getting unapplied entries (%v)", err)
		}
		return ents
	}
	return nil
}

func (l *raftLog) hasNextCommittedEnts() bool {
	off := max(l.applied+1, l.firstIndex())
	return l.committed+1 > off
}

// append与maybeAppend是向raftLog写入日志的方法。
// 二者的区别在于append不会检查给定的日志切片是否与已有日志有冲突，leader会直接调用该方法
// 因此leader向raftLog中追加日志时会调用该函数；
func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (lastnewi uint64, ok bool) {
	if l.matchTerm(index, logTerm) {
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		//说明既没有冲突又没有新日志，直接进行下一步处理
		case ci == 0:
			//检查给定的日志起点是否在committed索引位置之前，如果在其之前，这违背了Raft算法的Log Matching性质
		case ci <= l.committed:
			log.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
			//如果返回值大于committed，既可能是冲突发生在committed之后，也可能是有新日志，
			//但二者的处理方式都是相同的，即从将从冲突处或新日志处开始的日志覆盖或追加到当前日志中即可。
		default:
			offset := index + 1
			l.append(ents[ci-offset:]...)
		}
		//1、leader给follower复制日志时，如果复制的日志条目超过了单个消息的上限，
		//则可能出现leader传给follower的committed值大于该follower复制完这条消息中的日志后的最大index。
		//此时，该follower的新committed值为lastnewi。
		//2、follower能够跟上leader，leader传给follower的日志中有未确认被法定数量节点稳定存储的日志，
		//此时传入的committed比lastnewi小，该follower的新committed值为传入的committed值。
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

func (l *raftLog) matchTerm(i, term uint64) bool {
	t, err := l.term(i)
	if err != nil {
		return false
	}
	return t == term
}

// 如果给定的日志与已有的日志的index和term冲突，其会返回第一条冲突的日志条目的index。
// 如果没有冲突，且给定的日志的所有条目均已在已有日志中，返回0.
// 如果没有冲突，且给定的日志中包含已有日志中没有的新日志，返回第一条新日志的index。
func (l *raftLog) findConflict(ents []pb.Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.lastIndex() {
				log.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.term(ne.Index)), ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

func (l *raftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	//检查给定的日志起点是否在committed索引位置之前，如果在其之前，这违背了Raft算法的Log Matching性质
	if after := ents[0].Index - 1; after < l.committed {
		log.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	l.truncateAndAppend(ents)
	return l.lastIndex()
}

func (l *raftLog) truncateAndAppend(ents []pb.Entry) {
	after := ents[0].Index
	switch {
	// after is the next index in the u.unstableEnts directly append
	case after == l.offset+uint64(len(l.unstableEnts)):
		l.unstableEnts = append(l.unstableEnts, ents...)
	case after < l.offset:
		log.Infof("replace the unstable entries from index %d", after)
		if err := l.storage.Truncate(after); err != nil {
			log.Panicf("failed to truncate the unstable entries before index %d,err:%v", after, err)
		}
		l.offset = after
		l.unstableEnts = ents
	default:
		// after >= l.offset
		log.Infof("truncate the unstable entries before index %d", after)
		l.unstableEnts = append([]pb.Entry{}, l.unstableEnts[:after-l.offset]...)
		l.unstableEnts = append(l.unstableEnts, ents...)
	}
}

func (l *raftLog) findConflictByTerm(index uint64, term uint64) uint64 {
	if li := l.lastIndex(); index > li {
		// NB: such calls should not exist, but since there is a straightfoward
		// way to recover, do it.
		//
		// It is tempting to also check something about the first index, but
		// there is odd behavior with peers that have no log, in which case
		// lastIndex will return zero and firstIndex will return one, which
		// leads to calls with an index of zero into this method.
		log.Warnf("index(%d) is out of range [0, lastIndex(%d)] in findConflictByTerm",
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
	log.Panicf("unexpected error (%v)", err)
	return 0
}

func (l *raftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {
		if l.lastIndex() < tocommit {
			log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.lastIndex())
		}
		l.committed = tocommit
	}
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

func (l *raftLog) stableTo(i uint64) {
	if i >= l.offset {
		l.unstableEnts = l.unstableEnts[i+1-l.offset:]
		l.offset = i + 1
		l.shrinkEntriesArray()
	}
}

func (l *raftLog) shrinkEntriesArray() {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	if len(l.unstableEnts) == 0 {
		l.unstableEnts = nil
	} else if len(l.unstableEnts)*lenMultiple < cap(l.unstableEnts) {
		newEntries := make([]pb.Entry, len(l.unstableEnts))
		copy(newEntries, l.unstableEnts)
		l.unstableEnts = newEntries
	}
}
