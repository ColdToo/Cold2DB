package arenaskl

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

type splice struct {
	prev *node
	next *node
}

func (s *splice) init(prev, next *node) {
	s.prev = prev
	s.next = next
}

type Iterator struct {
	list  *Skiplist
	arena *Arena
	nd    *node
	value []uint64
}

func (it *Iterator) Init(list *Skiplist) {
	it.list = list
	it.arena = list.arena
	it.nd = nil
	it.value = make([]uint64, 3) //根据实际情况确定可能的value数量
}

func (it *Iterator) Valid() bool { return it.nd != nil }

func (it *Iterator) Next() {
	next := it.list.getNext(it.nd, 0)
	it.setNode(next, false)
}

func (it *Iterator) Prev() {
	prev := it.list.getPrev(it.nd, 0)
	it.setNode(prev, true)
}

// SeekToFirst move to head node
func (it *Iterator) SeekToFirst() {
	it.setNode(it.list.getNext(it.list.head, 0), false)
}

// SeekToLast move to tail node
func (it *Iterator) SeekToLast() {
	it.setNode(it.list.getPrev(it.list.tail, 0), true)
}

func (it *Iterator) setNode(nd *node, reverse bool) bool {
	success := false
	if nd != nil {
		it.value = nd.value
		it.nd = nd
		success = true
	}
	return success
}

func (it *Iterator) seekForSplice(key []byte, spl *[maxHeight]splice) (found bool) {
	var prev, next *node
	level := int(it.list.Height() - 1)
	prev = it.list.head

	//从最高层的头节点开始找key
	for {
		prev, next, found = it.list.findSpliceForLevel(key, level, prev)
		if next == nil {
			next = it.list.tail
		}
		// build spl
		spl[level].init(prev, next)
		if level == 0 {
			break
		}
		level--
	}
	return
}

func (it *Iterator) seekForBaseSplice(key []byte) (prev, next *node, found bool) {
	level := int(it.list.Height() - 1)

	prev = it.list.head
	//从最高层的头节点开始找key
	for {
		prev, next, found = it.list.findSpliceForLevel(key, level, prev)
		if found {
			break
		}
		if level == 0 {
			break
		}
		level--
	}
	return
}

func (it *Iterator) setValueIfDeleted(nd *node, val []byte) error {
	var newVal uint64
	var err error

	for {
		old := atomic.LoadUint64(&nd.value)
		if old != deletedVal {
			it.value = old
			it.nd = nd
			return ErrRecordExists
		}
		if newVal == 0 {
			newVal, err = it.list.allocVal(val)
			if err != nil {
				return err
			}
		}
		if atomic.CompareAndSwapUint64(&nd.value, old, newVal) {
			break
		}
	}

	it.value = newVal
	it.nd = nd
	return err
}

// put update

func (it *Iterator) PutOrUpdate(key, mv []byte) (err error) {
	if it.Seek(key) {
		return it.Set(mv)
	}
	return it.Put(key, mv)
}

func (it *Iterator) Seek(key []byte) (found bool) {
	var next *node
	_, next, found = it.seekForBaseSplice(key)
	present := it.setNode(next, false)
	return found && present
}

// Set set value 并不会更改之前的value而是为这个key添加一个新的value offset
func (it *Iterator) Set(val []byte) error {
	newVal, err := it.list.allocVal(val)
	if err != nil {
		return err
	}
	it.value = append(it.value, newVal)
	return nil
}

func (it *Iterator) Put(key []byte, val []byte) error {
	var spl [maxHeight]splice

	//寻找到要插入的位置
	it.seekForSplice(key, &spl)

	if it.list.testing {
		//这段代码是为了更好地测试并发性能
		//例如线程1执行到这段代码时，会调用runtime.Gosched()函数，让出执行权给线程2。线程2在这段时间内可能会修改splice的内容。然后，线程1重新获得执行权，继续执行后续的代码。
		//通过添加延迟，可以增加线程2修改splice的机会，从而更好地模拟并发环境下的竞争条件。
		runtime.Gosched()
	}

	nd, height, err := it.list.newNode(key, val)
	if err != nil {
		return err
	}

	value := nd.value
	ndOffset := it.arena.GetPointerOffset(unsafe.Pointer(nd))

	var found bool
	for i := 0; i < int(height); i++ {
		prev := spl[i].prev
		next := spl[i].next

		//若在最高层是有可能发现prev是空的，该height是新建的一层
		if prev == nil {
			// New node increased the height of the skiplist, so assume that the
			// new level has not yet been populated.
			if next != nil {
				panic("next is expected to be nil, since prev is nil")
			}

			prev = it.list.head
			next = it.list.tail
		}

		// +----------------+     +------------+     +----------------+
		// |      prev      |     |     nd     |     |      next      |
		// | prevNextOffset |---->|            |     |                |
		// |                |<----| prevOffset |     |                |
		// |                |     | nextOffset |---->|                |
		// |                |     |            |<----| nextPrevOffset |
		// +----------------+     +------------+     +----------------+
		//
		// 1. Initialize prevOffset and nextOffset to point to prev and next.
		// 2. CAS prevNextOffset to repoint from next to nd.
		// 3. CAS nextPrevOffset to repoint from prev to nd.
		for {
			prevOffset := it.arena.GetPointerOffset(unsafe.Pointer(prev))
			nextOffset := it.arena.GetPointerOffset(unsafe.Pointer(next))
			nd.tower[i].init(prevOffset, nextOffset)

			// Check whether next has an updated link to prev. If it does not,
			// that can mean one of two things:
			//   1. The thread that added the next node hasn't yet had a chance
			//      to add the prev link (but will shortly).
			//   2. Another thread has added a new node between prev and next.
			nextPrevOffset := next.prevOffset(i)
			if nextPrevOffset != prevOffset {
				// Determine whether #1 or #2 is true by checking whether prev
				// is still pointing to next. As long as the atomic operations
				// have at least acquire/release semantics (no need for
				// sequential consistency), this works, as it is equivalent to
				// the "publication safety" pattern.
				prevNextOffset := prev.nextOffset(i)
				if prevNextOffset == nextOffset {
					// Ok, case #1 is true, so help the other thread along by
					// updating the next node's prev link.
					next.casPrevOffset(i, nextPrevOffset, prevOffset)
				}
			}

			if prev.casNextOffset(i, nextOffset, ndOffset) {
				// Managed to insert nd between prev and next, so update the next
				// node's prev link and go to the next level.
				if it.list.testing {
					// Add delay to make it easier to test race between this thread
					// and another thread that sees the intermediate state between
					// setting next and setting prev.
					runtime.Gosched()
				}

				next.casPrevOffset(i, prevOffset, ndOffset)
				break
			}

			// CAS failed. We need to recompute prev and next. It is unlikely to
			// be helpful to try to use a different level as we redo the search,
			// because it is unlikely that lots of nodes are inserted between prev
			// and next.
			prev, next, found = it.list.findSpliceForLevel(key, i, prev)
			if found {
				if i != 0 {
					panic("how can another thread have inserted a node at a non-base level?")
				}

				return it.setValueIfDeleted(next, val)
			}
		}
	}

	it.value = value
	it.nd = nd
	return nil
}

// get

func (it *Iterator) Key() []byte {
	return it.nd.getKey(it.arena)
}

func (it *Iterator) Value() (values [][]byte) {
	for _, val := range it.value {
		valOffset, valSize := decodeValue(val)
		v := it.arena.GetBytes(valOffset, valSize)
		values = append(values, v)
	}
	return
}
