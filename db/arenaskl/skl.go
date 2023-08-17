package arenaskl

import (
	"bytes"
	"errors"
	"github.com/ColdToo/Cold2DB/utils"
	"math"
	"sync/atomic"
	"unsafe"
)

// Fastrand returns a lock free uint32 value.
//go:linkname Fastrand runtime.fastrand
import _ "unsafe" // required by go:linkname

const (
	maxHeight  = 20
	pValue     = 1 / math.E
	linksSize  = int(unsafe.Sizeof(links{}))
	deletedVal = 0
	// MaxNodeSize self-explanatory.
	MaxNodeSize = int(unsafe.Sizeof(node{}))
)

var (
	// ErrRecordExists record with this key already exists.
	ErrRecordExists = errors.New("record with this key already exists")

	// ErrRecordUpdated record was updated by another caller.
	ErrRecordUpdated = errors.New("record was updated by another caller")

	// ErrRecordDeleted record was deleted by another caller.
	ErrRecordDeleted = errors.New("record was deleted by another caller")
)

var (
	probabilities [maxHeight]uint32
)

func init() {
	// Precompute the skiplist probabilities so that only a single random number
	// needs to be generated and so that the optimal pvalue can be used (inverse
	// of Euler's number).
	p := 1.0
	for i := 0; i < maxHeight; i++ {
		probabilities[i] = uint32(float64(math.MaxUint32) * p)
		p *= pValue
	}
}

type Skiplist struct {
	arena  *Arena
	head   *node
	tail   *node
	height uint32 // Current height. 1 <= height <= maxHeight. CAS.
	// If set to true by tests, it easier to detect unusual race conditions.
	testing  bool
	IndexMap map[uint64]uint64 // store node cursor
}

func NewSkiplist(arena *Arena) *Skiplist {
	head, err := newNode(arena, maxHeight)
	if err != nil {
		panic("arenaSize is not large enough to hold the head node")
	}

	tail, err := newNode(arena, maxHeight)
	if err != nil {
		panic("arenaSize is not large enough to hold the tail node")
	}

	headOffset := arena.GetPointerOffset(unsafe.Pointer(head))
	tailOffset := arena.GetPointerOffset(unsafe.Pointer(tail))
	for i := 0; i < maxHeight; i++ {
		head.tower[i].nextOffset = tailOffset
		tail.tower[i].prevOffset = headOffset
	}

	skl := &Skiplist{
		arena:    arena,
		head:     head,
		tail:     tail,
		height:   1,
		IndexMap: make(map[uint64]uint64),
	}

	return skl
}

func (s *Skiplist) Height() uint32 { return atomic.LoadUint32(&s.height) }

func (s *Skiplist) Arena() *Arena { return s.arena }

func (s *Skiplist) Size() uint32 { return s.arena.Size() }

// 初始化一个新的key
func (s *Skiplist) newNode(key, val []byte, index uint64) (nd *node, height uint32, err error) {
	height = s.randomHeight()
	nd, err = newNode(s.arena, height)
	if err != nil {
		return
	}

	// Try to increase s.height via CAS.
	listHeight := s.Height()
	for height > listHeight {
		if atomic.CompareAndSwapUint32(&s.height, listHeight, height) {
			// Successfully increased skiplist.height.
			break
		}
		listHeight = s.Height()
	}

	nd.keyOffset, nd.keySize, err = s.allocKey(key)
	if err != nil {
		return
	}

	value, err := s.allocVal(val)
	nd.value = append(nd.value, value)
	s.IndexMap[index] = value
	return
}

func (s *Skiplist) storeValueCursor(key, val []byte) (nd *node, height uint32, err error) {
	return
}

func (s *Skiplist) randomHeight() uint32 {
	rnd := utils.Fastrand()
	h := uint32(1)
	for h < maxHeight && rnd <= probabilities[h] {
		h++
	}

	return h
}

// 在arena中为key分配一段内存,并将返回key在arena中的offset
func (s *Skiplist) allocKey(key []byte) (keyOffset uint32, keySize uint32, err error) {
	keySize = uint32(len(key))
	if keySize > math.MaxUint32 {
		panic("key is too large")
	}

	keyOffset, err = s.arena.Alloc(keySize, 0 /* overflow */, Align1)
	if err == nil {
		copy(s.arena.GetBytes(keyOffset, keySize), key)
	}
	return
}

// 在arena中为val分配一段内存,并将返回val在arena中的offset
func (s *Skiplist) allocVal(val []byte) (uint64, error) {
	if len(val) > math.MaxUint32 {
		panic("value is too large")
	}

	valSize := uint32(len(val))
	valOffset, err := s.arena.Alloc(valSize, 0 /* overflow */, Align1)
	if err != nil {
		return 0, err
	}

	copy(s.arena.GetBytes(valOffset, valSize), val)
	return encodeValue(valOffset, valSize), nil
}

func (s *Skiplist) findSpliceForLevel(key []byte, level int, start *node) (prev, next *node, found bool) {
	prev = start

	for {
		next = s.getNext(prev, level)
		nextKey := next.getKey(s.arena)
		if nextKey == nil {
			// Tail node key, so done.
			break
		}

		cmp := bytes.Compare(key, nextKey)
		if cmp == 0 {
			// Equality case.
			found = true
			break
		}

		if cmp < 0 {
			// We are done for this level, since prev.key < key < next.key.
			break
		}
		// Keep moving right on this level.
		prev = next
	}
	return
}

func (s *Skiplist) getNext(nd *node, h int) *node {
	offset := atomic.LoadUint32(&nd.tower[h].nextOffset)
	return (*node)(s.arena.GetPointer(offset))
}

func (s *Skiplist) getPrev(nd *node, h int) *node {
	offset := atomic.LoadUint32(&nd.tower[h].prevOffset)
	return (*node)(s.arena.GetPointer(offset))
}

func encodeValue(valOffset, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}

type links struct {
	nextOffset uint32
	prevOffset uint32
}

func (l *links) init(prevOffset, nextOffset uint32) {
	l.nextOffset = nextOffset
	l.prevOffset = prevOffset
}

type node struct {
	// Immutable fields, so no need to lock to access key.
	keyOffset uint32
	keySize   uint32

	// Multiple parts of the value are encoded as a single uint64 so that it
	// can be atomically loaded and stored:
	//   value offset: uint32 (bits 0-31)
	//   value size  : uint16 (bits 32-63)
	//   一个key可以对应多个 index、term的值
	value []uint64

	// Most nodes do not need to use the full height of the tower, since the
	// probability of each successive level decreases exponentially. Because
	// these elements are never accessed, they do not need to be allocated.
	// Therefore, when a node is allocated in the arena, its memory footprint
	// is deliberately truncated to not include unneeded tower elements.
	//
	// All accesses to elements should use CAS operations, with no need to lock.
	tower [maxHeight]links
}

func newNode(arena *Arena, height uint32) (nd *node, err error) {
	if height < 1 || height > maxHeight {
		panic("height cannot be less than one or greater than the max height")
	}

	// Compute the amount of the tower that will never be used, since the height
	// is less than maxHeight.
	unusedSize := (maxHeight - int(height)) * linksSize

	nodeOffset, err := arena.Alloc(uint32(MaxNodeSize-unusedSize), uint32(unusedSize), Align8)
	if err != nil {
		return
	}

	nd = (*node)(arena.GetPointer(nodeOffset))
	return
}

func (n *node) getKey(arena *Arena) []byte {
	return arena.GetBytes(n.keyOffset, n.keySize)
}

func (n *node) nextOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h].nextOffset)
}

func (n *node) prevOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h].prevOffset)
}

func (n *node) casNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h].nextOffset, old, val)
}

func (n *node) casPrevOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h].prevOffset, old, val)
}
