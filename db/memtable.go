package db

import (
	"bytes"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/arenaskl"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"sync"
	"time"
)

type MemOpt struct {
	memSize     int
	concurrency int
}

type Memtable struct {
	maxKey  []byte
	minKey  []byte
	CreatAt uint64
	sync.RWMutex
	sklIter  *arenaskl.Iterator
	skl      *arenaskl.Skiplist
	cfg      config.MemConfig
	maxIndex uint64
	minIndex uint64
}

func NewMemtable(cfg config.MemConfig) (*Memtable, error) {
	var sklIter = new(arenaskl.Iterator)
	arena := arenaskl.NewArena(uint32(cfg.MemtableSize) + uint32(arenaskl.MaxNodeSize))
	skl := arenaskl.NewSkiplist(arena)
	sklIter.Init(skl)
	table := &Memtable{cfg: cfg, skl: skl, sklIter: sklIter}
	return table, nil
}

func (mt *Memtable) put(k, v []byte) error {
	return mt.sklIter.Put(k, v)
}

func (mt *Memtable) Scan(low, high []byte) (err error, kvs []*marshal.KV) {
	// todo
	// 1、找到距离low最近的一个key
	// 2、获取该key的value
	// 3、next移动到下一个key判断，是否小于high
	// 4、读出value
	// 5、返回
	if found := mt.sklIter.Seek(low); !found {
		return code.ErrRecordExists, nil
	}

	for mt.sklIter.Valid() && bytes.Compare(mt.sklIter.Key(), high) != -1 {
		key, value := mt.sklIter.Key(), mt.sklIter.Value()
		mv := marshal.DecodeV(value)
		if mv.Type == marshal.TypeDelete {
			continue
		}
		if mv.ExpiredAt > 0 && mv.ExpiredAt <= time.Now().Unix() {
			continue
		}
		kvs = append(kvs, &marshal.KV{
			Key:   key,
			Value: mv,
		})
		mt.sklIter.Next()
	}
	return
}

func (mt *Memtable) Get(key []byte) (bool, []byte) {
	if found := mt.sklIter.Seek(key); !found {
		return false, nil
	}

	value, err := mt.sklIter.Get(key)
	if err == code.ErrRecordNotExists {
		return false, nil
	}
	mv := marshal.DecodeV(value)

	if mv.Type == marshal.TypeDelete {
		return true, nil
	}

	if mv.ExpiredAt > 0 && mv.ExpiredAt <= time.Now().Unix() {
		return true, nil
	}

	return false, mv.Value
}

func (mt *Memtable) All() []marshal.KV {
	sklIter := mt.sklIter
	var kvRecords []*marshal.KV

	for sklIter.SeekToFirst(); sklIter.Valid(); sklIter.Next() {
		key, valueStruct := sklIter.Key(), sklIter.Value()
		v := marshal.DecodeV(valueStruct)
		kvRecords = append(kvRecords, &marshal.KV{Key: key, Value: v})
	}
	return nil
}

func (mt *Memtable) Size() int {
	return int(mt.skl.Size())
}

type MemtableQueue struct {
	tables   []*Memtable
	size     int
	capacity int
}

func NewMemtableQueue(capacity int) *MemtableQueue {
	return &MemtableQueue{
		tables:   make([]*Memtable, capacity),
		size:     0,
		capacity: capacity,
	}
}

func (q *MemtableQueue) Enqueue(item *Memtable) {
	if q.size == q.capacity {
		// Resize the queue if it's full
		newCapacity := q.capacity * 2
		newtables := make([]*Memtable, newCapacity)
		copy(newtables, q.tables)
		q.tables = newtables
		q.capacity = newCapacity
	}
	q.tables[q.size] = item
	q.size++
}

func (q *MemtableQueue) Dequeue() *Memtable {
	if q.size == 0 {
		panic("Queue is empty")
	}
	item := q.tables[0]
	copy(q.tables, q.tables[1:])
	q.size--
	return item
}
