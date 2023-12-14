package db

import (
	"bytes"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/arenaskl"
	"github.com/ColdToo/Cold2DB/db/marshal"
)

const MB = 1024 * 1024

type MemOpt struct {
	memSize     int
	concurrency int
}

type Memtable struct {
	sklIter  *arenaskl.Iterator
	cfg      config.MemConfig
	maxKey   []byte
	minKey   []byte
	maxIndex uint64
	minIndex uint64
}

func NewMemtable(cfg config.MemConfig) (*Memtable, error) {
	var sklIter = new(arenaskl.Iterator)
	arena := arenaskl.NewArena(uint32(cfg.MemtableSize*MB) + uint32(arenaskl.MaxNodeSize))
	skl := arenaskl.NewSkiplist(arena)
	sklIter.Init(skl)
	table := &Memtable{cfg: cfg, sklIter: sklIter}
	return table, nil
}

func (mt *Memtable) put(k, v []byte) error {
	return mt.sklIter.Put(k, v)
}

func (mt *Memtable) get(key []byte) (bool, []byte) {
	if found := mt.sklIter.Seek(key); !found {
		return false, nil
	}
	value, _ := mt.sklIter.Get(key)
	return true, value
}

func (mt *Memtable) Scan(low, high []byte) (kvs []*marshal.BytesKV, err error) {
	// todo
	// 1、找到距离low最近的一个key
	// 2、获取该key的value
	// 3、next移动到下一个key判断，是否小于high
	// 4、读出value
	// 5、返回
	if found := mt.sklIter.Seek(low); !found {
		return nil, code.ErrRecordExists
	}

	for mt.sklIter.Valid() && bytes.Compare(mt.sklIter.Key(), high) != -1 {
		key, value := mt.sklIter.Key(), mt.sklIter.Value()
		kvs = append(kvs, &marshal.BytesKV{
			Key:   key,
			Value: value,
		})
		mt.sklIter.Next()
	}
	return
}

func (mt *Memtable) All() (kvs []*marshal.BytesKV) {
	for mt.sklIter.SeekToFirst(); mt.sklIter.Valid(); mt.sklIter.Next() {
		key, value := mt.sklIter.Key(), mt.sklIter.Value()
		kvs = append(kvs, &marshal.BytesKV{Key: key, Value: value})
	}
	return
}

func (mt *Memtable) Size() int {
	return mt.sklIter.Size()
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
		//todo 缓冲，此时应该不再接受写入，需要将immtable刷盘，等待memtable的数量恢复到和配置一样才能允许写入
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
