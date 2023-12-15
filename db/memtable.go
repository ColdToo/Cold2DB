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

type MemTable struct {
	sklIter  *arenaskl.Iterator
	cfg      config.MemConfig
	maxKey   []byte
	minKey   []byte
	maxIndex uint64
	minIndex uint64
}

func NewMemTable(cfg config.MemConfig) (*MemTable, error) {
	var sklIter = new(arenaskl.Iterator)
	arena := arenaskl.NewArena(uint32(cfg.MemTableSize*MB) + uint32(arenaskl.MaxNodeSize))
	skl := arenaskl.NewSkiplist(arena)
	sklIter.Init(skl)
	table := &MemTable{cfg: cfg, sklIter: sklIter}
	return table, nil
}

func (mt *MemTable) put(k, v []byte) error {
	return mt.sklIter.Put(k, v)
}

func (mt *MemTable) get(key []byte) (bool, []byte) {
	if found := mt.sklIter.Seek(key); !found {
		return false, nil
	}
	value, _ := mt.sklIter.Get(key)
	return true, value
}

func (mt *MemTable) Scan(low, high []byte) (kvs []*marshal.BytesKV, err error) {
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

func (mt *MemTable) All() (kvs []*marshal.BytesKV) {
	for mt.sklIter.SeekToFirst(); mt.sklIter.Valid(); mt.sklIter.Next() {
		key, value := mt.sklIter.Key(), mt.sklIter.Value()
		kvs = append(kvs, &marshal.BytesKV{Key: key, Value: value})
	}
	return
}

func (mt *MemTable) Size() int {
	return mt.sklIter.Size()
}

type MemTableQueue struct {
	tables   []*MemTable
	size     int
	capacity int
}

func NewMemTableQueue(capacity int) *MemTableQueue {
	return &MemTableQueue{
		tables:   make([]*MemTable, capacity),
		size:     0,
		capacity: capacity,
	}
}

func (q *MemTableQueue) Enqueue(item *MemTable) {
	if q.size == q.capacity {
		//todo 缓冲，此时应该不再接受写入，需要将immtable刷盘，等待memTable的数量恢复到和配置一样才能允许写入
		newCapacity := q.capacity * 2
		newtables := make([]*MemTable, newCapacity)
		copy(newtables, q.tables)
		q.tables = newtables
		q.capacity = newCapacity
	}
	q.tables[q.size] = item
	q.size++
}

func (q *MemTableQueue) Dequeue() *MemTable {
	if q.size == 0 {
		panic("Queue is empty")
	}
	item := q.tables[0]
	copy(q.tables, q.tables[1:])
	q.size--
	return item
}
