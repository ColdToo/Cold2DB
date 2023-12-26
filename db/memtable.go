package db

import (
	"bytes"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/arenaskl"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"sync"
)

const MB = 1024 * 1024

type MemOpt struct {
	memSize     int
	concurrency int
}

type MemTable struct {
	skl      *arenaskl.Skiplist
	cfg      config.MemConfig
	maxKey   []byte
	minKey   []byte
	maxIndex uint64
	minIndex uint64
}

func NewMemTable(cfg config.MemConfig) *MemTable {
	cfg.MemTableSize = cfg.MemTableSize * MB
	arena := arenaskl.NewArena(uint32(cfg.MemTableSize) + uint32(arenaskl.MaxNodeSize))
	skl := arenaskl.NewSkiplist(arena)
	table := &MemTable{cfg: cfg, skl: skl}
	return table
}

func (mt *MemTable) newSklIter() *arenaskl.Iterator {
	sklIter := new(arenaskl.Iterator)
	sklIter.Init(mt.skl)
	return sklIter
}

func (mt *MemTable) ConcurrentPut(kvBytes []*marshal.BytesKV) error {
	parts := make([][]*marshal.BytesKV, mt.cfg.Concurrency)
	//todo 优化:避免使用append追加
	//subPartSize := len(kvBytes) / mt.cfg.Concurrency
	//subpart := make([]*marshal.BytesKV, subPartSize)
	for i, kv := range kvBytes {
		part := i % mt.cfg.Concurrency
		parts[part] = append(parts[part], kv)
	}

	errC := make(chan error)
	wg := &sync.WaitGroup{}
	for _, part := range parts {
		wg.Add(1)
		go func(kvs []*marshal.BytesKV) {
			for _, kv := range kvs {
				sklIter := mt.newSklIter()
				err := sklIter.Put(kv.Key, kv.Value)
				if err != nil {
					errC <- err
					return
				}
			}
			wg.Done()
		}(part)
	}
	wg.Wait()

	return nil
}

func (mt *MemTable) Get(key []byte) (*marshal.KV, bool) {
	sklIter := mt.newSklIter()
	if found := sklIter.Seek(key); !found {
		return nil, false
	}
	value, _ := sklIter.Get(key)
	return &marshal.KV{Key: key, Data: marshal.DecodeData(value)}, true
}

func (mt *MemTable) Scan(low, high []byte) (kvs []*marshal.KV, err error) {
	sklIter := mt.newSklIter()
	if found := sklIter.Seek(low); !found {
		return nil, code.ErrRecordExists
	}

	for sklIter.Valid() && bytes.Compare(sklIter.Key(), high) != -1 {
		key, value := sklIter.Key(), sklIter.Value()
		kvs = append(kvs, &marshal.KV{Key: key, KeySize: len(key), Data: marshal.DecodeData(value)})
		sklIter.Next()
	}
	return
}

func (mt *MemTable) All() (kvs []*marshal.BytesKV) {
	sklIter := mt.newSklIter()
	sklIter.SeekToFirst()
	for sklIter.Valid() {
		key, value := sklIter.Key(), sklIter.Value()
		kvs = append(kvs, &marshal.BytesKV{
			Key:   key,
			Value: value,
		})
		sklIter.Next()
	}
	return
}

func (mt *MemTable) Size() int64 {
	return int64(mt.skl.Size())
}

type MemTableQueue struct {
	tables   []*MemTable
	size     int
	capacity int
}

func NewMemTableQueue(capacity int) *MemTableQueue {
	return &MemTableQueue{
		tables:   make([]*MemTable, 0),
		size:     0,
		capacity: capacity,
	}
}

func (q *MemTableQueue) Enqueue(item *MemTable) {
	if q.size == q.capacity {
		//todo 缓冲，memtable队列短暂扩容后此时应该不再接受写入，需要将immtable刷盘，等待memTable的数量恢复到和配置一样才能允许写入
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
