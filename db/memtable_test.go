package db

import (
	"github.com/ColdToo/Cold2DB/db/Mock"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"

	"github.com/ColdToo/Cold2DB/config"
)

var TestMemConfig = config.MemConfig{
	MemTableSize: 64,
	MemTableNums: 5,
	Concurrency:  3,
}

func TestMemTable_Get(t *testing.T) {
	mem, err := NewMemTable(TestMemConfig)
	if err != nil {
		t.Log(err)
	}

	kvs := Mock.OneKV
	for _, kv := range kvs {
		sklIter := mem.newSklIter()
		sklIter.Put(kv.Key, marshal.EncodeData(kv.Data))
	}

	kv, _ := mem.Get(kvs[0].Key)

	reflect.DeepEqual(kvs, kv)
}

func TestMemTable_Scan(t *testing.T) {
	//获取验证集
	kvs := Mock.KVS_RAND_35MB_HASDEL_UQKey
	min := 0
	max := len(kvs) - 1
	verifyKvs := make([]*marshal.KV, 0)
	lowIndex := Mock.CreateRandomIndex(min, max)
	lowKey := kvs[lowIndex].Key
	highKey := kvs[max].Key
	for lowIndex <= max {
		kv := kvs[lowIndex]
		verifyKvs = append(verifyKvs, kv)
		lowIndex++
	}

	mem, err := NewMemTable(TestMemConfig)
	if err != nil {
		t.Log(err)
	}
	//获取测试集
	sklIter := mem.newSklIter()
	for _, kv := range kvs {
		sklIter.Put(kv.Key, marshal.EncodeData(kv.Data))
	}
	allKvs, _ := mem.Scan(lowKey, highKey)
	reKvs := make([]*marshal.KV, 0)
	for _, kv := range allKvs {
		reKvs = append(reKvs, &marshal.KV{Key: kv.Key, KeySize: len(kv.Key), Data: marshal.DecodeData(kv.Value)})
	}

	reflect.DeepEqual(kvs, allKvs)
}

func TestMemTable_All(t *testing.T) {
	kvs := Mock.KVS_RAND_35MB_HASDEL_UQKey
	bytesKvs := make([]*marshal.BytesKV, 0)
	for _, kv := range kvs {
		bytesKvs = append(bytesKvs, &marshal.BytesKV{Key: kv.Key, Value: marshal.EncodeData(kv.Data)})
	}

	mem, err := NewMemTable(TestMemConfig)
	if err != nil {
		t.Log(err)
	}
	mem.ConcurrentPut(bytesKvs)

	allKvs := mem.All()
	verifyKvs := make([]marshal.KV, 0)
	for _, kv := range allKvs {
		verifyKvs = append(verifyKvs, marshal.KV{Key: kv.Key, KeySize: len(kv.Key), Data: marshal.DecodeData(kv.Value)})
	}

	reflect.DeepEqual(kvs, allKvs)
}

func TestMemTable_All1(t *testing.T) {
	mem, err := NewMemTable(TestMemConfig)
	if err != nil {
		t.Log(err)
	}
	it := mem.newSklIter()

	kvs := Mock.KVS_RAND_35MB_HASDEL_UQKey
	bytesKvs := make([]*marshal.BytesKV, 0)
	for _, kv := range kvs {
		bytesKvs = append(bytesKvs, &marshal.BytesKV{Key: kv.Key, Value: marshal.EncodeData(kv.Data)})
	}

	for _, kv := range bytesKvs {
		err := it.Put(kv.Key, kv.Value)
		if err != nil {
			t.Log(err)
		}
	}

	for _, kv := range kvs {
		if it.Seek(kv.Key) {
			data := marshal.DecodeData(it.Value())
			require.EqualValues(t, kv.Data, data)
		}
	}
}

func TestMemTable_Queue(t *testing.T) {
	queue := NewMemTableQueue(3)
	table1 := &MemTable{}
	table2 := &MemTable{}
	table3 := &MemTable{}

	queue.Enqueue(table1)
	queue.Enqueue(table2)
	queue.Enqueue(table3)

	if queue.size != 3 {
		t.Errorf("Expected queue size to be 3, but got %d", queue.size)
	}

	dequeuedTable := queue.Dequeue()
	if dequeuedTable != table1 {
		t.Error("Dequeued table does not match expected table")
	}

	if queue.size != 2 {
		t.Errorf("Expected queue size to be 2 after dequeue, but got %d", queue.size)
	}
}
