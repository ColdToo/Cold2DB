package db

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/ColdToo/Cold2DB/config"
	"github.com/stretchr/testify/assert"
)

var TestMemConfig = config.MemConfig{
	MemTableSize: 64,
	MemTableNums: 10,
	Concurrency:  5,
}

type KVmock struct {
	k []byte
	v []byte
}

func MockKV(size int) (kvList []KVmock) {
	for i := 0; i < size; i++ {
		k := []byte(strconv.Itoa(i))
		v := []byte(strconv.Itoa(i))
		kv := KVmock{k, v}
		kvList = append(kvList, kv)
	}
	return
}

func TestMemTable_WriteRead(t *testing.T) {
	mem, err := NewMemTable(TestMemConfig)
	if err != nil {
		t.Log(err)
	}

	kvs := MockKV(10000)
	verifyKVs := make([]KVmock, 0)
	for _, kv := range kvs {
		mem.put(kv.k, kv.v)
	}

	for _, kv := range kvs {
		if flag, v := mem.get(kv.k); flag {
			verifyKVs = append(verifyKVs, KVmock{k: kv.k, v: v})
		} else {
			t.Error("can not fund k")
		}
	}

	assert.EqualValues(t, kvs, verifyKVs)
}

func TestMemTable_Scan(t *testing.T) {
	mem, err := NewMemTable(TestMemConfig)
	if err != nil {
		t.Log(err)
	}

	//todo
	//1、low < min key  &&   high > max key
	//2、low > min key  &&   high < max key
	kvs := MockKV(10000)
	for _, kv := range kvs {
		mem.put(kv.k, kv.v)
	}

	low := []byte(strconv.Itoa(-1))
	high := []byte(strconv.Itoa(20000))

	scanKvs, err := mem.Scan(low, high)
	if err != nil {
		return
	}

	reflect.DeepEqual(kvs, scanKvs)

}

func TestMemTable_All(t *testing.T) {
	mem, err := NewMemTable(TestMemConfig)
	if err != nil {
		t.Log(err)
	}

	kvs := MockKV(10000)
	for _, kv := range kvs {
		mem.put(kv.k, kv.v)
	}
	allKvs := mem.All()

	reflect.DeepEqual(kvs, allKvs)
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
