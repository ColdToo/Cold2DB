package partition

import (
	"github.com/ColdToo/Cold2DB/db/marshal"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestBtreeIndexer_Get(t *testing.T) {
	getwd, _ := os.Getwd()
	indexer, err := NewIndexer(getwd, time.Now().Format("2006-01-02T15:04:05")+indexFileSuffixName)
	if err != nil {
		t.Fatal(err)
	}
	defer indexer.Close()

	tx, err := indexer.StartTx()

	key := []byte("testKey")
	value := []byte("testValue")
	ops := make([]*Op, 0) //(Insert, &marshal.BytesKV{Key: key, Value: value})
	ops = append(ops, &Op{Insert, &marshal.BytesKV{Key: key, Value: value}})

	err = indexer.Execute(tx, ops)
	if err != nil {
		t.Fatal(err)
	}
	tx.Commit()

	meta, err := indexer.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(meta, &marshal.BytesKV{Key: key, Value: value}) {
		t.Errorf("Get() returned unexpected value, expected: %v, got: %v", &marshal.BytesKV{Key: key, Value: value}, meta)
	}
}

func TestBtreeIndexer_Scan(t *testing.T) {
	getwd, _ := os.Getwd()
	indexer, err := NewIndexer(getwd, time.Now().Format("2006-01-02T15:04:05")+indexFileSuffixName)
	if err != nil {
		t.Fatal(err)
	}
	defer indexer.Close()

	tx, err := indexer.StartTx()
	low := []byte("a")
	high := []byte("z")
	ops := make([]*Op, 0)
	ops = append(ops, &Op{Insert, &marshal.BytesKV{Key: []byte("b"), Value: []byte("value1")}})
	ops = append(ops, &Op{Insert, &marshal.BytesKV{Key: []byte("c"), Value: []byte("value2")}})
	ops = append(ops, &Op{Insert, &marshal.BytesKV{Key: []byte("d"), Value: []byte("value3")}})
	err = indexer.Execute(tx, ops)
	if err != nil {
		t.Fatal(err)
	}
	tx.Commit()

	metaList, err := indexer.Scan(low, high)
	if err != nil {
		t.Fatal(err)
	}
	expectedMetaList := []*marshal.BytesKV{
		{Key: []byte("b"), Value: []byte("value1")},
		{Key: []byte("c"), Value: []byte("value2")},
		{Key: []byte("d"), Value: []byte("value3")},
	}
	if !reflect.DeepEqual(metaList, expectedMetaList) {
		t.Errorf("Scan() returned unexpected value, expected: %v, got: %v", expectedMetaList, metaList)
	}
}

func TestBtreeIndexer_Delete(t *testing.T) {
	getwd, _ := os.Getwd()
	indexer, err := NewIndexer(getwd, time.Now().Format("2006-01-02T15:04:05")+indexFileSuffixName)
	if err != nil {
		t.Fatal(err)
	}
	defer indexer.Close()

	tx, err := indexer.StartTx()
	key := []byte("testKey")
	value := []byte("testValue")
	ops := make([]*Op, 0)
	ops = append(ops, &Op{Insert, &marshal.BytesKV{Key: key, Value: value}})
	err = indexer.Execute(tx, ops)
	if err != nil {
		t.Fatal(err)
	}
	tx.Commit()

	tx, err = indexer.StartTx()
	ops = make([]*Op, 0)
	ops = append(ops, &Op{Delete, &marshal.BytesKV{Key: key, Value: value}})
	err = indexer.Execute(tx, ops)
	if err != nil {
		t.Fatal(err)
	}
	tx.Commit()

	meta, err := indexer.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if meta.Value != nil {
		t.Error("value should nil")
	}
}
