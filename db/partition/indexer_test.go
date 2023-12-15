package partition

import (
	"github.com/ColdToo/Cold2DB/db/marshal"
	"os"
	"reflect"
	"testing"
)

func TestBtreeIndexer(t *testing.T) {
	getwd, _ := os.Getwd()
	indexer, err := NewIndexer(getwd)
	if err != nil {
		t.Fatal(err)
	}
	defer indexer.Close()

	tx, err := indexer.StartTx()

	key := []byte("testKey")
	value := []byte("testValue")
	err = indexer.Insert(tx, []*marshal.BytesKV{{Key: key, Value: value}})
	if err != nil {
		t.Fatal(err)
	}

	low := []byte("a")
	high := []byte("z")
	kvs := []*marshal.BytesKV{
		{Key: []byte("b"), Value: []byte("value1")},
		{Key: []byte("c"), Value: []byte("value2")},
		{Key: []byte("d"), Value: []byte("value3")},
	}
	err = indexer.Insert(tx, kvs)
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

	meta, err := indexer.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(meta, &marshal.BytesKV{Key: key, Value: value}) {
		t.Errorf("Get() returned unexpected value, expected: %v, got: %v", &marshal.BytesKV{Key: key, Value: value}, meta)
	}

	if !reflect.DeepEqual(metaList, expectedMetaList) {
		t.Errorf("Scan() returned unexpected value, expected: %v, got: %v", expectedMetaList, metaList)
	}
}
