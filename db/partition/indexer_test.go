package partition

import (
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestBtreeIndexer(t *testing.T) {
	partitionDir, _ := os.Getwd()
	indexer, err := NewIndexer(partitionDir)
	if err != nil {
		t.Errorf("Error creating indexer: %v", err)
	}
	defer func() {
		if err := indexer.Close(); err != nil {
			t.Errorf("Error closing indexer: %v", err)
		}
	}()

	// Test Put
	metas := []*marshal.BytesKV{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
	}
	err = indexer.Put(metas)
	assert.NoError(t, err, "Put should not return an error")

	// Test Get
	meta, err := indexer.Get([]byte("key1"))
	assert.NoError(t, err, "Get should not return an error")
	assert.Equal(t, []byte("value1"), meta.Value, "Retrieved value should match")

	// Test Scan
	scanResult, err := indexer.Scan([]byte("key1"), []byte("key2"))
	assert.NoError(t, err, "Scan should not return an error")
	assert.Len(t, scanResult, 2, "Scan result length should be 2")

	// Test Delete
	err = indexer.Delete([]byte("key1"))
	assert.NoError(t, err, "Delete should not return an error")

	// Test Sync
	err = indexer.Sync()
	assert.NoError(t, err, "Sync should not return an error")
}
