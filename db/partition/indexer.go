package partition

import (
	"bytes"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"go.etcd.io/bbolt"
	"path/filepath"
	"time"
)

// bucket name for bolt db to store index data
var indexBucketName = []byte("index")

const (
	indexFileSuffixName = ".INDEX"
)

type Indexer interface {
	Put(meta []*marshal.BytesKV) (err error)

	Get(key []byte) (meta *marshal.BytesKV, err error)

	Scan(low, high []byte) (meta []*marshal.BytesKV, err error)

	Delete(key []byte) error

	DeleteBatch(keys [][]byte) error

	Sync() error

	Close() (err error)
}

func NewIndexer(partitionDir string) (Indexer, error) {
	indexer, err := bbolt.Open(filepath.Join(partitionDir, time.Now().Format("2006-01-02T15:04:05")+indexFileSuffixName), 0600,
		&bbolt.Options{
			NoSync:          true,
			InitialMmapSize: 1024,
			FreelistType:    bbolt.FreelistMapType,
		},
	)
	if err != nil {
		return nil, err
	}

	// begin a writable transaction to create the bucket if not exists
	tx, err := indexer.Begin(true)
	if err != nil {
		return nil, err
	}
	if _, err := tx.CreateBucketIfNotExists(indexBucketName); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &BtreeIndexer{
		index: indexer,
	}, nil
}

type BtreeIndexer struct {
	index *bbolt.DB
}

func (b *BtreeIndexer) Put(metas []*marshal.BytesKV) (err error) {
	return b.index.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		for _, meta := range metas {
			bucket.Put(meta.Key, meta.Value)
		}
		return nil
	})
}

func (b *BtreeIndexer) Get(key []byte) (meta *marshal.BytesKV, err error) {
	meta = &marshal.BytesKV{}
	err = b.index.View(func(tx *bbolt.Tx) error {
		value := tx.Bucket(indexBucketName).Get(key)
		if value != nil {
			meta = &marshal.BytesKV{Key: key, Value: value}
		}
		return nil
	})
	return
}

func (b *BtreeIndexer) Scan(low, high []byte) (meta []*marshal.BytesKV, err error) {
	err = b.index.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(indexBucketName).Cursor()
		for k, v := cursor.Seek(low); k != nil && bytes.Compare(k, high) <= 0; k, v = cursor.Next() {
			meta = append(meta, &marshal.BytesKV{Key: k, Value: v})
		}
		return nil
	})
	return
}

func (b *BtreeIndexer) Delete(key []byte) error {
	return b.index.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(indexBucketName).Delete(key)
	})
}

func (b *BtreeIndexer) DeleteBatch(keys [][]byte) error {
	return b.index.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		for _, key := range keys {
			if err := bucket.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BtreeIndexer) Sync() error {
	return b.index.Sync()
}

func (b *BtreeIndexer) Close() error {
	return b.index.Close()
}
