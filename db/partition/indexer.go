package partition

import (
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
	indexer, err := bbolt.Open(filepath.Join(partitionDir, time.Now().String()+indexFileSuffixName), 0600,
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
	return
}

func (b *BtreeIndexer) Scan(low, high []byte) (meta []*marshal.BytesKV, err error) {
	return
}

func (b *BtreeIndexer) Delete(key []byte) error {
	return nil
}

func (b *BtreeIndexer) DeleteBatch(keys [][]byte) error {
	return nil
}

func (b *BtreeIndexer) Sync() error {
	return nil
}

func (b *BtreeIndexer) Close() error {
	return nil
}
