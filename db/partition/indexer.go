package partition

import (
	"encoding/binary"
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
	Put(meta []*IndexerNode) (err error)

	Get(key []byte) (meta *IndexerMeta, err error)

	Scan(low, high []byte) (meta *[]IndexerMeta, err error)

	Delete(key []byte) error

	DeleteBatch(keys [][]byte) error

	Sync() error

	Close() (err error)
}

type IndexerNode struct {
	Key  []byte
	Meta *IndexerMeta
}

type IndexerMeta struct {
	Fid         int
	ValueOffset int
	ValueSize   int
	TimeStamp   int64
	ExpiredAt   int64
	valueCrc32  uint32
	Value       []byte //smaller value could be place in this
}

func EncodeIndexMeta(m *IndexerMeta) []byte {
	valueSize := len(m.Value)
	buf := make([]byte, 36+valueSize)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(m.Fid))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(m.ValueOffset))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(m.ValueSize))
	binary.LittleEndian.PutUint32(buf[24:28], m.valueCrc32)
	binary.LittleEndian.PutUint64(buf[28:36], uint64(m.TimeStamp))
	copy(buf[36:], m.Value)
	return buf
}

func DecodeIndexMeta(buf []byte) *IndexerMeta {
	m := &IndexerMeta{}
	m.Fid = int(binary.LittleEndian.Uint64(buf[0:8]))
	m.ValueOffset = int(binary.LittleEndian.Uint64(buf[8:16]))
	m.ValueSize = int(binary.LittleEndian.Uint64(buf[16:24]))
	m.valueCrc32 = binary.LittleEndian.Uint32(buf[24:28])
	m.TimeStamp = int64(binary.LittleEndian.Uint64(buf[28:36]))
	m.Value = make([]byte, len(buf)-36)
	copy(m.Value, buf[36:])
	return m
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

func (b *BtreeIndexer) Put(metas []*IndexerNode) (err error) {
	return b.index.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		for _, meta := range metas {
			vBytes := EncodeIndexMeta(meta.Meta)
			bucket.Put(meta.Key, vBytes)
		}
		return nil
	})
}

func (b *BtreeIndexer) Get(key []byte) (meta *IndexerMeta, err error) {
	return
}

func (b *BtreeIndexer) Scan(low, high []byte) (meta *[]IndexerMeta, err error) {
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
