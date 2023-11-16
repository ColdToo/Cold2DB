package partition

import (
	"encoding/binary"
	"errors"
	"fmt"
	"go.etcd.io/bbolt"
	"path/filepath"
)

// bucket name for bolt db to store index data
var indexBucketName = []byte("index")

var (
	// ErrDirPathNil indexer dir path is nil.
	ErrDirPathNil = errors.New("indexer dir path is nil")

	// ErrOptionsTypeNotMatch indexer options not match.
	ErrOptionsTypeNotMatch = errors.New("indexer options not match")
)

const (
	indexFileSuffixName = ".INDEX"
	metaSize            = 5 + 5 + 10
)

type Indexer interface {
	Put(meta *[]IndexerMeta) (err error)

	Get(key []byte) (meta *IndexerMeta, err error)

	Scan(low, high []byte) (meta *[]IndexerMeta, err error)

	Delete(key []byte) error

	DeleteBatch(keys [][]byte) error

	Sync() error

	Close() (err error)
}

type IndexerIter interface {
	First() (key, value []byte)

	Last() (key, value []byte)

	Seek(seek []byte) (key, value []byte)

	Next() (key, value []byte)

	Prev() (key, value []byte)

	Close() error
}

type IndexerNode struct {
	Key  []byte
	Meta *IndexerMeta
}

// IndexerMeta smaller value could be place in this struct
type IndexerMeta struct {
	Fid        int
	Offset     int
	ValueSize  int
	valueCrc32 uint32
	TimeStamp  int64
	ExpiredAt  int64
	Value      []byte
}

func EncodeMeta(m *IndexerMeta) []byte {
	valueSize := len(m.Value)
	buf := make([]byte, 36+valueSize)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(m.Fid))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(m.Offset))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(m.ValueSize))
	binary.LittleEndian.PutUint32(buf[24:28], m.valueCrc32)
	binary.LittleEndian.PutUint64(buf[28:36], uint64(m.TimeStamp))
	copy(buf[36:], m.Value)
	return buf
}

func DecodeMeta(buf []byte) *IndexerMeta {
	m := &IndexerMeta{}
	m.Fid = int(binary.LittleEndian.Uint64(buf[0:8]))
	m.Offset = int(binary.LittleEndian.Uint64(buf[8:16]))
	m.ValueSize = int(binary.LittleEndian.Uint64(buf[16:24]))
	m.valueCrc32 = binary.LittleEndian.Uint32(buf[24:28])
	m.TimeStamp = int64(binary.LittleEndian.Uint64(buf[28:36]))
	m.Value = make([]byte, len(buf)-36)
	copy(m.Value, buf[36:])
	return m
}

func NewIndexer(indexFileName string) (Indexer, error) {
	return OpenBtreeIndexer(indexFileName)
}

type BtreeIndexer struct {
	index *bbolt.DB
}

func OpenBtreeIndexer(dirPath string) (Indexer, error) {
	bbolt, err := bbolt.Open(filepath.Join(dirPath, fmt.Sprintf(indexFileSuffixName)), 0600,
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
	tx, err := bbolt.Begin(true)
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
		index: bbolt,
	}, nil
}

func (b *BtreeIndexer) Put(metas []*IndexerNode) (err error) {
	return b.index.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		for _, meta := range metas {
			vBytes := EncodeMeta(meta.Meta)
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
