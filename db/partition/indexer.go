package partition

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/config"
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
	separator           = "/"
	metaHeaderSize      = 5 + 5 + 10
)

type IndexerType int8

const (
	Btree IndexerType = iota
)

type IndexerNode struct {
	Key  []byte
	Meta *IndexerMeta
}

type IndexerMeta struct {
	TimeStamp int64
	ExpiredAt int64
	Value     []byte
	Fid       uint32
	Offset    int64
	ValueSize int
}

// EncodeMeta encode IndexerMeta as byte array.
func EncodeMeta(m *IndexerMeta) []byte {
	header := make([]byte, metaHeaderSize)
	var index int
	index += binary.PutVarint(header[index:], int64(m.Fid))
	index += binary.PutVarint(header[index:], m.Offset)
	index += binary.PutVarint(header[index:], int64(m.ValueSize))

	if m.Value != nil {
		buf := make([]byte, index+len(m.Value))
		copy(buf[:index], header[:])
		copy(buf[index:], m.Value)
		return buf
	}
	return header[:index]
}

// DecodeMeta decode meta byte as IndexerMeta.
func DecodeMeta(buf []byte) *IndexerMeta {
	m := &IndexerMeta{}
	var index int
	fid, n := binary.Varint(buf[index:])
	m.Fid = uint32(fid)
	index += n

	offset, n := binary.Varint(buf[index:])
	m.Offset = offset
	index += n

	esize, n := binary.Varint(buf[index:])
	m.ValueSize = int(esize)
	index += n

	m.Value = buf[index:]
	return m
}

// Indexer index data are stored in indexer.
type Indexer interface {
	Put(key []byte, meta *IndexerMeta) (err error)

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

func NewIndexer(indexCfg config.IndexConfig) (Indexer, error) {
	switch IndexerType(indexCfg.IndexerType) {
	case Btree:
		OpenBtreeIndexer(indexCfg.IndexerDir)
	default:
		panic("unknown indexer type")
	}
	return nil, nil
}

type BtreeIndexer struct {
}

func OpenBtreeIndexer(dirPath string) (Indexer, error) {
	// open bolt db
	tree, err := bbolt.Open(
		filepath.Join(dirPath, fmt.Sprintf(indexFileSuffixName)),
		0600,
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
	tx, err := tree.Begin(true)
	if err != nil {
		return nil, err
	}
	if _, err := tx.CreateBucketIfNotExists(indexBucketName); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return nil, nil
}

func (b *BtreeIndexer) Put(key []byte, meta *IndexerMeta) (err error) {
	return nil
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
