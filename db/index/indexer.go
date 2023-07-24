package index

import (
	"encoding/binary"
	"errors"
	"os"
)

var (
	// ErrDirPathNil indexer dir path is nil.
	ErrDirPathNil = errors.New("indexer dir path is nil")

	// ErrOptionsTypeNotMatch indexer options not match.
	ErrOptionsTypeNotMatch = errors.New("indexer options not match")
)

const (
	indexFileSuffixName = ".INDEX"
	separator           = string(os.PathSeparator)
	metaHeaderSize      = 5 + 5 + 10
)

// IndexerType type of indexer.
type IndexerType int8

const (
	// persist index
	BptreeBoltDB IndexerType = iota

	// inmemory index

	ArenaSkipList

	Bptree
)

type IndexerNode struct {
	Key  []byte
	Meta *IndexerMeta
}

type IndexerMeta struct {
	Value     []byte
	Fid       uint32
	Offset    int64
	ValueSize int
}

// Indexer index data are stored in indexer.
type Indexer interface {
	Put(key []byte, value []byte) (err error)

	PutBatch(kv []*IndexerNode) (offset int, err error)

	Get(key []byte) (meta *IndexerMeta, err error)

	Delete(key []byte) error

	DeleteBatch(keys [][]byte) error

	Sync() error

	Close() (err error)
}

// NewIndexer create a new Indexer by the given options, return an error, if any.
func NewIndexer(indexType IndexerType) (Indexer, error) {
	switch indexType {
	case BptreeBoltDB:
	case ArenaSkipList:
	case Bptree:
	default:
		panic("unknown indexer type")
	}
	return nil, nil
}

// IndexerIter .
type IndexerIter interface {
	// First moves the cursor to the first item in the bucket and returns its key and value.
	// If the bucket is empty then a nil key and value are returned.
	First() (key, value []byte)

	// Last moves the cursor to the last item in the bucket and returns its key and value.
	// If the bucket is empty then a nil key and value are returned.
	Last() (key, value []byte)

	// Seek moves the cursor to a given key and returns it.
	// If the key does not exist then the next key is used. If no keys follow, a nil key is returned.
	Seek(seek []byte) (key, value []byte)

	// Next moves the cursor to the next item in the bucket and returns its key and value.
	// If the cursor is at the end of the bucket then a nil key and value are returned.
	Next() (key, value []byte)

	// Prev moves the cursor to the previous item in the bucket and returns its key and value.
	// If the cursor is at the beginning of the bucket then a nil key and value are returned.
	Prev() (key, value []byte)

	// Close The close method must be called after the iterative behavior is over！
	Close() error
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
