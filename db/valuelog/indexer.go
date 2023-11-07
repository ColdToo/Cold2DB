package valuelog

import (
	"encoding/binary"
	"errors"
	"github.com/ColdToo/Cold2DB/config"
)

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

// IndexerType type of indexer.
type IndexerType int8

const (
	// persist index
	BptreeBoltDB IndexerType = iota
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

func NewIndexer(indexCfg config.IndexConfig) (Indexer, error) {
	switch IndexerType(indexCfg.IndexerType) {
	case BptreeBoltDB:
	default:
		panic("unknown indexer type")
	}
	return nil, nil
}

// IndexerIter .
type IndexerIter interface {
	First() (key, value []byte)

	Last() (key, value []byte)

	Seek(seek []byte) (key, value []byte)

	Next() (key, value []byte)

	Prev() (key, value []byte)

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
