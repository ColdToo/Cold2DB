package marshal

import (
	"encoding/binary"
	"github.com/ColdToo/Cold2DB/pb"
	"hash/crc32"
)

const (
	// ChunkHeaderSize Checksum Length  index
	//    4       4       8
	ChunkHeaderSize = 16

	Crc32Size  = 4
	EntrySize  = 4
	IndexSize  = 8
	TypeDelete = 1
)

type BytesKV struct {
	Key   []byte
	Value []byte
}

type KV struct {
	Key      []byte
	ApplySig int64
	Data     Data //作为value存储在跳表中
}

type Data struct {
	Index     uint64
	TimeStamp int64
	ExpiredAt int64
	Type      int8
	Value     []byte
}

func EncodeData(v Data) []byte {
	return nil
}

func DecodeData(v []byte) *Data {
	return nil
}

func EncodeKV(kv *KV) ([]byte, error) {
	return nil, nil
}

func DecodeKV(data []byte) (kv *KV) {
	return
}

// EncodeWALEntry  will encode entry into a byte slice.
// +-------+-----------+--------+-----------+
// |  crc  | entry size|  index |   entry
// +-------+-----------+--------+-----------+
// |----------HEADER------------|---BODY----+

type WalEntryHeader struct {
	Crc32     int
	EntrySize int
	Index     uint64
}

func (h WalEntryHeader) IsEmpty() bool {
	return h.Crc32 == 0 || h.EntrySize == 0 || h.Index == 0
}

func EncodeWALEntry(e *pb.Entry) ([]byte, int) {
	eBytes, _ := e.Marshal()
	eBytesSize := len(eBytes)
	var size = ChunkHeaderSize + eBytesSize
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[Crc32Size:], uint32(eBytesSize))
	binary.LittleEndian.PutUint64(buf[Crc32Size+EntrySize:], e.Index)
	copy(buf[Crc32Size+EntrySize+IndexSize:], eBytes)

	crc := crc32.ChecksumIEEE(buf[Crc32Size+EntrySize+IndexSize:])
	binary.LittleEndian.PutUint32(buf[:4], crc)
	return buf, size
}

func DecodeWALEntryHeader(buf []byte) (header WalEntryHeader) {
	crc32 := binary.LittleEndian.Uint32(buf[:Crc32Size])
	entrySize := binary.LittleEndian.Uint16(buf[Crc32Size : Crc32Size+EntrySize])
	index := binary.LittleEndian.Uint64(buf[Crc32Size+EntrySize : Crc32Size+EntrySize+IndexSize])
	header.Crc32 = int(crc32)
	header.EntrySize = int(entrySize)
	header.Index = index
	return
}

type IndexerNode struct {
	Key  []byte
	Meta *IndexerMeta
}

type IndexerMeta struct {
	Fid         int64
	ValueOffset int64
	ValueSize   int64
	TimeStamp   int64
	ExpiredAt   int64
	ValueCrc32  uint32
	Value       []byte //smaller value could be place in this
}

func EncodeIndexMeta(m *IndexerMeta) []byte {
	valueSize := len(m.Value)
	buf := make([]byte, 36+valueSize)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(m.Fid))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(m.ValueOffset))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(m.ValueSize))
	binary.LittleEndian.PutUint32(buf[24:28], m.ValueCrc32)
	binary.LittleEndian.PutUint64(buf[28:36], uint64(m.TimeStamp))
	copy(buf[36:], m.Value)
	return buf
}

func DecodeIndexMeta(buf []byte) *IndexerMeta {
	m := &IndexerMeta{}
	m.Fid = int64(int(binary.LittleEndian.Uint64(buf[0:8])))
	m.ValueOffset = int64(int(binary.LittleEndian.Uint64(buf[8:16])))
	m.ValueSize = int64(int(binary.LittleEndian.Uint64(buf[16:24])))
	m.ValueCrc32 = binary.LittleEndian.Uint32(buf[24:28])
	m.TimeStamp = int64(binary.LittleEndian.Uint64(buf[28:36]))
	m.Value = make([]byte, len(buf)-36)
	copy(m.Value, buf[36:])
	return m
}
