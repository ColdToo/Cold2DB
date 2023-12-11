package marshal

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"github.com/ColdToo/Cold2DB/log"
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

type KV struct {
	Key  []byte
	Data Data
}

func GobEncode(kv *KV) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(kv)
	if err != nil {
		log.Errorf("encode err:", err)
		return nil, err
	}
	return buf.Bytes(), nil
}

func GobDecode(data []byte) (kv KV) {
	err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(&kv)
	if err != nil {
		log.Panicf("decode err:", err)
	}
	return
}

type Data struct {
	Index     uint64
	TimeStamp int64
	ExpiredAt int64
	Type      int8
	Value     []byte
}

func EncodeData(v *Data) []byte {
	return nil
}

func DecodeData(v []byte) *Data {
	return nil
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

	// crc32
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
