package marshal

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"github.com/ColdToo/Cold2DB/pb"
	"hash/crc32"

	"github.com/ColdToo/Cold2DB/log"
)

const (
	// ChunkHeaderSize
	// Checksum Length  index
	//    4       4       8
	ChunkHeaderSize = 15
	Crc32Size       = 4
	EntrySize       = 4
	KeySize         = 2
	ValSize         = 2
	KVSize          = 4

	IndexSize     = 8
	TermSize      = 8
	ExpiredAtSize = 8
	KVTypeSize    = 1
)

// KVType type of Entry.
type KVType byte

const (
	TypeDelete KVType = iota + 1
)

type KV struct {
	Key []byte
	V
}

func GobEncode(KV any) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(KV)
	if err != nil {
		log.Errorf("encode err:", err)
		return nil, err
	}
	return buf.Bytes(), nil
}

func GobDecode(data []byte) (kv KV, err error) {
	err = gob.NewDecoder(bytes.NewBuffer(data)).Decode(&kv)
	if err != nil {
		log.Errorf("decode err:", err)
	}
	return
}

type V struct {
	Id        uint64
	Type      KVType
	ExpiredAt int64
	Value     []byte
}

func (v *V) encode() {

}

func DecodeV(v []byte) *V {
	return nil
}

type walEntryHeader struct {
	crc32     int32
	entrySize int32
	index     int64
}

// EncodeWALEntry  will encode entry into a byte slice.
// +-------+-----------+--------+-----------+
// |  crc  | entry size|  index |   entry
// +-------+-----------+--------+-----------+
// |----------HEADER------------|---BODY----+

func EncodeWALEntry(e *pb.Entry) ([]byte, int) {
	eBytes, _ := e.Marshal()
	eBytesSize := len(eBytes)
	var size = ChunkHeaderSize + eBytesSize
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[EntrySize:], uint32(eBytesSize))
	binary.LittleEndian.PutUint64(buf[EntrySize+IndexSize:], e.Index)
	copy(buf[Crc32Size+EntrySize+IndexSize:], e.Data)

	// crc32
	crc := crc32.ChecksumIEEE(buf[Crc32Size+EntrySize+IndexSize:])
	binary.LittleEndian.PutUint32(buf[:4], crc)
	return buf, size
}

func decodeWALEntryHeader(buf []byte) (header walEntryHeader) {
	crc32 := binary.LittleEndian.Uint32(buf[:Crc32Size])
	entrySize := binary.LittleEndian.Uint16(buf[Crc32Size : Crc32Size+EntrySize])
	index := binary.LittleEndian.Uint16(buf[Crc32Size+EntrySize : Crc32Size+EntrySize+IndexSize])
	header.crc32 = int32(crc32)
	header.entrySize = int32(entrySize)
	header.index = int64(index)
	return
}

/*
type LogEntry struct {
	Key       []byte
	Value     []byte
	Type      KVType
	ExpiredAt int64
}

// EncodeLogEntry  will encode entry into a byte slice.
// +-------+----------+------------+-----------+---------+-------+---------+
// |  crc  | key size | value size | expiredAt |   type  |  key  |  value  |
// +-------+----------+------------+-----------+---------+-------+---------+
// |---------------HEADER----------|------------------VALUE----------------|
// |--------------------------crc check------------------------------------|
func EncodeLogEntry(e *LogEntry) ([]byte, int) {
	if e == nil {
		return nil, 0
	}
	header := make([]byte, HeaderSize)
	// encode header.
	header[4] = byte(e.Type)
	var index = 5
	index += binary.PutVarint(header[index:], int64(len(e.Key)))
	index += binary.PutVarint(header[index:], int64(len(e.Value)))
	index += binary.PutVarint(header[index:], e.ExpiredAt)

	var size = index + len(e.Key) + len(e.Value)
	buf := make([]byte, size)
	copy(buf[:index], header[:])
	// key and value.
	copy(buf[index:], e.Key)
	copy(buf[index+len(e.Key):], e.Value)

	// crc32.
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], crc)
	return buf, size
}

func getEntryCrc(e *Entry, h []byte) uint32 {
	if e == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(h[:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)
	return crc
}
*/
