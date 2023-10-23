package logfile

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"hash/crc32"

	"github.com/ColdToo/Cold2DB/log"
)

// HeaderSize max entry header size.
// crc32   kSize	vSize
//
//	4    +   2   +   2       = 8
const (
	HeaderSize = 33
	Crc32Size  = 4
	KeySize    = 2
	ValSize    = 2
	KVSize     = 4

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

type Entry struct {
	Key       []byte
	Index     uint64
	Term      uint64
	Value     []byte
	Type      KVType
	ExpiredAt int64
}

type walEntryHeader struct {
	crc32     uint32
	kSize     uint16
	vSize     uint16
	ExpiredAt int64
	Index     uint64
	Term      uint64
	Type      KVType
}

// EncodeWALEntry  will encode entry into a byte slice.
// +-------+----------+------------+-----------+---------+---------+---------+-------+---------+
// |  crc  | key size | value size | expiredAt |  index  |   term  |   type  |  key  |  value  |
// +-------+----------+------------+-----------+---------+---------+---------+-------+---------+
// |-------------------------HEADER------------------------------------------|------BODY-------|
//
// |--------------------------crc check--------------------------------------------------------|
func (e *Entry) EncodeWALEntry() ([]byte, int) {
	var size = HeaderSize + len(e.Key) + len(e.Value)
	buf := make([]byte, size)
	//encode header
	binary.LittleEndian.PutUint16(buf[KVSize:], uint16(len(e.Key)))
	binary.LittleEndian.PutUint16(buf[KVSize+KeySize:], uint16(len(e.Value)))
	binary.LittleEndian.PutUint64(buf[2*KVSize:], uint64(e.ExpiredAt))
	binary.LittleEndian.PutUint64(buf[2*KVSize+ExpiredAtSize:], e.Index)
	binary.LittleEndian.PutUint64(buf[2*KVSize+ExpiredAtSize+IndexSize:], e.Term)
	buf[2*KVSize+ExpiredAtSize+IndexSize+TermSize] = byte(e.Type)
	//encode value
	copy(buf[2*KVSize+ExpiredAtSize+IndexSize+TermSize+KVTypeSize:], e.Key)
	copy(buf[2*KVSize+ExpiredAtSize+IndexSize+TermSize+KVTypeSize+len(e.Key):], e.Value)

	// crc32
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], crc)
	return buf, size
}

func decodeWALEntryHeader(buf []byte) (header walEntryHeader) {
	header.crc32 = binary.LittleEndian.Uint32(buf[:4])
	header.kSize = binary.LittleEndian.Uint16(buf[KVSize : KVSize+KeySize])
	header.vSize = binary.LittleEndian.Uint16(buf[KVSize+KeySize : KVSize+KeySize+ValSize])
	header.ExpiredAt = int64(binary.LittleEndian.Uint64(buf[2*KVSize : 2*KVSize+ExpiredAtSize]))
	header.Index = binary.LittleEndian.Uint64(buf[2*KVSize+ExpiredAtSize : 2*KVSize+ExpiredAtSize+IndexSize])
	header.Term = binary.LittleEndian.Uint64(buf[2*KVSize+ExpiredAtSize+IndexSize : 2*KVSize+ExpiredAtSize+IndexSize+TermSize])
	header.Type = KVType(buf[2*KVSize+ExpiredAtSize+IndexSize+TermSize])
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
