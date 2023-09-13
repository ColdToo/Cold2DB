package logfile

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"hash/crc32"

	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
)

// HeaderSize max entry header size.
// crc32   kSize	vSize
//
//	4    +   2   +   2       = 8
const (
	HeaderSize = 8
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
	Type      KVType
	Id        uint64
	ExpiredAt int64
	Key       []byte
	Value     []byte
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

type entryHeader struct {
	crc32 uint32
	kSize uint32
	vSize uint32
}

type Entry struct {
	Index     uint64
	Term      uint64
	Key       []byte
	Value     []byte
	Type      KVType
	ExpiredAt int64
}

// EncodeWALEntry  will encode entry into a byte slice.
// +-------+----------+------------+-----------+---------+---------+---------+-------+---------+
// |  crc  | key size | value size | expiredAt |  index  |   term  |   type  |  key  |  value  |
// +-------+----------+------------+-----------+---------+---------+---------+-------+---------+
// |---------------HEADER----------|----------------------------VALUE--------------------------|
//
// |--------------------------crc check--------------------------------------------------------|
func (e *Entry) EncodeWALEntry() ([]byte, int) {
	var size = HeaderSize + ExpiredAtSize + IndexSize + TermSize + KVTypeSize + len(e.Key) + len(e.Value)
	buf := make([]byte, size)
	//encode header
	binary.LittleEndian.PutUint16(buf[KVSize:], uint16(len(e.Key)))
	binary.LittleEndian.PutUint16(buf[KVSize+KeySize:], uint16(len(e.Key)))

	//encode value
	binary.LittleEndian.PutUint64(buf[HeaderSize:], uint64(e.ExpiredAt))
	binary.LittleEndian.PutUint64(buf[HeaderSize+ExpiredAtSize:], e.Index)
	binary.LittleEndian.PutUint64(buf[HeaderSize+ExpiredAtSize+IndexSize:], e.Term)
	buf[HeaderSize+ExpiredAtSize+IndexSize+TermSize] = byte(e.Type)
	copy(buf[HeaderSize+ExpiredAtSize+IndexSize+TermSize+KVTypeSize:], e.Key)
	copy(buf[HeaderSize+ExpiredAtSize+IndexSize+TermSize+KVTypeSize+len(e.Key):], e.Value)

	// crc32
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], crc)
	return buf, size
}

func decodeWALEntryHeader(buf []byte) (*entryHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	h := &entryHeader{
		crc32: binary.LittleEndian.Uint32(buf[:4]),
		typ:   KVType(buf[4]),
	}
	var index = 5
	ksize, n := binary.Varint(buf[index:])
	h.kSize = uint32(ksize)
	index += n

	vsize, n := binary.Varint(buf[index:])
	h.vSize = uint32(vsize)
	index += n

	expiredAt, n := binary.Varint(buf[index:])
	h.expiredAt = expiredAt
	return h, int64(index + n)
}

func (e *Entry) EncodeMemEntry() []byte {
	buf := make([]byte, IndexSize+TermSize+ExpiredAtSize+KVTypeSize+len(e.Value))
	binary.LittleEndian.PutUint64(buf[:], e.Index)
	binary.LittleEndian.PutUint64(buf[IndexSize:], e.Term)
	binary.LittleEndian.PutUint64(buf[IndexSize+TermSize:], uint64(e.ExpiredAt))
	copy(buf[IndexSize+TermSize+ExpiredAtSize:], string(e.Type))
	copy(buf[IndexSize+TermSize+ExpiredAtSize+KVTypeSize:], string(e.Type))
	return buf
}

func DecodeMemEntry(buf []byte) (e *Entry) {
	typ := make([]byte, 1)
	e.Index = binary.LittleEndian.Uint64(buf[:4])
	e.Index = binary.LittleEndian.Uint64(buf[4:9])
	e.Index = binary.LittleEndian.Uint64(buf[9:13])
	copy(typ, buf[13:14])
	copy(e.Value, buf[14:])
	e.Type = KVType(typ[0])
	return
}

func (e *Entry) TransToPbEntry() (pbEnt *pb.Entry) {
	kv := KV{
		Key: e.Key,
		//Value:     e.Value,
		Type:      e.Type,
		ExpiredAt: e.ExpiredAt,
	}
	buf, _ := GobEncode(kv)
	pbEnt = &pb.Entry{
		Index: e.Index,
		Term:  e.Term,
		Type:  pb.EntryNormal,
		Data:  buf,
	}
	return
}

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
