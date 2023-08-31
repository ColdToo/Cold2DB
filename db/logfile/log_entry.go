package logfile

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"hash/crc32"
)

// MaxHeaderSize max entry header size.
// crc32   kSize	vSize
//
//	4    +   2   +   2       = 8
const (
	KeySize       = 2
	ValSize       = 2
	MaxHeaderSize = 14
	IndexSize     = 8
	TermSize      = 8
	ExpiredAtSize = 8
	EntryTypeSize = 1
)

// EntryType type of Entry.
type EntryType byte

const (
	TypeDelete EntryType = iota + 1
)

type KV struct {
	Type      EntryType
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
	crc32     uint32 // check sum
	typ       EntryType
	kSize     uint32
	vSize     uint32
	expiredAt int64 // time.Unix
}

func decodeHeader(buf []byte) (*entryHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	h := &entryHeader{
		crc32: binary.LittleEndian.Uint32(buf[:4]),
		typ:   EntryType(buf[4]),
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

func getEntryCrc(e *WalEntry, h []byte) uint32 {
	if e == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(h[:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)
	return crc
}

type WalEntry struct {
	Index     uint64
	Term      uint64
	Key       []byte
	Value     []byte
	Type      EntryType
	ExpiredAt int64
}

// EncodeWalEntry  will encode entry into a byte slice.
// The encoded Entry looks like:
// +-------+----------+------------+-----------+---------+---------+---------+-------+---------+
// |  crc  | key size | value size | expiredAt |  index  |   term  |   type  |  key  |  value  |
// +-------+----------+------------+-----------+---------+---------+---------+-------+---------+
// |---------------HEADER----------|----------------------------VALUE--------------------------|
//
// |--------------------------crc check--------------------------------------------------------|
func (e *WalEntry) EncodeWalEntry() ([]byte, int) {
	if e == nil {
		return nil, 0
	}
	header := make([]byte, MaxHeaderSize)
	// encode header.
	var index = 4
	binary.LittleEndian.PutUint16(header[index:], uint16(len(e.Key)))
	binary.LittleEndian.PutUint16(header[index+KeySize:], uint16(len(e.Key)))

	// encode value
	index += KeySize + ValSize
	var size = index + len(e.Key) + len(e.Value) + ExpiredAtSize + IndexSize + TermSize + EntryTypeSize
	buf := make([]byte, size)
	copy(buf[:index], header[:])
	index = size
	binary.LittleEndian.PutUint64(buf[index:], uint64(e.ExpiredAt))
	binary.LittleEndian.PutUint64(buf[index+ExpiredAtSize:], uint64(e.ExpiredAt))
	binary.LittleEndian.PutUint64(buf[index+ExpiredAtSize+IndexSize:], e.Index)
	binary.LittleEndian.PutUint64(buf[index+ExpiredAtSize+IndexSize+TermSize:], e.Term)
	buf[index+ExpiredAtSize+IndexSize+TermSize+EntryTypeSize] = byte(e.Type)

	copy(buf[index+ExpiredAtSize+IndexSize+TermSize+EntryTypeSize:], e.Key)
	copy(buf[index+ExpiredAtSize+IndexSize+TermSize+EntryTypeSize+len(e.Key):], e.Value)

	// crc32.
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], crc)
	return buf, size
}

func (e *WalEntry) EncodeMemEntry() []byte {
	buf := make([]byte, IndexSize+TermSize+ExpiredAtSize+EntryTypeSize+len(e.Value))
	binary.LittleEndian.PutUint64(buf[:], e.Index)
	binary.LittleEndian.PutUint64(buf[IndexSize:], e.Term)
	binary.LittleEndian.PutUint64(buf[IndexSize+TermSize:], uint64(e.ExpiredAt))
	copy(buf[IndexSize+TermSize+ExpiredAtSize:], string(e.Type))
	copy(buf[IndexSize+TermSize+ExpiredAtSize+EntryTypeSize:], string(e.Type))
	return buf
}

func (e *WalEntry) TransToPbEntry() (pbEnt *pb.Entry) {
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

func DecodeMemEntry(buf []byte) (e *WalEntry) {
	typ := make([]byte, 1)
	e.Index = binary.LittleEndian.Uint64(buf[:4])
	e.Index = binary.LittleEndian.Uint64(buf[4:9])
	e.Index = binary.LittleEndian.Uint64(buf[9:13])
	copy(typ, buf[13:14])
	copy(e.Value, buf[14:])
	e.Type = EntryType(typ[0])
	return
}

type LogEntry struct {
	Key       []byte
	Value     []byte
	Type      EntryType
	ExpiredAt int64
}

// EncodeLogEntry  will encode entry into a byte slice.
// The encoded Entry looks like:
// +-------+----------+------------+-----------+---------+---------+---------+-------+---------+
// |  crc  | key size | value size | expiredAt |  index  |   term  |   type  |  key  |  value  |
// +-------+----------+------------+-----------+---------+---------+---------+-------+---------+
// |---------------HEADER----------|----------------------------VALUE--------------------------|
//
// |--------------------------crc check------------------------------------------------|
func EncodeLogEntry(e *LogEntry) ([]byte, int) {
	if e == nil {
		return nil, 0
	}
	header := make([]byte, MaxHeaderSize)
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
