package marshal

import (
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

var entries = []*pb.Entry{
	{
		Term:  1,
		Index: 1,
		Type:  pb.EntryNormal,
		Data:  []byte("hello world"),
	},
	{
		Term:  2,
		Index: 2,
		Type:  pb.EntryNormal,
		Data:  []byte("hello world"),
	},
	{
		Term:  3,
		Index: 3,
		Type:  pb.EntryNormal,
		Data:  []byte("hello world"),
	},
	{
		Term:  4,
		Index: 4,
		Type:  pb.EntryNormal,
		Data:  []byte("hello world"),
	},
}

func MarshalWALEntries(entries1 []*pb.Entry) (data []byte, bytesCount int) {
	data = make([]byte, 0)
	for _, e := range entries1 {
		wEntBytes, n := EncodeWALEntry(e)
		data = append(data, wEntBytes...)
		bytesCount += n
	}
	return
}

func TestEncodeANdDecodeWALEntry(t *testing.T) {
	entry1 := &pb.Entry{
		Term:  1,
		Index: 1,
		Type:  pb.EntryNormal,
		Data:  []byte("hello world"),
	}

	wEntBytes, _ := EncodeWALEntry(entry1)
	buf := make([]byte, ChunkHeaderSize)
	copy(buf, wEntBytes[:ChunkHeaderSize])
	header := DecodeWALEntryHeader(buf)
	entry2 := &pb.Entry{}

	entry2.Unmarshal(wEntBytes[ChunkHeaderSize : ChunkHeaderSize+header.EntrySize])
	assert.EqualValues(t, entry1, entry2)
}

func TestIndexMetaSerialization(t *testing.T) {
	m := &IndexerMeta{
		Fid:         123,
		ValueOffset: 45623232323,
		ValueSize:   789232323,
		TimeStamp:   1000000,
		ValueCrc32:  987654,
		Value:       []byte("test value 1123232"),
	}

	serialized := EncodeIndexMeta(m)
	deserialized := DecodeIndexMeta(serialized)

	if !reflect.DeepEqual(m, deserialized) {
		t.Errorf("Serialization and deserialization do not match")
	}
}
