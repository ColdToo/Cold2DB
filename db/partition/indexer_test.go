package partition

import (
	"reflect"
	"testing"
)

func TestEncodeDecodeMeta(t *testing.T) {
	meta := &IndexerMeta{
		Fid:         123,
		ValueOffset: 456,
		ValueSize:   789,
		valueCrc32:  987,
		TimeStamp:   1234567890,
		ExpiredAt:   9876543210,
		Value:       []byte("test value"),
	}

	encoded := EncodeMeta(meta)
	decoded := DecodeMeta(encoded)

	if !reflect.DeepEqual(meta, decoded) {
		t.Errorf("Decoded meta does not match original meta")
	}
}
