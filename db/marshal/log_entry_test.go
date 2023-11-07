package marshal

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestGobEncodeDecode(t *testing.T) {
	k := KV{
		Id:        1,
		Key:       []byte("key"),
		Value:     []byte("value"),
		Type:      TypeDelete,
		ExpiredAt: 1234567890,
	}

	encoded, err := GobEncode(k)
	assert.NoError(t, err)

	decoded, err := GobDecode(encoded)
	assert.NoError(t, err)

	assert.Equal(t, k, decoded)
}

func TestEncodeDecodeWALEntry(t *testing.T) {
	entry := &Entry{
		ExpiredAt: 1234567890,
		Index:     1,
		Term:      2,
		Type:      TypeDelete,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}

	encoded, _ := entry.EncodeWALEntry()
	header := decodeWALEntryHeader(encoded)
	t.Log(header)
}

func TestEncodeDecodeMemEntry(t *testing.T) {
	// 创建一个测试用的Entry对象
	entry := &Entry{
		ExpiredAt: 1234567890,
		Index:     1,
		Term:      2,
		Type:      TypeDelete,
		Value:     []byte("test value"),
	}

	// 编码Entry为字节切片
	encoded := entry.EncodeMemEntry()

	// 解码字节切片为Entry对象
	decoded := DecodeMemEntry(encoded)

	// 检查解码后的Entry是否与原始Entry相等
	if !reflect.DeepEqual(entry, decoded) {
		t.Errorf("Decoded entry does not match original entry")
	}
}
