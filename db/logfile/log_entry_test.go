package logfile

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
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

func TestEncodeAndDeocdeWalEntry(t *testing.T) {
	Entry := new(Entry)
	Entry.Index = 1
	Entry.Term = 2
	Entry.Type = TypeDelete
	Entry.Key = []byte("hlhf")
	Entry.Value = []byte("mll")
	Entry.ExpiredAt = time.Now().Unix()
	byteArr, size := Entry.EncodeWALEntry()
	assert.Equal(t, len(byteArr), size)
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
