package logfile

import (
	"github.com/stretchr/testify/assert"
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
	entry, err := DecodeWALEntry(byteArr)
	if err != nil {
		return
	}
	assert.Equal(t, entry, Entry)
}
