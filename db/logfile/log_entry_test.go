package logfile

import (
	"github.com/stretchr/testify/assert"
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
