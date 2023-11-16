package partition

import (
	"github.com/ColdToo/Cold2DB/config"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestNewIndexer(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("/tmp", "indexer-test"))
	assert.Nil(t, err)
	err = os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	type args struct {
		config config.IndexConfig
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"bptree-bolt", args{}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewIndexer(path)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewIndexer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				assert.NotNil(t, got)
			}
		})
	}
}

func TestEncodeDecodeMeta(t *testing.T) {
	meta := &IndexerMeta{
		Fid:        123,
		Offset:     456,
		ValueSize:  789,
		valueCrc32: 987,
		TimeStamp:  1234567890,
		ExpiredAt:  9876543210,
		Value:      []byte("test value"),
	}

	encoded := EncodeMeta(meta)
	decoded := DecodeMeta(encoded)

	if !reflect.DeepEqual(meta, decoded) {
		t.Errorf("Decoded meta does not match original meta")
	}
}
