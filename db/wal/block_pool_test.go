package wal

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func MockData(block []byte) {
	for i := 0; i < len(block); i++ {
		block[i] = 1
	}
}

func TestAlignedBlockAligned(t *testing.T) {
	assert.Equal(t, isAligned(NewBlockPool().GetBlock4()), true)
}
