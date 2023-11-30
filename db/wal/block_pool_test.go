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

func TestAlignedBlock(t *testing.T) {
	blockPool := NewBlockPool()

	block1, count1 := blockPool.AlignedBlock(1)
	assert.Equal(t, isAligned(block1), true)
	assert.Equal(t, Block4096*4, len(block1))
	assert.Equal(t, 4, count1)

	block2, count2 := blockPool.AlignedBlock(Block4096*4 + 1)
	assert.Equal(t, isAligned(block2), true)
	assert.Equal(t, len(block2), Block4096*8)
	assert.Equal(t, count2, 8)

	block3, count3 := blockPool.AlignedBlock(Block4096*8 + 1)
	assert.Equal(t, isAligned(block3), true)
	assert.Equal(t, Block4096*9, len(block3))
	assert.Equal(t, 9, count3)
}

func TestBlockPool_RecycleBlock(t *testing.T) {
	blockPool := NewBlockPool()
	block, _ := blockPool.AlignedBlock(1)
	assert.Equal(t, isAligned(block), true)
	MockData(block)
	blockPool.recycleBlock(block)
	newBlock, _ := blockPool.AlignedBlock(1)
	assert.Equal(t, &newBlock[0], &block[0])
}
