package wal

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func MockData(block []byte) {
	for i := 0; i < len(block); i++ {
		block[i] = 1
	}
}

func TestAlignedBlockAligned4(t *testing.T) {
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

func TestBlockPool_PutBlock(t *testing.T) {
	blockPool := NewBlockPool()
	block3, _ := blockPool.AlignedBlock(Block4096*8 + 1)
	assert.Equal(t, isAligned(block3), true)
	MockData(block3)
	fmt.Println(len(block3), cap(block3))
	fmt.Printf("%p", &block3)
	block3 = block3[:0]
	fmt.Printf("%p", &block3)
	fmt.Println(len(block3), cap(block3))
}
