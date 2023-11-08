package wal

import (
	"log"
	"unsafe"
)

const (
	AlignSize = 4096 //不管是ssd还是hhd都默认以4k对齐
	Block4096 = AlignSize
	num4      = 4
	num8      = 8
	Block4    = Block4096 * 4
	Block8    = Block4096 * 8
)

type BlockPool struct {
	Block4 []byte //4*4096   16KB
	Block8 []byte //8*4096   32KB
}

func NewBlockPool() (bp *BlockPool) {
	bp = new(BlockPool)
	bp.Block4 = alignedBlock(num4)
	bp.Block8 = alignedBlock(num8)
	return bp
}

func NewRaftBlockPool() (bp *BlockPool) {
	bp = new(BlockPool)
	bp.Block4 = alignedBlock(num4)
	return bp
}

// AlignedBlock 优先分配block4和block8，若均不能满足再分配自定义的block
func (b *BlockPool) AlignedBlock(n int) ([]byte, int) {
	if n < Block4 {
		return b.Block4, 4
	}

	if n < Block8 {
		return b.Block8, 8
	}

	nums := n / Block4096
	remain := n % Block4096
	if remain > 0 {
		nums++
	}

	return alignedBlock(nums), nums
}

func (b *BlockPool) recycleBlock(block []byte) {
	blockType := len(block)
	for i := 0; i < blockType; i++ {
		block[i] = 0
	}
	switch blockType {
	case Block4:
		b.Block4 = block
	case Block8:
		b.Block8 = block
	default:
		log.Panic("block type error")
	}
}

func alignedBlock(blockNums int) []byte {
	block := make([]byte, Block4096*blockNums)
	if isAligned(block) {
		return block
	} else {
		block = make([]byte, Block4096*blockNums+AlignSize)
	}

	a := alignment(block, AlignSize)
	offset := 0
	if a != 0 {
		offset = AlignSize - a
	}
	block = block[offset : offset+Block4096]

	if !isAligned(block) {
		log.Fatal("Failed to align block")
	}
	return block
}

func alignment(block []byte, AlignSize int) int {
	block0Addr := uintptr(unsafe.Pointer(&block[0]))
	alignSize := uintptr(AlignSize - 1)
	return int(block0Addr & alignSize)
}

func isAligned(block []byte) bool {
	return alignment(block, AlignSize) == 0
}
