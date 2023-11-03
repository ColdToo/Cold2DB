package wal

import (
	"log"
	"sync"
	"unsafe"
)

const (
	AlignSize = 4096 //不管是ssd还是hhd都默认以4k对齐
	Block4096 = 4096
	Block4    = Block4096 * 4
	Block8    = Block4096 * 8
)

type BlockPool struct {
	Block4 *sync.Pool //4*4096   16KB
	Block8 *sync.Pool //8*4096   32KB
}

func NewBlockPool() (bp *BlockPool) {
	bp = new(BlockPool)
	bp.Block4 = &sync.Pool{
		New: AlignedBlock4,
	}
	bp.Block8 = &sync.Pool{
		New: AlignedBlock8,
	}
	return bp
}

func (b *BlockPool) GetBlock4() []byte {
	return b.Block4.Get().([]byte)
}

func (b *BlockPool) GetBlock8() []byte {
	return b.Block4.Get().([]byte)
}

func (b *BlockPool) PutBlock(block []byte) {

	b.Block4.Put(block)
}

func AlignedBlock4() any {
	block := make([]byte, 0, Block4096*4+AlignSize)
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

func AlignedBlock8() any {
	block := make([]byte, 0, Block4096*8+AlignSize)
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

// AlignedBlock 优先分配block4和block8，若均不能满足再分配自定义的block
func (b *BlockPool) AlignedBlock(n int) ([]byte, int) {
	if n > Block4 {
		return b.GetBlock4(), 4
	}

	if n > Block8 {
		return b.GetBlock8(), 8
	}

	nums := n / Block4096
	remain := n % Block4096
	if remain > 0 {
		nums++
	}

	block := make([]byte, Block4096*nums+AlignSize)
	a := alignment(block, AlignSize)
	offset := 0
	if a != 0 {
		offset = AlignSize - a
	}
	block = block[offset : offset+Block4096]

	if !isAligned(block) {
		log.Fatal("Failed to align block")
	}
	return block, nums
}

func alignment(block []byte, AlignSize int) int {
	return int(uintptr(unsafe.Pointer(&block[0])) & uintptr(AlignSize-1))
}

func isAligned(block []byte) bool {
	return alignment(block, AlignSize) == 0
}

func AllocateBlockNums(size int) int {
	quotient := size / Block4096
	remainder := size % Block4096
	if remainder > 0 {
		return quotient + 1
	} else {
		return quotient
	}
}
