package wal

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/db/iooperator/directio"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/pb"
	"io"
	"os"
	"path/filepath"
)

type SegmentID = uint32

const DefaultIndex = 0

var (
	ErrClosed     = errors.New("the segment file is closed")
	ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
)

type segment struct {
	Index     int64 //该segment文件中的最小index
	Fd        *os.File
	blockPool *BlockPool

	Blocks           []byte
	blockNums        int //记录当前segment的blocks数量,也可以作为segment的偏移量使用
	BlocksOffset     int //当前Blocks的偏移量
	BlocksRemainSize int

	closed bool
}

func NewSegmentFile(dirPath string) (*segment, error) {
	fd, err := directio.OpenDirectIOFile(SegmentFileName(dirPath, 0>>1), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	_, err = fd.Seek(0, io.SeekStart)
	if err != nil {
		panic(fmt.Errorf("seek to the end of segment file %s failed: %v", ".SEG", err))
	}

	return &segment{
		Index:     DefaultIndex,
		Fd:        fd,
		blockPool: NewBlockPool(),
	}, nil
}

func OpenSegmentFile(walDirPath string, index int64) (*segment, error) {
	fd, err := directio.OpenDirectIOFile(SegmentFileName(walDirPath, index), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	_, err = fd.Seek(0, io.SeekStart)
	if err != nil {
		panic(fmt.Errorf("seek to the end of segment file %s failed: %v", ".SEG", err))
	}

	blockPool := NewBlockPool()
	//获取第一个segment file的第一个header中的index
	return &segment{
		Fd:        fd,
		blockPool: blockPool,
	}, nil
}

func SegmentFileName(WaldirPath string, index int64) string {
	return filepath.Join(WaldirPath, fmt.Sprintf("%014d"+SegSuffix, index))
}

func (seg *segment) Write(data []byte, bytesCount int) (err error) {
	//如果当前block能够写入
	if bytesCount < seg.BlocksRemainSize {
		copy(seg.Blocks[seg.BlocksOffset:len(data)], data)
		seg.BlocksOffset = seg.BlocksOffset + bytesCount
		seg.BlocksRemainSize = seg.BlocksRemainSize - bytesCount
		seg.Flush()
	} else {
		seg.blockPool.recycleBlock(seg.Blocks)
		//分配新的block
		newBlock, nums := seg.blockPool.AlignedBlock(len(data))
		seg.blockNums += nums
		seg.Blocks = newBlock
		copy(seg.Blocks[seg.BlocksOffset:len(data)], data)
		seg.BlocksRemainSize = 0
		seg.Flush()
	}

	return
}

func (seg *segment) Flush() error {
	_, err := seg.Fd.Seek(int64(seg.blockNums*Block4096), io.SeekStart)
	if err != nil {
		return err
	}

	_, err = seg.Fd.Write(seg.Blocks)
	if err != nil {
		return err
	}

	return nil

	//增加blockNums计数
}

func (seg *segment) Size() int {
	return seg.blockNums * Block4096
}

func (seg *segment) Close() error {
	if seg.closed {
		return nil
	}

	seg.closed = true
	return seg.Fd.Close()
}

func (seg *segment) Remove() error {
	if !seg.closed {
		seg.closed = true
		_ = seg.Fd.Close()
	}

	return os.Remove(seg.Fd.Name())
}

type segmentReader struct {
	segment *segment
	buffer  bytes.Reader
}

func NewReader(seg *segment) *segmentReader {
	make([]byte)
	seg.Fd.Write()
	return &segmentReader{
		segment: seg,
	}
}

func (sr *segmentReader) ReadHeader(p []byte) (eHeader marshal.WalEntryHeader, err error) {

}

func (sr *segmentReader) ReadEntry(p []byte) (n int, err error) {

}

func (sr *segmentReader) Next(p []byte) (n int, err error) {

}

// Node 由segment组成的有序单链表
type Node struct {
	Seg  *segment
	Next *Node
}

type OrderedSegmentList struct {
	Head *Node
}

func NewOrderedSegmentList() *OrderedSegmentList {
	return &OrderedSegmentList{}
}

func (oll *OrderedSegmentList) Insert(seg *segment) {
	newNode := &Node{Seg: seg}

	if oll.Head == nil || oll.Head.Seg.index >= seg.index {
		newNode.Next = oll.Head
		oll.Head = newNode
		return
	}

	current := oll.Head
	for current.Next != nil && current.Next.Seg.index < seg.index {
		current = current.Next
	}

	newNode.Next = current.Next
	current.Next = newNode
}

func (oll *OrderedSegmentList) Find(index int64) *segment {
	current := oll.Head
	var prev *Node

	for current != nil && current.Seg.index < index {
		prev = current
		current = current.Next
	}

	if current != nil && current.Seg.index == index {
		return current.Seg
	}

	if prev != nil {
		return prev.Seg
	}

	return nil
}

// 需要持久化的状态 persist index apply index
type stateSegment struct {
	Fd              *os.File
	blockPool       *BlockPool
	RaftState       pb.HardState //需要持久化的状态
	closed          bool
	Blocks          []byte
	BlockSize       int
	BlockRemainSize int
}

func OpenStateSegmentFile(walDirPath, fileName string) (rSeg *stateSegment, err error) {
	fd, err := directio.OpenDirectIOFile(filepath.Join(walDirPath, fileName), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	_, err = fd.Seek(0, io.SeekStart)
	if err != nil {
		panic(fmt.Errorf("seek to the end of segment file %s failed: %v", ".SEG", err))
	}

	blockPool := NewBlockPool()
	rSeg = new(stateSegment)
	rSeg.Fd = fd
	rSeg.RaftState = pb.HardState{}
	rSeg.blockPool = blockPool
	rSeg.Blocks = blockPool.Block4
	fileInfo, _ := rSeg.Fd.Stat()

	//若fsize不为0读取文件的数据到block并序列化到pb.HardState
	if fileInfo.Size() > 0 {
		rSeg.Fd.Read(rSeg.Blocks)
		header := marshal.DecodeRaftStateHeader(rSeg.Blocks)
		rSeg.RaftState.Unmarshal(rSeg.Blocks[header.HeaderSize:header.StateSize])
	}

	return rSeg, nil
}

func (seg *stateSegment) Persist(data []byte) (err error) {
	copy(seg.Blocks[0:len(data)], data)
	_, err = seg.Fd.Seek(0, io.SeekStart)
	if err != nil {
		return
	}
	_, err = seg.Fd.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func (seg *stateSegment) Remove() error {
	if !seg.closed {
		seg.closed = true
		_ = seg.Fd.Close()
	}

	return os.Remove(seg.Fd.Name())
}

func (seg *stateSegment) Close() error {
	if seg.closed {
		return nil
	}

	seg.closed = true
	return seg.Fd.Close()
}
