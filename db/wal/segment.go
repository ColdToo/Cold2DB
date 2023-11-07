package wal

import (
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/db/iooperator/directio"
	"github.com/ColdToo/Cold2DB/pb"
	"io"
	"os"
	"path/filepath"
)

type SegmentID = uint32

var (
	ErrClosed     = errors.New("the segment file is closed")
	ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
)

type segment struct {
	index               int64 //该segment文件中的最小index
	Fd                  *os.File
	blockPool           *BlockPool
	closed              bool
	blockNums           int
	currBlock           []byte
	currBlockSize       int
	currBlockRemainSize int
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

	blockPool := NewBlockPool()
	return &segment{
		Fd:        fd,
		blockPool: blockPool,
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
	if bytesCount+seg.currBlockRemainSize < seg.currBlockSize {
		seg.currBlock = append(seg.currBlock, data...)
		seg.currBlockRemainSize = seg.currBlockRemainSize - bytesCount
		seg.Flush()
	} else {
		seg.blockPool.PutBlock(seg.currBlock)
		//分配新的block
		newBlock, nums := seg.blockPool.AlignedBlock(len(data))
		seg.blockNums += nums
		seg.currBlock = newBlock
		seg.currBlock = append(seg.currBlock, data...)
		seg.currBlockRemainSize = 0
		seg.Flush()
	}

	return
}

func (seg *segment) Flush() error {
	_, err := seg.Fd.Seek(int64(seg.blockNums*Block4096), io.SeekStart)
	if err != nil {
		return err
	}

	_, err = seg.Fd.Write(seg.currBlock)
	if err != nil {
		return err
	}

	return nil

	//增加blockNums计数
}

func (seg *segment) Size() int {
	return seg.blockNums * Block4096
}

func (seg *segment) Remove() error {
	if !seg.closed {
		seg.closed = true
		_ = seg.Fd.Close()
	}

	return os.Remove(seg.Fd.Name())
}

func (seg *segment) Close() error {
	if seg.closed {
		return nil
	}

	seg.closed = true
	return seg.Fd.Close()
}

type segmentReader struct {
	segment     *segment
	blockNumber uint32
	chunkOffset int64
}

func (seg *segment) NewReader() *segmentReader {
	return &segmentReader{
		segment:     seg,
		blockNumber: 0,
		chunkOffset: 0,
	}
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

type raftSegment struct {
	Fd                  *os.File
	blockPool           *BlockPool
	RaftState           pb.HardState //需要持久化的状态
	closed              bool
	blockNums           int
	currBlock           []byte
	currBlockSize       int
	currBlockRemainSize int
}

func OpenRaftSegmentFile(walDirPath, fileName string) (*raftSegment, error) {
	fd, err := directio.OpenDirectIOFile(RaftSegmentFileName(walDirPath, fileName), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	_, err = fd.Seek(0, io.SeekStart)
	if err != nil {
		panic(fmt.Errorf("seek to the end of segment file %s failed: %v", ".SEG", err))
	}

	blockPool := NewBlockPool()
	return &raftSegment{
		Fd:        fd,
		blockPool: blockPool,
	}, nil
	//若fsize不为0读取文件的数据到block并序列化到pb.HardState
}

func RaftSegmentFileName(walDirPath, fileName string) string {
	return filepath.Join(walDirPath, fileName)
}

func (seg *raftSegment) Remove() error {
	if !seg.closed {
		seg.closed = true
		_ = seg.Fd.Close()
	}

	return os.Remove(seg.Fd.Name())
}

func (seg *raftSegment) Close() error {
	if seg.closed {
		return nil
	}

	seg.closed = true
	return seg.Fd.Close()
}

func (seg *raftSegment) WriteAndFlush(data []byte) (err error) {
	return
}
