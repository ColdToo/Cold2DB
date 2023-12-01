package wal

import (
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/db/iooperator"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type SegmentID = uint32

const (
	DefaultMinLogIndex = 0
	InitialBlockNum    = 1
)

type segment struct {
	Index     uint64 //该segment文件中的最小log index
	Fd        *os.File
	blockPool *BlockPool

	blocks        []byte //当前segment使用的blocks
	blockNums     int    //记录当前segment已分配的blocks数量
	segmentOffset int    //blocks写入segment文件的偏移量

	blocksOffset     int //当前Blocks的偏移量
	BlocksRemainSize int //当前Blocks剩余可以写字节数
	closed           bool
}

func NewSegmentFile(dirPath string) (*segment, error) {
	fd, err := iooperator.OpenDirectIOFile(SegmentFileName(dirPath, DefaultMinLogIndex), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	blockPool := NewBlockPool()
	//default use 4 block as new active segment blocks
	return &segment{
		Index:            DefaultMinLogIndex,
		Fd:               fd,
		blocks:           blockPool.Block4,
		blocksOffset:     0,
		BlocksRemainSize: Block4,
		blockNums:        num4,
		segmentOffset:    0,
		blockPool:        blockPool,
	}, nil
}

func OpenOldSegmentFile(walDirPath string, index uint64) (*segment, error) {
	fd, err := iooperator.OpenDirectIOFile(SegmentFileName(walDirPath, index), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	fileInfo, _ := fd.Stat()
	fSize := fileInfo.Size()
	blockNums := fSize / Block4096
	remain := fSize % Block4096
	if remain > 0 {
		blockNums++
	}

	return &segment{
		Index:     index,
		Fd:        fd,
		blockNums: int(blockNums),
	}, nil
}

func SegmentFileName(walDirPath string, index uint64) string {
	return filepath.Join(walDirPath, fmt.Sprintf("%014d"+SegSuffix, index))
}

func (seg *segment) Write(data []byte, bytesCount int, firstIndex uint64) (err error) {
	if bytesCount < seg.BlocksRemainSize {
		copy(seg.blocks[seg.blocksOffset:bytesCount], data)
	} else {
		seg.blockPool.recycleBlock(seg.blocks)
		seg.blocksOffset = 0
		newBlock, nums := seg.blockPool.AlignedBlock(bytesCount)
		seg.segmentOffset = seg.segmentOffset + nums*Block4096
		seg.BlocksRemainSize = nums * Block4096
		seg.blockNums = seg.blockNums + nums
		seg.blocks = newBlock
		copy(seg.blocks[seg.blocksOffset:bytesCount], data)
	}

	if err = seg.Flush(bytesCount); err == nil && seg.Index == DefaultMinLogIndex {
		seg.Index = firstIndex
		err = os.Rename(seg.Fd.Name(), SegmentFileName(filepath.Dir(seg.Fd.Name()), seg.Index))
		if err != nil {
			log.Errorf("rename segment file %s failed: %v", seg.Fd.Name(), err)
			return err
		}
	}

	return
}

func (seg *segment) Flush(bytesCount int) (err error) {
	_, err = seg.Fd.Seek(int64(seg.segmentOffset), io.SeekStart)
	if err != nil {
		return err
	}

	_, err = seg.Fd.Write(seg.blocks)
	if err != nil {
		return err
	}

	seg.blocksOffset += bytesCount
	seg.BlocksRemainSize -= bytesCount

	return
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
	if err := seg.Close(); err == nil {
		os.Remove(seg.Fd.Name())
	} else {
		log.Errorf("close segment file failed", err)
		return err
	}
	return nil
}

// OrderedSegmentList 由segment组成的有序单链表
type OrderedSegmentList struct {
	Head *Node
}

type Node struct {
	Seg  *segment
	Next *Node
}

func NewOrderedSegmentList() *OrderedSegmentList {
	return &OrderedSegmentList{}
}

func (oll *OrderedSegmentList) Insert(seg *segment) {
	newNode := &Node{Seg: seg}

	if oll.Head == nil || oll.Head.Seg.Index >= seg.Index {
		newNode.Next = oll.Head
		oll.Head = newNode
		return
	}

	current := oll.Head
	for current.Next != nil && current.Next.Seg.Index < seg.Index {
		current = current.Next
	}

	newNode.Next = current.Next
	current.Next = newNode
}

// Find find segment which segment.index<=index and next segment.index>index
func (oll *OrderedSegmentList) Find(index uint64) *segment {
	current := oll.Head
	var prev *Node

	for current != nil && current.Seg.Index < index {
		prev = current
		current = current.Next
	}

	if current != nil && current.Seg.Index == index {
		return current.Seg
	}

	if prev != nil {
		return prev.Seg
	}

	return nil
}

// restore memory and truncate wal will use reader
type segmentReader struct {
	blocks       []byte
	blocksOffset int // current read pointer in blocks
	blocksNums   int // blocks  number
	curBlockNum  int // current block position in blocks
}

func NewSegmentReader(seg *segment) *segmentReader {
	blocks := alignedBlock(seg.blockNums)
	_, err := seg.Fd.Read(blocks)
	if err != nil {
		return nil
	}
	return &segmentReader{
		blocks:      blocks,
		blocksNums:  seg.blockNums,
		curBlockNum: InitialBlockNum,
	}
}

func (sr *segmentReader) ReadHeader() (eHeader marshal.WalEntryHeader, err error) {
	// todo chunkHeaderSlice应该池化减少GC
	buf := make([]byte, marshal.ChunkHeaderSize)
	copy(buf, sr.blocks[sr.blocksOffset:sr.blocksOffset+marshal.ChunkHeaderSize])
	eHeader = marshal.DecodeWALEntryHeader(buf)

	if eHeader.IsEmpty() {
		//whether the current block is the last block in blocks？
		if sr.curBlockNum == sr.blocksNums {
			return eHeader, errors.New("EOF")
		}

		//move to next block
		sr.curBlockNum++
		sr.blocksOffset = sr.curBlockNum * Block4096
		copy(buf, sr.blocks[sr.blocksOffset:sr.blocksOffset+marshal.ChunkHeaderSize])
		eHeader = marshal.DecodeWALEntryHeader(buf)
		if eHeader.IsEmpty() {
			return eHeader, errors.New("EOF")
		}
		sr.blocksOffset += marshal.ChunkHeaderSize
		return
	}
	sr.blocksOffset += marshal.ChunkHeaderSize
	return
}

func (sr *segmentReader) ReadEntry(header marshal.WalEntryHeader) (ent *pb.Entry, err error) {
	ent = new(pb.Entry)
	err = ent.Unmarshal(sr.blocks[sr.blocksOffset : sr.blocksOffset+header.EntrySize])
	if err != nil {
		log.Panicf("unmarshal", err)
	}
	return
}

func (sr *segmentReader) Next(entrySize int) {
	sr.blocksOffset += entrySize
}

// raftStateSegment need persist status: apply index 、commit index
type raftStateSegment struct {
	Fd           *os.File
	RaftState    pb.HardState
	AppliedIndex uint64
	Blocks       []byte
	closed       bool
}

// StateSegment  will encode state into a byte slice.
// +-------+-----------+-----------+
// |  crc  | state size|   state   |
// +-------+-----------+-----------+
// |----------HEADER---|---BODY----+
func (seg *raftStateSegment) encodeRaftStateSegment() []byte {
	return nil
}

func (seg *raftStateSegment) decodeRaftStateSegment() {
	return
}

func OpenRaftStateSegment(walDirPath, fileName string) (rSeg *raftStateSegment, err error) {
	fd, err := iooperator.OpenDirectIOFile(filepath.Join(walDirPath, fileName), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	_, err = fd.Seek(0, io.SeekStart)
	if err != nil {
		panic(fmt.Errorf("seek to the end of segment file %s failed: %v", ".SEG", err))
	}

	blockPool := NewBlockPool()
	rSeg = new(raftStateSegment)
	rSeg.Fd = fd
	rSeg.RaftState = pb.HardState{}
	rSeg.Blocks = blockPool.Block4
	fileInfo, _ := rSeg.Fd.Stat()

	//若fsize不为0读取文件的数据到block并序列化到pb.HardState
	if fileInfo.Size() > 0 {
		rSeg.Fd.Read(rSeg.Blocks)
		rSeg.decodeRaftStateSegment()
	}

	return rSeg, nil
}

func (seg *raftStateSegment) Flush() (err error) {
	data := seg.encodeRaftStateSegment()
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

func (seg *raftStateSegment) Remove() error {
	if !seg.closed {
		seg.closed = true
		_ = seg.Fd.Close()
	}

	return os.Remove(seg.Fd.Name())
}

func (seg *raftStateSegment) Close() error {
	if seg.closed {
		return nil
	}

	seg.closed = true
	return seg.Fd.Close()
}

type KVStateSegment struct {
	lock         sync.Mutex
	Fd           *os.File
	PersistIndex uint64
	AppliedIndex uint64
	Blocks       []byte
	closed       bool
}

func (seg *KVStateSegment) encodeKVStateSegment() []byte {
	return nil
}

func (seg *KVStateSegment) decodeKVStateSegment() {
	return
}

func OpenKVStateSegment(walDirPath, fileName string) (kvSeg *KVStateSegment, err error) {
	fd, err := iooperator.OpenDirectIOFile(filepath.Join(walDirPath, fileName), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	_, err = fd.Seek(0, io.SeekStart)
	if err != nil {
		panic(fmt.Errorf("seek to the end of segment file %s failed: %v", ".SEG", err))
	}

	blockPool := NewBlockPool()
	kvSeg = new(KVStateSegment)
	kvSeg.Fd = fd
	kvSeg.Blocks = blockPool.Block4
	fileInfo, _ := kvSeg.Fd.Stat()

	//若fsize不为0读取文件的数据到block并序列化到pb.HardState
	if fileInfo.Size() > 0 {
		kvSeg.Fd.Read(kvSeg.Blocks)
		kvSeg.decodeKVStateSegment()
	}

	return kvSeg, nil
}

func (seg *KVStateSegment) Flush() (err error) {
	seg.lock.Lock()
	defer seg.lock.Unlock()

	data := seg.encodeKVStateSegment()
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

func (seg *KVStateSegment) Remove() error {
	if !seg.closed {
		seg.closed = true
		_ = seg.Fd.Close()
	}

	return os.Remove(seg.Fd.Name())
}

func (seg *KVStateSegment) Close() error {
	if seg.closed {
		return nil
	}

	seg.closed = true
	return seg.Fd.Close()
}
