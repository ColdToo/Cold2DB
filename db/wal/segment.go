package wal

import (
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/db/iooperator/directio"
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
	DefaultIndex    = 0
	InitialBlockNum = 1
)

var (
	ErrClosed     = errors.New("the segment file is closed")
	ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
)

type segment struct {
	Index     uint64 //该segment文件中的最小log index
	Fd        *os.File
	blockPool *BlockPool

	blocks        []byte //当前segment使用的blocks
	blockNums     int    //记录当前segment的blocks数量,也可以作为segment的偏移量使用
	segmentOffset int    //当前segment的偏移量

	blocksOffset     int //当前Blocks的偏移量
	BlocksRemainSize int //当前Blocks剩余可以写字节数
	closed           bool
}

func NewActSegmentFile(dirPath string) (*segment, error) {
	fd, err := directio.OpenDirectIOFile(SegmentFileName(dirPath, 0>>1), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	_, err = fd.Seek(0, io.SeekStart)
	if err != nil {
		log.Panicf("seek to the end of segment file %s failed: %v", ".SEG", err)
	}

	blockPool := NewBlockPool()
	//default use 4 block as new active segment blocks
	return &segment{
		Index:            DefaultIndex,
		Fd:               fd,
		blocks:           blockPool.Block4,
		blocksOffset:     0,
		BlocksRemainSize: Block4,
		blockNums:        num4,
		segmentOffset:    0,
		blockPool:        NewBlockPool(),
	}, nil
}

func OpenOldSegmentFile(walDirPath string, index uint64) (*segment, error) {
	fd, err := directio.OpenDirectIOFile(SegmentFileName(walDirPath, index), os.O_CREATE|os.O_RDWR, 0644)
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

	if err = seg.Flush(bytesCount); err == nil && seg.Index == DefaultIndex {
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

// restore memory and truncate wal will use reader
type segmentReader struct {
	persistIndex uint64
	appliedIndex uint64
	blocks       []byte
	blocksOffset int
	blocksNums   int
	curBlockNum  int
}

func NewSegmentReader(seg *segment, persistIndex, appliedIndex uint64) *segmentReader {
	blocks := alignedBlock(seg.blockNums)
	_, err := seg.Fd.Read(blocks)
	if err != nil {
		return nil
	}
	return &segmentReader{
		persistIndex: persistIndex,
		appliedIndex: appliedIndex,
		blocks:       blocks,
		blocksNums:   seg.blockNums,
		curBlockNum:  InitialBlockNum,
	}
}

func (sr *segmentReader) ReadHeaderAndNext() (eHeader marshal.WalEntryHeader, err error) {
	// todo chunkHeaderSlice应该作为pool
	buf := make([]byte, marshal.ChunkHeaderSize)
	copy(buf, sr.blocks[sr.blocksOffset:sr.blocksOffset+marshal.ChunkHeaderSize])

	eHeader = marshal.DecodeWALEntryHeader(buf)

	//如果header为空
	if eHeader.IsEmpty() {
		//当前是否是最后一个block？
		if sr.curBlockNum == sr.blocksNums {
			return eHeader, errors.New("EOF")
		}
		//移动到下一个block开始读取
		sr.curBlockNum++
		sr.blocksOffset = sr.curBlockNum * Block4096
		eHeader = marshal.DecodeWALEntryHeader(buf)
		if eHeader.IsEmpty() {
			return eHeader, errors.New("EOF")
		}
		sr.blocksOffset += eHeader.EntrySize + marshal.ChunkHeaderSize
		return
	}
	sr.blocksOffset += eHeader.EntrySize + marshal.ChunkHeaderSize
	return
}

func (sr *segmentReader) ReadEntries() (ents []*pb.Entry, err error) {
	for {
		header, err := sr.ReadHeaderAndNext()
		if err.Error() == "EOF" {
			break
		}
		b := make([]byte, header.EntrySize)
		ent := new(pb.Entry)
		ent.Unmarshal(b)
		ents = append(ents, ent)
	}
	return
}

func (sr *segmentReader) ReadKVs(kvC chan *marshal.KV, errC chan error) {
	for {
		header, err := sr.ReadHeaderAndNext()
		if err.Error() == "EOF" {
			errC <- err
			break
		}
		b := make([]byte, header.EntrySize)

		ent := new(pb.Entry)
		ent.Unmarshal(b)
		kv := marshal.GobDecode(ent.Data)
		kvC <- &kv
	}
}

// StateSegment need persist status: persist index、 apply index 、raft hardState
type StateSegment struct {
	lock         sync.Mutex
	Fd           *os.File
	RaftState    pb.HardState
	PersistIndex uint64
	AppliedIndex uint64
	raftBlocks   []byte
	kvBlocks     []byte
	closed       bool
}

// StateSegment  will encode state into a byte slice.
// +-------+-----------+-----------+
// |  crc  | state size|   state(RaftState、PersistIndex、AppliedIndex) |
// +-------+-----------+-----------+
// |----------HEADER---|---BODY----+
func (seg *StateSegment) encodeStateSegment() []byte {
	return nil
}

func (seg *StateSegment) decodeStateSegment() {
	return
}

func OpenStateSegmentFile(walDirPath, fileName string) (rSeg *StateSegment, err error) {
	fd, err := directio.OpenDirectIOFile(filepath.Join(walDirPath, fileName), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	_, err = fd.Seek(0, io.SeekStart)
	if err != nil {
		panic(fmt.Errorf("seek to the end of segment file %s failed: %v", ".SEG", err))
	}

	blockPool := NewBlockPool()
	rSeg = new(StateSegment)
	rSeg.Fd = fd
	rSeg.RaftState = pb.HardState{}
	rSeg.raftBlocks = blockPool.Block4
	rSeg.raftBlocks = blockPool.Block4
	fileInfo, _ := rSeg.Fd.Stat()

	//若fsize不为0读取文件的数据到block并序列化到pb.HardState
	if fileInfo.Size() > 0 {
		rSeg.Fd.Read(rSeg.raftBlocks)
		rSeg.decodeStateSegment()
	}

	return rSeg, nil
}

func (seg *StateSegment) Flush() (err error) {
	seg.lock.Lock()
	defer seg.lock.Unlock()

	data := seg.encodeStateSegment()
	copy(seg.raftBlocks[0:len(data)], data)
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

func (seg *StateSegment) Remove() error {
	if !seg.closed {
		seg.closed = true
		_ = seg.Fd.Close()
	}

	return os.Remove(seg.Fd.Name())
}

func (seg *StateSegment) Close() error {
	if seg.closed {
		return nil
	}

	seg.closed = true
	return seg.Fd.Close()
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
