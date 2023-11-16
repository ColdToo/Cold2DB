package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/db/iooperator/directio"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/pb"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
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
	closed           bool
}

func NewActSegmentFile(dirPath string) (*segment, error) {
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
		Index:            DefaultIndex,
		Fd:               fd,
		Blocks:           blockPool.getBlock4(),
		BlocksRemainSize: Block4,
		blockNums:        num4,
		BlocksOffset:     0,
		blockPool:        NewBlockPool(),
	}, nil
}

func OpenOldSegmentFile(walDirPath string, index int64) (*segment, error) {
	fd, err := directio.OpenDirectIOFile(SegmentFileName(walDirPath, index), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	fileInfo, _ := fd.Stat()
	fSize := fileInfo.Size() / Block4096
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

func SegmentFileName(walDirPath string, index int64) string {
	return filepath.Join(walDirPath, fmt.Sprintf("%014d"+SegSuffix, index))
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

// restore memory and truncate wal will use reader
type segmentReader struct {
	persistIndex int64
	appliedIndex int64
	segment      *segment
	buffer       *bytes.Reader

	blocksNums      int //缓冲区大小4096*blocksNums
	curBlocksOffset int //当前已经读到第几个block了
}

func NewSegmentReader(seg *segment, persistIndex, appliedIndex int64) *segmentReader {
	blocks := alignedBlock(seg.blockNums)
	seg.Fd.Write(blocks)
	buffer := bytes.NewReader(blocks)
	return &segmentReader{
		segment:      seg,
		persistIndex: persistIndex,
		appliedIndex: appliedIndex,
		buffer:       buffer,
		blocksNums:   seg.blockNums,
	}
}

func (sr *segmentReader) ReadHeaderAndNext() (eHeader marshal.WalEntryHeader, err error) {
	buf := make([]byte, marshal.ChunkHeaderSize)
	sr.buffer.Read(buf)
	eHeader = marshal.DecodeWALEntryHeader(buf)

	//如果header为空
	if eHeader.IsEmpty() {
		//当前是否是最后一个block？
		if sr.curBlocksOffset == sr.blocksNums {
			return eHeader, errors.New("EOF")
		}
		//移动到下一个blocks开始读取
		sr.curBlocksOffset++
		sr.buffer.Seek(int64(sr.curBlocksOffset*Block4096), io.SeekStart)
		eHeader = marshal.DecodeWALEntryHeader(buf)
		if eHeader.IsEmpty() {
			panic("this branch should not happen")
		}
		sr.buffer.Seek(int64(eHeader.EntrySize+marshal.ChunkHeaderSize), io.SeekCurrent)
		return
	}

	sr.buffer.Seek(int64(eHeader.EntrySize+marshal.ChunkHeaderSize), io.SeekCurrent)
	return
}

func (sr *segmentReader) ReadEntries() (ents []*pb.Entry, err error) {
	for {
		header, err := sr.ReadHeaderAndNext()
		if err.Error() == "EOF" {
			break
		}
		b := make([]byte, header.EntrySize)
		sr.buffer.Seek(int64(header.EntrySize), io.SeekCurrent)
		sr.buffer.Read(b)
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
		sr.buffer.Seek(int64(header.EntrySize), io.SeekCurrent)
		sr.buffer.Read(b)

		ent := new(pb.Entry)
		ent.Unmarshal(b)
		kv := marshal.GobDecode(ent.Data)
		kvC <- &kv
	}
}

// StateSegment need persist status: persist index、 apply index 、raft hardState
type StateSegment struct {
	lock            sync.Mutex
	Fd              *os.File
	blockPool       *BlockPool
	RaftState       pb.HardState
	PersistIndex    int64
	AppliedIndex    int64
	Blocks          []byte
	BlockSize       int
	BlockRemainSize int
	closed          bool
}

// StateSegment  will encode state into a byte slice.
// +-------+-----------+-----------+
// |  crc  | state size|   state(RaftState、PersistIndex、AppliedIndex) |
// +-------+-----------+-----------+
// |----------HEADER---|---BODY----+
func (seg *StateSegment) encodeStateSegment() []byte {
	stBytes, _ := st.Marshal()
	stBytesSize := len(stBytes)
	var size = RaftChunkHeaderSize + stBytesSize
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[Crc32Size:], uint32(stBytesSize))
	copy(buf[Crc32Size+StateSize:], stBytes)

	// crc32
	crc := crc32.ChecksumIEEE(buf[Crc32Size+StateSize:])
	binary.LittleEndian.PutUint32(buf[:4], crc)
	return buf
}

func (seg *StateSegment) decodeStateSegment() {
	crc32 := binary.LittleEndian.Uint32(buf[:Crc32Size])
	stateSize := binary.LittleEndian.Uint16(buf[Crc32Size : Crc32Size+StateSize])
	header.crc32 = int32(crc32)
	header.StateSize = int32(stateSize)
	header.HeaderSize = RaftChunkHeaderSize
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
	rSeg.blockPool = blockPool
	rSeg.Blocks = blockPool.Block4
	fileInfo, _ := rSeg.Fd.Stat()

	//若fsize不为0读取文件的数据到block并序列化到pb.HardState
	if fileInfo.Size() > 0 {
		rSeg.Fd.Read(rSeg.Blocks)
		rSeg.decodeStateSegment()
	}

	return rSeg, nil
}

func (seg *StateSegment) Persist() (err error) {
	seg.lock.Lock()
	defer seg.lock.Unlock()

	data := seg.encodeStateSegment()
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

func (oll *OrderedSegmentList) Find(index int64) *segment {
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
