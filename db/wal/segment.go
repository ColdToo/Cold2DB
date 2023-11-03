package wal

import (
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/db/iooperator/directio"
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
	index               int64
	Fd                  *os.File
	blockPool           *BlockPool
	closed              bool
	blockNums           int
	currBlock           []byte
	currBlockSize       int
	currBlockRemainSize int
}

type segmentReader struct {
	segment     *segment
	blockNumber uint32
	chunkOffset int64
}

func NewSegmentFile(dirPath string) (*segment, error) {
	fd, err := directio.OpenDirectIOFile(SegmentFileName(dirPath, SegSuffix, 0>>1), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
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

func SegmentFileName(WaldirPath string, extName string, index uint) string {
	return filepath.Join(WaldirPath, fmt.Sprintf("%014d"+extName, index))
}

func (seg *segment) NewReader() *segmentReader {
	return &segmentReader{
		segment:     seg,
		blockNumber: 0,
		chunkOffset: 0,
	}
}

// Remove removes the segment file.
func (seg *segment) Remove() error {
	if !seg.closed {
		seg.closed = true
		_ = seg.Fd.Close()
	}

	return os.Remove(seg.Fd.Name())
}

// Close closes the segment file.
func (seg *segment) Close() error {
	if seg.closed {
		return nil
	}

	seg.closed = true
	return seg.Fd.Close()
}

func (seg *segment) Size() int64 {
	return int64(seg.blockNums * Block4096)
}

func (seg *segment) Write(data []byte) (err error) {
	//如果当前block能够写入
	if len(data)+seg.currBlockRemainSize < seg.currBlockSize {
		seg.currBlock = append(seg.currBlock, data...)
		seg.currBlockRemainSize = seg.currBlockRemainSize - len(data)
		seg.Flush()
	} else {
		seg.blockPool.PutBlock(seg.currBlock)
		//分配新的block
		seg.currBlock, nums = seg.blockPool.AlignedBlock(len(data))
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
