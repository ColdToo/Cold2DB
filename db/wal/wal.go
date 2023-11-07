package wal

import (
	"bytes"
	"errors"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/pb"
	"io"
	"os"
	"sort"
	"sync"
)

const (
	// ChunkHeaderSize
	// Checksum Length  index
	//    4       3       8
	ChunkHeaderSize = 15
	FileModePerm    = 0644
	SegSuffix       = ".SEG"
)

var (
	ErrValueTooLarge       = errors.New("the data size can't larger than segment size")
	ErrPendingSizeTooLarge = errors.New("the upper bound of pendingWrites can't larger than segment size")
)

type WAL struct {
	Config         config.WalConfig
	ActiveSegment  *segment
	OlderSegments  map[SegmentID]*segment
	SegmentPipe    chan *segment
	OrderSegmentList OrderedSegmentList
	RaftStateSegment    *segment //保存需要持久化的raft状态
	RaftHardState  pb.HardState //需要持久化的状态
}

type Reader struct {
	segmentReaders []*segmentReader
	currentReader  int
}

func (r *Reader) Next() ([]byte, error) {
	if r.currentReader >= len(r.segmentReaders) {
		return nil, nil, io.EOF
	}

	data, position, err := r.segmentReaders[r.currentReader].Next()
	if err == io.EOF {
		r.currentReader++
		return r.Next()
	}
	return data, position, err
}

func NewWal(config config.WalConfig) (*WAL, error) {
	wal := &WAL{
		Config:        config,
		OlderSegments: make(map[SegmentID]*segment),
	}

	acSegment, err := NewSegmentFile(config.WalDirPath)
	if err != nil {
		return nil, err
	}
	wal.ActiveSegment = acSegment

	return wal, nil
}

func (wal *WAL) NewReaderWithMax(segId SegmentID) *Reader {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	// get all segment readers.
	var segmentReaders []*wal.segmentReader
	for _, segment := range wal.olderSegments {
		if segId == 0 || segment.id <= segId {
			reader := segment.NewReader()
			segmentReaders = append(segmentReaders, reader)
		}
	}
	if segId == 0 || wal.ActiveSegment.id <= segId {
		reader := wal.ActiveSegment.NewReader()
		segmentReaders = append(segmentReaders, reader)
	}

	// sort the segment readers by segment id.
	sort.Slice(segmentReaders, func(i, j int) bool {
		return segmentReaders[i].segment.id < segmentReaders[j].segment.id
	})

	return &Reader{
		segmentReaders: segmentReaders,
		currentReader:  0,
	}
}

func (wal *WAL) NewReader() *Reader {
	return wal.NewReaderWithMax(0)
}

func (wal *WAL) Write(entries []*pb.Entry) error {
	//segment文件应该尽量均匀，若此次entries太大那么直接写入新的segment文件中

	// if the active segment file is full, sync it and create a new one.
	if wal.activeSegmentIsFull(int64(len(data))) {
		if err := wal.rotateActiveSegment(); err != nil {
			return nil, err
		}
	}

	// write the data to the active segment file.
	err := wal.ActiveSegment.Write(data)
	if err != nil {
		return err
	}

	return
}

func (wal *WAL) activeSegmentIsFull(delta int64) bool {
	//应尽可能使segment大小均匀，这样查找能提高查找某个entry的效率
	actSegSize := wal.ActiveSegment.Size()
	comSize := actSegSize + delta
	if comSize > wal.Config.SegmentSize {
		if actSegSize*2 > wal.Config.SegmentSize {
			return false
		}
	}
	return true
}

func (wal *WAL) rotateActiveSegment() error {
	//从active pipeline获取已经创建好的pipeline
	newSegment := <-wal.SegmentPipe
	wal.OrderIndexList.Insert(wal.ActiveSegment.index, wal.ActiveSegment.Fd)
	wal.ActiveSegment = newSegment
	return nil
}

func (wal *WAL) PersistRaftStatus(state pb.HardState) {
	wal.RaftHardState = state
	marshal, err := wal.RaftHardState.Marshal()
	if err != nil {
		return
	}
	bytes.NewBuffer()
	wal.RaftSegment.
}

func (wal *WAL) Truncate(index int) error {
	return nil
}

func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	for wal.OrderIndexList.Head != nil {
		err := wal.OrderIndexList.Head.Data.Value.Close()
		if err != nil {
			return err
		}
		wal.OrderIndexList.Head = wal.OrderIndexList.Head.Next
	}
	// close the active segment file.
	return wal.ActiveSegment.Close()
}

func (wal *WAL) Delete() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	for wal.OrderIndexList.Head != nil {
		err := os.Remove(wal.OrderIndexList.Head.Data.Value.Name())
		if err != nil {
			return err
		}
		wal.OrderIndexList.Head = wal.OrderIndexList.Head.Next
	}

	// delete the active segment file.
	return wal.ActiveSegment.Remove()
}
