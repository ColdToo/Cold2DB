package wal

import (
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/pb"
	"os"
)

const (
	FileModePerm = 0644
	SegSuffix    = ".SEG"
	RaftSuffix   = ".RAFT-SEG"
	KVSuffix     = ".KV-SEG"
)

type WAL struct {
	Config           config.WalConfig
	ActiveSegment    *segment
	SegmentPipe      chan *segment
	OrderSegmentList *OrderedSegmentList
	RaftStateSegment *raftStateSegment //保存需要持久化的raft相关状态
	KVStateSegment   *KVStateSegment   //保存需要持久化的raft相关状态
}

func NewWal(config config.WalConfig) (*WAL, error) {
	acSegment, err := NewActSegmentFile(config.WalDirPath)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		Config:           config,
		OrderSegmentList: NewOrderedSegmentList(),
		ActiveSegment:    acSegment,
	}

	return wal, nil
}

func (wal *WAL) Write(entries []*pb.Entry) error {
	//segment文件应该尽量均匀，若此次entries太大那么直接写入新的segment文件中
	data := make([]byte, 0)
	bytesCount := 0

	for _, e := range entries {
		wEntBytes, n := marshal.EncodeWALEntry(e)
		data = append(data, wEntBytes...)
		bytesCount += n
	}

	// if the active segment file is full, sync it and create a new one.
	if wal.activeSegmentIsFull(bytesCount) {
		if err := wal.rotateActiveSegment(); err != nil {
			return err
		}
	}

	return wal.ActiveSegment.Write(data, bytesCount, entries[0].Index)
}

func (wal *WAL) activeSegmentIsFull(delta int) bool {
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
	wal.OrderSegmentList.Insert(wal.ActiveSegment)
	wal.ActiveSegment = newSegment
	return nil
}

func (wal *WAL) Truncate(index uint64) error {
	//truncate掉index之后的所有segment包括当前的active segment
	seg := wal.OrderSegmentList.Find(index)
	reader := NewSegmentReader(seg, 0, 0)
	header, err := reader.ReadHeader()
	if err != nil {
		return err
	}
	if header.Index == index {

	}

	reader.Next(header.EntrySize)
	return nil
}

func (wal *WAL) Close() error {
	for wal.OrderSegmentList.Head != nil {
		err := wal.OrderSegmentList.Head.Seg.Close()
		if err != nil {
			return err
		}
		wal.OrderSegmentList.Head = wal.OrderSegmentList.Head.Next
	}
	// close the active segment file.
	return wal.ActiveSegment.Close()
}

func (wal *WAL) Delete() error {
	for wal.OrderSegmentList.Head != nil {
		err := os.Remove(wal.OrderSegmentList.Head.Seg.Fd.Name())
		if err != nil {
			return err
		}
		wal.OrderSegmentList.Head = wal.OrderSegmentList.Head.Next
	}

	// delete the active segment file.
	return wal.ActiveSegment.Remove()
}
