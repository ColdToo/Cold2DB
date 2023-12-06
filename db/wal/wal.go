package wal

import (
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"os"
	"path/filepath"
	"strings"
)

const (
	FileModePerm = 0644
	SegSuffix    = ".SEG"
	TMPSuffix    = ".TMP"
	RaftSuffix   = ".RAFT-SEG"
	KVSuffix     = ".KV-SEG"
)

type WAL struct {
	WalDirPath    string
	SegmentSize   int
	ActiveSegment *segment
	SegmentPipe   chan *segment
	//todo ordered segment 是否会有并发问题
	OrderSegmentList *OrderedSegmentList
	RaftStateSegment *raftStateSegment //保存需要持久化的raft相关状态
	KVStateSegment   *KVStateSegment   //保存需要持久化的kv相关状态
}

func NewWal(config config.WalConfig) (*WAL, error) {
	acSegment, err := NewSegmentFile(config.WalDirPath, config.SegmentSize)
	if err != nil {
		return nil, err
	}

	segmentPipe := make(chan *segment, 5)

	// segment pipeline boosts the throughput of the WAL.
	go func() {
		for {
			acSegment, err = NewSegmentFile(config.WalDirPath, config.SegmentSize)
			if err != nil {
				log.Panicf("create a new segment file error", err)
			}
			segmentPipe <- acSegment
		}
	}()

	wal := &WAL{
		WalDirPath:       config.WalDirPath,
		SegmentSize:      config.SegmentSize * 2 * 1024 * 1024,
		OrderSegmentList: NewOrderedSegmentList(),
		ActiveSegment:    acSegment,
		SegmentPipe:      segmentPipe,
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

	// if current active segment file is full, create a new one.
	if wal.activeSegmentIsFull(bytesCount) {
		wal.rotateActiveSegment()
	}

	return wal.ActiveSegment.Write(data, bytesCount, entries[0].Index)
}

func (wal *WAL) activeSegmentIsFull(delta int) bool {
	//应尽可能使segment大小均匀，这样查找能提高查找某个entry的效率
	//active segment size 是根据目前已分配的block数量来计算的，而不是实际数据占用空间
	actSegSize := wal.ActiveSegment.Size()
	totalSize := actSegSize + delta

	// 1、total size > wal.segmentSize
	// 2、delta > wal.SegmentSize*0.5
	// 3、已经分配了内存空间
	return totalSize > wal.SegmentSize && float64(delta) > float64(wal.SegmentSize)*0.5 && wal.ActiveSegment.blockNums != 0
}

func (wal *WAL) rotateActiveSegment() {
	//从active pipeline获取已经创建好的pipeline
	newSegment := <-wal.SegmentPipe
	wal.OrderSegmentList.Insert(wal.ActiveSegment)
	wal.ActiveSegment = newSegment
}

// Truncate truncate掉index之后的所有segment包括当前的active segment
func (wal *WAL) Truncate(index uint64) error {
	wal.OrderSegmentList.Insert(wal.ActiveSegment)
	wal.ActiveSegment = <-wal.SegmentPipe

	seg := wal.OrderSegmentList.Find(index)
	reader := NewSegmentReader(seg)
	for {
		header, err := reader.ReadHeader()
		if err != nil {
			return err
		}
		reader.Next(header.EntrySize)
		if header.Index == index {
			err = seg.Fd.Truncate(int64(reader.blocksOffset))
			if err != nil {
				log.Errorf("Truncate segment file failed", err)
			}
			break
		}
	}

	wal.OrderSegmentList.truncate(index)
	return nil
}

func (wal *WAL) Close() error {
	node := wal.OrderSegmentList.Head
	for node != nil {
		err := node.Seg.Close()
		if err != nil {
			return err
		}
		node = node.Next
	}
	// close the active segment file.
	return wal.ActiveSegment.Close()
}

func (wal *WAL) Remove() error {
	node := wal.OrderSegmentList.Head
	for node != nil {
		err := node.Seg.Remove()
		if err != nil {
			return err
		}
		node = node.Next
	}

	wal.ActiveSegment.Remove()

	filepath.Walk(wal.WalDirPath, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && strings.HasSuffix(path, ".TMP") {
			os.Remove(path)
		}
		return nil
	})
	return nil
}
