package wal

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var TestDirPath, _ = os.Getwd()

var entries1 = []*pb.Entry{
	{
		Term:  1,
		Index: 1,
		Type:  pb.EntryNormal,
		Data:  []byte("hello world1"),
	},
	{
		Term:  2,
		Index: 2,
		Type:  pb.EntryNormal,
		Data:  []byte("hello world2"),
	},
	{
		Term:  3,
		Index: 3,
		Type:  pb.EntryNormal,
		Data:  []byte("hello world3"),
	},
	{
		Term:  4,
		Index: 4,
		Type:  pb.EntryNormal,
		Data:  []byte("hello world4"),
	},
}

func TestSegmentFileName(t *testing.T) {
	var id int64
	segmentName := SegmentFileName("tmp", 1)
	_, err := fmt.Sscanf(segmentName, "%d.SEG", &id)
	if err != nil {
		fmt.Println("提取失败:", err)
	}
	fmt.Println(segmentName)
	fmt.Println(id)
}

func TestSegmentFile_NewSegmentFile(t *testing.T) {
	segment, err := NewSegmentFile(TestWALConfig1.WalDirPath, TestWALConfig1.SegmentSize)
	if err != nil {
		t.Errorf("Expected nil, but got %v", err)
	}
	assert.EqualValues(t, segment.Index, DefaultMinLogIndex)
	assert.EqualValues(t, segment.Size(), Block4)
	assert.EqualValues(t, segment.BlocksRemainSize, Block4)
	assert.EqualValues(t, segment.Fd.Name(), SegmentFileName(TestDirPath, DefaultMinLogIndex))
	assert.EqualValues(t, segment.blocksOffset, 0)
	segment.Close()
	segment.Remove()
}

func TestSegmentFile_Write(t *testing.T) {
	segment, err := NewSegmentFile(TestWALConfig1.WalDirPath, TestWALConfig1.SegmentSize)
	if err != nil {
		t.Errorf("Expected nil, but got %v", err)
	}

	//todo 测试不同分支的write
	// 1、第一次就写入超过block4096*4的场景
	data, bytesCount := MarshalWALEntries(entries1)
	segment.Write(data, bytesCount, entries1[0].Index)

	assert.EqualValues(t, len(segment.blocks), bytesCount+segment.BlocksRemainSize)
	assert.EqualValues(t, bytesCount, segment.blocksOffset)
}

//

func MockSegmentWrite(entries []*pb.Entry) *segment {
	segment, _ := NewSegmentFile(TestWALConfig1.WalDirPath, TestWALConfig1.SegmentSize)
	data, bytesCount := MarshalWALEntries(entries)
	segment.Write(data, bytesCount, entries[0].Index)
	return segment
}

func TestSegmentReader_Block4(t *testing.T) {
	segment := MockSegmentWrite(entries1)
	seg, err := OpenOldSegmentFile(TestDirPath, segment.Index)
	if err != nil {
		t.Error(err)
	}
	reader := NewSegmentReader(seg)
	ents := make([]*pb.Entry, 0)
	//确保读出的数据正确
	assert.EqualValues(t, segment.blocks, reader.blocks)
	for {
		header, err := reader.ReadHeader()
		if err != nil && err.Error() == "EOF" {
			break
		}
		entry, err := reader.ReadEntry(header)
		if err != nil {
			t.Error(err)
		}
		reader.Next(header.EntrySize)
		ents = append(ents, entry)
	}
	assert.EqualValues(t, entries1, ents)
}

//

func TestOrderedSegmentList(t *testing.T) {
	// Create a new OrderedSegmentList
	oll := NewOrderedSegmentList()

	// Create some segments
	seg1 := &segment{Index: 1}
	seg2 := &segment{Index: 2}
	seg3 := &segment{Index: 3}
	seg4 := &segment{Index: 5}
	seg5 := &segment{Index: 9}
	seg6 := &segment{Index: 6}

	// Insert segments into the OrderedSegmentList
	oll.Insert(seg2)
	oll.Insert(seg1)
	oll.Insert(seg3)
	oll.Insert(seg5)
	oll.Insert(seg6)
	oll.Insert(seg4)

	// Test Find method
	foundSeg := oll.Find(2)
	if foundSeg != seg2 {
		t.Errorf("Expected segment with index 2, but got segment with index %d", foundSeg.Index)
	}

	// Test Find method with non-existent index
	FoundSeg := oll.Find(4)
	if FoundSeg.Index != 3 {
		t.Errorf("Expected segment with index 3, but got segment with index %d", FoundSeg.Index)
	}

	for oll.Head != nil {
		fmt.Println(oll.Head.Seg.Index)
		oll.Head = oll.Head.Next
	}
}
