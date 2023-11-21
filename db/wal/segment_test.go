package wal

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/db/marshal"
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
		Data:  []byte("hello world"),
	},
	{
		Term:  2,
		Index: 2,
		Type:  pb.EntryNormal,
		Data:  []byte("hello world"),
	},
	{
		Term:  3,
		Index: 3,
		Type:  pb.EntryNormal,
		Data:  []byte("hello world"),
	},
	{
		Term:  4,
		Index: 4,
		Type:  pb.EntryNormal,
		Data:  []byte("hello world"),
	},
}

func MarshalWALEntries(entries1 []*pb.Entry) (data []byte, bytesCount int) {
	data = make([]byte, 0)
	for _, e := range entries1 {
		wEntBytes, n := marshal.EncodeWALEntry(e)
		data = append(data, wEntBytes...)
		bytesCount += n
	}
	return
}

func TestNewActSegmentFile(t *testing.T) {
	segment, err := NewActSegmentFile(TestDirPath)
	if err != nil {
		t.Errorf("Expected nil, but got %v", err)
	}
	assert.EqualValues(t, segment.Index, DefaultIndex)
	assert.EqualValues(t, segment.Size(), Block4)
	assert.EqualValues(t, segment.BlocksRemainSize, Block4)
	assert.EqualValues(t, segment.Fd.Name(), SegmentFileName(TestDirPath, DefaultIndex))
	assert.EqualValues(t, segment.blocksOffset, 0)
	segment.Close()
	segment.Remove()
}

func TestSegment_Write(t *testing.T) {
	segment, err := NewActSegmentFile(TestDirPath)
	if err != nil {
		t.Errorf("Expected nil, but got %v", err)
	}

	data, bytesCount := MarshalWALEntries(entries1)
	segment.Write(data, bytesCount, entries1[0].Index)

	assert.EqualValues(t, len(segment.blocks), bytesCount+segment.BlocksRemainSize)
	assert.EqualValues(t, bytesCount, segment.blocksOffset)
}

func TestOpenOldSegmentFile(t *testing.T) {
	TestSegment_Write(t)
	_, err := OpenOldSegmentFile(TestDirPath, entries1[0].Index)
	if err != nil {
		t.Errorf("Expected nil, but got %v", err)
	}
}

func TestSegmentReader_ReadHeaderAndNext(t *testing.T) {
	TestSegment_Write(t)
	oldSeg, err := OpenOldSegmentFile(TestDirPath, entries1[0].Index)
	if err != nil {
		t.Errorf("Expected nil, but got %v", err)
	}
	reader := NewSegmentReader(oldSeg, 0, 0)
	for {
		header, err := reader.ReadHeader()
		if err != nil && err.Error() == "EOF" {
			return
		}
		reader.Next(header.EntrySize)
		fmt.Println(header.Index)
	}
}

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
