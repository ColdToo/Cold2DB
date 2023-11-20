package wal

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/pb"
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

func TestNewActSegmentFile(t *testing.T) {
	segment, err := NewActSegmentFile(TestDirPath)
	if err != nil {
		t.Errorf("Expected nil, but got %v", err)
	}
	segment.Close()
	segment.Remove()
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

func TestSegment_Write(t *testing.T) {
	segment, err := NewActSegmentFile(TestDirPath)
	if err != nil {
		t.Errorf("Expected nil, but got %v", err)
	}

	//segment文件应该尽量均匀，若此次entries太大那么直接写入新的segment文件中
	data := make([]byte, 0)
	bytesCount := 0
	for _, e := range entries1 {
		wEntBytes, n := marshal.EncodeWALEntry(e)
		data = append(data, wEntBytes...)
		bytesCount += n
	}
	segment.Write(data, bytesCount, entries1[0].Index)
}

func TestSegmentReader_ReadEntries(t *testing.T) {

}

func TestOpenOldSegmentFile(t *testing.T) {
	NewSegmentReader()
	OpenOldSegmentFile(TestDirPath, 1)
}
