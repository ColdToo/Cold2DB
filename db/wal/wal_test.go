package wal

import (
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWAL_Truncate(t *testing.T) {
	truncateIndex := uint64(25000)
	wal, err := NewWal(TestWALConfig1)
	if err != nil {
		t.Fatal(err)
	}
	ents := SplitEntries(10000, Entries61MB)
	for _, e := range ents {
		err = wal.Write(e)
		if err != nil {
			t.Fatal(err)
		}
	}

	truncBeforeSeg := wal.OrderSegmentList.Find(truncateIndex)
	entries1 := readEntriesBySeg(truncBeforeSeg)

	wal.Truncate(truncateIndex)

	truncAfterSeg := wal.OrderSegmentList.Find(truncateIndex)
	entries2 := readEntriesBySeg(truncAfterSeg)

	entries3 := make([]*pb.Entry, 0)
	for i := 0; i < len(entries1); i++ {
		if entries1[i].Index <= truncateIndex {
			entries3 = append(entries3, entries1[i])
		}
	}

	assert.EqualValues(t, entries2, entries3)
	err = wal.Close()
	err = wal.Remove()
}

func TestWAL_Write(t *testing.T) {
	var err error
	defer func() {
		if err != nil {
			t.Log(err)
		}
	}()
	wal, err := NewWal(TestWALConfig64)
	if err != nil {
		t.Fatal(err)
	}
	err = wal.Write(Entries61MB)
	err = wal.Close()
	err = wal.Remove()
}
