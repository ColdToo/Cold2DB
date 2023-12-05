package wal

import (
	"fmt"
	"testing"
)

func TestCreateEntries(t *testing.T) {
	_, bytesCount := MarshalWALEntries(CreateEntries(5000, 250))
	fmt.Printf(CreatEntriesFmt, 1, 10, ConvertSize(bytesCount))
}

func TestWAL_Truncate(t *testing.T) {
	wal, err := NewWal(TestWALConfig1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = wal.Close()
		err = wal.Remove()
	}()
	ents := SplitEntries(10000, Entries61MB)
	for _, e := range ents {
		err = wal.Write(e)
		if err != nil {
			t.Fatal(err)
		}
	}
	wal.Truncate(25000)
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
