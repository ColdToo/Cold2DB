package db

import (
	"github.com/ColdToo/Cold2DB/db/logfile"
	"path/filepath"
	"testing"
	"time"
)

func TestCold2DB_OpenMemtable(t *testing.T) {
	path, err := filepath.Abs("dbtest/walfile")
	if err != nil {
		t.Log(err)
	}
	memOpt := memOpt{
		walDirPath: path,
		walFileId:  time.Now().Unix(),
		fsize:      2048,
		ioType:     logfile.MMap,
		memSize:    2048,
	}

	wal, err := logfile.OpenLogFile(memOpt.walDirPath, memOpt.walFileId, memOpt.fsize, logfile.WALLog, memOpt.ioType)
	if err != nil {
		return
	}
	entry := &logfile.Entry{
		ExpiredAt: 1234567890,
		Index:     1,
		Term:      2,
		Type:      logfile.TypeDelete,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}

	walByteEncode, _ := entry.EncodeWALEntry()
	err = wal.Write(walByteEncode)
	if err != nil {
		t.Log(err)
	}

	// Open the memtable
	memManager := &memManager{}
	memtable, err := memManager.openMemtable(memOpt)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the contents of the memtable
	found, value := memtable.get(entry.Key)
	if !found {
		t.Fatal("Entry not found in memtable")
	}
	if string(value) != string(entry.Value) {
		t.Fatalf("Expected value %s, but got %s", string(entry.Value), string(value))
	}
}
