package wal

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/pb"
	"os"
	"testing"
)

const CreatEntriesFmt = "create entries nums %d, data length %d, bytes count %s"

var Entries61MB = CreateEntries(500000, 100)

func CreateEntries(num int, length int) []*pb.Entry {
	entries := make([]*pb.Entry, num)
	for i := 0; i < num; i++ {
		entry := &pb.Entry{
			Term:  uint64(i + 1),
			Index: uint64(i + 1),
			Type:  pb.EntryNormal,
			Data:  generateData(length),
		}
		entries[i] = entry
	}
	return entries
}

func generateData(length int) []byte {
	data := make([]byte, length)
	for i := 0; i < length; i++ {
		data[i] = 'a'
	}
	return data
}

func ConvertSize(size int) string {
	units := []string{"B", "KB", "MB", "GB"}
	if size == 0 {
		return "0" + units[0]
	}
	i := 0
	for size >= 1024 {
		size /= 1024
		i++
	}
	return fmt.Sprintf("%.f", float64(size)) + units[i]
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

func TestCreateEntries(t *testing.T) {
	_, bytesCount := MarshalWALEntries(CreateEntries(500000, 100))
	fmt.Printf(CreatEntriesFmt, 1, 10, ConvertSize(bytesCount))
}

var walDirPath, _ = os.Getwd()

var TestWALConfig = config.WalConfig{
	WalDirPath:  walDirPath,
	SegmentSize: 64,
}

func TestWAL_Truncate(t *testing.T) {

}

func TestWAL_Write(t *testing.T) {
	var err error
	defer func() {
		if err != nil {
			t.Log(err)
		}
	}()
	wal, err := NewWal(TestWALConfig)
	if err != nil {
		t.Fatal(err)
	}
	err = wal.Write(Entries61MB)
	err = wal.Close()
	err = wal.Remove()
}
