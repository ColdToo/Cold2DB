package wal

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/pb"
	"math/rand"
	"os"
	"time"
)

const CreatEntriesFmt = "create entries nums %d, data length %d, bytes count %s"

var Entries61MB = CreateEntries(50000, 250)
var Entries133MB = CreateEntries(500000, 250)
var Entries1MB = CreateEntries(5000, 250)
var Entries5 = CreateEntries(5, 250)

var TestWalDirPath, _ = os.Getwd()

var TestWALConfig64 = config.WalConfig{
	WalDirPath:  TestWalDirPath,
	SegmentSize: 64,
}

var TestWALConfig1 = config.WalConfig{
	WalDirPath:  TestWalDirPath,
	SegmentSize: 1,
}

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

func SplitEntries(interval int, entries []*pb.Entry) [][]*pb.Entry {
	totalEntries := make([][]*pb.Entry, 0)
	count := 0
	subEntries := make([]*pb.Entry, 0)
	for _, e := range entries {
		if count <= interval {
			subEntries = append(subEntries, e)
			count++
		} else {
			totalEntries = append(totalEntries, subEntries)
			subEntries = make([]*pb.Entry, 0)
			count = 0
		}
	}
	return totalEntries
}

func generateData(length int) []byte {
	rand.Seed(time.Now().UnixNano())
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	data := make([]byte, length)
	for i := 0; i < length; i++ {
		data[i] = byte(rand.Intn(len(letterBytes)))
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

func MockSegmentWrite(entries []*pb.Entry) *segment {
	segment, _ := NewSegmentFile(TestWALConfig1.WalDirPath, TestWALConfig1.SegmentSize)
	data, bytesCount := MarshalWALEntries(entries)
	segment.Write(data, bytesCount, entries[0].Index)
	return segment
}
