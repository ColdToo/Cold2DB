package db

import (
	"bytes"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/Mock"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/db/wal"
	"github.com/google/uuid"
	"os"
	"sync"
	"testing"
)

func MockDataMemTable(kvs []*marshal.KV) *MemTable {
	mem := NewMemTable(TestMemConfig)
	mem.ConcurrentPut(Mock.KVsTransToByteKVs(kvs))
	return mem
}

func MockVlogFlush(mockKvs []*marshal.KV) *ValueLog {
	kvs := MockDataMemTable(mockKvs).All()
	partitionRecords := make([][]*marshal.KV, 3)
	Mock.CreateValueLogDirIfNotExist(Mock.VlogPath)
	vlogCfg := Mock.VlogCfg
	stateSegment, err := wal.OpenKVStateSegment(Mock.FilePath, uuid.New().String()+wal.SegSuffix)
	if err != nil {
		panic(err)
	}
	tableC := make(chan *MemTable, 1)
	errC := make(chan error)
	vlog, err := OpenValueLog(vlogCfg, tableC, stateSegment)
	if err != nil {
		panic(err)
	}

	for _, record := range kvs {
		p := vlog.getKeyPartition(record.Key)
		kv := new(marshal.KV)
		kv.Key = record.Key
		kv.KeySize = len(record.Key)
		kv.Data = marshal.DecodeData(record.Value)
		partitionRecords[p] = append(partitionRecords[p], kv)
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < vlog.vlogCfg.PartitionNums; i++ {
		if len(partitionRecords[i]) == 0 {
			continue
		}
		wg.Add(1)
		go vlog.partitions[i].PersistKvs(partitionRecords[i], wg, errC)
	}
	wg.Wait()
	return vlog
}

func TestValueLog_Open(t *testing.T) {
	Mock.CreateValueLogDirIfNotExist(Mock.VlogPath)
	vlogCfg := config.ValueLogConfig{
		ValueLogDir:   Mock.VlogPath,
		PartitionNums: 3,
	}
	tableC := make(chan *MemTable)
	stateSegment, err := wal.OpenKVStateSegment(Mock.FilePath, uuid.New().String())
	if err != nil {
		t.Errorf("OpenKVStateSegment returned error: %v", err)
	}
	_, err = OpenValueLog(vlogCfg, tableC, stateSegment)
	if err != nil {
		t.Errorf("OpenValueLog returned error: %v", err)
	}
	os.RemoveAll(Mock.VlogPath)
}

func TestValueLog_ListenAndFlush(t *testing.T) {
	Mock.CreateValueLogDirIfNotExist(Mock.VlogPath)

	tableC := make(chan *MemTable, 3)
	stateSegment, err := wal.OpenKVStateSegment(Mock.FilePath, uuid.New().String()+wal.SegSuffix)
	if err != nil {
		t.Errorf("OpenKVStateSegment returned error: %v", err)
	}
	vlog, err := OpenValueLog(Mock.VlogCfg, tableC, stateSegment)
	if err != nil {
		t.Errorf("OpenValueLog returned error: %v", err)
	}

	tableC <- MockDataMemTable(Mock.KVS_RAND_35MB_HASDEL_UQKey)
	vlog.ListenAndFlush()
}

func TestValueLog_Scan(t *testing.T) {
	kvs := Mock.KVS_SORT_27KB_NODEL_UQKey
	vlog := MockVlogFlush(kvs)
	max := len(kvs) - 1
	defer func() {
		vlog.Delete()
	}()

	//验证集
	verifyKvs := make([]*marshal.KV, 0)
	lowIndex := Mock.CreateRandomIndex(max)
	lowKey := kvs[lowIndex].Key
	highKey := kvs[max].Key
	for lowIndex <= max {
		kv := kvs[lowIndex]
		verifyKvs = append(verifyKvs, kv)
		lowIndex++
	}

	scanKvs, err := vlog.Scan(lowKey, highKey)
	if err != nil {
		return
	}
	sortScanKvs := Mock.SortKVSByKey(scanKvs)

	for i := 0; i < len(sortScanKvs); i++ {
		if bytes.Compare(sortScanKvs[i].Key, verifyKvs[i].Key) != 0 {
			t.Errorf("sortScanKvs[i].Key!= verifyKvs[i].Key")
		}
		if bytes.Compare(sortScanKvs[i].Data.Value, verifyKvs[i].Data.Value) != 0 {
			t.Errorf("kv.Data.Value!= findKv.Data.Value")
		}
		if sortScanKvs[i].Data.TimeStamp != verifyKvs[i].Data.TimeStamp {
			t.Errorf("kv.Data.Time!= findKv.Data.Time")
		}
	}
}

func TestValueLog_Get(t *testing.T) {
	kvs := Mock.KVS_RAND_35MB_NODEL_UQKey
	vlog := MockVlogFlush(kvs)
	defer func() {
		vlog.Delete()
	}()
	max := len(kvs) - 1

	Index := Mock.CreateRandomIndex(max)
	kv := kvs[Index]
	findKv, err := vlog.Get(kv.Key)
	if err != nil {
		return
	}

	if bytes.Compare(kv.Data.Value, findKv.Data.Value) != 0 {
		t.Errorf("kv.Data.Value!= findKv.Data.Value")
	}

	if kv.Data.TimeStamp != findKv.Data.TimeStamp {
		t.Errorf("kv.Data.Time!= findKv.Data.Time")
	}
}
