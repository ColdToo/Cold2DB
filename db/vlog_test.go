package db

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/Mock"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/db/wal"
	"github.com/google/uuid"
	"os"
	"path"
	"testing"
)

var filePath, _ = os.Getwd()
var partitionDir1 = path.Join(filePath, fmt.Sprintf(PartitionFormat, 1))
var partitionDir2 = path.Join(filePath, fmt.Sprintf(PartitionFormat, 2))
var partitionDir3 = path.Join(filePath, fmt.Sprintf(PartitionFormat, 3))
var vlogPath = path.Join(filePath, "VLOG")

func CreatValueLogDirIfNotExist(vlogDir string) {
	if _, err := os.Stat(vlogDir); err != nil {
		if err := os.Mkdir(vlogDir, 0755); err != nil {
			println(err)
		}
	}
}

func TestValueLog_Open(t *testing.T) {
	CreatValueLogDirIfNotExist(vlogPath)
	vlogCfg := config.ValueLogConfig{
		ValueLogDir:   vlogPath,
		PartitionNums: 3,
	}
	tableC := make(chan *MemTable)
	stateSegment, err := wal.OpenKVStateSegment(filePath, uuid.New().String())
	if err != nil {
		t.Errorf("OpenKVStateSegment returned error: %v", err)
	}
	_, err = OpenValueLog(vlogCfg, tableC, stateSegment)
	if err != nil {
		t.Errorf("OpenValueLog returned error: %v", err)
	}
	os.RemoveAll(vlogPath)
}

func TestValueLog_ListenAndFlush(t *testing.T) {
	CreatValueLogDirIfNotExist(vlogPath)
	vlogCfg := config.ValueLogConfig{
		ValueLogDir:   vlogPath,
		PartitionNums: 3,
	}
	kvStateMentFile := path.Join(filePath, uuid.New().String())
	defer func() {
		os.RemoveAll(kvStateMentFile)
	}()

	tableC := make(chan *MemTable, 3)
	stateSegment, err := wal.OpenKVStateSegment(filePath, uuid.New().String()+wal.SegSuffix)
	if err != nil {
		t.Errorf("OpenKVStateSegment returned error: %v", err)
	}
	vlog, err := OpenValueLog(vlogCfg, tableC, stateSegment)
	if err != nil {
		t.Errorf("OpenValueLog returned error: %v", err)
	}

	tableC <- MockImMemTable()

	vlog.ListenAndFlush()
}

func MockImMemTable() *MemTable {
	mem, _ := NewMemTable(TestMemConfig)
	// todo 跳表有性能问题
	kvs := Mock.KVs27KBNoDelOp
	sklIter := mem.newSklIter()
	for _, kv := range kvs {
		sklIter.Put(kv.Key, marshal.EncodeData(kv.Data))
	}
	return mem
}
