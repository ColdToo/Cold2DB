package db

import (
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/Mock"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/db/wal"
	"reflect"
	"testing"
)

var MockDBCfg = &config.DBConfig{
	DBPath:           Mock.DBPath,
	MemTableNums:     10,
	MemTablePipeSize: 10,
	WalConfig: config.WalConfig{
		WalDirPath: Mock.WALPath,
	},
	ValueLogConfig: config.ValueLogConfig{
		ValueLogDir: Mock.ValueLogPath,
	},
	MemConfig: config.MemConfig{
		MemTableSize: 60,
		Concurrency:  3,
	},
}

func MockKVStorage(dbCfg *config.DBConfig) (C2 *C2KV) {
	dbCfgCheck(dbCfg)
	C2 = new(C2KV)
	var err error
	C2.dbCfg = dbCfg
	memFlushC := make(chan *MemTable, dbCfg.MemTableNums)
	C2.memTablePipe = make(chan *MemTable, dbCfg.MemTablePipeSize)
	C2.immtableQ = NewMemTableQueue(dbCfg.MemTableNums)
	C2.activeMem = NewMemTable(dbCfg.MemConfig)
	C2.memFlushC = memFlushC
	if C2.wal, err = wal.NewWal(dbCfg.WalConfig); err != nil {
		println(err)
	}
	if C2.valueLog, err = OpenValueLog(dbCfg.ValueLogConfig, memFlushC, C2.wal.KVStateSegment); err != nil {
		println(err)
	}
	go func() {
		for {
			C2.memTablePipe <- NewMemTable(dbCfg.MemConfig)
		}
	}()
	return
}

func TestKVStorage_dbCfgCheck(t *testing.T) {
	dbCfgCheck(MockDBCfg)
}

func TestKVStorage_PersistUnstableEnts(t *testing.T) {
	entSlices := Mock.ENTS_5GROUP_5000NUMS_250LENGTH
	C2KV := MockKVStorage(MockDBCfg)
	for _, ents := range entSlices {
		if err := C2KV.PersistUnstableEnts(ents); err != nil {
			t.Error(err)
		}
	}
}

func TestKVStorage_RestoreMemFromWAL(t *testing.T) {
	C2KV := MockKVStorage(MockDBCfg)
	PersisitIndex := 5665
	ApplyIndex := 11123
	C2KV.wal.KVStateSegment.PersistIndex = uint64(PersisitIndex)
	C2KV.wal.RaftStateSegment.AppliedIndex = uint64(ApplyIndex)
	C2KV.restoreMemEntries()
	C2KV.restoreImmTable()
}

func TestKVStorage_KVOperate_GET(t *testing.T) {
	kvs := Mock.KVS_RAND_27KB_HASDEL_UQKey
	C2KV := MockKVStorage(MockDBCfg)
	err := C2KV.Put(kvs)
	if err != nil {
		t.Error(err)
	}
	//获取验证集
	max := len(kvs) - 1
	Index := Mock.CreateRandomIndex(max)
	kv := kvs[Index]
	reKv, err := C2KV.Get(kv.Key)
	if err != nil {
		t.Error(err)
	}
	reflect.DeepEqual(kv.Data, reKv.Data)
}

func TestKVStorage_KVOperate_SCAN(t *testing.T) {
	kvs := Mock.KVS_RAND_27KB_HASDEL_UQKey
	C2KV := MockKVStorage(MockDBCfg)
	err := C2KV.Put(kvs)
	if err != nil {
		t.Error(err)
	}

	//获取验证集
	max := len(kvs) - 1
	verifyKvs := make([]*marshal.KV, 0)
	lowIndex := Mock.CreateRandomIndex(max)
	lowKey := kvs[lowIndex].Key
	highKey := kvs[max].Key
	for lowIndex <= max {
		kv := kvs[lowIndex]
		verifyKvs = append(verifyKvs, kv)
		lowIndex++
	}

	allKvs, _ := C2KV.Scan(lowKey, highKey)
	reflect.DeepEqual(verifyKvs, allKvs)
}
