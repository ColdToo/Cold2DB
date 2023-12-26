package db

import (
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/Mock"
	"github.com/ColdToo/Cold2DB/db/wal"
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

func TestKVStorage_dbCfgCheck(t *testing.T) {
	dbCfgCheck(MockDBCfg)
}

func TestKVStorage_restoreMemoryFromWAL(t *testing.T) {
	C2KV := MockKVStorage(MockDBCfg)
	C2KV.PersistUnstableEnts(Mock.Entries61MB)
	C2KV.restoreMemoryFromWAL()
}

func MockKVStorage(dbCfg *config.DBConfig) (C2 *C2KV) {
	dbCfgCheck(dbCfg)
	C2 = new(C2KV)
	var err error
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
	return
}
