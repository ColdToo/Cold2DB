package db

import (
	"errors"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/index"
	"github.com/ColdToo/Cold2DB/db/logfile"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/utils"
	"os"
	"sync"
)

var Cold2 *Cold2KV

type Cold2KV struct {
	memManager *memManager

	flushLock sync.RWMutex // guarantee flush and compaction exclusive.

	mu sync.RWMutex

	flushChn chan *memtable //memManger的flushChn

	log  logfile.LogFile

	snapShotter SnapShotter
}

func GetStorage() (Storage, error) {
	if Cold2 != nil {
		return Cold2, nil
	} else {
		return nil, errors.New("db is no init complete")
	}
}

func InitDB(dbCfg *config.DBConfig) {
	var err error
	err = dbCfgCheck(dbCfg)
	if err != nil {
		log.Panic("check db cfg failed")
	}

	Cold2 = new(Cold2KV)
	Cold2.memManager, err = NewMemManger(dbCfg.MemConfig)
	if err != nil {
		log.Panic("init db memManger failed")
	}

	/*Cold2.indexer, err = index.NewIndexer(dbCfg.IndexConfig)
	if err != nil {
		return err
	}

	Cold2.vlog, err = initValueLog(dbCfg.ValueLogConfig)
	if err != nil {
		return err
	}*/
}

func dbCfgCheck(dbCfg *config.DBConfig) error {
	if !utils.PathExist(dbCfg.DBPath) {
		if err := os.MkdirAll(dbCfg.DBPath, os.ModePerm); err != nil {
			return err
		}
	}
	if !utils.PathExist(dbCfg.IndexConfig.IndexerDir) {
		if err := os.MkdirAll(dbCfg.IndexConfig.IndexerDir, os.ModePerm); err != nil {
			return err
		}
	}
	if !utils.PathExist(dbCfg.WalConfig.WalDirPath) {
		if err := os.MkdirAll(dbCfg.WalConfig.WalDirPath, os.ModePerm); err != nil {
			return err
		}
	}
	if !utils.PathExist(dbCfg.ValueLogConfig.ValueLogDir) {
		if err := os.MkdirAll(dbCfg.ValueLogConfig.ValueLogDir, os.ModePerm); err != nil {
			return err
		}
	}
	if dbCfg.MemConfig.MemtableNums <= 5 || dbCfg.MemConfig.MemtableNums > 20 {
		return errors.New("")
	}
	return nil
}

func (db *Cold2KV) Get(key []byte) (val []byte, err error) {
	flag, val := db.memManager.activeMem.Get(key)
	if !flag {
		return nil, errors.New("the key is not exist")
	}
	//todo 若memtable找不到则查询索引
	return
}

func (db *Cold2KV) Scan(lowKey []byte, highKey []byte) (err error) {
	return err
}

func (db *Cold2KV) SaveCommittedEntries(entries []*logfile.KV) (err error) {
	db.memManager.
	return nil
}

func (db *Cold2KV) SaveHardState(st pb.HardState) error {
	db.memManager.wal.PersistRaftStatus(st)
	return nil
}

func (db *Cold2KV) SaveEntries(entries []*pb.Entry) error {
	err := db.memManager.wal.Write(entries)
	if err != nil {
		return err
	}
	db.memManager.entries = append(db.memManager.entries, entries...)
	return nil
}

func (db *Cold2KV) Entries(lo, hi uint64) (entries []*pb.Entry, err error) {
	if int(lo) < len(db.memManager.entries) {
		return nil, errors.New("some entries is compacted")
	}
	return
}

func (db *Cold2KV) Term(i uint64) (uint64, error) {
	//如果i已经applied返回compact错误，若没有则返回对应term
	return 0, errors.New("the specific index entry is compacted")
}

func (db *Cold2KV) AppliedIndex() uint64 {
	return db.memManager.appliedIndex
}

func (db *Cold2KV) FirstIndex() uint64 {
	return db.memManager.firstIndex
}

func (db *Cold2KV) LastIndex() uint64 {
	return db.memManager.firstIndex
}

func (db *Cold2KV) GetSnapshot() (pb.Snapshot, error) {
	return pb.Snapshot{}, nil
}

func (db *Cold2KV) GetHardState() (pb.HardState, pb.ConfState, error) {
	return pb.HardState{}, pb.ConfState{}, nil
}

// CompactionAndFlush 定期将immtable刷入vlog,更新内存索引以及压缩部分日志
func (db *Cold2KV) CompactionAndFlush() {
	//当有可以刷新的memtable时，将其刷入vlog，如何设计通知memManger已经刷新某个memtable
	imtable := <-db.flushChn
	for _, kv := range imtable.All(){

	}
	//indexer通知某块索引页中大量key的fid不同需要针对该fid进行compaction
}

func (db *Cold2KV) Close() {

}
