package db

import (
	"errors"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/flock"
	"github.com/ColdToo/Cold2DB/db/index"
	"github.com/ColdToo/Cold2DB/db/logfile"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/utils"
	"os"
	"sync"
)

var Cold2 *Cold2DB

type Cold2DB struct {
	memManager *memManager

	hardStateLog *hardStateLog

	flushLock sync.RWMutex // guarantee flush and compaction exclusive.

	mu sync.RWMutex

	flushChn chan *memtable

	indexer index.Indexer

	// Prevent concurrent db using.
	// At least one FileLockGuard(cf/indexer/vlog dirs are all the same).
	// And at most three FileLockGuards(cf/indexer/vlog dirs are all different).
	dirLocks []*flock.FileLockGuard

	snapShotter SnapShotter
}

func GetDB() (DB, error) {
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

	Cold2 = new(Cold2DB)
	Cold2.memManager, err = NewMemManger(dbCfg.MemConfig)
	if err != nil {
		log.Panic("init db memManger failed")
	}

	Cold2.hardStateLog, err = initHardStateLog(dbCfg.HardStateLogConfig)
	if err != nil {
		log.Panic("init db hardStateLog failed")
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
	if !utils.PathExist(dbCfg.MemConfig.WalDirPath) {
		if err := os.MkdirAll(dbCfg.MemConfig.WalDirPath, os.ModePerm); err != nil {
			return err
		}
	}
	if !utils.PathExist(dbCfg.IndexConfig.IndexerDir) {
		if err := os.MkdirAll(dbCfg.IndexConfig.IndexerDir, os.ModePerm); err != nil {
			return err
		}
	}
	if !utils.PathExist(dbCfg.HardStateLogConfig.HardStateLogDir) {
		if err := os.MkdirAll(dbCfg.HardStateLogConfig.HardStateLogDir, os.ModePerm); err != nil {
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

// CompactionAndFlush 定期将immtable刷入vlog,更新内存索引以及压缩部分日志
func (db *Cold2DB) CompactionAndFlush() {

}

// raft write interface

func (db *Cold2DB) Get(key []byte) (val []byte, err error) {
	flag, val := db.memManager.activeMem.Get(key)
	if !flag {
		return nil, errors.New("the key is not exist")
	}
	//todo 若memtable找不到则查询索引
	return
}

func (db *Cold2DB) Scan(lowKey []byte, highKey []byte) (err error) {
	return err
}

func (db *Cold2DB) PutBatch(entries []logfile.Entry) (err error) {
	return err
}

func (db *Cold2DB) Put(entries []logfile.Entry) (err error) {
	if len(entries) == 1 {
		//todo 判断是否有activeMemTable,防止activeMemTable转移为Immtable时写入数据出错
		return db.memManager.activeMem.put(entries[0])
	} else {
		return db.memManager.activeMem.putBatch(entries)
	}
}

func (db *Cold2DB) IsRestartNode() bool {
	DirEntries, err := os.ReadDir(db.memManager.walDirPath)
	if err != nil {
		log.Errorf("open wal dir failed")
	}

	if len(DirEntries) > 0 {
		return true
	}
	return false
}

func (db *Cold2DB) SaveHardState(st pb.HardState) error {
	return nil
}

func (db *Cold2DB) Close() {

}

// raft read interface

func (db *Cold2DB) Entries(lo, hi uint64) (entries []*pb.Entry, err error) {
	if lo < db.memManager.firstIndex {
		return nil, errors.New("some entries is compacted")
	}
	//1、low和high都在active memtable
	if _, ok := db.memManager.activeMem.skl.IndexMap[lo]; ok {
		if _, ok := db.memManager.activeMem.skl.IndexMap[hi]; ok {
			for i := lo; i < hi; i++ {
				walEntry := db.memManager.getEntryByIndex(db.memManager.activeMem, i)
				entries = append(entries, walEntry.TransToPbEntry())
			}
		}
		return
	}

	//2、low在active memtable,hi在immtable
	if _, ok := db.memManager.activeMem.skl.IndexMap[lo]; ok {
		for i := lo; i < hi; i++ {
			walEntry := db.memManager.getEntryByIndex(db.memManager.activeMem, i)
			if walEntry == nil {
				db.memManager.getEntriesByRange(i, hi)
			}
			entries = append(entries, walEntry.TransToPbEntry())
		}
		return
	}

	walEntries := db.memManager.getEntriesByRange(lo, hi)
	for _, walE := range walEntries {
		entries = append(entries, walE.TransToPbEntry())
	}

	return
}

func (db *Cold2DB) Term(i uint64) (uint64, error) {
	return 0, errors.New("the specific index entry is compacted")
}

func (db *Cold2DB) AppliedIndex() uint64 {
	return db.memManager.appliedIndex
}

func (db *Cold2DB) FirstIndex() (uint64, error) {
	return db.memManager.firstIndex, nil
}

func (db *Cold2DB) GetSnapshot() (pb.Snapshot, error) {
	return pb.Snapshot{}, nil
}

func (db *Cold2DB) GetHardState() (pb.HardState, pb.ConfState, error) {
	return pb.HardState{}, pb.ConfState{}, nil
}
