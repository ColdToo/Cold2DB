package db

import (
	"errors"
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

type DB interface {
	Get(key []byte) (val []byte, err error)
	Put(entries []logfile.WalEntry) (err error)
	Scan(lowKey []byte, highKey []byte) (err error)
	IsRestartNode() bool
	SetHardState(st pb.HardState) error
}

type Cold2DB struct {
	// put key to arena skl and wal , update activeMemtable trans act to imm
	memManager *memManager

	vlog *valueLog

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

func GetDB() (*Cold2DB, error) {
	if Cold2 != nil {
		return Cold2, nil
	} else {
		return nil, errors.New("db is no init complete")
	}
}

func InitDB(dbCfg DBConfig) error {
	var err error
	err = dbCfgCheck(dbCfg)
	if err != nil {
		return err
	}

	Cold2 = new(Cold2DB)
	Cold2.memManager, err = NewMemManger(dbCfg.MemConfig)
	if err != nil {
		return err
	}

	Cold2.indexer, err = index.NewIndexer(dbCfg.IndexConfig)
	if err != nil {
		return err
	}

	Cold2.hardStateLog, err = initHardStateLog(dbCfg.HardStateLogConfig)
	if err != nil {
		return err
	}

	Cold2.vlog, err = initValueLog(dbCfg.ValueLogConfig)
	if err != nil {
		return err
	}

	return nil
}

func dbCfgCheck(dbCfg DBConfig) error {
	if !utils.PathExist(dbCfg.DBPath) {
		if err := os.MkdirAll(dbCfg.DBPath, os.ModePerm); err != nil {
			return err
		}
	}
	if !utils.PathExist(dbCfg.DBPath) {
		if err := os.MkdirAll(dbCfg.DBPath, os.ModePerm); err != nil {
			return err
		}
	}
	if !utils.PathExist(dbCfg.DBPath) {
		if err := os.MkdirAll(dbCfg.DBPath, os.ModePerm); err != nil {
			return err
		}
	}
	return nil
}

// CompactionAndFlush 定期将immtable刷入vlog,更新内存索引以及压缩部分日志
func (db *Cold2DB) CompactionAndFlush() {

}

// raft write interface

func (db *Cold2DB) Get(key []byte) (val []byte, err error) {
	flag, val := db.memManager.activeMem.get(key)
	if !flag {
		return nil, errors.New("the key is not exist")
	}
	//todo 若memtable找不到则查询索引
	return
}

func (db *Cold2DB) Scan(lowKey []byte, highKey []byte) (err error) {
	return err
}

func (db *Cold2DB) PutBatch(entries []logfile.WalEntry) (err error) {
	return err
}

func (db *Cold2DB) Put(entries []logfile.WalEntry) (err error) {
	if len(entries) == 1 {
		err := db.memManager.activeMem.put(entries[0])
		if err != nil {
			return err
		}
	} else {
		err := db.memManager.activeMem.putBatch(entries)
		if err != nil {
			return err
		}
	}
	return
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

func (db *Cold2DB) SetHardState(st pb.HardState) error {
	return nil
}

// raft read interface

func (db *Cold2DB) Entries(lo, hi uint64) (entries []*pb.Entry, err error) {
	//先确定low和high在哪个memtable,先确定low的区间，再确定high的区间
	//先查询是否在active memtable

	//low和high都在active memtable
	if _, ok := db.memManager.activeMem.skl.IndexMap[lo]; ok {
		if _, ok := db.memManager.activeMem.skl.IndexMap[hi]; ok {
			for i := lo; i < hi; i++ {
				db.memManager.getEntryByIndex(db.memManager.activeMem, i)
			}
		}
	}

}

func (db *Cold2DB) Term(i uint64) (uint64, error) {
	return db.memManager.activeMem
}

func (db *Cold2DB) AppliedIndex() uint64 {
	return db.memManager.appliedIndex
}

func (db *Cold2DB) FirstIndex() uint64 {
	return db.memManager.firstIndex
}

// 通过snapshoter获取
func (db *Cold2DB) GetSnapshot() (pb.Snapshot, error) {
	return pb.Snapshot{}, nil
}

func (db *Cold2DB) GetHardState() (pb.HardState, pb.ConfState, error) {
	return pb.HardState{}, pb.ConfState{}, nil
}
