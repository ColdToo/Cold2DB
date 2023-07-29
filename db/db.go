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
}

type Cold2DB struct {
	// put key to arena skl and wal , update activeMemtable trans act to imm
	memManager *memManager

	vlog *valueLog

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

	err = initValueLog(dbCfg.ValueLogConfig)
	if err != nil {
		return err
	}

	go Cold2.CompactionAndFlush()
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

// ListenAndFlush 定期将immtable刷入vlog,更新内存索引
func (db *Cold2DB) CompactionAndFlush() {

}

// kv

func (db *Cold2DB) Get(key []byte) (val []byte, err error) {
	flag, val := db.activeMem.get(key)
	if !flag {
		return nil, errors.New("the key is not exist")
	}
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
		err := db.activeMem.put(entries[0])
		if err != nil {
			return err
		}
	} else {
		err := db.activeMem.putBatch(entries)
		if err != nil {
			return err
		}
	}
	return
}

// raft

func (db *Cold2DB) InitialState() (pb.HardState, pb.ConfState, error) {
	return db.hardState, *ms.snapshot.Metadata.ConfState, nil
}

func (db *Cold2DB) SetHardState(st pb.HardState) error {
	return nil
}

func (db *Cold2DB) Entries(lo, hi uint64) ([]pb.Entry, error) {
	offset := ms.ents[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 {
		log.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}

	ents := ms.ents[lo-offset : hi-offset]
	if len(ms.ents) == 1 && len(ents) != 0 {
		// only contains dummy entries.
		return nil, ErrUnavailable
	}
	return ents, nil
}

func (db *Cold2DB) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

func (db *Cold2DB) LastIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(), nil
}

func (db *Cold2DB) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

func (db *Cold2DB) GetSnapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}

func (db *Cold2DB) IsRestartNode() bool {
	DirEntries, err := os.ReadDir(db.walDirPath)
	if err != nil {
		log.Errorf("open wal dir failed")
	}

	if len(DirEntries) > 0 {
		return true
	}
	return false
}
