package db

import (
	"github.com/ColdToo/Cold2DB/db/flock"
	"github.com/ColdToo/Cold2DB/db/index"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/utils"
	"go.etcd.io/etcd/raft/raftpb"
	"os"
	"sync"
)

type DB interface {
	Get(key []byte) (val []byte, err error)
	Put(key, val []byte) (err error)
	Del(key []byte) (err error)
}

var Cold2 *Cold2DB

type Cold2DB struct {
	activeMem *memtable

	immuMems []*memtable

	flushChn chan *memtable

	vlog *valueLog

	indexer index.Indexer
	// When the active memtable is full, send it to the flushChn, see listenAndFlush.
	flushLock sync.RWMutex // guarantee flush and compaction exclusive.

	mu sync.RWMutex

	// Prevent concurrent db using.
	// At least one FileLockGuard(cf/indexer/vlog dirs are all the same).
	// And at most three FileLockGuards(cf/indexer/vlog dirs are all different).
	dirLocks []*flock.FileLockGuard

	// 快照管理器
	snapShotter SnapShotter

	// 保存raft的Hard状态
	raftHardState raftpb.HardState
}

func InitDB(dbCfg *DBConfig) error {
	err := dbCfgCheck(dbCfg)
	if err != nil {
		return err
	}

	if !utils.PathExist(dbCfg.DBPath) {
		if err := os.MkdirAll(dbCfg.DBPath, os.ModePerm); err != nil {
			return err
		}
	}

	err := initMemtable(dbCfg)
	if err != nil {
		return err
	}

	log, err := openValueLog(dbCfg)
	if err != nil {
		return err
	}

	//开启后台合并协程

	return nil
}

func dbCfgCheck(dbCfg *DBConfig) error {
	var err error
	if dbCfg.ValueLogGCRatio >= 1.0 || dbCfg.ValueLogGCRatio <= 0.0 {
		return ErrInvalidVLogGCRatio
	}
	Cold2 = new(Cold2DB)
	Cold2.flushChn = make(chan *memtable, dbCfg.MemtableNums-1)
	err = initMemtable(dbCfg)
	if err != nil {
		return err
	}
	Cold2.immuMems = make([]*memtable, dbCfg.MemtableNums-1)

}

func ListenAndFlush() {

}

// implement db basic operate interface

func (db *Cold2DB) Get(key []byte) (val []byte, err error) {
	return
}

func (db *Cold2DB) Put(key, val []byte) (err error) {
	return
}

func (db *Cold2DB) Del(key []byte) (err error) {
	return
}

// implement raft storage interface

func (db *Cold2DB) InitialState() (pb.HardState, pb.ConfState, error) {
	return db.hardState, *ms.snapshot.Metadata.ConfState, nil
}

func (db *Cold2DB) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

func (db *Cold2DB) Entries(lo, hi uint64) ([]pb.Entry, error) {
	ms.Lock()
	defer ms.Unlock()
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

func (db *Cold2DB) lastIndex() uint64 {
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

func (db *Cold2DB) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

func (db *Cold2DB) firstIndex() uint64 {
	return ms.ents[0].Index + 1
}

func (db *Cold2DB) GetSnapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}
