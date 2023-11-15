package db

import (
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/db/wal"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/utils"
	"os"
	"strings"
	"time"
)

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
// snapshot is temporarily unavailable.
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

//go:generate mockgen -source=./db.go -destination=../mocks/db.go -package=mock
type Storage interface {
	Get(key []byte) (val []byte, err error)
	Scan(lowKey []byte, highKey []byte) (err error)

	SaveHardState(st pb.HardState) error
	SaveEntries(entries []*pb.Entry) error
	SaveCommittedEntries(entries []*marshal.KV) error

	GetHardState() (pb.HardState, pb.ConfState, error)
	Entries(lo, hi uint64) ([]*pb.Entry, error)
	Term(i uint64) (uint64, error)
	AppliedIndex() uint64
	LastIndex() uint64
	FirstIndex() uint64
	GetSnapshot() (pb.Snapshot, error)

	Close()
}

var C2 *C2KV

type C2KV struct {
	activeMem *Memtable

	immuMems []*Memtable

	flushChn chan *Memtable

	memtablePipe chan *Memtable
	//raft log entries
	entries []*pb.Entry

	wal *wal.WAL

	valueLog *ValueLog
}

func GetStorage() (Storage, error) {
	if C2 != nil {
		return C2, nil
	} else {
		return nil, errors.New("db is no init complete")
	}
}

func OpenDB(dbCfg *config.DBConfig) {
	err := dbCfgCheck(dbCfg)
	if err != nil {
		log.Panic("check db cfg failed")
	}

	C2 = new(C2KV)
	tableFlushC := make(chan *Memtable, dbCfg.MemConfig.MemtableNums-1)
	C2.flushChn = tableFlushC

	memOpt := MemOpt{
		memSize: dbCfg.MemConfig.MemtableSize,
	}
	C2.activeMem, err = newMemtable(memOpt)
	if err != nil {
		return
	}

	C2.wal, err = wal.NewWal(dbCfg.WalConfig)

	C2.restoreMemoryFromWAL()

	C2.valueLog, err = OpenValueLog(dbCfg.ValueLogConfig, tableFlushC)
	if err != nil {
		log.Panic("open Value log failed")
	}
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

func (db *C2KV) restoreMemoryFromWAL() {
	files, err := os.ReadDir(db.wal.Config.WalDirPath)
	if err != nil {
		log.Panicf("open wal dir failed", err)
	}
	if len(files) == 0 {
		return
	}

	var id int64
	for _, file := range files {
		if strings.HasSuffix(file.Name(), wal.SegSuffix) {
			_, err = fmt.Sscanf(file.Name(), "%d.SEG", &id)
			if err != nil {
				log.Panicf("can not scan file")
			}
			segmentFile, err := wal.OpenOldSegmentFile(db.wal.Config.WalDirPath, id)
			if err != nil {
				log.Panicf("open segment file failed")
			}
			//根据segment文件名中的index对segment进行排序
			db.wal.OrderSegmentList.Insert(segmentFile)
		}

		if strings.HasSuffix(file.Name(), wal.RaftSuffix) {
			raftSegmentFile, err := wal.OpenStateSegmentFile(db.wal.Config.WalDirPath, file.Name())
			if err != nil {
				return
			}
			db.wal.StateSegment = raftSegmentFile
		}
	}

	//创建一个新的state segment file
	if db.wal.StateSegment == nil {
		wal.OpenStateSegmentFile(db.wal.Config.WalDirPath, time.Now().String())
	}

	// 启动两个goroutine分别将old segment file加载到Immemtble和enties中

	go db.restoreMemEntries()

	go db.restoreImMemTable()

	return
}

func (db *C2KV) restoreImMemTable() {
	persistIndex := db.wal.StateSegment.PersistIndex
	appliedIndex := db.wal.StateSegment.AppliedIndex
	Node := db.wal.OrderSegmentList.Head

	kvC := make(chan *marshal.KV, 1000)
	signalC := make(chan error)
	memTable := <-db.memtablePipe

	//先定位要读取的segment
	for Node != nil {
		if persistIndex >= Node.Seg.Index && persistIndex <= Node.Next.Seg.Index {
			break
		}
		Node = Node.Next
	}

	for Node != nil {
		Seg := Node.Seg
		reader := wal.NewSegmentReader(Seg, persistIndex, appliedIndex)

		go reader.ReadKVs(kvC, signalC)

		select {
		case kv := <-kvC:
			err := memTable.put(kv)
			if err.Error() == "memtable is full" {
				db.immuMems = append(db.immuMems, memTable)
				//获取一个新的memtable写入，并将该memtable加入immtable中
				memTable = <-db.memtablePipe
				memTable.put(kv)
			} else {
				log.Panicf("read segment failed")
			}
		case err := <-signalC:
			if err != nil {
				log.Panicf("read segment failed")
			} else if err.Error() == "EOF" {
				Node = Node.Next
				continue
			}
		}
	}
	return
}

func (db *C2KV) restoreMemEntries() {
	appliedIndex := db.wal.StateSegment.AppliedIndex
	Node := db.wal.OrderSegmentList.Head

	//先定位要读取的segment
	for Node != nil {
		if appliedIndex >= Node.Seg.Index && appliedIndex <= Node.Next.Seg.Index {
			break
		}
		Node = Node.Next
	}

	for Node != nil {
		Seg := Node.Seg
		reader := wal.NewSegmentReader(Seg, 0, appliedIndex)
		kvs, err := reader.ReadEntries()
		if err != nil {
			log.Panic("read header failed")
		}
		db.entries = append(db.entries, kvs...)
		Node = Node.Next
	}
	return

}

func (db *C2KV) Get(key []byte) (val []byte, err error) {
	flag, val := db.activeMem.Get(key)
	if !flag {
		return nil, errors.New("the key is not exist")
	}
	//todo 若memtable找不到则查询索引
	return
}

func (db *C2KV) Scan(lowKey []byte, highKey []byte) (err error) {
	return err
}

func (db *C2KV) Entries(lo, hi uint64) (entries []*pb.Entry, err error) {
	if int(lo) < len(db.entries) {
		return nil, errors.New("some entries is compacted")
	}
	return
}

func (db *C2KV) SaveCommittedEntries(entries []*marshal.KV) (err error) {
	db.activeMem.Put(entries)
	return nil
}

func (db *C2KV) SaveHardState(st pb.HardState) error {
	db.wal.StateSegment.RaftState = st
	enStateBytes, _ := marshal.EncodeRaftState(st)
	return db.wal.StateSegment.Persist(enStateBytes)
}

func (db *C2KV) SaveEntries(entries []*pb.Entry) error {
	err := db.wal.Write(entries)
	if err != nil {
		return err
	}
	db.entries = append(db.entries, entries...)
	return nil
}

func (db *C2KV) Term(i uint64) (uint64, error) {
	//如果i已经applied返回compact错误，若没有则返回对应term
	return 0, errors.New("the specific index entry is compacted")
}

func (db *C2KV) AppliedIndex() uint64 {
	return 0
}

func (db *C2KV) FirstIndex() uint64 {
	return 0
}

func (db *C2KV) LastIndex() uint64 {
	return 0
}

func (db *C2KV) GetSnapshot() (pb.Snapshot, error) {
	return pb.Snapshot{}, nil
}

func (db *C2KV) GetHardState() (pb.HardState, pb.ConfState, error) {
	return pb.HardState{}, pb.ConfState{}, nil
}

func (db *C2KV) Close() {

}
