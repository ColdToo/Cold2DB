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

//go:generate mockgen -source=./db.go -destination=../mocks/db.go -package=mock
type Storage interface {
	Get(key []byte) (val []byte, err error)
	Scan(lowKey []byte, highKey []byte) (err error)
	Put(kvs []*marshal.KV) error

	PersistHardState(st pb.HardState) error
	PersistUnstableEnts(entries []*pb.Entry) error

	GetHardState() (pb.HardState, pb.ConfState, error)
	Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error)
	Term(i uint64) (uint64, error)
	AppliedIndex() uint64
	LastIndex() uint64
	FirstIndex() uint64
	GetSnapshot() (pb.Snapshot, error)
	Truncate(index uint64) error

	Close()
}

var C2 *C2KV

type C2KV struct {
	activeMem *Memtable

	immuMems []*Memtable

	flushChn chan *Memtable

	memtablePipe chan *Memtable

	logOffset uint64

	entries []*pb.Entry //stable raft log entries

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
	memTableFlushC := make(chan *Memtable, dbCfg.MemConfig.MemtableNums-1)
	C2.flushChn = memTableFlushC

	memOpt := MemOpt{
		memSize: dbCfg.MemConfig.MemtableSize,
	}
	C2.activeMem, err = newMemtable(memOpt)
	if err != nil {
		return
	}

	C2.wal, err = wal.NewWal(dbCfg.WalConfig)

	C2.restoreMemoryFromWAL()

	C2.valueLog, err = OpenValueLog(dbCfg.ValueLogConfig, memTableFlushC, C2.wal.KVStateSegment)
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

	var id uint64
	for _, file := range files {
		fName := file.Name()
		if strings.HasSuffix(fName, wal.SegSuffix) {
			_, err = fmt.Sscanf(fName, "%d.SEG", &id)
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

		if strings.HasSuffix(fName, wal.RaftSuffix) {
			db.wal.RaftStateSegment, err = wal.OpenRaftStateSegment(db.wal.Config.WalDirPath, file.Name())
			if err != nil {
				return
			}
		}
		if strings.HasSuffix(fName, wal.KVSuffix) {
			db.wal.KVStateSegment, err = wal.OpenKVStateSegment(db.wal.Config.WalDirPath, file.Name())
			if err != nil {
				return
			}
		}
	}

	if db.wal.RaftStateSegment == nil {
		db.wal.RaftStateSegment, err = wal.OpenRaftStateSegment(db.wal.Config.WalDirPath, time.Now().String())
		if err != nil {
			return
		}
	}

	if db.wal.KVStateSegment == nil {
		db.wal.KVStateSegment, err = wal.OpenKVStateSegment(db.wal.Config.WalDirPath, time.Now().String())
		if err != nil {
			return
		}
	}

	// 启动两个goroutine分别将old segment file加载到Immemtble和enties中

	go db.restoreMemEntries()

	go db.restoreImMemTable()

	return
}

// ----------------- |  restore imm table |    restore mem entries     |
// -----------persist-index----------apply-index-------------------commit-index----------stable-index-----------

func (db *C2KV) restoreImMemTable() {
	persistIndex := db.wal.KVStateSegment.PersistIndex
	appliedIndex := db.wal.RaftStateSegment.AppliedIndex
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
		reader := wal.NewSegmentReader(Seg)

		go func() {
			for {
				header, err := reader.ReadHeader()
				ent, err := reader.ReadEntry(header)
				if err != nil {
					if err.Error() == "EOF" {
						break
					} else {
						log.Panicf("read header failed", err)
					}
				}
				if header.Index < persistIndex {
					continue
				}
				if header.Index == appliedIndex {
					break
				}

				kv := marshal.GobDecode(ent.Data)
				kvC <- &kv
				reader.Next(header.EntrySize)
			}
		}()

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
	appliedIndex := db.wal.RaftStateSegment.AppliedIndex

	//locate the read position of segment
	Node := db.wal.OrderSegmentList.Head
	for Node != nil {
		if appliedIndex >= Node.Seg.Index && appliedIndex <= Node.Next.Seg.Index {
			break
		}
		Node = Node.Next
	}

	for Node != nil {
		ents := make([]*pb.Entry, 0)
		Seg := Node.Seg
		reader := wal.NewSegmentReader(Seg)
		for {
			header, err := reader.ReadHeader()
			ent, err := reader.ReadEntry(header)
			if err != nil {
				if err.Error() == "EOF" {
					break
				} else {
					log.Panicf("read header failed", err)
				}
			}

			if header.Index < appliedIndex {
				continue
			}

			ents = append(ents, ent)
			reader.Next(header.EntrySize)
		}

		db.entries = append(db.entries, ents...)
		//todo 更新offset
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

//

func (db *C2KV) Entries(lo, hi, maxSize uint64) (entries []*pb.Entry, err error) {
	if int(lo) < len(db.entries) {
		return nil, errors.New("some entries is compacted")
	}
	return
}

func (db *C2KV) Put(kvs []*marshal.KV) (err error) {
	db.activeMem.Put(kvs)
	return nil
}

func (db *C2KV) PersistHardState(st pb.HardState) error {
	db.wal.RaftStateSegment.RaftState = st
	return db.wal.RaftStateSegment.Flush()
}

func (db *C2KV) PersistUnstableEnts(entries []*pb.Entry) error {
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

func (db *C2KV) Truncate(index uint64) error {
	return db.wal.Truncate(index)
}
