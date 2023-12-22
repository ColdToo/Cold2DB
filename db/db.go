package db

import (
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/code"
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
	Get(key []byte) (kv *marshal.KV, err error)
	Scan(lowKey []byte, highKey []byte) (kvs []*marshal.KV, err error)
	Put(kvs []*marshal.KV) error

	PersistHardState(st pb.HardState) error
	PersistUnstableEnts(entries []*pb.Entry) error
	Truncate(index uint64) error
	GetHardState() (pb.HardState, pb.ConfState, error)
	Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error)
	Term(i uint64) (uint64, error)
	AppliedIndex() uint64
	FirstIndex() uint64
	GetSnapshot() (pb.Snapshot, error)

	Close()
}

var C2 *C2KV

type C2KV struct {
	dbCfg *config.DBConfig

	activeMem *MemTable

	immtableQ *MemTableQueue

	flushC chan *MemTable

	memTablePipe chan *MemTable

	wal *wal.WAL

	valueLog *ValueLog

	logOffset uint64

	entries []*pb.Entry //stable raft log entries
}

func GetStorage() (Storage, error) {
	if C2 != nil {
		return C2, nil
	} else {
		return nil, code.ErrDBNotInit
	}
}

func dbCfgCheck(dbCfg *config.DBConfig) (err error) {
	if !utils.PathExist(dbCfg.DBPath) {
		if err = os.MkdirAll(dbCfg.DBPath, os.ModePerm); err != nil {
			return err
		}
	}
	if !utils.PathExist(dbCfg.WalConfig.WalDirPath) {
		if err = os.MkdirAll(dbCfg.WalConfig.WalDirPath, os.ModePerm); err != nil {
			return err
		}
	}
	if !utils.PathExist(dbCfg.ValueLogConfig.ValueLogDir) {
		if err = os.MkdirAll(dbCfg.ValueLogConfig.ValueLogDir, os.ModePerm); err != nil {
			return err
		}
	}
	if dbCfg.MemConfig.MemTableNums <= 5 || dbCfg.MemConfig.MemTableNums > 20 {
		return code.ErrIllegalMemTableNums
	}
	return nil
}

func OpenDB(dbCfg *config.DBConfig) {
	err := dbCfgCheck(dbCfg)
	if err != nil {
		log.Panic("check db cfg failed")
	}

	C2 = new(C2KV)
	memTableFlushC := make(chan *MemTable, dbCfg.MemConfig.MemTableNums-1)
	C2.memTablePipe = make(chan *MemTable, dbCfg.MemConfig.MemTableNums/2)
	C2.immtableQ = NewMemTableQueue(dbCfg.MemConfig.MemTableNums)
	C2.flushC = memTableFlushC
	C2.activeMem, err = NewMemTable(dbCfg.MemConfig)
	if err != nil {
		return
	}

	C2.wal, err = wal.NewWal(dbCfg.WalConfig)
	C2.restoreMemoryFromWAL()

	C2.valueLog, err = OpenValueLog(dbCfg.ValueLogConfig, memTableFlushC, C2.wal.KVStateSegment)
	if err != nil {
		log.Panic("open Value log failed").Record()
	}

	go func() {
		for {
			memTable, err := NewMemTable(dbCfg.MemConfig)
			if err != nil {
				log.Panicf("create a new segment file error", err)
			}
			C2.memTablePipe <- memTable
		}
	}()
}

func (db *C2KV) restoreMemoryFromWAL() {
	files, err := os.ReadDir(db.wal.WalDirPath)
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
			segmentFile, err := wal.OpenOldSegmentFile(db.wal.WalDirPath, id)
			if err != nil {
				log.Panicf("open segment file failed")
			}
			//根据segment文件名中的index对segment进行排序
			db.wal.OrderSegmentList.Insert(segmentFile)
		}

		if strings.HasSuffix(fName, wal.RaftSuffix) {
			db.wal.RaftStateSegment, err = wal.OpenRaftStateSegment(db.wal.WalDirPath, file.Name())
			if err != nil {
				return
			}
		}
		if strings.HasSuffix(fName, wal.KVSuffix) {
			db.wal.KVStateSegment, err = wal.OpenKVStateSegment(db.wal.WalDirPath, file.Name())
			if err != nil {
				return
			}
		}
	}

	if db.wal.RaftStateSegment == nil {
		db.wal.RaftStateSegment, err = wal.OpenRaftStateSegment(db.wal.WalDirPath, time.Now().String())
		if err != nil {
			return
		}
	}

	if db.wal.KVStateSegment == nil {
		db.wal.KVStateSegment, err = wal.OpenKVStateSegment(db.wal.WalDirPath, time.Now().String())
		if err != nil {
			return
		}
	}

	go db.restoreMemEntries()

	go db.restoreImmTable()

	return
}

func (db *C2KV) restoreImmTable() {
	persistIndex := db.wal.KVStateSegment.PersistIndex
	appliedIndex := db.wal.RaftStateSegment.AppliedIndex
	Node := db.wal.OrderSegmentList.Head

	kvC := make(chan *marshal.KV, 1000)
	signalC := make(chan error)
	//memTable := <-db.memTablePipe

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

				kv := marshal.DecodeKV(ent.Data)
				kvC <- kv
				reader.Next(header.EntrySize)
			}
		}()

		select {
		case _ = <-kvC:
			//memTable.put(kv.Key, marshal.EncodeData(kv.Data))
			return
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

// kv operate

func (db *C2KV) Get(key []byte) (kv *marshal.KV, err error) {
	flag, val := db.activeMem.Get(key)
	if !flag {
		return db.valueLog.Get(key)
	}
	return &marshal.KV{Key: key, Data: marshal.DecodeData(val)}, nil
}

func (db *C2KV) Scan(lowKey []byte, highKey []byte) (kvs []*marshal.KV, err error) {
	/*kvChan := make(chan *marshal.KV)
	go func() {
		kvs, err := db.valueLog.Scan(lowKey, highKey)
		if err != nil {
			return
		}
	}()

	for _, mem := range db.immtableQ.tables {
		kvs, err := mem.Scan(lowKey, highKey)
		if err != nil {
			return
		}
	}*/
	return kvs, err
}

func (db *C2KV) Put(kvs []*marshal.KV) (err error) {
	kvBytes := make([]*marshal.BytesKV, len(kvs))
	var bytesCount int64
	for i, kv := range kvs {
		dataBytes := marshal.EncodeData(kv.Data)
		bytesCount += int64(len(dataBytes) + len(kv.Key))
		kvBytes[i] = &marshal.BytesKV{Key: kv.Key, Value: dataBytes}
	}

	//判断是否超出当前memTable大小，若超过获取新的memTable
	if bytesCount+db.activeMem.Size() > db.activeMem.cfg.MemTableSize {
		if C2.immtableQ.size > C2.immtableQ.capacity/2 {
			C2.flushC <- C2.immtableQ.Dequeue()
		}
		db.immtableQ.Enqueue(db.activeMem)
		db.activeMem = <-db.memTablePipe
	}
	return db.activeMem.ConcurrentPut(kvBytes)
}

// raft log

func (db *C2KV) Entries(lo, hi, maxSize uint64) (entries []*pb.Entry, err error) {
	if int(lo) < len(db.entries) {
		return nil, errors.New("some entries is compacted")
	}
	return
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

func (db *C2KV) GetSnapshot() (pb.Snapshot, error) {
	return pb.Snapshot{}, nil
}

func (db *C2KV) GetHardState() (pb.HardState, pb.ConfState, error) {
	return pb.HardState{}, pb.ConfState{}, nil
}

func (db *C2KV) Truncate(index uint64) error {
	return db.wal.Truncate(index)
}

func (db *C2KV) Close() {

}
