package db

import (
	"errors"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/db/wal"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/utils"
	"os"
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

	memFlushC chan *MemTable

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
			return
		}
	}
	if !utils.PathExist(dbCfg.WalConfig.WalDirPath) {
		if err = os.MkdirAll(dbCfg.WalConfig.WalDirPath, os.ModePerm); err != nil {
			return
		}
	}
	if !utils.PathExist(dbCfg.ValueLogConfig.ValueLogDir) {
		if err = os.MkdirAll(dbCfg.ValueLogConfig.ValueLogDir, os.ModePerm); err != nil {
			return
		}
	}
	return nil
}

func OpenKVStorage(dbCfg *config.DBConfig) {
	var err error
	if err = dbCfgCheck(dbCfg); err != nil {
		log.Panicf("db config check failed", err)
	}
	C2 = new(C2KV)
	memFlushC := make(chan *MemTable, dbCfg.MemTableNums)
	C2.memTablePipe = make(chan *MemTable, dbCfg.MemTablePipeSize)
	C2.immtableQ = NewMemTableQueue(dbCfg.MemTableNums)
	C2.activeMem = NewMemTable(dbCfg.MemConfig)
	C2.memFlushC = memFlushC
	if C2.wal, err = wal.NewWal(dbCfg.WalConfig); err != nil {
		log.Panicf("open wal failed", err)
	}

	if C2.wal.OrderSegmentList.Head != nil {
		go C2.restoreMemEntries()

		go C2.restoreImmTable()
	}

	if C2.valueLog, err = OpenValueLog(dbCfg.ValueLogConfig, memFlushC, C2.wal.KVStateSegment); err != nil {
		log.Panicf("open Value log failed", err)
	}

	go func() {
		for {
			C2.memTablePipe <- NewMemTable(dbCfg.MemConfig)
		}
	}()
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
	val, flag := db.activeMem.Get(key)
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

	if bytesCount+db.activeMem.Size() > db.activeMem.cfg.MemTableSize {
		if C2.immtableQ.size > C2.immtableQ.capacity/2 {
			C2.memFlushC <- C2.immtableQ.Dequeue()
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
