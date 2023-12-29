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
	"sync"
)

//go:generate mockgen -source=./db.go -destination=../mocks/db.go -package=mock
type Storage interface {
	Get(key []byte) (kv *marshal.KV, err error)
	Scan(lowKey []byte, highKey []byte) (kvs []*marshal.KV, err error)
	Put(kvs []*marshal.KV) error

	PersistUnstableEnts(entries []*pb.Entry) error
	PersistHardState(st pb.HardState) error
	Truncate(index uint64) error
	GetHardState() (pb.HardState, pb.ConfState, error)
	Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error)

	Term(i uint64) (uint64, error)
	AppliedIndex() uint64
	// FirstIndex returns the index of the first log entry that is
	// possibly available via Entries (older entries have been incorporated
	// into the latest Snapshot; if storage only contains the dummy entry the
	// first log entry is not available).
	FirstIndex() uint64
	LastIndex() uint64

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

func OpenKVStorage(dbCfg *config.DBConfig) (C2 *C2KV, err error) {
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
		go C2.restoreImMemTable()
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

//persistIndex............AppliedIndex.....committedIndex.......stableIndex......
//    |_________imm-table_________|___________ entries______________|

func (db *C2KV) restoreImMemTable() {
	persistIndex := db.wal.KVStateSegment.PersistIndex
	appliedIndex := db.wal.RaftStateSegment.AppliedIndex
	Node := db.wal.OrderSegmentList.Head
	kvC := make(chan *marshal.KV, 1000)
	var bytesCount int64
	errC := make(chan error)
	signalC := make(chan any)
	memTable := <-db.memTablePipe

	for Node != nil {
		if Node.Seg.Index <= persistIndex && Node.Next.Seg.Index > persistIndex {
			break
		}
		Node = Node.Next
	}

	go func() {
	Loop:
		for Node != nil {
			Seg := Node.Seg
			reader := wal.NewSegmentReader(Seg)
			for {
				header, err := reader.ReadHeader()
				if err != nil {
					if err.Error() == "EOF" {
						break
					}
					log.Panicf("read header failed", err)
				}
				if header.Index < persistIndex {
					reader.Next(header.EntrySize)
					continue
				}
				if header.Index > appliedIndex {
					break Loop
				}

				ent, err := reader.ReadEntry(header)
				if err != nil {
					log.Panicf("read entry failed", err)
				}
				kv := marshal.DecodeKV(ent.Data)
				kvC <- kv
				reader.Next(header.EntrySize)
			}
			Node = Node.Next
		}
		close(signalC)
		close(kvC)
	}()

	for {
		select {
		case kv := <-kvC:
			dataBytes := marshal.EncodeData(kv.Data)
			bytesCount += int64(len(dataBytes) + len(kv.Key))
			db.maybeRotateMemTable(bytesCount)
			if err := memTable.Put(&marshal.BytesKV{Key: kv.Key, Value: dataBytes}); err != nil {
				return
			}
		case err := <-errC:
			log.Panicf("read kv failed", err)
			return
		case <-signalC:
			for kv := range kvC {
				dataBytes := marshal.EncodeData(kv.Data)
				bytesCount += int64(len(dataBytes) + len(kv.Key))
				db.maybeRotateMemTable(bytesCount)
				if err := memTable.Put(&marshal.BytesKV{Key: kv.Key, Value: dataBytes}); err != nil {
					return
				}
			}
			return
		}
	}
}

func (db *C2KV) restoreMemEntries() {
	appliedIndex := db.wal.RaftStateSegment.AppliedIndex
	committedIndex := db.wal.RaftStateSegment.CommittedIndex
	Node := db.wal.OrderSegmentList.Head
	for Node != nil {
		if Node.Seg.Index <= appliedIndex && Node.Next.Seg.Index >= appliedIndex {
			break
		}
		Node = Node.Next
	}

ExitLoop:
	for Node != nil {
		ents := make([]*pb.Entry, 0)
		Seg := Node.Seg
		reader := wal.NewSegmentReader(Seg)
		for {
			header, err := reader.ReadHeader()
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				log.Panicf("read header failed", err)
			}
			if header.Index < appliedIndex {
				reader.Next(header.EntrySize)
				continue
			}
			if header.Index > committedIndex {
				//todo 是否应该truncate掉committedIndex之后的日志？
				break ExitLoop
			}

			ent, err := reader.ReadEntry(header)
			if err != nil {
				log.Panicf("read entry failed", err)
			}

			ents = append(ents, ent)
			reader.Next(header.EntrySize)
		}

		db.entries = append(db.entries, ents...)
		Node = Node.Next
	}
	return
}

// kv operate

func (db *C2KV) Get(key []byte) (kv *marshal.KV, err error) {
	kv, flag := db.activeMem.Get(key)
	if !flag {
		return db.valueLog.Get(key)
	}
	return kv, nil
}

func (db *C2KV) Scan(lowKey []byte, highKey []byte) ([]*marshal.KV, error) {
	kvSlice := make([]*marshal.KV, 0)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		kvs, err := db.valueLog.Scan(lowKey, highKey)
		if err != nil {
			return
		}
		kvSlice = append(kvSlice, kvs...)
		wg.Done()
	}()

	var memsKvs []*marshal.KV
	for _, mem := range db.immtableQ.tables {
		memKvs, err := mem.Scan(lowKey, highKey)
		if err != nil {
			return nil, err
		}
		memsKvs = append(memsKvs, memKvs...)
	}
	activeMemKvs, err := db.activeMem.Scan(lowKey, highKey)
	if err != nil {
		return nil, err
	}
	memsKvs = append(memsKvs, activeMemKvs...)
	wg.Wait()
	kvSlice = append(kvSlice, memsKvs...)
	return kvSlice, nil
}

func (db *C2KV) Put(kvs []*marshal.KV) (err error) {
	kvBytes := make([]*marshal.BytesKV, len(kvs))
	var bytesCount int64
	for i, kv := range kvs {
		dataBytes := marshal.EncodeData(kv.Data)
		bytesCount += int64(len(dataBytes) + len(kv.Key))
		kvBytes[i] = &marshal.BytesKV{Key: kv.Key, Value: dataBytes}
	}
	db.maybeRotateMemTable(bytesCount)
	return db.activeMem.ConcurrentPut(kvBytes)
}

func (db *C2KV) maybeRotateMemTable(bytesCount int64) {
	if bytesCount+db.activeMem.Size() > db.activeMem.cfg.MemTableSize {
		if db.immtableQ.size > db.immtableQ.capacity/2 {
			db.memFlushC <- db.immtableQ.Dequeue()
		}
		db.immtableQ.Enqueue(db.activeMem)
		db.activeMem = <-db.memTablePipe
	}
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

// /
func (db *C2KV) FirstIndex() uint64 {
	return 0
}

func (db *C2KV) LastIndex() uint64 {
	return 0
}

// /
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
