package db

import (
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/db/valuelog"
	"github.com/ColdToo/Cold2DB/db/wal"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/utils"
	"os"
	"strings"
)

var Cold2 *Cold2KV

type Cold2KV struct {
	activeMem *Memtable

	immuMems []*Memtable

	flushChn chan *Memtable

	memtablePipe chan *Memtable
	//raft log entries
	entries []*pb.Entry

	wal *wal.WAL

	valueLog *valuelog.ValueLog
}

func GetStorage() (Storage, error) {
	if Cold2 != nil {
		return Cold2, nil
	} else {
		return nil, errors.New("db is no init complete")
	}
}

func InitDB(dbCfg *config.DBConfig) {
	err := dbCfgCheck(dbCfg)
	if err != nil {
		log.Panic("check db cfg failed")
	}

	Cold2 = new(Cold2KV)
	tableFlushC := make(chan *Memtable, dbCfg.MemConfig.MemtableNums-1)
	Cold2.flushChn = tableFlushC
	Cold2.immuMems = make([]*Memtable, dbCfg.MemConfig.MemtableNums-1)

	memOpt := MemOpt{
		fsize:   int64(dbCfg.MemConfig.MemtableSize),
		memSize: dbCfg.MemConfig.MemtableSize,
	}
	Cold2.activeMem, err = newMemtable(memOpt)
	if err != nil {
		return
	}

	Cold2.wal, err = wal.NewWal(dbCfg.WalConfig)

	Cold2.restoreMemoryFromWAL()

	Cold2.valueLog, err = valuelog.OpenValueLog(dbCfg.ValueLogConfig)
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

func (db *Cold2KV) restoreMemoryFromWAL() {
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
			segmentFile, err := wal.OpenSegmentFile(db.wal.Config.WalDirPath, id)
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

	db.wal.OrderSegmentList.Find(int64(db.wal.StateSegment.RaftState.Applied))
	//通过applied index找到对应的最小segment file
	//启动两个goroutine分别将segment file加载到Immemtble和enties中
	return
}

func (db *Cold2KV) Get(key []byte) (val []byte, err error) {
	flag, val := db.activeMem.Get(key)
	if !flag {
		return nil, errors.New("the key is not exist")
	}
	//todo 若memtable找不到则查询索引
	return
}

func (db *Cold2KV) Scan(lowKey []byte, highKey []byte) (err error) {
	return err
}

func (db *Cold2KV) Entries(lo, hi uint64) (entries []*pb.Entry, err error) {
	if int(lo) < len(db.entries) {
		return nil, errors.New("some entries is compacted")
	}
	return
}

func (db *Cold2KV) SaveCommittedEntries(entries []*marshal.KV) (err error) {
	return nil
}

func (db *Cold2KV) SaveHardState(st pb.HardState) error {
	db.wal.StateSegment.RaftState = st
	enStateBytes, _ := marshal.EncodeRaftState(st)
	return db.wal.StateSegment.Persist(enStateBytes)
}

func (db *Cold2KV) SaveEntries(entries []*pb.Entry) error {
	err := db.wal.Write(entries)
	if err != nil {
		return err
	}
	db.entries = append(db.entries, entries...)
	return nil
}

func (db *Cold2KV) Term(i uint64) (uint64, error) {
	//如果i已经applied返回compact错误，若没有则返回对应term
	return 0, errors.New("the specific index entry is compacted")
}

func (db *Cold2KV) AppliedIndex() uint64 {
	return 0
}

func (db *Cold2KV) FirstIndex() uint64 {
	return 0
}

func (db *Cold2KV) LastIndex() uint64 {
	return 0
}

func (db *Cold2KV) GetSnapshot() (pb.Snapshot, error) {
	return pb.Snapshot{}, nil
}

func (db *Cold2KV) GetHardState() (pb.HardState, pb.ConfState, error) {
	return pb.HardState{}, pb.ConfState{}, nil
}

func (db *Cold2KV) Close() {

}
