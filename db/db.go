package db

import (
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/iooperator/directio"
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

	for _, file := range files {
		if strings.Contains(file.Name(), ".SEG") {
			fd, err := directio.OpenDirectFile(file.Name(), os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				log.Panicf("can not ")
			}

			var id int64
			_, err = fmt.Sscanf(file.Name(), "%d.SEG", &id)
			if err != nil {
				log.Panicf("can not scan file")
			}
			//根据segment文件名中的index对segment进行排序
			db.wal.OrderSegmentList.Insert(id, fd)
		}

		if strings.Contains(file.Name(), ".RAFT") {
			fd, err := directio.OpenDirectFile(file.Name(), os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				log.Panicf("can not ")
			}
			db.wal.RaftStateSegment.Fd = fd
			//todo 将HsSegment中的数据序列化到hardstate中
		}
	}

	//通过applied index找到对应的最小segment file
	//启动两个goroutine分别将segment file加载到Immemtble和enties中
	go m.reopenEntries()
	go m.reopenImMemtable()
	return
}

func (db *Cold2KV) Get(key []byte) (val []byte, err error) {
	flag, val := db.memManager.activeMem.Get(key)
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
	if int(lo) < len(db.memManager.entries) {
		return nil, errors.New("some entries is compacted")
	}
	return
}

func (db *Cold2KV) SaveCommittedEntries(entries []*valuelog.KV) (err error) {
	db.memManager.
	return nil
}

func (db *Cold2KV) SaveHardState(st pb.HardState) error {
	db.memManager.wal.PersistRaftStatus(st)
	return nil
}

func (db *Cold2KV) SaveEntries(entries []*pb.Entry) error {
	err := db.memManager.wal.Write(entries)
	if err != nil {
		return err
	}
	db.memManager.entries = append(db.memManager.entries, entries...)
	return nil
}

func (db *Cold2KV) Term(i uint64) (uint64, error) {
	//如果i已经applied返回compact错误，若没有则返回对应term
	return 0, errors.New("the specific index entry is compacted")
}

func (db *Cold2KV) AppliedIndex() uint64 {
	return db.memManager.appliedIndex
}

func (db *Cold2KV) FirstIndex() uint64 {
	return db.memManager.firstIndex
}

func (db *Cold2KV) LastIndex() uint64 {
	return db.memManager.firstIndex
}

func (db *Cold2KV) GetSnapshot() (pb.Snapshot, error) {
	return pb.Snapshot{}, nil
}

func (db *Cold2KV) GetHardState() (pb.HardState, pb.ConfState, error) {
	return pb.HardState{}, pb.ConfState{}, nil
}

func (db *Cold2KV) Close() {

}
