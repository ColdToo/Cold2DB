package db

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/arenaskl"
	"github.com/ColdToo/Cold2DB/db/iooperator/directio"
	"github.com/ColdToo/Cold2DB/db/logfile"
	"github.com/ColdToo/Cold2DB/db/wal"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"os"
	"strings"
	"sync"
	"time"
)

type memManager struct {
	firstIndex uint64

	appliedIndex uint64

	activeMem *memtable

	immuMems []*memtable

	flushChn chan *memtable

	//raft log entries
	entries []*pb.Entry

	wal *wal.WAL
}

func NewMemManger(memCfg config.MemConfig) (manager *memManager, err error) {
	memManger := new(memManager)
	memManger.flushChn = make(chan *memtable, memCfg.MemtableNums-1)
	memManger.immuMems = make([]*memtable, memCfg.MemtableNums-1)

	memOpt := MemOpt{
		fsize:   int64(memCfg.MemtableSize),
		memSize: memCfg.MemtableSize,
	}

	manager.wal, err = wal.NewWal(memCfg.WalConfig)

	memManger.activeMem, err = memManger.newMemtable(memOpt)
	if err != nil {
		return
	}

	files, err := os.ReadDir(manager.wal.Config.WalDirPath)
	if err != nil {
		log.Panicf("open wal dir failed", err)
	}

	if len(files) != 0 {
		memManger.reopenImMemtableAndEntries(files)
	}

	return memManger, nil
}

func (m *memManager) newMemtable(memOpt MemOpt) (*memtable, error) {
	var sklIter = new(arenaskl.Iterator)
	arena := arenaskl.NewArena(memOpt.memSize + uint32(arenaskl.MaxNodeSize))
	skl := arenaskl.NewSkiplist(arena)
	sklIter.Init(skl)
	table := &memtable{memOpt: memOpt, skl: skl, sklIter: sklIter}
	return table, nil
}

func (m *memManager) reopenImMemtableAndEntries(files []os.DirEntry) {
	for _, file := range files {
		if strings.Contains(file.Name(), ".SEG") {
			//else
			fd, err := directio.OpenDirectFile(file.Name(), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
			if err != nil {
				log.Panicf("can not ")
			}

			var id int64
			_, err = fmt.Sscanf(file.Name(), "%d.SEG", &id)
			if err != nil {
				log.Panicf("can not scan file")
			}
			m.wal.OrderIndexList.Insert(id, fd)
		}

		if strings.Contains(file.Name(), ".RAFT") {
			fd, err := directio.OpenDirectFile(file.Name(), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
			if err != nil {
				log.Panicf("can not ")
			}
			m.wal.HsSegment.Fd = fd
			//todo 将数据序列化到hardstate中
		}
	}
	//通过applied index找到对应的最小segment file
	//启动两个goroutine分别将segment file加载到Immemtble和enties中
	go m.reopenEntries()
	go m.reopenImMemtable()
	return
}

func (m *memManager) reopenImMemtable() {
	appliedIndex := m.wal.RaftHardState.Applied
}

func (m *memManager) reopenEntries() {
	appliedIndex := m.wal.RaftHardState.Applied

}

func (m *memManager) openMemtable(memOpt MemOpt) (*memtable, error) {
	var sklIter = new(arenaskl.Iterator)
	arena := arenaskl.NewArena(memOpt.memSize + uint32(arenaskl.MaxNodeSize))
	skl := arenaskl.NewSkiplist(arena)
	sklIter.Init(skl)
	table := &memtable{memOpt: memOpt, skl: skl, sklIter: sklIter}

	return table, nil
}

type memtable struct {
	CreatAt uint64
	sync.RWMutex
	//todo iterator里既有arena也有skiplist是否合理？
	sklIter  *arenaskl.Iterator
	skl      *arenaskl.Skiplist
	memOpt   MemOpt
	maxIndex uint64
	minIndex uint64
}

// options held by memtable for opening new memtables.
type MemOpt struct {
	fsize   int64
	memSize uint32
}

// todo put重写
func (mt *memtable) put(kv logfile.KV) error {
	return nil
}

func (mt *memtable) putBatch(entries []logfile.KV) error {
	return nil
}

func (mt *memtable) putInMemtable(kv logfile.KV) {
}

func (mt *memtable) Get(key []byte) (bool, []byte) {
	if found := mt.sklIter.Seek(key); !found {
		return false, nil
	}

	value, err := mt.sklIter.Get(key)
	if err == arenaskl.ErrRecordNotExists {
		return false, nil
	}
	mv := logfile.DecodeV(value)

	if mv.Type == logfile.TypeDelete {
		return true, nil
	}

	if mv.ExpiredAt > 0 && mv.ExpiredAt <= time.Now().Unix() {
		return true, nil
	}

	return false, mv.Value
}
