package db

import (
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/arenaskl"
	"github.com/ColdToo/Cold2DB/db/logfile"
	"github.com/ColdToo/Cold2DB/db/wal"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

	go memManger.reopenImMemtable(memManger.wal)

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

func (m *memManager) reopenImMemtable(wal *wal.WAL) {
	var fids []int64
	for _, entry := range wal.OlderSegments {
		if !strings.HasSuffix(entry.Name(), logfile.WalSuffixName) {
			continue
		}
		splitNames := strings.Split(entry.Name(), ".")
		fid, _ := strconv.Atoi(splitNames[0])
		fids = append(fids, int64(fid))
	}
	// 根据文件timestamp排序
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})

	immtableC := make(chan *memtable, len(DirEntries))
	wg := sync.WaitGroup{}
	for _, fid := range fids {
		memOpt.walFileId = fid
		wg.Add(1)
		go func() {
			defer wg.Done()
			table, err := m.openMemtable(memOpt)
			if err != nil {
				log.Errorf("", err)
			}
			immtableC <- table
		}()
	}
	wg.Wait()
	close(immtableC)
	for table := range immtableC {
		m.immuMems = append(m.immuMems, table)
	}
	return
}

func (m *memManager) openMemtable(memOpt MemOpt) (*memtable, error) {
	var sklIter = new(arenaskl.Iterator)
	arena := arenaskl.NewArena(memOpt.memSize + uint32(arenaskl.MaxNodeSize))
	skl := arenaskl.NewSkiplist(arena)
	sklIter.Init(skl)
	table := &memtable{memOpt: memOpt, skl: skl, sklIter: sklIter}

	var offset int64 = 0
	var entry *logfile.Entry
	var size int64
	for {
		if entry, size, err = wal.ReadWALEntry(offset); err == nil {
			offset += size
			wal.WriteAt += size

			mv := &logfile.Entry{
				Index:     entry.Index,
				Term:      entry.Term,
				ExpiredAt: entry.ExpiredAt,
				Value:     entry.Value,
				Type:      entry.Type,
			}
			mvBuf := mv.EncodeMemEntry()

			var err error
			err = table.sklIter.Put(entry.Key, mvBuf)
			if err != nil {
				log.Errorf("put value into skip list err.%+v", err)
				return nil, err
			}
		}

		if err == io.EOF || err == logfile.ErrEndOfEntry {
			break
		}
	}

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

func (mt *memtable) isFull(delta uint32) bool {
	if mt.skl.Size()+delta >= mt.memOpt.memSize {
		return true
	}
	if mt.wal == nil {
		return false
	}

	walSize := atomic.LoadInt64(&mt.wal.WriteAt)
	return walSize >= int64(mt.memOpt.memSize)
}
