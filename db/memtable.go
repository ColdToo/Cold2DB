package db

import (
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/arenaskl"
	"github.com/ColdToo/Cold2DB/db/logfile"
	"github.com/ColdToo/Cold2DB/log"
	"io"
	"os"
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

	walDirPath string
}

func NewMemManger(memCfg config.MemConfig) (manager *memManager, err error) {
	memManger := new(memManager)
	memManger.walDirPath = memCfg.WalDirPath
	memManger.flushChn = make(chan *memtable, memCfg.MemtableNums-1)
	memManger.immuMems = make([]*memtable, memCfg.MemtableNums-1)

	memOpt := MemOpt{
		walFileId:  time.Now().Unix(),
		walDirPath: memCfg.WalDirPath,
		fsize:      int64(memCfg.MemtableSize),
		ioType:     logfile.MMap,
		memSize:    memCfg.MemtableSize,
	}

	memManger.activeMem, err = memManger.newMemtable(memOpt)
	if err != nil {
		return
	}

	go memManger.reopenImMemtable(memOpt)

	return memManger, nil
}

func (m *memManager) newMemtable(memOpt MemOpt) (*memtable, error) {
	var sklIter = new(arenaskl.Iterator)
	arena := arenaskl.NewArena(memOpt.memSize + uint32(arenaskl.MaxNodeSize))
	skl := arenaskl.NewSkiplist(arena)
	sklIter.Init(skl)
	table := &memtable{memOpt: memOpt, skl: skl, sklIter: sklIter}

	wal, err := logfile.OpenLogFile(memOpt.walDirPath, memOpt.walFileId, memOpt.fsize*2, logfile.WALLog, memOpt.ioType)
	if err != nil {
		return nil, err
	}
	table.wal = wal

	return table, nil
}

func (m *memManager) reopenImMemtable(memOpt MemOpt) {
	DirEntries, err := os.ReadDir(memOpt.walDirPath)
	if err != nil {
		log.Errorf("", err)
		return
	}

	if len(DirEntries) <= 0 {
		log.Info("没有wal文件,不用初始化immtable")
		return
	}

	var fids []int64
	for _, entry := range DirEntries {
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

	wal, err := logfile.OpenLogFile(memOpt.walDirPath, memOpt.walFileId, memOpt.fsize*2, logfile.WALLog, memOpt.ioType)
	if err != nil {
		return nil, err
	}
	table.wal = wal

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
	wal      *logfile.LogFile
	memOpt   MemOpt
	maxIndex uint64
	minIndex uint64
}

// options held by memtable for opening new memtables.
type MemOpt struct {
	walDirPath string
	walFileId  int64
	fsize      int64
	ioType     logfile.IOType
	memSize    uint32
	bytesFlush uint32
}

// todo 写入WAL就返回还是写入Memtable再返回？
func (mt *memtable) put(entry logfile.Entry) error {
	buf, _ := entry.EncodeWALEntry()
	if err := mt.wal.Write(buf); err != nil {
		return err
	}
	if err := mt.syncWAL(); err != nil {
		return err
	}
	mt.putInMemtable(entry)
	return nil
}

func (mt *memtable) putBatch(entries []logfile.Entry) error {
	for _, entry := range entries {
		buf, _ := entry.EncodeWALEntry()
		if err := mt.wal.Write(buf); err != nil {
			return err
		}
	}
	if err := mt.syncWAL(); err != nil {
		return err
	}

	// todo 写入WAL就返回还是写入Memtable再返回？
	return nil
}

func (mt *memtable) putInMemtable(entry logfile.Entry) {
	memEntryBuf := entry.MemEntry.EncodeMemEntry()
	err := mt.sklIter.Put(entry.Key, memEntryBuf)
	if err != nil {
		log.Errorf("", err)
		return
	}
}

func (mt *memtable) Get(key []byte) (bool, []byte) {
	if found := mt.sklIter.Seek(key); !found {
		return false, nil
	}

	value, err := mt.sklIter.Get(key)
	if err == arenaskl.ErrRecordNotExists {
		return false, nil
	}
	mv := logfile.DecodeMemEntry(value)

	if mv.Type == logfile.TypeDelete {
		return true, nil
	}

	if mv.ExpiredAt > 0 && mv.ExpiredAt <= time.Now().Unix() {
		return true, nil
	}

	return false, mv.Value
}

func (mt *memtable) syncWAL() error {
	mt.wal.RLock()
	defer mt.wal.RUnlock()
	return mt.wal.Sync()
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

func (mt *memtable) walFileId() int64 {
	return mt.wal.Fid
}

func (mt *memtable) deleteWal() error {
	mt.wal.Lock()
	defer mt.wal.Unlock()
	return mt.wal.Delete()
}
