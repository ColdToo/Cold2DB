package db

import (
	"encoding/binary"
	"github.com/ColdToo/Cold2DB/db/arenaskl"
	"github.com/ColdToo/Cold2DB/db/index"
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
	activeMem *memtable

	immuMems []*memtable

	flushChn chan *memtable

	indexer index.Indexer

	walDirPath string

	indexDirPath string
}

type memtable struct {
	sync.RWMutex
	sklIter *arenaskl.Iterator
	skl     *arenaskl.Skiplist
	wal     *logfile.LogFile
	memOpt  memOpt
}

// options held by memtable for opening new memtables.
type memOpt struct {
	walDirPath string
	walFileId  int64
	fsize      int64
	ioType     logfile.IOType
	memSize    uint32
	bytesFlush uint32
}

type memValue struct {
	Term      uint64
	Index     uint64
	value     []byte
	typ       logfile.EntryType
	expiredAt int64
}

func NewMemManger(memCfg MemConfig) (err error) {
	memManger := new(memManager)
	Cold2.memManager = memManger
	memManger.walDirPath = memCfg.WalDirPath
	memManger.flushChn = make(chan *memtable, memCfg.MemtableNums-1)
	memManger.immuMems = make([]*memtable, memCfg.MemtableNums-1)

	var ioType = logfile.BufferedIO
	if memCfg.WalMMap {
		ioType = logfile.MMap
	}
	memOpt := memOpt{
		walFileId:  time.Now().Unix(),
		walDirPath: memCfg.WalDirPath,
		fsize:      int64(memCfg.MemtableSize),
		ioType:     ioType,
		memSize:    memCfg.MemtableSize,
	}

	memManger.activeMem, err = memManger.newMemtable(memOpt)
	if err != nil {
		return err
	}

	go memManger.reopenImMemtable(memOpt)
	Cold2.memManager = memManger
	return nil
}

func (m *memManager) reopenImMemtable(memOpt memOpt) {
	DirEntries, err := os.ReadDir(memOpt.walDirPath)
	if err != nil {
		log.Error(err)
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
				log.Error(err)
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

func (m *memManager) openMemtable(memOpt memOpt) (*memtable, error) {
	var sklIter = new(arenaskl.Iterator)
	arena := arenaskl.NewArena(memOpt.memSize + uint32(arenaskl.MaxNodeSize))
	skl := arenaskl.NewSkiplist(arena)
	sklIter.Init(skl)
	table := &memtable{memOpt: memOpt, skl: skl, sklIter: sklIter}

	// open wal log file.
	wal, err := logfile.OpenLogFile(memOpt.walDirPath, memOpt.walFileId, memOpt.fsize*2, logfile.WAL, memOpt.ioType)
	if err != nil {
		return nil, err
	}
	table.wal = wal

	// load wal entries into memory.
	var offset int64 = 0
	for {
		if entry, size, err := wal.ReadWALEntry(offset); err == nil {
			offset += size
			// No need to use atomic updates.
			// This function is only be executed in one goroutine at startup.
			wal.WriteAt += size

			mv := &memValue{
				Index:     entry.Index,
				Term:      entry.Term,
				expiredAt: entry.ExpiredAt,
				value:     entry.Value,
				typ:       entry.Type,
			}
			mvBuf := mv.encode()

			var err error
			if table.sklIter.Seek(entry.Key) {
				err = table.sklIter.Set(mvBuf)
			} else {
				err = table.sklIter.Put(entry.Key, mvBuf)
			}
			if err != nil {
				log.Errorf("put value into skip list err.%+v", err)
				return nil, err
			}
		}

		if err == io.EOF || err == logfile.ErrEndOfEntry {
			break
		}

		return nil, err
	}

	return table, nil
}

func (m *memManager) newMemtable(memOpt memOpt) (*memtable, error) {
	var sklIter = new(arenaskl.Iterator)
	arena := arenaskl.NewArena(memOpt.memSize + uint32(arenaskl.MaxNodeSize))
	skl := arenaskl.NewSkiplist(arena)
	sklIter.Init(skl)
	table := &memtable{memOpt: memOpt, skl: skl, sklIter: sklIter}

	wal, err := logfile.OpenLogFile(memOpt.walDirPath, memOpt.walFileId, memOpt.fsize*2, logfile.WAL, memOpt.ioType)
	if err != nil {
		return nil, err
	}
	table.wal = wal

	return table, nil
}

// todo 写入WAL就返回还是写入Memtable再返回？
func (mt *memtable) put(entry logfile.WalEntry) error {
	buf, _ := logfile.EncodeWalEntry(&entry)
	if err := mt.wal.Write(buf); err != nil {
		return err
	}
	if err := mt.syncWAL(); err != nil {
		return err
	}
	go mt.putInMemtable(entry)
	return nil
}

func (mt *memtable) putBatch(entries []logfile.WalEntry) error {
	for _, entry := range entries {
		buf, _ := logfile.EncodeWalEntry(&entry)
		if err := mt.wal.Write(buf); err != nil {
			return err
		}
	}
	if err := mt.syncWAL(); err != nil {
		return err
	}

	// todo 写入WAL就返回还是写入Memtable再返回？
	go mt.putInMemtableBatch(entries)
	return nil
}

func (mt *memtable) putInMemtable(entry logfile.WalEntry) {
	mv := memValue{Term: entry.Term, Index: entry.Index, value: entry.Value, typ: entry.Type, expiredAt: entry.ExpiredAt}
	mvBuf := mv.encode()
	if mt.sklIter.Seek(entry.Key) {
		err := mt.sklIter.Set(mvBuf)
		if err != nil {
			log.Error(err)
		}
	}
	err := mt.sklIter.Put(entry.Key, mvBuf)
	if err != nil {
		log.Error(err)
	}
}

func (mt *memtable) putInMemtableBatch(entries []logfile.WalEntry) {
	for _, entry := range entries {
		mt.putInMemtable(entry)
	}
}

func (mt *memtable) get(key []byte) (bool, []byte) {
	mt.Lock()
	defer mt.Unlock()
	if found := mt.sklIter.Seek(key); !found {
		return false, nil
	}

	mv := decodeMemValue(mt.sklIter.Value())
	// ignore deleted key.
	if mv.typ == byte(logfile.TypeDelete) {
		return true, nil
	}
	// ignore expired key.
	if mv.expiredAt > 0 && mv.expiredAt <= time.Now().Unix() {
		return true, nil
	}
	return false, mv.value
}

func (mt *memtable) syncWAL() error {
	mt.wal.RLock()
	defer mt.wal.RUnlock()
	return mt.wal.Sync()
}

func (mt *memtable) isFull(delta uint32) bool {
	if mt.skl.Size()+delta >= mt.memCfg.memSize {
		return true
	}
	if mt.wal == nil {
		return false
	}

	walSize := atomic.LoadInt64(&mt.wal.WriteAt)
	return walSize >= int64(mt.memCfg.memSize)
}

func (mt *memtable) logFileId() int64 {
	return mt.wal.Fid
}

func (mt *memtable) deleteWal() error {
	mt.wal.Lock()
	defer mt.wal.Unlock()
	return mt.wal.Delete()
}

func (mv *memValue) encode() []byte {
	head := make([]byte, 11)
	head[0] = mv.typ
	var index = 1
	index += binary.PutVarint(head[index:], mv.expiredAt)

	buf := make([]byte, len(mv.value)+index)
	copy(buf[:index], head[:])
	copy(buf[index:], mv.value)
	return buf
}

func decodeMemValue(buf []byte) memValue {
	var index = 1
	ex, n := binary.Varint(buf[index:])
	index += n
	return memValue{typ: buf[0], value: buf[index:]}
}
