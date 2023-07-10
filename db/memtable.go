package db

import (
	"encoding/binary"
	"github.com/ColdToo/Cold2DB/db/arenaskl"
	"github.com/ColdToo/Cold2DB/db/logfile"
	"github.com/ColdToo/Cold2DB/log"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type memtable struct {
	sync.RWMutex
	sklIter      *arenaskl.Iterator
	skl          *arenaskl.Skiplist
	wal          *logfile.LogFile
	bytesWritten uint32 // number of bytes written, used for flush wal file.
	memCfg       memCfg
}

// options held by memtable for opening new memtables.
type memCfg struct {
	walDirPath  string
	walFileName string
	fsize       int64
	ioType      logfile.IOType
	memSize     uint32
	bytesFlush  uint32
}

type memValue struct {
	Term      uint64
	Index     uint64
	value     []byte
	expiredAt int64
	typ       byte
}

func initMemtable(dbCfg *DBConfig) (err error) {
	var ioType = logfile.BufferedIO
	if dbCfg.WalMMap {
		ioType = logfile.MMap
	}

	memCfg := memCfg{
		walDirPath: dbCfg.WalDirPath,
		fsize:      int64(dbCfg.MemtableSize),
		ioType:     ioType,
		memSize:    dbCfg.MemtableSize,
	}

	// read wal dir.
	DirEntries, err := os.ReadDir(dbCfg.WalDirPath)
	if err != nil {
		return err
	}

	// if more than zero load the wal file to memtable
	if len(DirEntries) > 0 {
		var fnames []string
		for _, entry := range DirEntries {
			if !strings.HasSuffix(entry.Name(), logfile.WalSuffixName) {
				continue
			}
			splitNames := strings.Split(entry.Name(), ".")
			fname := splitNames[0]
			if err != nil {
				return err
			}
			fnames = append(fnames, fname)
		}

		//根据timestamp排序
		sort.Slice(fnames, func(i, j int) bool {
			return fnames[i] < fnames[j]
		})

		// todo load memtables in concurrency
		// newest wal is active memtable
		for i, fname := range fnames {
			memCfg.walFileName = fname
			table, err := openMemtable(memCfg)
			if err != nil {
				return err
			}
			if i == 0 {
				Cold2.activeMem = table
			} else {
				Cold2.immuMems = append(Cold2.immuMems, table)
			}
		}

	} else {
		Cold2.activeMem, err = newMemtable(memCfg)
	}

	return nil
}

func openMemtable(memCfg memCfg) (*memtable, error) {
	var sklIter = new(arenaskl.Iterator)
	arena := arenaskl.NewArena(memCfg.memSize + uint32(arenaskl.MaxNodeSize))
	skl := arenaskl.NewSkiplist(arena)
	sklIter.Init(skl)
	table := &memtable{memCfg: memCfg, skl: skl, sklIter: sklIter}

	// open wal log file.
	wal, err := logfile.OpenLogFile(memCfg.walDirPath, memCfg.walFileName, memCfg.fsize*2, logfile.WAL, memCfg.ioType)
	if err != nil {
		return nil, err
	}
	table.wal = wal

	// load wal entries into memory.
	var offset int64 = 0
	for {
		if entry, size, err := wal.ReadLogEntry(offset); err == nil {
			offset += size
			// No need to use atomic updates.
			// This function is only be executed in one goroutine at startup.
			wal.WriteAt += size

			mv := &memValue{
				value:     entry.Value,
				expiredAt: entry.ExpiredAt,
				typ:       byte(entry.Type),
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
		} else {
			if err == io.EOF || err == logfile.ErrEndOfEntry {
				break
			}
			return nil, err
		}
	}
	return table, nil
}

func newMemtable(memCfg memCfg) (*memtable, error) {
	var sklIter = new(arenaskl.Iterator)
	arena := arenaskl.NewArena(memCfg.memSize + uint32(arenaskl.MaxNodeSize))
	skl := arenaskl.NewSkiplist(arena)
	sklIter.Init(skl)
	table := &memtable{memCfg: memCfg, skl: skl, sklIter: sklIter}

	wal, err := logfile.OpenLogFile(memCfg.walDirPath, memCfg.walFileName, memCfg.fsize*2, logfile.WAL, memCfg.ioType)
	if err != nil {
		return nil, err
	}
	table.wal = wal

	return table, nil
}

// put new writes to memtable.
func (mt *memtable) put(key []byte, value []byte, deleted bool) error {
	entry := &logfile.LogEntry{
		Key:   key,
		Value: value,
	}

	if deleted {
		entry.Type = logfile.TypeDelete
	}

	// write entrty into wal first.
	buf, sz := logfile.EncodeEntry(entry)
	if mt.wal != nil {
		if err := mt.wal.Write(buf); err != nil {
			return err
		}

		// if Sync is ture in WriteOptions or bytesWritten has reached bytesFlush, syncWal will be true.
		var syncWal = opts.Sync
		if mt.opts.bytesFlush > 0 {
			writes := atomic.AddUint32(&mt.bytesWritten, uint32(sz))
			if writes > mt.opts.bytesFlush {
				syncWal = true
				atomic.StoreUint32(&mt.bytesWritten, 0)
			}
		}
		if syncWal {
			if err := mt.syncWAL(); err != nil {
				return err
			}
		}
	}

	// write data into skip list .
	mv := memValue{value: value, expiredAt: entry.ExpiredAt, typ: byte(entry.Type)}
	mvBuf := mv.encode()
	if mt.sklIter.Seek(key) {
		return mt.sklIter.Set(mvBuf)
	}
	return mt.sklIter.Put(key, mvBuf)
}

// get value from memtable.
// if the specified key is marked as deleted or expired, a true bool value is returned.
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

// delete operation is to put a key and a special tombstone value.
func (mt *memtable) delete(key []byte, opts WriteOptions) error {
	return mt.put(key, nil, true, opts)
}

func (mt *memtable) syncWAL() error {
	mt.wal.RLock()
	defer mt.wal.RUnlock()
	return mt.wal.Sync()
}

func (mt *memtable) isFull(delta uint32) bool {
	if mt.skl.Size()+delta+paddedSize >= mt.opts.memSize {
		return true
	}
	if mt.wal == nil {
		return false
	}

	walSize := atomic.LoadInt64(&mt.wal.WriteAt)
	return walSize >= int64(mt.opts.memSize)
}

func (mt *memtable) logFileId() uint32 {
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
	return memValue{typ: buf[0], expiredAt: ex, value: buf[index:]}
}
