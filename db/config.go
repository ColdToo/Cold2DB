package db

import (
	"time"
)

type DBConfig struct {
	// DBPath db path, will be created automatically if not exist.
	DBPath string

	// wal dir path, one memtable corresponds one wal
	WalDirPath string

	// MemtableSize represents the maximum size in bytes for a memtable.
	// It means that each memtable will occupy so much memory.
	// Default value is 64MB.
	MemtableSize uint32

	// MemtableNums represents maximum number of memtables to keep in memory before flushing.
	// Default value is 5.
	MemtableNums int

	// MemSpaceWaitTimeout represents timeout for waiting enough memtable space to write.
	// In this scenario will wait: memtable has reached the maximum nums, and has no time to be flushed into disk.
	// Default value is 100ms.
	MemSpaceWaitTimeout time.Duration

	// IndexerDir dir path to store index meta data, default value is dir path.
	IndexerDir string

	// WalMMap represents whether to use memory map for reading and writing on WAL.
	// Setting false means to use standard file io.
	// Default value is false.
	WalMMap bool

	// ValueLogDir dir path to store value log file, default value is dir path.
	ValueLogDir string

	// ValueLogFileSize size of a single value log file.
	// Default value is 1GB.
	ValueLogFileSize int64

	// ValueLogMmap similar to WalMMap, default value is false.
	ValueLogMmap bool

	// ValueLogGCRatio if discarded data in value log exceeds this ratio, it can be picked up for compaction(garbage collection)
	// And if there are many files reached the ratio, we will pick the highest one by one.
	// The recommended ratio is 0.5, half of the file can be compacted.
	// Default value is 0.5.
	ValueLogGCRatio float64

	// ValueLogGCInterval a background groutine will check and do gc periodically according to the interval.
	// If you don`t want value log file be compacted, set it a Zero time.
	// Default value is 10 minutes.
	ValueLogGCInterval time.Duration

	// Sync is whether to synchronize writes through os buffer cache and down onto the actual disk.
	// Setting sync is required for durability of a single write operation, but also results in slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (machine does not) then no writes will be lost.
	//
	// In other words, Sync being false has the same semantics as a write
	// system call. Sync being true means write followed by fsync.

	// Default value is false.
	Sync bool
}
