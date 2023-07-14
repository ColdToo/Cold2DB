package db

import (
	"time"
)

type DBConfig struct {
	// DBPath db path, will be created automatically if not exist.
	DBPath string

	WalConfig

	ValueLogConfig

	MemTableConfig

	BufferConfig
}

type WalConfig struct {
	// wal dir path, one memtable corresponds one wal
	WalDirPath string

	WalSync bool

	WalMMap bool
}

type ValueLogConfig struct {
	ValueLogDir string

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
}

type MemTableConfig struct {
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
}

type IndexConfig struct {
	// Persist index,IndexerDir dir path to store index meta data
	IndexerDir string

	IndexerType int8
}

type BufferConfig struct {
	// Buffer dir path, one memtable corresponds one wal
	BufferDirPath string
}
