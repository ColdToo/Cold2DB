package config

import "time"

type DBConfig struct {
	DBPath string

	MemConfig MemConfig

	ValueLogConfig ValueLogConfig

	IndexConfig IndexConfig

	WalConfig WalConfig
}

type MemConfig struct {
	// MemtableSize represents the maximum size in bytes for a memtable.
	// It means that each memtable will occupy so much memory.
	// Default value is 64MB.
	MemtableSize uint32

	// MemtableNums represents maximum number of memtables to keep in memory before flushing.
	// Default value is 5.
	MemtableNums int

	// MemSpaceWaitTimeout represents timeout for waiting enough memtable space to write.
	// Default value is 100ms.
	MemSpaceWaitTimeout time.Duration

	WalConfig WalConfig

	// Persist index,IndexerDir dir path to store index meta data
	IndexerDir string

	//memory index or persist index
	IndexerType int8
}

type WalConfig struct {
	// wal dir path, one memtable corresponds one wal
	WalDirPath string

	// SegmentSize specifies the maximum size of each segment file in bytes. SegmentSize int64
}

type ValueLogConfig struct {
	ValueLogDir string

	ValueLogFileSize int64

	// ValueLogMmap similar to WalMMap, default value is false.
	ValueLogMmap bool
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
