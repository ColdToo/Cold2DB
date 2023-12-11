package config

type DBConfig struct {
	DBPath string

	MemConfig MemConfig

	ValueLogConfig ValueLogConfig

	WalConfig WalConfig
}

type MemConfig struct {
	// MemtableSize represents the maximum size in bytes for a memtable, Default value is 64MB. MB Unit
	MemtableSize int

	// MemtableNums represents maximum number of memtables to keep in memory before flushing.
	// Default value is 5.
	MemtableNums int

	// 写入memtable并发度
	Concurrency int
}

type WalConfig struct {
	WalDirPath string

	SegmentSize int //specifies the maximum size of each segment file in bytes. SegmentSize int64
}

type ValueLogConfig struct {
	ValueLogDir string

	PartitionNums int

	SSTsize int
}
