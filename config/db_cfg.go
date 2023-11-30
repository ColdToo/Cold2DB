package config

type DBConfig struct {
	DBPath string

	MemConfig MemConfig

	ValueLogConfig ValueLogConfig

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

	WalConfig WalConfig

	// Persist index,IndexerDir dir path to store index meta data
	IndexerDir string

	//memory index or persist index
	IndexerType int8
}

type WalConfig struct {
	WalDirPath string

	SegmentSize int //specifies the maximum size of each segment file in bytes. SegmentSize int64
}

type ValueLogConfig struct {
	ValueLogDir string

	PartitionNums int
}
