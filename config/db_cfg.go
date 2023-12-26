package config

type DBConfig struct {
	DBPath string

	MemConfig MemConfig

	ValueLogConfig ValueLogConfig

	WalConfig WalConfig

	// Default value is 5.
	MemTableNums int

	// pre allocate  default value is 1
	MemTablePipeSize int
}

type MemConfig struct {
	// Default value is 64MB. MB Unit
	MemTableSize int64

	// 写入memTable并发度
	Concurrency int
}

type WalConfig struct {
	WalDirPath string

	SegmentSize int //specifies the maximum size of each segment file in bytes. SegmentSize int64
}

type ValueLogConfig struct {
	ValueLogDir string

	PartitionNums int

	SSTSize int
}
