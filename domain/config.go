package domain

import (
	"time"
)

type Config struct {
	ZapConf    *ZapConfig
	DBConfig   *DBConfig
	RaftConfig *RaftConfig
}

type ZapConfig struct {
	Level         string `mapstructure:"level" json:"level" yaml:"level"`                            // 级别
	Prefix        string `mapstructure:"prefix" json:"prefix" yaml:"prefix"`                         // 日志前缀
	Format        string `mapstructure:"format" json:"format" yaml:"format"`                         // 输出
	Director      string `mapstructure:"director" json:"director"  yaml:"director"`                  // 日志文件夹
	EncodeLevel   string `mapstructure:"encode-level" json:"encode-level" yaml:"encode-level"`       // 编码级
	StacktraceKey string `mapstructure:"stacktrace-key" json:"stacktrace-key" yaml:"stacktrace-key"` // 栈名

	MaxAge       int  `mapstructure:"max-age" json:"max-age" yaml:"max-age"`                      // 日志留存时间
	ShowLine     bool `mapstructure:"show-line" json:"show-line" yaml:"show-line"`                // 显示行
	LogInConsole bool `mapstructure:"log-in-console" json:"log-in-console" yaml:"log-in-console"` // 输出控制台
}

type RaftConfig struct {
	ElectionTick  int
	HeartbeatTick int
	Nodes         []Node `yaml:"nodes"`
}

type Node struct {
	ID    int    `yaml:"id"`
	EAddr string `yaml:"eAddr"`
	IAddr string `yaml:"iAddr"`
}

type DBConfig struct {
	DBPath string

	MemConfig MemConfig

	ValueLogConfig ValueLogConfig

	IndexConfig IndexConfig

	HardStateLogConfig HardStateLogConfig
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
	// In this scenario will wait: memtable has reached the maximum nums, and has no time to be flushed into disk.
	// Default value is 100ms.
	MemSpaceWaitTimeout time.Duration

	// wal dir path, one memtable corresponds one wal
	WalDirPath string

	WalSync bool

	WalMMap bool

	// Persist index,IndexerDir dir path to store index meta data
	IndexerDir string

	IndexerType int8
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
}

type HardStateLogConfig struct {
	HardStateLogDir string
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
