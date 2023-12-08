package db

import (
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/log"
	"testing"
)

var testDBPath = "dbtest/walfile"
var TestMemConfig = config.MemConfig{
	MemtableSize: 1024,
	MemtableNums: 10,
	Concurrency:  5,
}

func TestNewMemtable(t *testing.T) {
	NewMemtable(TestMemConfig)
}

func InitLog() {
	cfg := &config.ZapConfig{
		Level:         "debug",
		Format:        "console",
		Prefix:        "[C2KV]",
		Director:      "./log",
		ShowLine:      true,
		EncodeLevel:   "LowercaseColorLevelEncoder",
		StacktraceKey: "stacktrace",
		LogInConsole:  true,
	}
	log.InitLog(cfg)
}
