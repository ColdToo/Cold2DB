package db

import (
	"github.com/ColdToo/Cold2DB/config"
	"testing"
)

func TestInitDB(t *testing.T) {
	dbCfg := &config.DBConfig{
		DBPath: "./path/to/db",
		MemConfig: config.MemConfig{
			WalDirPath: "./path/to/wal",
		},
		IndexConfig: config.IndexConfig{
			IndexerDir: "./path/to/indexer",
		},
		HardStateLogConfig: config.HardStateLogConfig{
			HardStateLogDir: "./path/to/hardstate",
		},
		ValueLogConfig: config.ValueLogConfig{
			ValueLogDir: "./path/to/partition",
		},
	}
	InitDB(dbCfg)
}
