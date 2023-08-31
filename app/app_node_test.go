package main

import (
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/log"
)

func initLog() {
	cfg := &config.ZapConfig{
		Level:         "debug",
		Format:        "console",
		Prefix:        "[Cold2DB]",
		Director:      "./log",
		ShowLine:      true,
		EncodeLevel:   "LowercaseColorLevelEncoder",
		StacktraceKey: "stacktrace",
		LogInConsole:  true,
	}
	log.InitLog(cfg)
}
