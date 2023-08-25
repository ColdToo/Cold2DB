package log

import (
	"github.com/ColdToo/Cold2DB/config"
	"testing"
)

func TestInitLog(t *testing.T) {
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
	InitLog(cfg)
}
