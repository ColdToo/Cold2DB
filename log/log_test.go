package log

import (
	"github.com/ColdToo/Cold2DB/config"
	"testing"
	"time"
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
	Debug("测试日志功能是否正常").Str("time", time.Now().String()).Record()
	Debugf("测试日志功能是否正常%s", time.Now().String())
	Info("测试日志功能是否正常").Str("time", time.Now().String()).Record()
	Warn("测试日志功能是否正常").Str("time", time.Now().String()).Record()
	Error("测试日志功能是否正常").Str("time", time.Now().String()).Record()
	Panic("测试日志功能是否正常").Str("time", time.Now().String()).Record()
	Fatal("测试日志功能是否正常").Str("time", time.Now().String()).Record()
}
