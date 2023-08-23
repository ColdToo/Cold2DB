package log

import (
	"github.com/ColdToo/Cold2DB/domain"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"go.uber.org/zap/zapcore"
	"os"
	"path"
	"time"
)

var FileRotatelogs = new(fileRotatelogs)

type fileRotatelogs struct{}

// GetWriteSyncer 获取 zapcore.WriteSyncer
// Author [SliverHorn](https://github.com/SliverHorn)
func (r *fileRotatelogs) GetWriteSyncer(level string, config *domain.ZapConfig) (zapcore.WriteSyncer, error) {
	fileWriter, err := rotatelogs.New(
		path.Join(config.Director, "%Y-%m-%d", level+".log"),
		rotatelogs.WithClock(rotatelogs.Local),
		rotatelogs.WithMaxAge(time.Duration(config.MaxAge)*24*time.Hour), // 日志留存时间
		rotatelogs.WithRotationTime(time.Hour*24),
	)
	if config.LogInConsole {
		return zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout), zapcore.AddSync(fileWriter)), err
	}
	return zapcore.AddSync(fileWriter), err
}
