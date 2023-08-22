package log

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/domain"
	"github.com/ColdToo/Cold2DB/utils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"strings"
	"time"
)

var log *zap.Logger

func InitLog(config *domain.ZapConfig) {
	if ok := utils.PathExist(config.Director); !ok { // 判断是否有Director文件夹
		fmt.Printf("create %v directory\n", config)
		_ = os.Mkdir(config.Director, os.ModePerm)
	}

	cores := getZapCores(config)
	log = zap.New(zapcore.NewTee(cores...))

	if domain.Conf.ZapConf.ShowLine {
		log = log.WithOptions(zap.AddCaller())
	}
}

func Debug(msg string) *Fields {
	if !log.Core().Enabled(zapcore.DebugLevel) {
		return newFields("", nil, true)
	}
	return newFields(msg, log, false)
}

func Info(msg string) *Fields {
	if !log.Core().Enabled(zapcore.DebugLevel) {
		return newFields("", nil, true)
	}
	return newFields(msg, log, false)
}

func Warn(msg string) *Fields {
	if !log.Core().Enabled(zapcore.DebugLevel) {
		return newFields("", nil, true)
	}
	return newFields(msg, log, false)
}

func Error(msg string) *Fields {
	if !log.Core().Enabled(zapcore.ErrorLevel) {
		return newFields("", nil, true)
	}
	return newFields(msg, log, false)
}

func Panic(msg string) *Fields {
	if !log.Core().Enabled(zapcore.DebugLevel) {
		return newFields("", nil, true)
	}
	return newFields(msg, log, false)
}

func Fatal(msg string) *Fields {
	if !log.Core().Enabled(zapcore.DebugLevel) {
		return newFields("", nil, true)
	}
	return newFields(msg, log, false)
}

func Infof(msg string, param ...any) {

}

func Warnf(msg string, param ...any) {

}

func Errorf(msg string, param ...any) {

}

func Debugf(msg string, param ...any) {

}

func Panicf(msg string, param ...any) *Fields {
	if !log.Core().Enabled(zapcore.DebugLevel) {
		return newFields("", nil, true)
	}
	return newFields(msg, log, false)
}

type Fields struct {
	level  zapcore.Level
	zap    *zap.Logger
	msg    string
	fields []zapcore.Field
	skip   bool
}

func newFields(msg string, l *zap.Logger, skip bool) (fields *Fields) {
	fields.msg = msg
	fields.zap = l
	fields.skip = skip
	return fields
}

func (f *Fields) Str(key string, val string) *Fields {
	if f.skip {
		return f
	}

	f.fields = append(f.fields, zapcore.Field{Key: key, Type: zapcore.StringType, String: val})
	return f
}

func (f *Fields) Strs(key string, val []string) *Fields {
	if f.skip {
		return f
	}

	f.fields = append(f.fields, zapcore.Field{Key: key, Type: zapcore.StringType, Interface: val})
	return f
}

func (f *Fields) Int(key string, val int) *Fields {
	if f.skip {
		return f
	}

	f.fields = append(f.fields, zapcore.Field{Key: key, Type: zapcore.Int32Type, Integer: int64(val)})
	return f
}

func (f *Fields) Err(key string, err error) *Fields {
	if err == nil || f.skip {
		return f
	}

	f.fields = append(f.fields, zapcore.Field{Key: key, Type: zapcore.ErrorType, Interface: err})
	return f
}

func (f *Fields) Bool(key string, val bool) *Fields {
	if f.skip {
		return f
	}
	var ival int64
	if val {
		ival = 1
	}
	f.fields = append(f.fields, zapcore.Field{Key: key, Type: zapcore.ErrorType, Integer: ival})
	return f
}

func (f *Fields) Record() {
	if f.skip {
		return
	}
	switch f.level {
	case zapcore.DebugLevel:
		f.zap.Debug(f.msg, f.fields...)
	case zapcore.InfoLevel:
		f.zap.Info(f.msg, f.fields...)
	case zapcore.WarnLevel:
		f.zap.Warn(f.msg, f.fields...)
	case zapcore.ErrorLevel:
		f.zap.Error(f.msg, f.fields...)
	case zapcore.PanicLevel:
		f.zap.Panic(f.msg, f.fields...)
	case zapcore.FatalLevel:
		f.zap.Fatal(f.msg, f.fields...)
	}
}

func getZapCores(config *domain.ZapConfig) []zapcore.Core {
	cores := make([]zapcore.Core, 0, 7)
	for level := transportLevel(config.Level); level <= zapcore.FatalLevel; level++ {
		cores = append(cores, getEncoderCore(level, getLevelPriority(level), config))
	}
	return cores
}

func transportLevel(level string) zapcore.Level {
	Level := strings.ToLower(level)
	switch Level {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.WarnLevel
	case "dpanic":
		return zapcore.DPanicLevel
	case "panic":
		return zapcore.PanicLevel
	case "fatal":
		return zapcore.FatalLevel
	default:
		return zapcore.DebugLevel
	}
}

func getEncoderCore(l zapcore.Level, level zap.LevelEnablerFunc, config *domain.ZapConfig) zapcore.Core {
	writer, err := FileRotatelogs.GetWriteSyncer(l.String()) // 使用file-rotatelogs进行日志分割
	if err != nil {
		fmt.Printf("Get Write Syncer Failed err:%v", err.Error())
		return nil
	}

	return zapcore.NewCore(getEncoder(config), writer, level)
}

func getLevelPriority(level zapcore.Level) zap.LevelEnablerFunc {
	switch level {
	case zapcore.DebugLevel:
		return func(level zapcore.Level) bool { // 调试级别
			return level == zap.DebugLevel
		}
	case zapcore.InfoLevel:
		return func(level zapcore.Level) bool { // 日志级别
			return level == zap.InfoLevel
		}
	case zapcore.WarnLevel:
		return func(level zapcore.Level) bool { // 警告级别
			return level == zap.WarnLevel
		}
	case zapcore.ErrorLevel:
		return func(level zapcore.Level) bool { // 错误级别
			return level == zap.ErrorLevel
		}
	case zapcore.DPanicLevel:
		return func(level zapcore.Level) bool { // dpanic级别
			return level == zap.DPanicLevel
		}
	case zapcore.PanicLevel:
		return func(level zapcore.Level) bool { // panic级别
			return level == zap.PanicLevel
		}
	case zapcore.FatalLevel:
		return func(level zapcore.Level) bool { // 终止级别
			return level == zap.FatalLevel
		}
	default:
		return func(level zapcore.Level) bool { // 调试级别
			return level == zap.DebugLevel
		}
	}
}

func zapEncodeLevel(encodeLevel string) zapcore.LevelEncoder {
	switch {
	case encodeLevel == "LowercaseLevelEncoder": // 小写编码器(默认)
		return zapcore.LowercaseLevelEncoder
	case encodeLevel == "LowercaseColorLevelEncoder": // 小写编码器带颜色
		return zapcore.LowercaseColorLevelEncoder
	case encodeLevel == "CapitalLevelEncoder": // 大写编码器
		return zapcore.CapitalLevelEncoder
	case encodeLevel == "CapitalColorLevelEncoder": // 大写编码器带颜色
		return zapcore.CapitalColorLevelEncoder
	default:
		return zapcore.LowercaseLevelEncoder
	}
}

func getEncoder(config *domain.ZapConfig) zapcore.Encoder {
	if config.Format == "json" {
		return zapcore.NewJSONEncoder(getEncoderConfig(config))
	}
	return zapcore.NewConsoleEncoder(getEncoderConfig(config))
}

func getEncoderConfig(config *domain.ZapConfig) zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		MessageKey:     "message",
		LevelKey:       "level",
		TimeKey:        "time",
		NameKey:        "logger",
		CallerKey:      "caller",
		StacktraceKey:  config.StacktraceKey,
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapEncodeLevel(config.EncodeLevel),
		EncodeTime:     customTimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.FullCallerEncoder,
	}
}

func customTimeEncoder(t time.Time, encoder zapcore.PrimitiveArrayEncoder) {
	encoder.AppendString(t.Format("2006/01/02 - 15:04:05.000"))
}
