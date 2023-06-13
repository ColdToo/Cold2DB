package domain

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/utils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"strings"
	"time"
)

type Logger struct {
	zap *zap.Logger
}

func NewLog() *Logger {
	Zap := NewZap()
	return &Logger{zap: Zap}
}

func NewZap() (logger *zap.Logger) {
	if ok, _ := utils.PathExists(RaftConf.ZapConf.Director); !ok { // 判断是否有Director文件夹
		fmt.Printf("create %v directory\n", RaftConf.ZapConf.Director)
		_ = os.Mkdir(RaftConf.ZapConf.Director, os.ModePerm)
	}

	cores := Zap.GetZapCores()
	logger = zap.New(zapcore.NewTee(cores...))

	if RaftConf.ZapConf.ShowLine {
		logger = logger.WithOptions(zap.AddCaller())
	}

	return logger
}

func (l *Logger) Debug(msg string) *Fields {
	return newFields(msg, l.zap)
}

func (l *Logger) Info(msg string) *Fields {
	return newFields(msg, l.zap)
}

func (l *Logger) Warn(msg string) *Fields {
	return newFields(msg, l.zap)
}

func (l *Logger) Error(msg string) *Fields {
	return newFields(msg, l.zap)
}

func (l *Logger) Panic(msg string) *Fields {
	return newFields(msg, l.zap)
}

func (l *Logger) Fatal(msg string) *Fields {
	return newFields(msg, l.zap)
}

func (l *Logger) checkNeedRecord(msg string) bool {
	// todo 通过判断日志等级选择是否要打印此次日志
	return true
}

type Fields struct {
	Level  zapcore.Level
	zap    *zap.Logger
	msg    string
	fields []zapcore.Field
}

func newFields(msg string, l *zap.Logger) (fields *Fields) {
	// TODO 从 sync pool里面复用
	fields.msg = msg
	fields.zap = l
	return fields
}

func (f *Fields) Str(key string, val string) *Fields {
	// key val 存入field
	f.fields = append(f.fields, zapcore.Field{Key: key, Type: zapcore.StringType, String: val})
	return f
}

func (f *Fields) Int(key string, val int) *Fields {
	// key val 存入field
	f.fields = append(f.fields, zapcore.Field{Key: key, Type: zapcore.Int32Type, Integer: int64(val)})
	return f
}

func (f *Fields) Err(key string, err error) *Fields {
	if err == nil {
		return f
	}

	f.fields = append(f.fields, zapcore.Field{Key: key, Type: zapcore.ErrorType, Interface: err})
	return f
}

func (f *Fields) Bool(key string, val bool) *Fields {
	var ival int64
	if val {
		ival = 1
	}
	f.fields = append(f.fields, zapcore.Field{Key: key, Type: zapcore.ErrorType, Integer: ival})
	return f
}

func (f *Fields) Record() {
	switch f.Level {
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

// ZapEncodeLevel 根据 EncodeLevel 返回 zapcore.LevelEncoder
func (z *ZapConfig) ZapEncodeLevel() zapcore.LevelEncoder {
	switch {
	case z.EncodeLevel == "LowercaseLevelEncoder": // 小写编码器(默认)
		return zapcore.LowercaseLevelEncoder
	case z.EncodeLevel == "LowercaseColorLevelEncoder": // 小写编码器带颜色
		return zapcore.LowercaseColorLevelEncoder
	case z.EncodeLevel == "CapitalLevelEncoder": // 大写编码器
		return zapcore.CapitalLevelEncoder
	case z.EncodeLevel == "CapitalColorLevelEncoder": // 大写编码器带颜色
		return zapcore.CapitalColorLevelEncoder
	default:
		return zapcore.LowercaseLevelEncoder
	}
}

// TransportLevel 根据字符串转化为 zapcore.Level
func (z *ZapConfig) TransportLevel() zapcore.Level {
	z.Level = strings.ToLower(z.Level)
	switch z.Level {
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

var Zap = new(ZapInfo)

type ZapInfo struct{}

// GetEncoder 获取 zapcore.Encoder
// Author [SliverHorn](https://github.com/SliverHorn)
func (z *ZapInfo) GetEncoder() zapcore.Encoder {
	if RaftConf.ZapConf.Format == "json" {
		return zapcore.NewJSONEncoder(z.GetEncoderConfig())
	}
	return zapcore.NewConsoleEncoder(z.GetEncoderConfig())
}

// GetEncoderConfig 获取zapcore.EncoderConfig
// Author [SliverHorn](https://github.com/SliverHorn)
func (z *ZapInfo) GetEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		MessageKey:     "message",
		LevelKey:       "level",
		TimeKey:        "time",
		NameKey:        "logger",
		CallerKey:      "caller",
		StacktraceKey:  RaftConf.ZapConf.StacktraceKey,
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    RaftConf.ZapConf.ZapEncodeLevel(),
		EncodeTime:     z.CustomTimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.FullCallerEncoder,
	}
}

// GetEncoderCore 获取Encoder的 zapcore.Core
// Author [SliverHorn](https://github.com/SliverHorn)
func (z *ZapInfo) GetEncoderCore(l zapcore.Level, level zap.LevelEnablerFunc) zapcore.Core {
	writer, err := FileRotatelogs.GetWriteSyncer(l.String()) // 使用file-rotatelogs进行日志分割
	if err != nil {
		fmt.Printf("Get Write Syncer Failed err:%v", err.Error())
		return nil
	}

	return zapcore.NewCore(z.GetEncoder(), writer, level)
}

// CustomTimeEncoder 自定义日志输出时间格式
// Author [SliverHorn](https://github.com/SliverHorn)
func (z *ZapInfo) CustomTimeEncoder(t time.Time, encoder zapcore.PrimitiveArrayEncoder) {
	encoder.AppendString(RaftConf.ZapConf.Prefix + t.Format("2006/01/02 - 15:04:05.000"))
}

// GetZapCores 根据配置文件的Level获取 []zapcore.Core
// Author [SliverHorn](https://github.com/SliverHorn)
func (z *ZapInfo) GetZapCores() []zapcore.Core {
	cores := make([]zapcore.Core, 0, 7)
	for level := RaftConf.ZapConf.TransportLevel(); level <= zapcore.FatalLevel; level++ {
		cores = append(cores, z.GetEncoderCore(level, z.GetLevelPriority(level)))
	}
	return cores
}

// GetLevelPriority 根据 zapcore.Level 获取 zap.LevelEnablerFunc
// Author [SliverHorn](https://github.com/SliverHorn)
func (z *ZapInfo) GetLevelPriority(level zapcore.Level) zap.LevelEnablerFunc {
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
