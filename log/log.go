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

func InitLog() {
	if ok, _ := utils.PathExists(domain.Conf.ZapConf.Director); !ok { // 判断是否有Director文件夹
		fmt.Printf("create %v directory\n", domain.Conf.ZapConf.Director)
		_ = os.Mkdir(domain.Conf.ZapConf.Director, os.ModePerm)
	}

	cores := domain.Conf.ZapConf.GetZapCores()
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

// GetEncoder 获取 zapcore.Encoder
func (z *ZapConfig) GetEncoder() zapcore.Encoder {
	if domain.Conf.ZapConf.Format == "json" {
		return zapcore.NewJSONEncoder(z.GetEncoderConfig())
	}
	return zapcore.NewConsoleEncoder(z.GetEncoderConfig())
}

// GetEncoderConfig 获取zapcore.EncoderConfig
func (z *ZapConfig) GetEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		MessageKey:     "message",
		LevelKey:       "level",
		TimeKey:        "time",
		NameKey:        "logger",
		CallerKey:      "caller",
		StacktraceKey:  domain.Conf.ZapConf.StacktraceKey,
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    domain.Conf.ZapConf.ZapEncodeLevel(),
		EncodeTime:     z.CustomTimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.FullCallerEncoder,
	}
}

// GetEncoderCore 获取Encoder的 zapcore.Core
func (z *ZapConfig) GetEncoderCore(l zapcore.Level, level zap.LevelEnablerFunc) zapcore.Core {
	writer, err := FileRotatelogs.GetWriteSyncer(l.String()) // 使用file-rotatelogs进行日志分割
	if err != nil {
		fmt.Printf("Get Write Syncer Failed err:%v", err.Error())
		return nil
	}

	return zapcore.NewCore(z.GetEncoder(), writer, level)
}

// CustomTimeEncoder 自定义日志输出时间格式
func (z *ZapConfig) CustomTimeEncoder(t time.Time, encoder zapcore.PrimitiveArrayEncoder) {
	encoder.AppendString(domain.Conf.ZapConf.Prefix + t.Format("2006/01/02 - 15:04:05.000"))
}

// GetZapCores 根据配置文件的Level获取 []zapcore.Core
func (z *ZapConfig) GetZapCores() []zapcore.Core {
	cores := make([]zapcore.Core, 0, 7)
	for level := domain.Conf.ZapConf.TransportLevel(); level <= zapcore.FatalLevel; level++ {
		cores = append(cores, z.GetEncoderCore(level, z.GetLevelPriority(level)))
	}
	return cores
}

// GetLevelPriority 根据 zapcore.Level 获取 zap.LevelEnablerFunc
func (z *ZapConfig) GetLevelPriority(level zapcore.Level) zap.LevelEnablerFunc {
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
