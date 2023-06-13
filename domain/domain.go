package domain

import (
	"github.com/spf13/viper"
)

var (
	Viper    *viper.Viper
	Log      *Logger
	RaftConf *RaftConfig
)

func Init() {
	Viper = InitViper()
	Log = NewLog()
}
