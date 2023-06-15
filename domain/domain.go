package domain

import (
	"github.com/spf13/viper"
)

var (
	Viper    *viper.Viper
	RaftConf *RaftConfig
)

func Init() {
	Viper = InitViper()
	InitLog()
}
