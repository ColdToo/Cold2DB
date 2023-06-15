package domain

import (
	"github.com/ColdToo/Cold2DB/log"
	"github.com/spf13/viper"
)

var (
	Viper    *viper.Viper
	RaftConf *RaftConfig
)

func Init() {
	Viper = InitViper()
	log.InitLog()
}
