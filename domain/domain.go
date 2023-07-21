package domain

import (
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/spf13/viper"
)

var (
	Viper *viper.Viper
	Conf  *Config
)

func Init() {
	InitViper()
	log.InitLog()
	db.InitDB()
}
