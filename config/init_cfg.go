package config

import (
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

var (
	Viper *viper.Viper
	Conf  *Config
)

type Config struct {
	ZapConf    *ZapConfig
	DBConfig   *DBConfig
	RaftConfig *RaftConfig
}

func InitConfig() {
	defaultConfigPath := "/Users/hlhf/GolandProjects/Cold2DB/bin/config.yaml"
	Viper = viper.New()
	Viper.SetConfigFile(defaultConfigPath)
	Viper.SetConfigType("yaml")
	err := Viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	Conf = new(Config)
	if err = Viper.Unmarshal(Conf); err != nil {
		fmt.Println(err)
	}

	Viper.WatchConfig()
	Viper.OnConfigChange(func(e fsnotify.Event) {
		fmt.Println("config file changed:", e.Name)
		if err = Viper.Unmarshal(&Config{}); err != nil {
			fmt.Println(err)
		}
	})
}

func GetZapConf() *ZapConfig {
	return Conf.ZapConf
}

func GetRaftConf() *RaftConfig {
	return Conf.RaftConfig
}

func GetDBConf() *DBConfig {
	return Conf.DBConfig
}
