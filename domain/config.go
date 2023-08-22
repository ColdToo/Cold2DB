package domain

import (
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

type Config struct {
	//ZapConf    *log.ZapConfig
	//DBConfig   *db.DBConfig
	//RaftConfig *raft.RaftConfig
}

func InitViper() *viper.Viper {
	defaultConfigPath := "/bin/config.yaml"
	v := viper.New()
	v.SetConfigFile(defaultConfigPath)
	v.SetConfigType("yaml")
	err := v.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	if err = v.Unmarshal(&Config{}); err != nil {
		fmt.Println(err)
	}

	//watch config
	v.WatchConfig()
	v.OnConfigChange(func(e fsnotify.Event) {
		fmt.Println("config file changed:", e.Name)
		if err = v.Unmarshal(&Config{}); err != nil {
			fmt.Println(err)
		}
	})
	Viper = v
	return v
}

func GetConfig() *Config {
	return Conf
}
