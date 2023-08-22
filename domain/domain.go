package domain

import (
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

var (
	Viper *viper.Viper
	Conf  *Config
)

func InitConfig() {
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
