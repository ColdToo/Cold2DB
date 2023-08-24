package config

import (
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"strings"
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

func GetLocalInfo() (localIpAddr string, localId int, peerUrl []string) {
	raftConf := Conf.RaftConfig
	for _, node := range raftConf.Nodes {
		if strings.Contains(node.EAddr, "127.0.0.1") && strings.Contains(node.IAddr, "127.0.0.1") {
			localId = node.ID
			localIpAddr = node.EAddr
		}
		peerUrl = append(peerUrl, node.IAddr)
	}
	return
}
