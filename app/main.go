package main

import (
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
)

func main() {
	config.InitConfig()
	log.InitLog(config.GetZapConf())
	db.InitDB(config.GetDBConf())
	log.Info("start success").Record()

	localIpAddr, localId, peerUrl := config.GetLocalInfo()
	proposeC := make(chan []byte)
	confChangeC := make(chan pb.ConfChange)
	kvApiStopC := make(chan struct{})

	kvStore := NewKVStore(proposeC)
	StartAppNode(localId, peerUrl, proposeC, confChangeC, kvApiStopC, kvStore, config.GetRaftConf(), localIpAddr)
	ServeHttpKVAPI(kvStore, localIpAddr, confChangeC, kvApiStopC)
}
