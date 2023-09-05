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

	localIpAddr, localId, peerUrl := config.GetLocalInfo()
	raftConfig := config.GetRaftConf()
	proposeC := make(chan []byte, raftConfig.RequestLimit)
	confChangeC := make(chan pb.ConfChange)
	kvHTTPStopC := make(chan struct{})

	kvStore := NewKVStore(proposeC, raftConfig.RequestTimeout)
	StartAppNode(localId, peerUrl, proposeC, confChangeC, kvHTTPStopC, kvStore, raftConfig, localIpAddr)
	ServeHttpKVAPI(kvStore, localIpAddr, confChangeC, kvHTTPStopC)
}
