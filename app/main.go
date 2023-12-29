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
	kvStorage, err := db.OpenKVStorage(config.GetDBConf())
	if err != nil {
		return
	}

	localIpAddr, localId, nodes := config.GetLocalInfo()
	raftConfig := config.GetRaftConf()
	proposeC := make(chan []byte, raftConfig.RequestLimit)
	confChangeC := make(chan pb.ConfChange)
	kvHTTPStopC := make(chan struct{})

	kvStore := NewKVStore(proposeC, raftConfig.RequestTimeout, kvStorage)
	StartAppNode(localId, nodes, proposeC, confChangeC, kvHTTPStopC, kvStore, raftConfig, localIpAddr)

	ServeHttpKVAPI(kvStore, localIpAddr, confChangeC, kvHTTPStopC)
}
