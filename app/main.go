package main

import (
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
)

func main() {
	config.InitConfig()
	localIpAddr, localId, nodes := config.GetLocalInfo()
	raftConfig := config.GetRaftConf()
	log.InitLog(config.GetZapConf())
	proposeC := make(chan []byte, raftConfig.RequestLimit)
	confChangeC := make(chan pb.ConfChange)
	kvServiceStopC := make(chan struct{})
	monitorKV := make(map[int64]chan struct{})

	kvStorage, err := db.OpenKVStorage(config.GetDBConf())
	if err != nil {
		log.Panicf("open kv storage failed", err)
	}

	kvStore := NewKVService(proposeC, confChangeC, raftConfig.RequestTimeout, kvStorage, monitorKV)
	StartAppNode(localId, nodes, proposeC, confChangeC, kvServiceStopC, kvStorage, raftConfig, localIpAddr, monitorKV)
	ServeHttpKVAPI(kvStore, localIpAddr, kvServiceStopC)
}
