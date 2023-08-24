package main

import (
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"strings"
)

func main() {
	config.InitConfig()
	log.InitLog(config.GetZapConf())
	db.InitDB(config.GetDBConf())

	raftConf := config.GetRaftConf()
	localIpAddr, localId, peerUrl := getLocalInfo(raftConf)
	proposeC := make(chan []byte)
	defer close(proposeC)
	confChangeC := make(chan pb.ConfChange)
	defer close(confChangeC)
	doneC := make(chan struct{})

	kvStore := NewKVStore(proposeC)
	StartAppNode(localId, peerUrl, proposeC, confChangeC, doneC, kvStore, raftConf, localIpAddr)
	ServeHttpKVAPI(kvStore, localIpAddr, confChangeC, doneC)
}

func getLocalInfo(raftConf *config.RaftConfig) (localIpAddr string, localId int, peerUrl []string) {
	for _, node := range raftConf.Nodes {
		if strings.Contains(node.EAddr, "127.0.0.1") && strings.Contains(node.IAddr, "127.0.0.1") {
			localId = node.ID
			localIpAddr = node.EAddr
		}
		peerUrl = append(peerUrl, node.IAddr)
	}
	return
}
