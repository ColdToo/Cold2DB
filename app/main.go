package main

import (
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/domain"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"strings"
)

func main() {
	domain.InitConfig()
	log.InitLog(domain.GetZapConf())
	err := db.InitDB(domain.GetDBConf())
	if err != nil {
		log.Panic("init db failed")
	}

	proposeC := make(chan []byte)
	defer close(proposeC)
	confChangeC := make(chan pb.ConfChange)
	defer close(confChangeC)
	errorC := make(chan error)
	defer close(errorC)

	var localIpAddr string
	var localId int
	var peerurl []string
	raftConf := domain.GetRaftConf()
	for _, node := range raftConf.Nodes {
		if strings.Contains(node.EAddr, "127.0.0.1") && strings.Contains(node.IAddr, "127.0.0.1") {
			localId = node.ID
			localIpAddr = node.EAddr
		}
		peerurl = append(peerurl, node.IAddr)
	}

	kvStore := NewKVStore(proposeC)
	StartAppNode(localId, peerurl, proposeC, confChangeC, errorC, kvStore, raftConf, localIpAddr)
	ServeHttpKVAPI(kvStore, localIpAddr, confChangeC, errorC)
}
