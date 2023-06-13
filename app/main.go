package main

import (
	"flag"
	"github.com/ColdToo/Cold2DB/domain"
	"github.com/ColdToo/Cold2DB/raftproto"
	"strings"
)

func main() {
	//raftexample --id 1 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 12380
	//raftexample --id 2 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 22380
	//raftexample --id 3 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 32380
	cluster := flag.String("cluster", "127.0.0.1:9021", "存储集群")
	localId := flag.Int("ID", 1, "节点ID")
	kvport := flag.Int("port", 9081, "节点提供存储服务的kv端口")
	join := flag.Bool("join", false, "是否加入已经存在的集群")
	flag.Parse()

	domain.Init()

	proposeC := make(chan kv)
	defer close(proposeC)
	confChangeC := make(chan raftproto.ConfChange)
	defer close(confChangeC)
	errC := make(chan error)
	defer close(errC)
	commitC := make(chan *commit)
	defer close(commitC)
	errorC := make(chan error)
	defer close(errorC)

	kvStore := NewKVStore(proposeC, commitC, errorC)

	StartAppNode(*localId, strings.Split(*cluster, ","), *join, proposeC, confChangeC, commitC, errorC)

	ServeHttpKVAPI(kvStore, *kvport, confChangeC, errC)
}
