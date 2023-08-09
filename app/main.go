package main

import (
	"bytes"
	"flag"
	"github.com/ColdToo/Cold2DB/pb"
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

	proposeC := make(chan bytes.Buffer)
	defer close(proposeC)
	confChangeC := make(chan pb.ConfChange)
	defer close(confChangeC)
	errorC := make(chan error)
	defer close(errorC)

	kvStore := NewKVStore(proposeC)
	StartAppNode(*localId, strings.Split(*cluster, ","), *join, proposeC, confChangeC, errorC)
	ServeHttpKVAPI(kvStore, *kvport, confChangeC, errorC)
}
