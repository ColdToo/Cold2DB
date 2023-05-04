package main

import (
	"flag"
	"go.etcd.io/etcd/raft/raftpb"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "存储集群")
	id := flag.Int("ID", 1, "节点ID")
	kvport := flag.Int("port", 9081, "节点提供存储服务的kv端口")
	join := flag.Bool("join", false, "是否加入已经存在的集群")
	flag.Parse()
	print(cluster)

	//用于接收客户端发送的消息
	proposeC := make(chan string)
	defer close(proposeC)
	//用于接收客户端发送的配置消息
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	NewAppNode()
}
