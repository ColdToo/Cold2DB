package main

import "flag"

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "存储集群")
	id := flag.Int("ID", 1, "节点ID")
	kvport := flag.Int("port", 9081, "节点提供存储服务的kv端口")
	join := flag.Bool("join", false, "是否加入已经存在的集群")
	flag.Parse()
	print(cluster)

	//创建一个raft节点
}
