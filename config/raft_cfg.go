package config

type RaftConfig struct {
	ElectionTick   int
	HeartbeatTick  int
	RequestLimit   int
	RequestTimeout int
	Nodes          []Node `yaml:"nodes"`
}

type Node struct {
	ID    uint64 `yaml:"id"`
	EAddr string `yaml:"eAddr"`
	IAddr string `yaml:"iAddr"`
}
