package config

type RaftConfig struct {
	ElectionTick  int
	HeartbeatTick int
	Nodes         []Node `yaml:"nodes"`
}

type Node struct {
	ID    int    `yaml:"id"`
	EAddr string `yaml:"eAddr"`
	IAddr string `yaml:"iAddr"`
}
