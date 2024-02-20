package raft

func newTestConfig(id uint64, peers []uint64, election, heartbeat int, storage Storage) *Config {
	return &Config{
		ID:            id,
		peers:         peers,
		ElectionTick:  election,
		HeartbeatTick: heartbeat,
		Storage:       storage,
	}
}

func newTestRaft(id uint64, peers []uint64, election, heartbeat int, storage Storage) *Raft {
	return newRaft(newTestConfig(id, peers, election, heartbeat, storage))
}
