package raft

import "go.uber.org/zap"

type raftLog struct {
	storage Storage

	unstable unstable

	committed uint64

	applied uint64

	logger zap.Logger
}
