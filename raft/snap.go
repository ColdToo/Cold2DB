package raft

type SnapshotStatus int

const (
	SnapshotFinish  SnapshotStatus = 1
	SnapshotFailure SnapshotStatus = 2
)
