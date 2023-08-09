package raft

import (
	"errors"
	"github.com/ColdToo/Cold2DB/pb"
)

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
// snapshot is temporarily unavailable.
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

type Storage interface {
	InitialState() (pb.HardState, pb.ConfState, error)

	// Entries 返回指定范围的Entries
	Entries(lo, hi uint64) []*pb.Entry

	Term(i uint64) (uint64, error)

	AppliedIndex() (uint64, error)

	FirstIndex() (uint64, error)

	// GetSnapshot  返回最新的快照
	GetSnapshot() (pb.Snapshot, error)
}
