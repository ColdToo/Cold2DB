package db

import (
	"errors"
	"github.com/ColdToo/Cold2DB/db/valuelog"
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

//go:generate mockgen -source=./db.go -destination=../mocks/db.go -package=mock
type Storage interface {
	Get(key []byte) (val []byte, err error)
	Scan(lowKey []byte, highKey []byte) (err error)

	SaveHardState(st pb.HardState) error
	SaveEntries(entries []*pb.Entry) error
	SaveCommittedEntries(entries []*valuelog.KV) error

	GetHardState() (pb.HardState, pb.ConfState, error)
	Entries(lo, hi uint64) ([]*pb.Entry, error)
	Term(i uint64) (uint64, error)
	AppliedIndex() uint64
	LastIndex() uint64
	FirstIndex() uint64
	GetSnapshot() (pb.Snapshot, error)

	Close()
}
