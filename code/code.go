package code

import "errors"

const (
	NodeInIErr = "NodeInIErr"

	ClusterId   = "cluster-id"
	LocalId     = "local-member-id"
	RemoteId    = "remote-peer-id"
	RemoteIp    = "remote-peer-ip"
	RemoteUrls  = "remote-peer-urls"
	RemoteReqId = "remote-peer-req-id"

	MessageProcErr = "MessageProcErr"

	FailedReadMessage  = "FailedReadMessage"
	TickErr            = "TickErr"
	ErrProposalDropped = "ErrProposalDropped"
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
