package raft

type unstable struct {
	// storage contains all stable entries since the last snapshot.
	storage raft.Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []raftproto.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *raftproto.Snapshot

	// Your Data Here (2A).
}
