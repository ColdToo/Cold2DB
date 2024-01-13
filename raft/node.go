package raft

import (
	"context"
	"errors"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
)

//go:generate mockgen -source=./raft_node.go -destination=../mocks/raft_node.go -package=mock

type Node interface {
	// Tick increments the internal logical clock for the Node by a single tick. Election
	// timeouts and heartbeat timeouts are in units of ticks.
	Tick()

	// Propose proposes that data be appended to the log. Note that proposals can be lost without
	// notice, therefore it is user's job to ensure proposal retries.
	Propose(ctx context.Context, data []byte) error
	// ProposeConfChange proposes a configuration change. Like any proposal
	ProposeConfChange(ctx context.Context, cc pb.ConfChange) error
	// TransferLeadership attempts to transfer leadership to the given transferee.
	TransferLeadership(ctx context.Context, lead, transferee uint64)
	// Campaign causes the Node to transition to candidate state and start campaigning to become leader.
	Campaign(ctx context.Context) error
	// Step advances the state machine using the given message. ctx.Err() will be returned, if any.
	Step(ctx context.Context, msg pb.Message) error

	// ApplyConfChange applies a config change (previously passed to
	// ProposeConfChange) to the node. This must be called whenever a config
	// change is observed in Ready.CommittedEntries, except when the app decides
	// to reject the configuration change (i.e. treats it as a noop instead), in
	// which case it must not be called.
	ApplyConfChange(cc pb.ConfChange) *pb.ConfState

	// Ready returns a channel that returns the current point-in-time state.
	// Users of the Node must call Advance after retrieving the state returned by Ready.
	//
	// NOTE: No committed entries from the next Ready may be applied until all committed entries
	// and snapshots from the previous one have finished.
	Ready() <-chan Ready
	// Advance notifies the Node that the application has saved progress up to the last Ready.
	// It prepares the node to return the next available Ready.
	//
	// The application should generally call Advance after it applies the entries in last Ready.
	//
	// However, as an optimization, the application may call Advance while it is applying the
	// commands. For example. when the last Ready contains a snapshot, the application might take
	// a long time to apply the snapshot data. To continue receiving Ready without blocking raft
	// progress, it can call Advance before finishing applying the last ready.
	Advance()

	// ReportUnreachable reports the given node is not reachable for the last send.
	ReportUnreachable(id uint64)
	// ReportSnapshot reports the status of the sent snapshot. The id is the raft ID of the follower
	// who is to receive the snapshot, and the status is SnapshotFinish or SnapshotFailure.
	// Calling ReportSnapshot with SnapshotFinish is a no-op. But, any failure in applying a
	// snapshot (for e.g., while streaming it from leader to follower), should be reported to the
	// leader with SnapshotFailure. When leader sends a snapshot to a follower, it pauses any raft
	// log probes until the follower can apply the snapshot and advance its state. If the follower
	// can't do that, for e.g., due to a crash, it could end up in a limbo, never getting any
	// updates from the leader. Therefore, it is crucial that the application ensures that any
	// failure in snapshot sending is caught and reported back to the leader; so it can resume raft
	// log probing in the follower.
	ReportSnapshot(id uint64, status SnapshotStatus)

	// ReadIndex request a read state. The read state will be set in the ready.
	// Read state has a read index. Once the application advances further than the read
	// index, any linearizable read requests issued before the read request can be
	// processed safely. The read state will have the same rctx attached.
	// Note that request can be lost without notice, therefore it is user's job
	// to ensure read index retries.
	ReadIndex(ctx context.Context, rctx []byte) error

	// Status returns the current status of the raft state machine.
	Status() Status
	// Stop performs any necessary termination of the Node.
	Stop()
}

var (
	emptyState = pb.HardState{}
	ErrStopped = errors.New("raft: stopped")
)

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64 // must use atomic operations to access; keep 64-bit aligned.
	RaftState StateType
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

type msgWithResult struct {
	m      pb.Message
	result chan error
}

type raftNode struct {
	rawNode     *rawNode
	confStateC  chan pb.ConfState
	confChangeC chan pb.ConfChange
	readyC      chan Ready
	propC       chan msgWithResult
	receiveC    chan pb.Message
	advanceC    chan struct{}
	tickC       chan struct{}
	doneC       chan struct{}
	stopC       chan struct{}
	statusC     chan chan Status
	AdvanceC    chan struct{}
	ErrorC      chan error
	status      chan chan Status
}

func StartRaftNode(raftConfig *config.RaftConfig, storage db.Storage) (Node, error) {
	opts := &raftOpts{
		ID:               raftConfig.ID,
		electionTimeout:  raftConfig.ElectionTick,
		heartbeatTimeout: raftConfig.HeartbeatTick,
	}

	rn, err := NewRawNode(opts, storage)
	if err != nil {
		return nil, err
	}

	rN := &raftNode{
		propC:       make(chan msgWithResult),
		receiveC:    make(chan pb.Message),
		confChangeC: make(chan pb.ConfChange),
		confStateC:  make(chan pb.ConfState),
		readyC:      make(chan Ready),
		advanceC:    make(chan struct{}),
		// make tickc a buffered chan, so raft node can buffer some ticks when the node
		// is busy processing raft messages. Raft node will resume process buffered
		// ticks when it becomes idle.
		tickC:   make(chan struct{}, 128),
		doneC:   make(chan struct{}),
		stopC:   make(chan struct{}),
		statusC: make(chan chan Status),
		rawNode: rn,
	}
	rN.serveAppNode()
	return rN, nil
}

func (rn *raftNode) serveAppNode() {
	var propC chan msgWithResult
	var readyC chan Ready
	var advanceC chan struct{}
	var rd Ready

	r := rn.rawNode.raft

	for {
		if advanceC != nil {
			readyC = nil
		} else if rn.rawNode.HasReady() {
			rd = rn.rawNode.readyWithoutAccept()
			readyC = rn.readyC
		}

		select {
		case <-rn.tickC:
			rn.rawNode.Tick()
		case m := <-rn.receiveC:
			if pr := r.trk.Progress[m.From]; pr != nil {
				r.Step(m)
			}
		case pm := <-propC:
			m := pm.m
			m.From = r.id
			err := r.Step(m)
			if pm.result != nil {
				pm.result <- err
				close(pm.result)
			}

		case readyC <- rd:
			rn.rawNode.acceptReady(rd)
			advanceC = rn.advanceC
		case <-advanceC:
			rn.rawNode.Advance(rd)
			rd = Ready{}
			advanceC = nil

		case <-rn.stopC:
			close(rn.doneC)
			return
		}
	}
}

func (rn *raftNode) Campaign(ctx context.Context) error {
	return rn.step(ctx, pb.Message{Type: pb.MsgHup})
}

func (rn *raftNode) ProposeConfChange(ctx context.Context, cc pb.ConfChange) error {
	msg, err := confChangeToMsg(cc)
	if err != nil {
		return err
	}
	return rn.Step(ctx, msg)
}

func confChangeToMsg(c pb.ConfChange) (pb.Message, error) {
	//typ, data, err := pb.MarshalConfChange(c)
	//if err != nil {
	//	return pb.Message{}, err
	//}
	//return pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Type: typ, Data: data}}}, nil
	return pb.Message{}, nil
}

func (rn *raftNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	var cs pb.ConfState
	select {
	case rn.confChangeC <- cc:
	case <-rn.doneC:
	}
	select {
	case cs = <-rn.confStateC:
	case <-rn.doneC:
	}
	return &cs
}

func (rn *raftNode) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	select {
	// manually set 'from' and 'to', so that leader can voluntarily transfers its leadership
	case rn.receiveC <- pb.Message{Type: pb.MsgTransferLeader, From: transferee, To: lead}:
	case <-rn.doneC:
	case <-ctx.Done():
	}
}

func (rn *raftNode) Propose(ctx context.Context, data []byte) error {
	return rn.stepWait(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}})
}

func (rn *raftNode) Step(ctx context.Context, m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.Type) {
		// TODO: return an error?
		return nil
	}
	return rn.step(ctx, m)
}

func (rn *raftNode) step(ctx context.Context, m pb.Message) error {
	return rn.stepWithWaitOption(ctx, m, false)
}

func (rn *raftNode) stepWait(ctx context.Context, m pb.Message) error {
	return rn.stepWithWaitOption(ctx, m, true)
}

func (rn *raftNode) stepWithWaitOption(ctx context.Context, m pb.Message, wait bool) error {
	if m.Type != pb.MsgProp {
		select {
		case rn.receiveC <- m:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-rn.doneC:
			return ErrStopped
		}
	}
	ch := rn.propC
	pm := msgWithResult{m: m}
	if wait {
		pm.result = make(chan error, 1)
	}
	select {
	case ch <- pm:
		if !wait {
			return nil
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-rn.doneC:
		return ErrStopped
	}
	select {
	case err := <-pm.result:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-rn.doneC:
		return ErrStopped
	}
	return nil
}

type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// ReadStates can be used for node to serve linearizable read requests locally
	// when its applied index is greater than the index in ReadState.
	// Note that the readState will be returned when raft receives msgReadIndex.
	// The returned is only valid for the request that requested to read.
	ReadStates []ReadState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MsgSnap message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message

	// MustSync indicates whether the HardState and Entries must be synchronously
	// written to disk or if an asynchronous write is permissible.
	MustSync bool

	HardState pb.HardState

	UnstableEntries []*pb.Entry //需要持久化的entries
}

func newReady(r *raft, prevSoftSt *SoftState, prevHardSt pb.HardState) Ready {
	rd := Ready{
		Entries:          r.raftLog.unstableEntries(),
		CommittedEntries: r.raftLog.nextApplyEnts(),
		Messages:         r.msgs,
	}
	if softSt := r.softState(); !softSt.equal(prevSoftSt) {
		rd.SoftState = softSt
	}
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, prevHardSt) {
		rd.HardState = hardSt
	}

	rd.MustSync = MustSync(r.hardState(), prevHardSt, len(rd.Entries))
	return rd
}

func (rd Ready) containsUpdates() bool {
	return rd.SoftState != nil || !IsEmptyHardState(rd.HardState) ||
		!IsEmptySnap(rd.Snapshot) || len(rd.Entries) > 0 ||
		len(rd.CommittedEntries) > 0 || len(rd.Messages) > 0 || len(rd.ReadStates) != 0
}

// appliedCursor extracts from the Ready the highest index the client has
// applied (once the Ready is confirmed via Advance). If no information is
// contained in the Ready, returns zero.
func (rd Ready) appliedCursor() uint64 {
	if n := len(rd.CommittedEntries); n > 0 {
		return rd.CommittedEntries[n-1].Index
	}
	if index := rd.Snapshot.Metadata.Index; index > 0 {
		return index
	}
	return 0
}

func (rn *raftNode) Ready() <-chan Ready { return rn.readyC }

func (rn *raftNode) Advance() {
	select {
	case rn.advanceC <- struct{}{}:
	case <-rn.doneC:
	}
}

// MustSync 在这里，"同步写入"指的是将数据立即写入到持久化存储（如磁盘）中，
// 并且在写入完成之前阻塞其他操作，以确保数据的持久性和一致性。
// 这是为了保证在发生故障或重启时，系统能够恢复到一个一致的状态。
func mustSync(st, prevst pb.HardState, entsnum int) bool {
	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	// currentTerm
	// votedFor
	// log entries[]
	return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term
}

func (rn *raftNode) Tick() {
	select {
	case rn.tickC <- struct{}{}:
	case <-rn.doneC:
	default:
		log.Warnf("%d A tick missed to fire. Node blocks too long!", rn.rawNode.raft.id)
	}
}

func (rn *raftNode) Stop() {
	select {
	case rn.stopC <- struct{}{}:
		// Not already stopped, so trigger it
	case <-rn.doneC:
		// Node has already been stopped - no need to do anything
		return
	}
	// Block until the stop has been acknowledged by run()
	<-rn.doneC
}

func (rn *raftNode) ReportUnreachable(id uint64) {
	select {
	case rn.receiveC <- pb.Message{Type: pb.MsgUnreachable, From: id}:
	case <-rn.doneC:
	}
}

func (rn *raftNode) ReportSnapshot(id uint64, status SnapshotStatus) {
	rej := status == SnapshotFailure
	select {
	case rn.receiveC <- pb.Message{Type: pb.MsgSnapStatus, From: id, Reject: rej}:
	case <-rn.doneC:
	}
}

func (rn *raftNode) ReadIndex(ctx context.Context, rctx []byte) error {
	return rn.step(ctx, pb.Message{Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: rctx}}})
}

func (rn *raftNode) Status() Status {
	c := make(chan Status)
	select {
	case rn.status <- c:
		return <-c
	case <-rn.doneC:
		return Status{}
	}
}

// MustSync returns true if the hard state and count of Raft entries indicate
// that a synchronous write to persistent storage is required.
func MustSync(st, prevst pb.HardState, entsnum int) bool {
	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	// currentTerm
	// votedFor
	// log entries[]
	return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term
}
