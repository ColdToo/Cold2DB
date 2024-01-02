package raft

import (
	"context"
	"errors"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/pb"
)

var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

//go:generate mockgen -source=./raft_node.go -destination=../mocks/raft_node.go -package=mock

// Node represents a node in a raft cluster.
type Node interface {
	// Tick increments the internal logical clock for the Node by a single tick. Election
	// timeouts and heartbeat timeouts are in units of ticks.
	Tick()
	// Campaign causes the Node to transition to candidate state and start campaigning to become leader.
	Campaign(ctx context.Context) error
	// Propose proposes that data be appended to the log. Note that proposals can be lost without
	// notice, therefore it is user's job to ensure proposal retries.
	Propose(ctx context.Context, data []byte) error
	// ProposeConfChange proposes a configuration change. Like any proposal, the
	// configuration change may be dropped with or without an error being
	// returned. In particular, configuration changes are dropped unless the
	// leader has certainty that there is no prior unapplied configuration
	// change in its log.
	//
	// The method accepts either a pb.ConfChange (deprecated) or pb.ConfChangeV2
	// message. The latter allows arbitrary configuration changes via joint
	// consensus, notably including replacing a voter. Passing a ConfChangeV2
	// message is only allowed if all Nodes participating in the cluster run a
	// version of this library aware of the V2 API. See pb.ConfChangeV2 for
	// usage details and semantics.
	ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error

	// Step advances the state machine using the given message. ctx.Err() will be returned, if any.
	Step(ctx context.Context, msg pb.Message) error

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
	// ApplyConfChange applies a config change (previously passed to
	// ProposeConfChange) to the node. This must be called whenever a config
	// change is observed in Ready.CommittedEntries, except when the app decides
	// to reject the configuration change (i.e. treats it as a noop instead), in
	// which case it must not be called.
	//
	// Returns an opaque non-nil ConfState protobuf which must be recorded in
	// snapshots.
	ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState

	// TransferLeadership attempts to transfer leadership to the given transferee.
	TransferLeadership(ctx context.Context, lead, transferee uint64)

	// ReadIndex request a read state. The read state will be set in the ready.
	// Read state has a read index. Once the application advances further than the read
	// index, any linearizable read requests issued before the read request can be
	// processed safely. The read state will have the same rctx attached.
	// Note that request can be lost without notice, therefore it is user's job
	// to ensure read index retries.
	ReadIndex(ctx context.Context, rctx []byte) error

	// Status returns the current status of the raft state machine.
	Status() Status
	// ReportUnreachable reports the given node is not reachable for the last send.
	ReportUnreachable(id uint64)
	// ReportSnapshot reports the status of the sent snapshot. The id is the raft ID of the follower
	// who is meant to receive the snapshot, and the status is SnapshotFinish or SnapshotFailure.
	// Calling ReportSnapshot with SnapshotFinish is a no-op. But, any failure in applying a
	// snapshot (for e.g., while streaming it from leader to follower), should be reported to the
	// leader with SnapshotFailure. When leader sends a snapshot to a follower, it pauses any raft
	// log probes until the follower can apply the snapshot and advance its state. If the follower
	// can't do that, for e.g., due to a crash, it could end up in a limbo, never getting any
	// updates from the leader. Therefore, it is crucial that the application ensures that any
	// failure in snapshot sending is caught and reported back to the leader; so it can resume raft
	// log probing in the follower.
	ReportSnapshot(id uint64, status SnapshotStatus)
	// Stop performs any necessary termination of the Node.
	Stop()
}

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead     uint64 // must use atomic operations to access; keep 64-bit aligned.
	RaftRole Role
}

//type RaftNode struct {
//	raft       *raft
//	prevSoftSt *SoftState
//	prevHardSt pb.HardState
//}

type msgWithResult struct {
	m      pb.Message
	result chan error
}

type raftNode struct {
	raftNode    rawNode
	confStateC  chan pb.ConfState
	confChangeC chan pb.ConfChange
	ReadyC      chan *Ready
	propc       chan msgWithResult
	recvc       chan pb.Message

	advancec chan struct{}
	tickc    chan struct{}
	done     chan struct{}
	stop     chan struct{}
	status   chan chan Status
	AdvanceC chan struct{}
	ErrorC   chan error
}

func StartRaftNode(raftConfig *config.RaftConfig, storage db.Storage) (Node, error) {
	opts := &raftOpts{
		ID:            raftConfig.ID,
		ElectionTick:  raftConfig.ElectionTick,
		HeartbeatTick: raftConfig.HeartbeatTick,
		Peers:         raftConfig.Peers,
	}

	rn, err := NewRaftNode(c)
	rn.serveAppNode()
	return rn, nil
}

func (rn *raftNode) serveAppNode() {
	var readyC chan *Ready
	var advanceC chan struct{}
	var rd *Ready

	r := rn.raft
	lead := None

	for {
		if advanceC != nil {
			readyC = nil
		} else if rd = rn.Ready(); rd != nil {
			readyC = rn.ReadyC
		}

		select {
		case readyC <- rd:
			advanceC = rn.AdvanceC

		case <-advanceC:
			advanceC = nil

		case <-rn.stop:
			close(rn.done)
			return
		}
	}
}

// Step 网络层通过该方法处理message信息
func (rn *raftNode) Step(m *pb.Message) error {
	return rn.Raft.Step(m)
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

///////

// Propose 用于kv请求提议
func (rn *raftNode) Propose(buffer []byte) error {
	ent := pb.Entry{Data: buffer}
	ents := make([]pb.Entry, 0)
	ents = append(ents, ent)
	return rn.Raft.Step(&pb.Message{
		Type:    pb.MsgProp,
		From:    rn.Raft.id,
		Entries: ents})
}

// ProposeConfChange 用于配置变更信息处理
func (rn *raftNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{Type: pb.EntryConfChange, Data: data}
	return rn.Raft.Step(&pb.Message{
		Type:    pb.MsgProp,
		Entries: []pb.Entry{ent},
	})
}

////

func (rn *raftNode) Tick() {
	rn.Raft.tick()
}

// Advance 做下一轮ready的预处理
func (rn *raftNode) Advance() {
	// 每当appNode处理完一次ready后需要更新raftlog的first index 和 applied index
	rn.Raft.RaftLog.RefreshFirstAndAppliedIndex()
	// 需要将log中的entries进行裁剪

	// raft算法层可以进行下一轮ready
	rn.AdvanceC <- struct{}{}
}

func (rn *raftNode) newReady() Ready {
	rd := Ready{
		CommittedEntries: rn.Raft.raftLog.NextApplyEnts(),
	}

	if len(rn.Raft.msgs) > 0 {
		rd.Messages = rn.Raft.msgs
		//todo 清空msg防止重复发送，这里会不会出现并发问题
		rn.Raft.msgs = make([]*pb.Message, 0)
	}

	hardState := pb.HardState{
		Term:    rn.Raft.Term,
		Vote:    rn.Raft.VoteFor,
		Applied: rn.Raft.raftLog.AppliedIndex(),
	}

	if !isHardStateEqual(rn.prevHardSt, hardState) {
		rd.HardState = hardState
	}

	return rd
}

// 1、需要持久化的状态有改变
// 2、有待applied的entries
// 3、有待发送给其他节点的msg

func (rn *raftNode) Ready() (rd *Ready) {
	rd = new(Ready)

	hardState := pb.HardState{
		Term:    rn.Raft.Term,
		Vote:    rn.Raft.VoteFor,
		Applied: rn.Raft.raftLog.AppliedIndex(),
	}

	if !IsEmptyHardState(hardState) && !isHardStateEqual(rn.prevHardSt, hardState) {
		rd.HardState = hardState
	}

	if len(rn.Raft.msgs) > 0 {
		rd.Messages = rn.Raft.msgs
		//todo 清空msg防止重复发送，这里会不会出现并发问题
		rn.Raft.msgs = make([]*pb.Message, 0)
	}

	if rn.Raft.RaftLog.HasNextApplyEnts() {
		rd.CommittedEntries = rn.Raft.RaftLog.NextApplyEnts()
	}
	return
}

func (rn *raftNode) GetReadyC() chan *Ready {
	return rn.ReadyC
}

func (rn *raftNode) GetErrorC() chan error {
	return rn.ErrorC
}

//////

// 配置变更

func (rn *raftNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(&pb.Message{Type: pb.MsgTransferLeader, From: transferee})
}

func (rn *raftNode) ApplyConfChange(cc pb.ConfChange) {
	return
}

//网络层报告接口

func (rn *raftNode) ReportUnreachable(id uint64) {
	return
}

func (rn *raftNode) ReportSnapshot(id uint64, status SnapshotStatus) {
	return
}

func (rn *raftNode) Stop() {
	// todo 回收raft相关资源
}
