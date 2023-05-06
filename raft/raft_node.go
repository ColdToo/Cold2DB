package raft

import (
	"errors"
	"github.com/ColdToo/Cold2DB/raftproto"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

type SoftState struct {
	LeaderID  uint64
	RaftState RoleType
}

// RaftNode is a wrapper of Raft.
type RaftNode struct {
	Raft *Raft
	// Your Data Here (2A).
}

// NewRaftNode returns a new RaftNode given configuration and a list of raft peers.
func NewRaftNode(config *Config) (*RaftNode, error) {
	raft, err := NewRaft(config)
	if err != nil {
		return nil, err
	}

	return &RaftNode{Raft: raft}, nil
}

// Tick 由应用层定时触发Tick
func (rn *RaftNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RaftNode to transition to candidate state.
func (rn *RaftNode) Campaign() error {
	return rn.Raft.Step(&raftproto.Message{
		MsgType: raftproto.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RaftNode) Propose(data []byte) error {
	ent := raftproto.Entry{Data: data}
	return rn.Raft.Step(&raftproto.Message{
		MsgType: raftproto.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*raftproto.Entry{&ent}})
}

// Step advances the state machine using the given message.
func (rn *RaftNode) Step(m *raftproto.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RaftNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == Leader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RaftNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(&raftproto.Message{MsgType: raftproto.MessageType_MsgTransferLeader, From: transferee})
}

// todo ready
// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	raftproto.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []raftproto.Entry // 待持久化

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot raftproto.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.可以提交的Entry
	CommittedEntries []raftproto.Entry // 待 apply

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []raftproto.Message // 待发送
}

// Ready returns the current point-in-time state of this RaftNode.
func (rn *RaftNode) Ready() Ready {
	// Your Code Here (2A).
	return Ready{}
}

// HasReady 由应用层调用该方法，如果发现由Ready则进行处理，然后再调用Advance方法表示该轮已经处理完成
func (rn *RaftNode) HasReady() bool {
	r := rn.raft
	if !r.softState().equal(rn.prevSoftSt) {
		return true
	}
	if hardSt := r.hardState(); !IsEmptyHardState(hardSt) && !isHardStateEqual(hardSt, rn.prevHardSt) {
		return true
	}
	if r.raftLog.hasPendingSnapshot() {
		return true
	}
	if len(r.msgs) > 0 || len(r.raftLog.unstableEntries()) > 0 || r.raftLog.hasNextEnts() {
		return true
	}
	if len(r.readStates) != 0 {
		return true
	}
	return false
}

// Advance notifies the RaftNode that the application has applied and saved progress in the last Ready results.
func (rn *RaftNode) Advance(rd Ready) {
	// Your Code Here (2A).
}

// ProposeConfChange proposes a config change.
func (rn *RaftNode) ProposeConfChange(cc *raftproto.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := raftproto.Entry{EntryType: raftproto.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(&raftproto.Message{
		MsgType: raftproto.MessageType_MsgPropose,
		Entries: []*raftproto.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RaftNode) ApplyConfChange(cc *raftproto.ConfChange) *raftproto.ConfState {
	if cc.NodeId == None {
		return &raftproto.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case raftproto.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case raftproto.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &raftproto.ConfState{Nodes: nodes(rn.Raft)}
}

func (rn *RaftNode) Stop() {

}

type Peer struct {
	ID      uint64
	Context []byte
}

// StartNode returns a new Node given configuration and a list of raft peers.
// It appends a ConfChangeAddNode entry for each given peer to the initial log.
//
// Peers must not be zero length; call RestartNode in that case.
func StartRaftNode(c *Config, peers []Peer) RaftNode {
	if len(peers) == 0 {
		panic("no peers given; use RestartNode instead")
	}
	rn, err := NewRaftNode(c)
	if err != nil {
		panic(err)
	}
	rn.Bootstrap(peers)
	go rn.run()
	return &rn
}

// RestartNode is similar to StartNode but does not take a list of peers.
// The current membership of the cluster will be restored from the Storage.
// If the caller has an existing state machine, pass in the last log index that
// has been applied to it; otherwise use zero.
func RestartRaftNode(c *Config) RaftNode {
	rn, err := NewRaftNode(c)
	if err != nil {
		panic(err)
	}
	go rn.run()
	return &rn
}

// Bootstrap initializes the RawNode for first use by appending configuration
// changes for the supplied peers. This method returns an error if the Storage
// is nonempty.
//
// It is recommended that instead of calling this method, applications bootstrap
// their state manually by setting up a Storage that has a first index > 1 and
// which stores the desired ConfState as its InitialState.
func (rn *RaftNode) Bootstrap(peers []Peer) error {
	if len(peers) == 0 {
		return errors.New("must provide at least one peer to Bootstrap")
	}
	lastIndex, err := rn.raft.raftLog.storage.LastIndex()
	if err != nil {
		return err
	}

	if lastIndex != 0 {
		return errors.New("can't bootstrap a nonempty Storage")
	}

	// We've faked out initial entries above, but nothing has been
	// persisted. Start with an empty HardState (thus the first Ready will
	// emit a HardState update for the app to persist).
	rn.prevHardSt = emptyState

	// TODO(tbg): remove StartNode and give the application the right tools to
	// bootstrap the initial membership in a cleaner way.
	rn.raft.becomeFollower(1, None)
	ents := make([]pb.Entry, len(peers))

	// 把集群中的其他节点都封装成添加节点的配置变更信息，添加到非持久化预写日志当中；
	for i, peer := range peers {
		cc := pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: peer.ID, Context: peer.Context}
		data, err := cc.Marshal()
		if err != nil {
			return err
		}

		ents[i] = pb.Entry{Type: pb.EntryConfChange, Term: 1, Index: uint64(i + 1), Data: data}
	}
	rn.raft.raftLog.append(ents...)

	// Now apply them, mainly so that the application can call Campaign
	// immediately after StartNode in tests. Note that these nodes will
	// be added to raft twice: here and when the application's Ready
	// loop calls ApplyConfChange. The calls to addNode must come after
	// all calls to raftLog.append so progress.next is set after these
	// bootstrapping entries (it is an error if we try to append these
	// entries since they have already been committed).
	// We do not set raftLog.applied so the application will be able
	// to observe all conf changes via Ready.CommittedEntries.
	//
	// TODO(bdarnell): These entries are still unstable; do we need to preserve
	// the invariant that committed < unstable?
	rn.raft.raftLog.committed = uint64(len(ents))
	for _, peer := range peers {
		rn.raft.applyConfChange(pb.ConfChange{NodeID: peer.ID, Type: pb.ConfChangeAddNode}.AsV2())
	}
	return nil
}

func (rn *RaftNode) run() {
	var propc chan msgWithResult
	var readyc chan Ready
	var advancec chan struct{}
	var rd Ready

	r := rn.Raft

	lead := None

	for {
		if advancec != nil {
			// advance channel不为空，说明还在等应用调用Advance接口通知已经处理完毕了本次的ready数据
			readyc = nil
		} else if rn.HasReady() {
			// Populate a Ready. Note that this Ready is not guaranteed to
			// actually be handled. We will arm readyc, but there's no guarantee
			// that we will actually send on it. It's possible that we will
			// service another channel instead, loop around, and then populate
			// the Ready again. We could instead force the previous Ready to be
			// handled first, but it's generally good to emit larger Readys plus
			// it simplifies testing (by emitting less frequently and more
			// predictably).
			rd = n.rn.readyWithoutAccept()
			readyc = n.readyc
		}

		if lead != r.LeaderID {
			if r.hasLeader() {
				if lead == None {
					r.logger.Infof("raft.node: %x elected leader %x at term %d", r.id, r.lead, r.Term)
				} else {
					r.logger.Infof("raft.node: %x changed leader from %x to %x at term %d", r.id, lead, r.lead, r.Term)
				}
				propc = n.propc
			} else {
				r.logger.Infof("raft.node: %x lost leader %x at term %d", r.id, lead, r.Term)
				propc = nil
			}
			lead = r.lead
		}

		select {
		// TODO: maybe buffer the config propose if there exists one (the way
		// described in raft dissertation)
		case pm := <-propc:
			// 接收来自应用层的的消息
			m := pm.m
			m.From = r.id
			err := r.Step(m)
			if pm.result != nil {
				pm.result <- err
				close(pm.result)
			}
		case m := <-n.recvc:
			// 处理其他节点发送过来的提交值
			// filter out response message from unknown From.
			if pr := r.prs.Progress[m.From]; pr != nil || !IsResponseMsg(m.Type) {
				r.Step(m)
			}
		case cc := <-n.confc:
			_, okBefore := r.prs.Progress[r.id]
			cs := r.applyConfChange(cc)
			// If the node was removed, block incoming proposals. Note that we
			// only do this if the node was in the config before. Nodes may be
			// a member of the group without knowing this (when they're catching
			// up on the log and don't have the latest config) and we don't want
			// to block the proposal channel in that case.
			//
			// NB: propc is reset when the leader changes, which, if we learn
			// about it, sort of implies that we got readded, maybe? This isn't
			// very sound and likely has bugs.
			if _, okAfter := r.prs.Progress[r.id]; okBefore && !okAfter {
				var found bool
			outer:
				for _, sl := range [][]uint64{cs.Voters, cs.VotersOutgoing} {
					for _, id := range sl {
						if id == r.id {
							found = true
							break outer
						}
					}
				}
				if !found {
					propc = nil
				}
			}
			select {
			case n.confstatec <- cs:
			case <-n.done:
			}
		case <-rn.tickc:
			//接收到应用层的定时 Tick 调用时，会根据 raft 节点的角色，使用其对应的 tick 函数进行处理：
			rn.Tick()
		case readyc <- rd:
			n.rn.acceptReady(rd)
			// 修改advance channel不为空，等待接收advance消息
			advancec = n.advancec
		case <-advancec:
			rn.Advance(rd)
			rd = Ready{}
			advancec = nil
		case c := <-n.status:
			c <- getStatus(r)
		case <-n.stop:
			close(n.done)
			return
		}
	}

}
