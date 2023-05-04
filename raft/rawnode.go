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

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	raft, err := NewRaft(config)
	if err != nil {
		return nil, err
	}

	return &RawNode{Raft: raft}, nil
}

// Tick 由应用层定时触发Tick
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(&raftproto.Message{
		MsgType: raftproto.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := raftproto.Entry{Data: data}
	return rn.Raft.Step(&raftproto.Message{
		MsgType: raftproto.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*raftproto.Entry{&ent}})
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m *raftproto.Message) error {
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
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == Leader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
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

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	// Your Code Here (2A).
	return Ready{}
}

// HasReady 由应用层调用该方法，如果发现由Ready则进行处理，然后再调用Advance方法表示该轮已经处理完成
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).
}

// todo config

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc *raftproto.ConfChange) error {
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
func (rn *RawNode) ApplyConfChange(cc *raftproto.ConfChange) *raftproto.ConfState {
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
