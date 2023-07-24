package raft

import (
	"bytes"
	"errors"
	"github.com/ColdToo/Cold2DB/pb"
)

var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

type SoftState struct {
	LeaderID uint64
	RaftRole Role
}

type msgWithResult struct {
	m      pb.Message
	result chan error
}

type Peer struct {
	ID      uint64
	Context []byte
}

type Ready struct {
	*SoftState

	pb.HardState

	Entries []pb.Entry // 待持久化

	CommittedEntries []pb.Entry // 待apply

	Messages []pb.Message // 待发送给其他节点的message
}

type RaftNode struct {
	Raft *Raft

	propC    chan msgWithResult
	recvC    chan pb.Message
	confC    chan pb.ConfChange
	tickC    chan struct{}
	done     chan struct{}
	stop     chan struct{}
	ReadyC   chan Ready
	AdvanceC chan struct{}

	prevSoftSt *SoftState
	prevHardSt pb.HardState
}

func NewRaftNode(config *Opts) (*RaftNode, error) {
	raft, err := NewRaft(config)
	if err != nil {
		return nil, err
	}
	ReadyC := make(chan Ready, 0)
	AdvanceC := make(chan struct{}, 0)
	return &RaftNode{Raft: raft, ReadyC: ReadyC, AdvanceC: AdvanceC}, nil
}

func StartRaftNode(c *Opts, peers []Peer) *RaftNode {
	if len(peers) == 0 {
		panic("no peers given; use RestartNode instead")
	}
	rn, err := NewRaftNode(c)
	if err != nil {
		panic(err)
	}
	go rn.run()
	return rn
}

func RestartRaftNode(c *Opts) *RaftNode {
	//从storage恢复一些hard state
	rn, err := NewRaftNode(c)
	if err != nil {
		panic(err)
	}
	go rn.run()
	return rn
}

func (rn *RaftNode) Tick() {
	rn.Raft.Tick()
}

func (rn *RaftNode) Step(m *pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.Type) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Progress[m.From]; pr != nil || !IsResponseMsg(m.Type) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Campaign causes this RaftNode to transition to candidate state.
func (rn *RaftNode) Campaign() error {
	return rn.Raft.Step(&pb.Message{
		Type: pb.MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RaftNode) Propose(buffer bytes.Buffer) error {
	ent := pb.Entry{Data: buffer.Bytes()}
	ents := make([]*pb.Entry, 0)
	ents = append(ents, &ent)
	return rn.Raft.Step(&pb.Message{
		Type:    pb.MsgProp,
		From:    rn.Raft.id,
		Entries: ents})
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RaftNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(&pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}

// ProposeConfChange proposes a config change.
func (rn *RaftNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{Type: pb.EntryConfChange, Data: data}
	return rn.Raft.Step(&pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RaftNode) ApplyConfChange(cc *pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// GetProgress return the Progress of this node and its peers, if this node is leader.
func (rn *RaftNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.Role == Leader {
		for id, p := range rn.Raft.Progress {
			prs[id] = *p
		}
	}
	return prs
}

func (rn *RaftNode) run() {
	var readyC chan Ready
	var advanceC chan struct{}
	var rd Ready

	r := rn.Raft

	for {
		// advance channel不为空，说明应用层还在处理上一轮ready
		if advanceC != nil {
			readyC = nil
		} else if rn.HasReady() {
			rd = rn.newReady()
			readyC = rn.ReadyC
		}

		select {
		case m := <-rn.recvC:
			if pr := r.Progress[m.From]; pr != nil {
				err := r.Step(&m)
				if err != nil {
					return
				}
			}

		case readyC <- rd:
			rn.alterRaftStatus(rd)
			advanceC = rn.AdvanceC

		case <-rn.stop:
			close(rn.done)
			return
		}
	}

}

func (rn *RaftNode) newReady() Ready {
	rd := Ready{
		Entries:          rn.Raft.RaftLog.unstableEntries(),
		CommittedEntries: rn.Raft.RaftLog.nextApplyEnts(),
	}
	if len(rn.Raft.msgs) > 0 {
		rd.Messages = rn.Raft.msgs
	}
	if rn.prevSoftSt.LeaderID != rn.Raft.LeaderID || rn.prevSoftSt.RaftRole != rn.Raft.Role {
		rn.prevSoftSt.LeaderID = rn.Raft.LeaderID
		rn.prevSoftSt.RaftRole = rn.Raft.Role
		rd.SoftState = rn.prevSoftSt
	}
	hardState := pb.HardState{
		Term:   rn.Raft.Term,
		Vote:   rn.Raft.VoteFor,
		Commit: rn.Raft.RaftLog.committed,
	}
	if !isHardStateEqual(rn.prevHardSt, hardState) {
		rd.HardState = hardState
	}

	// clear msg
	rn.Raft.msgs = make([]pb.Message, 0)
	return rd
}

func (rn *RaftNode) HasReady() bool {
	hardState := pb.HardState{
		Term:   rn.Raft.Term,
		Vote:   rn.Raft.VoteFor,
		Commit: rn.Raft.RaftLog.committed,
	}
	if !IsEmptyHardState(hardState) && !isHardStateEqual(rn.prevHardSt, hardState) {
		return true
	}
	if len(rn.Raft.msgs) > 0 || len(rn.Raft.RaftLog.nextApplyEnts()) > 0 || len(rn.Raft.RaftLog.unstableEntries()) > 0 {
		return true
	}
	return false
}

func (rn *RaftNode) Advance(rd Ready) {
	if len(rd.Entries) > 0 {
		rn.Raft.RaftLog.stabled = rd.Entries[len(rd.Entries)-1].Index
	}
	if len(rd.CommittedEntries) > 0 {
		rn.Raft.RaftLog.applied = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
	}
	if !IsEmptyHardState(rd.HardState) {
		rn.prevHardSt = rd.HardState
	}
}

// 每一轮算法层投递完 Ready 后，会重置raft的一些状态比如把 raft.msgs 置为空，保证消息不被重复发送到应用层
func (rn *RaftNode) alterRaftStatus(rd Ready) {
	if rd.SoftState != nil {
		rn.prevSoftSt = rd.SoftState
	}
	//每一轮算法层投递完 Ready 后，会把 raft.msgs 置为空，保证消息不被重复发送到应用层：
	rn.Raft.msgs = nil
}

//网络层报告接口

func (rn *RaftNode) ReportUnreachable(id uint64) {
	return
}

func (rn *RaftNode) ReportSnapshot(id uint64, status SnapshotStatus) {
	return
}

func (rn *RaftNode) Stop() {

}
