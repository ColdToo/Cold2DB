package raft

import (
	"bytes"
	"errors"
	"github.com/ColdToo/Cold2DB/pb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

type SoftState struct {
	LeaderID  uint64
	RaftState Role
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

// RaftNode is a wrapper of Raft.
type RaftNode struct {
	Raft *Raft

	propC      chan msgWithResult
	recvC      chan pb.Message
	confC      chan pb.ConfChange
	confstateC chan pb.ConfState
	readyC     chan Ready
	advanceC   chan struct{}
	tickC      chan struct{}
	done       chan struct{}
	stop       chan struct{}

	prevSoftSt *SoftState
	prevHardSt pb.HardState
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
	rn.Raft.Tick()
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

// Step 驱动raft层
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

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RaftNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(&pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RaftNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == Leader {
		for id, p := range rn.Raft.Progress {
			prs[id] = *p
		}
	}
	return prs
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

func StartRaftNode(c *Config, peers []Peer) *RaftNode {
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

func RestartRaftNode(c *Config) *RaftNode {
	rn, err := NewRaftNode(c)
	if err != nil {
		panic(err)
	}
	go rn.run()
	return rn
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
			rd = rn.newReady(rn.Raft, rn.prevSoftSt, rn.prevHardSt)
			readyC = rn.readyC
		}

		select {
		case m := <-rn.recvC:
			// 处理其他节点发送过来的提交值
			// filter out response message from unknown From.
			if pr := r.Progress[m.From]; pr != nil || !IsResponseMsg(m.Type) {
				err := r.Step(&m)
				if err != nil {
					return
				}
			}

		case readyC <- rd:
			//将rd投喂给应用层后,对raft相关字段进行更新
			rn.alterRaftStatus(rd)
			// 修改advance channel不为空，等待接收advance消息
			advanceC = rn.advanceC

		case <-rn.stop:
			close(rn.done)
			return
		}
	}

}

func (rn *RaftNode) newReady(r *Raft, prevSoftSt *SoftState, prevHardSt pb.HardState) Ready {
	rd := Ready{
		Entries:          r.RaftLog.unstableEntries(),
		CommittedEntries: r.RaftLog.nextEnts(),
		Messages:         r.msgs,
	}
	if softSt := r.softState(); !softSt.equal(prevSoftSt) {
		rd.SoftState = softSt
	}
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, prevHardSt) {
		rd.HardState = hardSt
	}
	if r.raftLog.unstable.snapshot != nil {
		rd.Snapshot = *r.raftLog.unstable.snapshot
	}
	if len(r.readStates) != 0 {
		rd.ReadStates = r.readStates
	}
	rd.MustSync = MustSync(r.hardState(), prevHardSt, len(rd.Entries))
	return rd
}

//关闭raft层（相关数据结构）
func (rn *RaftNode) Stop() {

}

func (rn *RaftNode) ReportUnreachable(id uint64) {
	return
}

func (rn *RaftNode) ReportSnapshot(id uint64, status SnapshotStatus) {
	return
}

func (rn *RaftNode) Ready() Ready {
	rd := Ready{
		Entries:          rn.Raft.RaftLog.unstableEntries(),
		CommittedEntries: rn.Raft.RaftLog.nextEnts(),
	}
	if len(rn.Raft.msgs) > 0 {
		rd.Messages = rn.Raft.msgs
	}
	if rn.prevSoftState.Lead != rn.Raft.Lead ||
		rn.prevSoftState.RaftState != rn.Raft.State {
		rn.prevSoftState.Lead = rn.Raft.Lead
		rn.prevSoftState.RaftState = rn.Raft.State
		rd.SoftState = rn.prevSoftState
	}
	hardState := pb.HardState{
		Term:   rn.Raft.Term,
		Vote:   rn.Raft.Vote,
		Commit: rn.Raft.RaftLog.committed,
	}
	if !isHardStateEqual(rn.prevHardState, hardState) {
		rd.HardState = hardState
	}
	// clear msg
	rn.Raft.msgs = make([]pb.Message, 0)
	if !IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) {
		rd.Snapshot = *rn.Raft.RaftLog.pendingSnapshot
		rn.Raft.RaftLog.pendingSnapshot = nil
	}
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
	if len(rn.Raft.msgs) > 0 || len(rn.Raft.RaftLog.nextEnts()) > 0 || len(rn.Raft.RaftLog.unstableEntries()) > 0 {
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
