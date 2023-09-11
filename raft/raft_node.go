package raft

import (
	"errors"
	"github.com/ColdToo/Cold2DB/pb"
)

var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

//go:generate mockgen -source=./raft_node.go -destination=../mocks/raft_node.go -package=mock
type RaftLayer interface {
	Advance()
	GetReadyC() chan Ready
	Step(m *pb.Message) error
	Tick()
	Propose(buffer []byte) error
	ProposeConfChange(cc pb.ConfChange) error
	ApplyConfChange(cc pb.ConfChange)
	ReportUnreachable(id uint64)
	GetErrorC() chan error
	ReportSnapshot(id uint64, status SnapshotStatus)
	Stop()
}

type RaftNode struct {
	Raft       *Raft
	prevHardSt pb.HardState
	confC      chan pb.ConfChange
	ReadyC     chan Ready

	AdvanceC chan struct{}
	ErrorC   chan error
	done     chan struct{}
	stop     chan struct{}
}

type Peer struct {
	ID      uint64
	Context []byte
}

type Ready struct {
	HardState pb.HardState

	CommittedEntries []*pb.Entry // 待apply的entry

	Messages []*pb.Message // 待发送给其他节点的message
}

func newRaftNode(opt *RaftOpts) (*RaftNode, error) {
	raft, err := NewRaft(opt)
	if err != nil {
		return nil, err
	}
	ReadyC := make(chan Ready)
	AdvanceC := make(chan struct{})
	rn := &RaftNode{Raft: raft, ReadyC: ReadyC, AdvanceC: AdvanceC}
	rn.run()
	return rn, nil
}

func StartRaftNode(c *RaftOpts, peers []Peer) RaftLayer {
	rn, err := newRaftNode(c)
	if err != nil {
		panic(err)
	}
	return rn
}

func RestartRaftNode(c *RaftOpts) RaftLayer {
	rn, err := newRaftNode(c)
	if err != nil {
		panic(err)
	}
	return rn
}

// Step 网络层通过该方法处理message信息
func (rn *RaftNode) Step(m *pb.Message) error {
	return rn.Raft.Step(m)
}

// Propose 用于kv请求提议
func (rn *RaftNode) Propose(buffer []byte) error {
	ent := pb.Entry{Data: buffer}
	ents := make([]pb.Entry, 0)
	ents = append(ents, ent)
	return rn.Raft.Step(&pb.Message{
		Type:    pb.MsgProp,
		From:    rn.Raft.id,
		Entries: ents})
}

// ProposeConfChange 用于配置变更信息处理
func (rn *RaftNode) ProposeConfChange(cc pb.ConfChange) error {
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

func (rn *RaftNode) Tick() {
	rn.Raft.tick()
}

func (rn *RaftNode) Advance() {
	// 每当appNode处理完一次ready后需要更新raftlog的first index 和 applied index
	rn.Raft.RaftLog.RefreshFirstAndAppliedIndex()
	//需要将log中的entries进行裁剪
	rn.AdvanceC <- struct{}{}
}

func (rn *RaftNode) newReady() Ready {
	rd := Ready{
		CommittedEntries: rn.Raft.RaftLog.NextApplyEnts(),
	}

	if len(rn.Raft.msgs) > 0 {
		rd.Messages = rn.Raft.msgs
		//todo 清空msg防止重复发送，这里会不会出现并发问题
		rn.Raft.msgs = make([]*pb.Message, 0)
	}

	hardState := pb.HardState{
		Term:    rn.Raft.Term,
		Vote:    rn.Raft.VoteFor,
		Applied: rn.Raft.RaftLog.AppliedIndex(),
	}

	if !isHardStateEqual(rn.prevHardSt, hardState) {
		rd.HardState = hardState
	}

	return rd
}

// 1、需要持久化的状态有改变
// 2、有待applied的entries
// 3、有待发送给其他节点的msg

func (rn *RaftNode) Ready() (rd Ready) {
	rd = Ready{}

	hardState := pb.HardState{
		Term:    rn.Raft.Term,
		Vote:    rn.Raft.VoteFor,
		Applied: rn.Raft.RaftLog.AppliedIndex(),
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
		CommittedEntries := rn.Raft.RaftLog.NextApplyEnts()
	}
	return
}

func (rn *RaftNode) run() {
	var readyC chan Ready
	var advanceC chan struct{}
	var rd Ready

	for {
		// 应用层通过将advanceC置为nil来标识,如果advanceC
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

func (rn *RaftNode) GetReadyC() chan Ready {
	return rn.ReadyC
}

func (rn *RaftNode) GetErrorC() chan error {
	return rn.ErrorC
}

// 配置变更

func (rn *RaftNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(&pb.Message{Type: pb.MsgTransferLeader, From: transferee})
}

func (rn *RaftNode) ApplyConfChange(cc pb.ConfChange) {
	return
}

//网络层报告接口

func (rn *RaftNode) ReportUnreachable(id uint64) {
	return
}

func (rn *RaftNode) ReportSnapshot(id uint64, status SnapshotStatus) {
	return
}

func (rn *RaftNode) Stop() {
	// todo 回收raft相关资源
}
