package raft

import (
	"errors"
	"github.com/ColdToo/Cold2DB/log"
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
	Raft *Raft

	recvC chan pb.Message
	confC chan pb.ConfChange

	ReadyC   chan Ready
	AdvanceC chan struct{}

	ErrorC chan error
	done   chan struct{}
	stop   chan struct{}

	prevSoftSt *SoftState
	prevHardSt pb.HardState
}

type SoftState struct {
	LeaderID uint64
	RaftRole Role
}

type Peer struct {
	ID      uint64
	Context []byte
}

type Ready struct {
	SoftState SoftState

	HardState pb.HardState

	CommittedEntries []*pb.Entry // 待apply的entry

	Messages []*pb.Message // 待发送给其他节点的message
}

func newRaftNode(config *RaftOpts) (*RaftNode, error) {
	raft, err := NewRaft(config)
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
	if len(peers) == 0 {
		panic("no peers given; use RestartNode instead")
	}
	rn, err := newRaftNode(c)
	if err != nil {
		panic(err)
	}
	return rn
}

func RestartRaftNode(c *RaftOpts) RaftLayer {
	//todo restart和start的区别是什么
	rn, err := newRaftNode(c)
	if err != nil {
		panic(err)
	}
	return rn
}

func (rn *RaftNode) Tick() {
	rn.Raft.tick()
}

func (rn *RaftNode) Step(m *pb.Message) error {
	if IsLocalMsg(m.Type) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Progress[m.From]; pr != nil || !IsResponseMsg(m.Type) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Propose 用于kv请求propose
func (rn *RaftNode) Propose(buffer []byte) error {
	ent := pb.Entry{Data: buffer}
	ents := make([]pb.Entry, 0)
	ents = append(ents, ent)
	return rn.Raft.Step(&pb.Message{
		Type:    pb.MsgProp,
		From:    rn.Raft.id,
		Entries: ents})
}

func (rn *RaftNode) Advance() {
	//appnode处理完一次后需要更新raftlog的first applied
	rn.Raft.RaftLog.RefreshFirstAndAppliedIndex()
	//需要将log中的entryies进行裁剪
	rn.AdvanceC <- struct{}{}
}

func (rn *RaftNode) GetReadyC() chan Ready {
	return rn.ReadyC
}

func (rn *RaftNode) GetErrorC() chan error {
	return rn.ErrorC
}

func (rn *RaftNode) newReady() Ready {
	rd := Ready{
		CommittedEntries: rn.Raft.RaftLog.nextApplyEnts(),
	}
	if len(rn.Raft.msgs) > 0 {
		rd.Messages = rn.Raft.msgs
	}

	//todo 应用层拿soft state干嘛？
	if rn.prevSoftSt.LeaderID != rn.Raft.LeaderID || rn.prevSoftSt.RaftRole != rn.Raft.Role {
		rn.prevSoftSt.LeaderID = rn.Raft.LeaderID
		rn.prevSoftSt.RaftRole = rn.Raft.Role
		rd.SoftState = rn.prevSoftSt
	}

	hardState := pb.HardState{
		Term:    rn.Raft.Term,
		Vote:    rn.Raft.VoteFor,
		Applied: rn.Raft.RaftLog.applied,
	}
	if !isHardStateEqual(rn.prevHardSt, hardState) {
		rd.HardState = hardState
	}

	rn.Raft.msgs = make([]*pb.Message, 0)
	return rd
}

func (rn *RaftNode) hasReady() bool {
	hardState := pb.HardState{
		Term:    rn.Raft.Term,
		Vote:    rn.Raft.VoteFor,
		Applied: rn.Raft.RaftLog.applied,
	}

	//硬状态有改变、有待applied的entries、有待发送给其他节点的msg
	if !IsEmptyHardState(hardState) && !isHardStateEqual(rn.prevHardSt, hardState) {
		return true
	}
	if len(rn.Raft.msgs) > 0 || rn.Raft.RaftLog.hasNextApplyEnts() {
		return true
	}
	return false
}

func (rn *RaftNode) run() {
	var readyC chan Ready
	var advanceC chan struct{}
	var rd Ready

	for {
		// 应用层通过将advanceC置为nil来标识,如果advanceC
		if advanceC != nil {
			readyC = nil
		} else if rn.hasReady() {
			rd = rn.newReady()
			readyC = rn.ReadyC
		}

		select {
		// todo 网络层获取要处理的message,网络层应该直接通过Propose调用，还是通过该channel进行异步调用？
		case m := <-rn.recvC:
			if pr := rn.Raft.Progress[m.From]; pr != nil {
				err := rn.Raft.Step(&m)
				if err != nil {
					log.Errorf("", err)
				}
			}

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

// 节点变更
// Campaign todo leaderTransfree
func (rn *RaftNode) Campaign() error {
	return rn.Raft.Step(&pb.Message{
		Type: pb.MsgHup,
	})
}

func (rn *RaftNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(&pb.Message{Type: pb.MsgTransferLeader, From: transferee})
}

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

}
