package raft

import (
	"bytes"
	"errors"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
)

var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

type SoftState struct {
	LeaderID uint64
	RaftRole Role
}

type Peer struct {
	ID      uint64
	Context []byte
}

type Ready struct {
	*SoftState

	pb.HardState

	CommittedEntries []*pb.Entry // 待apply的entry

	Messages []pb.Message // 待发送给其他节点的message
}

type RaftNode struct {
	Raft *Raft

	recvC chan pb.Message
	confC chan pb.ConfChange

	ReadyC   chan Ready
	AdvanceC chan struct{}

	done chan struct{}
	stop chan struct{}

	prevSoftSt *SoftState
	prevHardSt pb.HardState
}

func NewRaftNode(config *RaftOpts) (*RaftNode, error) {
	raft, err := NewRaft(config)
	if err != nil {
		return nil, err
	}
	ReadyC := make(chan Ready)
	AdvanceC := make(chan struct{})
	return &RaftNode{Raft: raft, ReadyC: ReadyC, AdvanceC: AdvanceC}, nil
}

func StartRaftNode(c *RaftOpts, peers []Peer) *RaftNode {
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

func RestartRaftNode(c *RaftOpts) *RaftNode {
	//todo restart和start的区别是什么
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
	if IsLocalMsg(m.Type) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Progress[m.From]; pr != nil || !IsResponseMsg(m.Type) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Propose 用于kv请求propose
func (rn *RaftNode) Propose(buffer bytes.Buffer) error {
	ent := pb.Entry{Data: buffer.Bytes()}
	ents := make([]*pb.Entry, 0)
	ents = append(ents, &ent)
	return rn.Raft.Step(&pb.Message{
		Type:    pb.MsgProp,
		From:    rn.Raft.id,
		Entries: ents})
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
		Term:   rn.Raft.Term,
		Vote:   rn.Raft.VoteFor,
		Commit: rn.Raft.RaftLog.committed,
	}
	if !isHardStateEqual(rn.prevHardSt, hardState) {
		rd.HardState = hardState
	}

	//todo 此时会不会有协程写入数据到msgs？
	rn.Raft.msgs = make([]pb.Message, 0)
	return rd
}

func (rn *RaftNode) HasReady() bool {
	hardState := pb.HardState{
		Term:    rn.Raft.Term,
		Vote:    rn.Raft.VoteFor,
		Applied: rn.Raft.RaftLog.applied,
	}

	if !IsEmptyHardState(hardState) && !isHardStateEqual(rn.prevHardSt, hardState) {
		return true
	}
	if len(rn.Raft.msgs) > 0 || len(rn.Raft.RaftLog.nextApplyEnts()) > 0 {
		return true
	}
	return false
}

func (rn *RaftNode) Advance() {
	//todo  appnode处理完一次后需要更新raftlog的first applied
	rn.Raft.RaftLog.RefreshFirstAndAppliedIndex()
	rn.AdvanceC <- struct{}{}
}

func (rn *RaftNode) run() {
	var readyC chan Ready
	var advanceC chan struct{}
	var rd Ready

	for {
		// 应用层通过将advanceC置为nil来标识,如果advanceC
		if advanceC != nil {
			readyC = nil
		} else if rn.HasReady() {
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

// Campaign todo leaderTransfree
func (rn *RaftNode) Campaign() error {
	return rn.Raft.Step(&pb.Message{
		Type: pb.MsgHup,
	})
}

//节点变更

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
		Entries: []*pb.Entry{&ent},
	})
}

func (rn *RaftNode) ApplyConfChange(cc *pb.ConfChange) *pb.ConfState {
	return nil
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
