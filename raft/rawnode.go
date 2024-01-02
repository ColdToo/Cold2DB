// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/ColdToo/Cold2DB/pb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// rawNode is a thread-unsafe Node.
// The methods of this struct correspond to the methods of Node and are described more fully there.
type rawNode struct {
	raft       *raft
	prevSoftSt *SoftState
	prevHardSt pb.HardState
}

func NewRawNode(config *opts) (*rawNode, error) {
	r := newRaft(config)
	rn := &rawNode{
		raft: r,
	}
	rn.prevSoftSt = r.softState()
	rn.prevHardSt = r.hardState()
	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *rawNode) Tick() {
	rn.raft.tick()
}

// TickQuiesced advances the internal logical clock by a single tick without
// performing any other state machine processing. It allows the caller to avoid
// periodic heartbeats and elections when all of the peers in a Raft group are
// known to be at the same state. Expected usage is to periodically invoke Tick
// or TickQuiesced depending on whether the group is "active" or "quiesced".
//
// WARNING: Be very careful about using this method as it subverts the Raft
// state machine. You should probably be using Tick instead.
func (rn *rawNode) TickQuiesced() {
	rn.raft.electionElapsed++
}

// Campaign causes this rawNode to transition to candidate state.
func (rn *rawNode) Campaign() error {
	return rn.raft.Step(pb.Message{
		Type: pb.MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *rawNode) Propose(data []byte) error {
	return rn.raft.Step(pb.Message{
		Type: pb.MsgProp,
		From: rn.raft.id,
		Entries: []pb.Entry{
			{Data: data},
		}})
}

// ProposeConfChange proposes a config change. See (Node).ProposeConfChange for
// details.
func (rn *rawNode) ProposeConfChange(cc pb.ConfChangeI) error {
	m, err := confChangeToMsg(cc)
	if err != nil {
		return err
	}
	return rn.raft.Step(m)
}

// ApplyConfChange applies a config change to the local node. The app must call
// this when it applies a configuration change, except when it decides to reject
// the configuration change, in which case no call must take place.
func (rn *rawNode) ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState {
	cs := rn.raft.applyConfChange(cc.AsV2())
	return &cs
}

// Step advances the state machine using the given message.
func (rn *rawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.Type) {
		return ErrStepLocalMsg
	}
	if pr := rn.raft.prs.Progress[m.From]; pr != nil || !IsResponseMsg(m.Type) {
		return rn.raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the outstanding work that the application needs to handle. This
// includes appending and applying entries or a snapshot, updating the HardState,
// and sending messages. The returned Ready() *must* be handled and subsequently
// passed back via Advance().
func (rn *rawNode) Ready() Ready {
	rd := rn.readyWithoutAccept()
	rn.acceptReady(rd)
	return rd
}

// readyWithoutAccept returns a Ready. This is a read-only operation, i.e. there
// is no obligation that the Ready must be handled.
func (rn *rawNode) readyWithoutAccept() Ready {
	return newReady(rn.raft, rn.prevSoftSt, rn.prevHardSt)
}

// acceptReady is called when the consumer of the rawNode has decided to go
// ahead and handle a Ready. Nothing must alter the state of the rawNode between
// this call and the prior call to Ready().
func (rn *rawNode) acceptReady(rd Ready) {
	if rd.SoftState != nil {
		rn.prevSoftSt = rd.SoftState
	}
	if len(rd.ReadStates) != 0 {
		rn.raft.readStates = nil
	}
	rn.raft.msgs = nil
}

// HasReady called when rawNode user need to check if any Ready pending.
// Checking logic in this method should be consistent with Ready.containsUpdates().
func (rn *rawNode) HasReady() bool {
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

// Advance notifies the rawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *rawNode) Advance(rd Ready) {
	if !IsEmptyHardState(rd.HardState) {
		rn.prevHardSt = rd.HardState
	}
	rn.raft.advance(rd)
}

// Status returns the current status of the given group. This allocates, see
// BasicStatus and WithProgress for allocation-friendlier choices.
func (rn *rawNode) Status() Status {
	status := getStatus(rn.raft)
	return status
}

// BasicStatus returns a BasicStatus. Notably this does not contain the
// Progress map; see WithProgress for an allocation-free way to inspect it.
func (rn *rawNode) BasicStatus() BasicStatus {
	return getBasicStatus(rn.raft)
}

// ProgressType indicates the type of replica a Progress corresponds to.
type ProgressType byte

const (
	// ProgressTypePeer accompanies a Progress for a regular peer replica.
	ProgressTypePeer ProgressType = iota
	// ProgressTypeLearner accompanies a Progress for a learner replica.
	ProgressTypeLearner
)

// WithProgress is a helper to introspect the Progress for this node and its
// peers.
func (rn *rawNode) WithProgress(visitor func(id uint64, typ ProgressType, pr tracker.Progress)) {
	rn.raft.prs.Visit(func(id uint64, pr *tracker.Progress) {
		typ := ProgressTypePeer
		if pr.IsLearner {
			typ = ProgressTypeLearner
		}
		p := *pr
		p.Inflights = nil
		visitor(id, typ, p)
	})
}

// ReportUnreachable reports the given node is not reachable for the last send.
func (rn *rawNode) ReportUnreachable(id uint64) {
	_ = rn.raft.Step(pb.Message{Type: pb.MsgUnreachable, From: id})
}

// ReportSnapshot reports the status of the sent snapshot.
func (rn *rawNode) ReportSnapshot(id uint64, status SnapshotStatus) {
	rej := status == SnapshotFailure

	_ = rn.raft.Step(pb.Message{Type: pb.MsgSnapStatus, From: id, Reject: rej})
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *rawNode) TransferLeader(transferee uint64) {
	_ = rn.raft.Step(pb.Message{Type: pb.MsgTransferLeader, From: transferee})
}

// ReadIndex requests a read state. The read state will be set in ready.
// Read State has a read index. Once the application advances further than the read
// index, any linearizable read requests issued before the read request can be
// processed safely. The read state will have the same rctx attached.
func (rn *rawNode) ReadIndex(rctx []byte) {
	_ = rn.raft.Step(pb.Message{Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: rctx}}})
}
