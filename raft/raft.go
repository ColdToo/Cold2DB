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
	"fmt"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/raft/quorum"
	"github.com/ColdToo/Cold2DB/raft/tracker"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"
)

const None uint64 = 0
const noLimit = math.MaxUint64

var SendEmptyMessage bool = true

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

type ReadOnlyOption int

const (
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	ReadOnlySafe ReadOnlyOption = iota
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	ReadOnlyLeaseBased
)

// CampaignType represents the type of campaigning
// the reason we use the type of string instead of uint64
// is because it's simpler to compare and fill in raft entries
type CampaignType string

// Possible values for CampaignType
const (
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	campaignPreElection CampaignType = "CampaignPreElection"
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	campaignElection CampaignType = "CampaignElection"
	// campaignTransfer represents the type of leader transfer
	campaignTransfer CampaignType = "CampaignTransfer"
)

// StateType represents the role of a node in a cluster.
type StateType uint64

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
}

func (st StateType) String() string {
	return stmap[st]
}

type raftOpts struct {
	Id uint64

	peers []uint64

	electionTimeout int

	heartbeatTimeout int

	storage db.Storage
}

func (c *raftOpts) validate() error {
	if c.Id == None {
		return errors.New("cannot use none as id")
	}

	if c.heartbeatTimeout <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.electionTimeout <= c.heartbeatTimeout {
		return errors.New("election tick must be greater than heartbeat tick")
	}
	return nil
}

type raft struct {
	raftOpts *raftOpts

	id   uint64
	lead uint64
	Term uint64
	vote uint64
	// leadTransferee is id of the leader transfer target when its value is not zero.
	leadTransferee uint64
	state          StateType

	raftLog *raftLog
	//用于追踪节点的相关信息
	trk tracker.ProgressTracker
	//需要发送给其他节点的消息
	msgs []pb.Message

	//不同角色指向不同的stepFunc
	stepFunc stepFunc
	//不同角色指向不同的tick驱动函数
	tick func()

	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int
	// number of ticks since it reached last electionTimeout when it is leader
	// or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
}

func newRaft(opts *raftOpts) (r *raft, err error) {
	storage := opts.storage
	if err = opts.validate(); err != nil {
		log.Panicf("verify raft options failed", err)
	}
	rLog := newRaftLog(storage)
	hs, _, err := storage.InitialState()
	if err != nil {
		log.Panicf("get hard state  from storage failed", err)
	}

	if !IsEmptyHardState(hs) {
		r.loadHardState(hs)
	}

	trk := tracker.MakeProgressTracker(opts.peers)

	r = &raft{
		id:       opts.Id,
		raftOpts: opts,
		lead:     None,
		raftLog:  rLog,
		trk:      trk,
	}

	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for _, n := range r.trk.VoterNodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	log.Infof("newraft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm())
	return
}

func (r *raft) loadHardState(state pb.HardState) {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		log.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.raftLog.committed, r.raftLog.lastIndex())
	}
	r.raftLog.committed = state.Commit
	r.Term = state.Term
	r.vote = state.Vote
}

func (r *raft) softState() *SoftState { return &SoftState{Lead: r.lead, RaftState: r.state} }

func (r *raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.vote,
		Commit: r.raftLog.committed,
	}
}

func (r *raft) becomeLeader() {
	if r.state == StateFollower && len(r.trk.Voters) != 1 {
		log.Panic("invalid transition [follower -> leader]")
	}
	r.stepFunc = stepLeader
	r.tick = r.tickHeartbeat
	r.reset(r.Term)
	r.lead = r.id
	r.state = StateLeader
	r.trk.Progress[r.id].BecomeReplicate()

	// 成为 leader 后,每个 follower 的 match 为0, next为最后一条日志的下一条日志
	lastIndex := r.raftLog.lastIndex()
	for pr := range r.trk.Progress {
		r.trk.Progress[pr].Next = lastIndex + 1
		r.trk.Progress[pr].Match = 0
	}

	// 更新自己的 match 和 next
	r.trk.Progress[r.id].Next = lastIndex + 1
	r.trk.Progress[r.id].Match = lastIndex

	//在成为leader后需要插入一条空日志
	emptyEnt := pb.Entry{Data: nil}
	r.appendEntry(emptyEnt)
	// 发送追加日志
	r.bcastAppend()
	log.Infof("peer:%x became leader at term %d", r.id, r.Term)
}

func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.reset(term)
	r.stepFunc = stepFollower
	r.tick = r.tickElection
	r.state = StateFollower
	r.lead = lead
	log.Infof("peer %x became follower at term %d", r.id, r.Term)
}

func (r *raft) becomeCandidate() {
	r.reset(r.Term + 1)
	r.stepFunc = stepCandidate
	r.tick = r.tickElection
	r.vote = r.id
	r.state = StateCandidate
	log.Infof("peer %x became candidate at term %d", r.id, r.Term)
}

func (r *raft) tickElection() {
	r.electionElapsed++
	if r.promotable() && r.electionElapsed >= r.randomizedElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, Type: pb.MsgHup})
	}
}

func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	//选举超时，开始选举
	if r.electionElapsed >= r.raftOpts.electionTimeout {
		r.electionElapsed = 0
		// If current leader cannot transfer leadership in electionTimeout, cancel leader transfer
		if r.state == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
		r.becomeCandidate()
		r.sendAllRequestVote()
	}

	//心跳超时，发送心跳
	if r.heartbeatElapsed >= r.raftOpts.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{From: r.id, Type: pb.MsgBeat})
	}
}

func (r *raft) Step(m pb.Message) error {
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		log.Infof("peer: %x [term: %d] received a %s message with higher term from peer:%x [term: %d]",
			r.id, r.Term, m.Type, m.From, m.Term)
		if m.Type == pb.MsgApp || m.Type == pb.MsgHeartbeat || m.Type == pb.MsgSnap {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	}

	switch m.Type {
	case pb.MsgHup:
		r.hup()
	case pb.MsgVote:
		canVote := r.vote == m.From || (r.vote == None && r.lead == None)
		if canVote && r.raftLog.isUpToDate(m.Index, m.LogTerm) {
			log.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: m.Term, Type: voteRespMsgType(m.Type)})
			if m.Type == pb.MsgVote {
				r.electionElapsed = 0
				r.vote = m.From
			}
		} else {
			log.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, Type: voteRespMsgType(m.Type), Reject: true})
		}
	default:
		err := r.stepFunc(r, m)
		if err != nil {
			return err
		}
	}
	return nil
}

type stepFunc func(r *raft, m pb.Message) error

func stepLeader(r *raft, m pb.Message) error {
	// These message types do not require any progress for m.From.
	switch m.Type {
	case pb.MsgBeat:
		r.bcastHeartbeat()
	case pb.MsgProp:
		return r.handlePropMsg(m)
	}

	// All other message types require a progress for m.From (pr).
	pr := r.trk.Progress[m.From]
	if pr == nil {
		log.Debugf("%x no progress available for %x", r.id, m.From)
		return nil
	}

	switch m.Type {
	case pb.MsgAppResp:
		r.handleAppendResponse(m, pr)
	case pb.MsgHeartbeatResp:
		r.handleHeartbeatResponse(m, pr)
	case pb.MsgUnreachable:
		r.handleMsgUnreachableStatus(m, pr)
	case pb.MsgTransferLeader:
		r.handleTransferLeader(m, pr)
	}
	return nil
}

func stepFollower(r *raft, m pb.Message) error {
	switch m.Type {
	case pb.MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MsgApp:
		r.handleAppendEntries(m)
	}
	return nil
}

func stepCandidate(r *raft, m pb.Message) error {
	switch m.Type {
	case pb.MsgApp:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleHeartbeat(m)
	case pb.MsgVoteResp:
		r.handleRequestVoteResponse(m)
	case pb.MsgTimeoutNow:
		log.Debugf("%x [term %d state %v] ignored MsgTimeoutNow from %x", r.id, r.Term, r.state, m.From)
	}
	return nil
}

// ------------------- leader behavior -------------------

func (r *raft) bcastHeartbeat() {
	r.bcastHeartbeatWithCtx(nil)
}

func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {
	for _, id := range r.trk.Voters.Slice() {
		if r.id != id {
			r.sendHeartbeat(id, ctx)
		}
	}
}

func (r *raft) sendHeartbeat(to uint64, ctx []byte) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	commit := min(r.trk.Progress[to].Match, r.raftLog.committed)
	_, ok := r.trk.Progress[to]
	if !ok {
		log.Panic("peer not in the cluster")
	}
	m := pb.Message{
		From:    r.id,
		To:      to,
		Type:    pb.MsgHeartbeat,
		Commit:  commit,
		Term:    r.Term,
		Context: ctx,
	}

	r.send(m)
}

func (r *raft) handleHeartbeatResponse(m pb.Message, pr *tracker.Progress) {
	pr.RecentActive = true
	pr.ProbeSent = false

	//如果该节点的match index小于leader当前最后一条日志，则为其调用sendAppend方法来复制新日志。
	if pr.Match < r.raftLog.lastIndex() {
		r.sendAppend(m.From)
	}

	return
}

func (r *raft) handleSnapStatus(m pb.Message, pr *tracker.Progress) (err error) {
	//todo
	return err
}

func (r *raft) handleMsgUnreachableStatus(m pb.Message, pr *tracker.Progress) {
	if pr.State == tracker.StateReplicate {
		pr.BecomeProbe()
	}
	log.Debugf("%x failed to send message to %x because it is unreachable [%s]", r.id, m.From, pr)
}

func (r *raft) handleTransferLeader(m pb.Message, pr *tracker.Progress) {
	leadTransferee := m.From
	lastLeadTransferee := r.leadTransferee
	if lastLeadTransferee != None {
		if lastLeadTransferee == leadTransferee {
			log.Infof("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
				r.id, r.Term, leadTransferee, leadTransferee)

		}
		r.abortLeaderTransfer()
		log.Infof("%x [term %d] abort previous transferring leadership to %x", r.id, r.Term, lastLeadTransferee)
	}
	if leadTransferee == r.id {
		log.Debugf("%x is already leader. Ignored transferring leadership to self", r.id)

	}
	// Transfer leadership to third party.
	log.Infof("%x [term %d] starts to transfer leadership to %x", r.id, r.Term, leadTransferee)
	// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
	r.electionElapsed = 0
	r.leadTransferee = leadTransferee
	if pr.Match == r.raftLog.lastIndex() {
		r.sendTimeoutNow(leadTransferee)
		log.Infof("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, leadTransferee, leadTransferee)
	} else {
		r.sendAppend(leadTransferee)
	}
}

func (r *raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To: to, Type: pb.MsgTimeoutNow})
}

func (r *raft) handleAppendResponse(m pb.Message, pr *tracker.Progress) {
	pr.RecentActive = true
	if m.Reject {
		log.Debugf("%x received MsgAppResp(rejected, hint: (index %d, term %d)) from %x for index %d", r.id, m.RejectHint, m.LogTerm, m.From, m.Index)
		nextProbeIdx := m.RejectHint
		//在正常情况下，领导者的日志比追随者的日志长，追随者的日志是领导者日志的前缀。在这种情况下，第一次探测（probe）会揭示追随者的日志结束位置（即RejectHint），随后的探测会成功。
		//然而，在网络分区或系统过载的情况下，可能会出现较大的不一致日志尾部，这会导致探测过程非常耗时，甚至可能导致服务中断。
		//为了优化探测过程，实现了一种策略：如果追随者在RejectHint索引处有一个未提交的日志尾部，领导者会根据追随者返回的LogTerm来决定下一次探测的位置。如果追随者的LogTerm大于0，
		//领导者会检查自己的日志，确定在哪些索引处的探测肯定会失败，因为这些索引处的日志项的任期大于追随者的LogTerm。这样，领导者就可以跳过这些索引，只探测那些可能成功的索引。
		// For example, if the leader has:
		//
		//   idx        1 2 3 4 5 6 7 8 9
		//              -----------------
		//   term (L)   1 3 3 3 5 5 5 5 5
		//   term (F)   1 1 1 1 2 2
		//   follower会返回 logTerm 2 index 6,此时leader只需要拿到2这个logTerm去寻找<=该term的日志即可快速定位冲突的日志
		if m.LogTerm > 0 {
			nextProbeIdx = r.raftLog.findConflictIdxByTerm(m.RejectHint, m.LogTerm)
		}
		//调用了MaybeDecrTo方法回退其Next索引。如果回退失败，说明这是一条过期的消息，不做处理；如果回退成功，且该节点为StateReplicate状态，
		//则调用BecomeProbe使其转为StateProbe状态来查找最后一条匹配日志的位置。回退成功时，还会再次为该节点调用sendAppend方法，以为其发送MsgApp消息。
		if pr.MaybeDecreaseTo(m.Index, nextProbeIdx) {
			log.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
			if pr.State == tracker.StateReplicate {
				pr.BecomeProbe()
			}
			r.sendAppend(m.From)
		}
		return
	}

	oldPaused := pr.IsPaused()
	if pr.MaybeUpdate(m.Index) {
		switch {
		//如果该follower处于StateProbe状态且现在跟上了进度，则将其转为StateReplica状态
		case pr.State == tracker.StateProbe:
			pr.BecomeReplicate()
		case pr.State == tracker.StateSnapshot && pr.Match >= pr.PendingSnapshot:
			// TODO(tbg): we should also enter this branch if a snapshot is
			// received that is below pr.PendingSnapshot but which makes it
			// possible to use the log again.
			log.Debugf("%x recovered from needing snapshot, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
			// Transition back to replicating state via probing state
			// (which takes the snapshot into account). If we didn't
			// move to replicating state, that would only happen with
			// the next round of appends (but there may not be a next
			// round for a while, exposing an inconsistent RaftStatus).
			pr.BecomeProbe()
			pr.BecomeReplicate()
		case pr.State == tracker.StateReplicate:
		}

		if r.maybeCommit() {
			// committed index has progressed for the term, so it is safe
			// to respond to pending read index requests
			r.bcastAppend()
		} else if oldPaused {
			// If we were paused before, this node may be missing the
			// latest commit index, so send it.
			r.sendAppend(m.From)
		}

		// 如果是正在 transfer 的目标，transfer
		if m.From == r.leadTransferee {
			r.Step(pb.Message{Type: pb.MsgTransferLeader, From: m.From})
		}
	}

}

func (r *raft) handlePropMsg(m pb.Message) error {
	if len(m.Entries) == 0 {
		log.Panicf("%x stepped empty MsgProp", r.id)
	}
	if r.trk.Progress[r.id] == nil {
		return ErrProposalDropped
	}
	if r.leadTransferee != None {
		log.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
		return ErrProposalDropped
	}
	r.handleConfigEntry(m.Entries)
	r.appendEntry(m.Entries...)
	r.bcastAppend()
	return nil
}

func (r *raft) handleConfigEntry(ents []pb.Entry) {
	//todo
	return
}

func (r *raft) appendEntry(es ...pb.Entry) {
	li := r.raftLog.lastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	r.raftLog.truncateAndAppend(es)
	return
}

func (r *raft) bcastAppend() {
	for _, id := range r.trk.Voters.Slice() {
		if r.id != id {
			r.sendAppend(id)
		}
	}
}

func (r *raft) sendAppend(to uint64) {
	r.maybeSendAppend(to)
}

func (r *raft) maybeSendAppend(to uint64) bool {
	pr := r.trk.Progress[to]
	if pr.IsPaused() {
		return false
	}

	m := pb.Message{}
	m.To = to

	prevLogIndex := pr.Next - 1
	prevLogTerm, errt := r.raftLog.term(prevLogIndex)
	ents, erre := r.raftLog.slice(pr.Next, r.raftLog.lastIndex()+1)

	// todo send snapshot if we failed to get term or entries
	if errt != nil || erre != nil {
	}

	m.Type = pb.MsgApp
	m.From = r.id
	m.Term = r.Term
	m.To = to
	m.Index = prevLogIndex
	m.LogTerm = prevLogTerm
	m.Entries = ents
	m.Commit = r.raftLog.committed
	r.send(m)
	return true
}

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
func (r *raft) maybeCommit() bool {
	return false
}

// ------------------ follower behavior ------------------

func (r *raft) handleHeartbeat(m pb.Message) {
	r.electionElapsed = 0
	r.lead = m.From
	r.raftLog.commitTo(m.Commit)
	r.send(pb.Message{To: m.From, Type: pb.MsgHeartbeatResp, Context: m.Context})
}

func (r *raft) handleAppendEntries(m pb.Message) {
	//如果用于日志匹配的条目在committed之前，说明这是一条过期的消息，因此直接返回MsgAppResp消息，
	//并将消息的Index字段置为committed的值，以让leader快速更新该follower的next index。
	if m.Index < r.raftLog.committed {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
		return
	}

	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: mlastIndex})
		return
	}

	log.Infof("%x [logterm: %d, index: %d] rejected MsgApp [logterm: %d, index: %d] from %x",
		r.id, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)

	hintIndex := min(m.Index, r.raftLog.lastIndex())
	hintIndex = r.raftLog.findConflictIdxByTerm(hintIndex, m.LogTerm)
	hintTerm, err := r.raftLog.term(hintIndex)
	if err != nil {
		log.Panicf(fmt.Sprintf("term(%d) must be valid, but got %v", hintIndex, err))
	}
	r.send(pb.Message{
		To:         m.From,
		Type:       pb.MsgAppResp,
		Index:      m.Index,
		Reject:     true,
		RejectHint: hintIndex,
		LogTerm:    hintTerm,
	})
}

// ------------------ candidate behavior ------------------

// promotable indicates whether state machine can be promoted to leader,
// which is true when its own id is in progress list.
func (r *raft) promotable() bool {
	pr := r.trk.Progress[r.id]
	return pr != nil
}

// 选举可以由heartbeat timeout触发或者客户端主动发起选举触发
func (r *raft) hup() {
	if r.state == StateLeader {
		log.Debugf("%x ignoring MsgHup because already leader", r.id)
		return
	}

	if !r.promotable() {
		log.Warnf("%x is unpromotable and can not campaign", r.id)
		return
	}

	log.Infof("%x is starting a new election at term %d", r.id, r.Term)
	r.campaign()
}

func (r *raft) campaign() {
	var term uint64
	var voteMsg pb.MessageType

	r.becomeCandidate()
	voteMsg = pb.MsgVote
	term = r.Term

	if _, _, res := r.poll(r.id, voteRespMsgType(voteMsg), true); res == quorum.VoteWon {
		r.becomeLeader()
		return
	}

	for _, id := range r.trk.Voters.Slice() {
		if id == r.id {
			continue
		}
		log.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.Term)

		r.send(pb.Message{Term: term, To: id, Type: voteMsg, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm()})
	}
}

func (r *raft) handleRequestVoteResponse(m pb.Message) {
	gr, rj, res := r.poll(m.From, m.Type, !m.Reject)
	log.Infof("%x has received %d %s votes and %d vote rejections", r.id, gr, m.Type, rj)
	switch res {
	case quorum.VoteWon:
		r.becomeLeader()
		r.bcastAppend()
	case quorum.VoteLost:
		r.becomeFollower(r.Term, None)
	}
}

func (r *raft) sendAllRequestVote() {
	for _, id := range r.trk.Voters.Slice() {
		if id == r.id {
			continue
		}

		log.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), pb.MsgVote, id, r.Term)

		r.send(pb.Message{Term: r.Term, To: id, Type: pb.MsgVote, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm()})
	}
}

// id from peer  t 预选举或选举 v 是否拒绝
func (r *raft) poll(id uint64, t pb.MessageType, v bool) (granted int, rejected int, result quorum.VoteResult) {
	if v {
		log.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		log.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	r.trk.RecordVote(id, v)
	return r.trk.TallyVotes()
}

// ------------------ public behavior ------------------------

func (r *raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.vote = None
	}
	r.lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.abortLeaderTransfer()
	r.trk.ResetVotes()
}

func (r *raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.raftOpts.electionTimeout + globalRand.Intn(r.raftOpts.electionTimeout)
}

func (r *raft) send(m pb.Message) {
	r.msgs = append(r.msgs, m)
}

func (r *raft) advance(rd Ready) {
	if newApplied := rd.appliedCursor(); newApplied > 0 {
		r.raftLog.appliedTo(newApplied)
	}

	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		r.raftLog.stableTo(e.Index)
	}
}

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}
