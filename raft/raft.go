package raft

import (
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/raft/confchange"
	"github.com/ColdToo/Cold2DB/raft/tracker"
	"math"
	"math/rand"
	"strings"
)

type Role uint8

const None uint64 = 0
const noLimit = math.MaxUint64
var SendEmptyMessage bool = true

const (
	Leader Role = iota
	Follower
	Candidate
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

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

type raftOpts struct {
	ID uint64

	ElectionTick int

	HeartbeatTick int

	maxNextEntSize uint64

	Peers []config.Peer

	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
	// MaxSizePerMsg limits the max byte size of each append message. Smaller
	// value lowers the raft recovery cost(initial probing and message lost
	// during normal operation). On the other side, it might affect the
	// throughput during normal replication. Note: math.MaxUint64 for unlimited,
	// 0 for at most one entry per message.
	MaxSizePerMsg uint64
	// MaxCommittedSizePerReady limits the size of the committed entries which
	// can be applied.
	MaxCommittedSizePerReady uint64
	// MaxUncommittedEntriesSize limits the aggregate byte size of the
	// uncommitted entries that may be appended to a leader's log. Once this
	// limit is exceeded, proposals will begin to return ErrProposalDropped
	// errors. Note: 0 for no limit.
	MaxUncommittedEntriesSize uint64
	// MaxInflightMsgs limits the max number of in-flight append messages during
	// optimistic replication phase. The application transportation layer usually
	// has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
	// overflowing that sending buffer.
	// limit the proposal rate?
	MaxInflightMsgs int
	// CheckQuorum specifies if the leader should check quorum activity. Leader
	// steps down when quorum is not active for an electionTimeout.
	CheckQuorum bool
	// PreVote enables the Pre-Vote algorithm described in raft thesis section
	// 9.6. This prevents disruption when a node that has been partitioned away
	// rejoins the cluster.
	PreVote bool
	// ReadOnlyOption specifies how the read only request is processed.
	//
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	//
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	// CheckQuorum MUST be enabled if ReadOnlyOption is ReadOnlyLeaseBased.
	ReadOnlyOption ReadOnlyOption
	// Logger is the logger used for raft log. For multinode which can host
	// multiple raft group, each raft group can have its own logger
	// Logger Logger
	// max msg can send to follower
	maxMsgSize uint64

	maxUncommittedSize uint64
	//heartbeat interval是leader发送心跳的间隔时间。
	//election timeout是follower多久没收到心跳要重新选举的时间。
	//etcd默认heartbeat interval是100ms，election timeout是[1000,2000]ms。
	//heartbeat interval一般小于election timeout。
	heartbeatTimeout int

	electionTimeout int
}

func (c *raftOpts) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.MaxUncommittedEntriesSize == 0 {
		c.MaxUncommittedEntriesSize = noLimit
	}

	// default MaxCommittedSizePerReady to MaxSizePerMsg because they were
	// previously the same parameter.
	if c.MaxCommittedSizePerReady == 0 {
		c.MaxCommittedSizePerReady = c.MaxSizePerMsg
	}

	if c.MaxInflightMsgs <= 0 {
		return errors.New("max inflight messages must be greater than 0")
	}

	if c.ReadOnlyOption == ReadOnlyLeaseBased && !c.CheckQuorum {
		return errors.New("CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased")
	}

	return nil
}

type raft struct {
	id   uint64
	lead uint64
	Term uint64
	Role Role
	// isLearner is true if the local raft node is a learner.
	isLearner bool
	raftOpts *raftOpts

	raftLog  *raftLog
	//用于追踪节点的进度
	trk tracker.ProgressTracker
	//需要发送给其他节点的的消息
	msgs []*pb.Message
	readOnly *readOnly
	//不同角色指向不同的stepFunc
	stepFunc stepFunc
	//不同角色指向不同的tick驱动函数
	tick func()

	// number of ticks since it reached last electionTimeout when it is leader
	// or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int

	// votes records
	Vote     uint64
	votes       map[uint64]bool
	voteCount   int //获取的认可票数
	rejectCount int //获取的拒绝票数

	realElectionTimeout int

	leadTransferee bool
}

func newraft(opts *raftOpts,storage db.Storage) (r *raft, err error) {
	if err = opts.validate(); err != nil {
		log.Panicf("verify raft options failed",err)
	}
	rLog := newraftLog(storage, opts.MaxCommittedSizePerReady)
	r = &raft{
		id:                        opts.ID,
		lead:                      None,
		isLearner:                 false,
		raftLog:                   rLog,
		trk:                       tracker.MakeProgressTracker(opts.MaxInflightMsgs),
		raftOpts:                  opts,
		readOnly: 				   newReadOnly(opts.ReadOnlyOption),
	}

	//todo retore hardstate
	hs, _, err := storage.GetHardState()
	//cfg, trk, err := confchange.Restore(confchange.Changer{Tracker:   r.trk, LastIndex: raftlog.lastIndex()}, cs)
	//if err != nil {
	//	panic(err)
	//}
	//assertConfStatesEquivalent(log, cs, r.switchToConfig(cfg, trk))

	if !IsEmptyHardState(hs) {
		r.loadHardState(hs)
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
	if state.Committed < r.raftLog.committed || state.Committed > r.raftLog.lastIndex() {
		log.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Committed, r.raftLog.committed, r.raftLog.lastIndex())
	}
	r.raftLog.committed = state.Committed
	r.Term = state.Term
	r.Vote = state.Vote
}

// Step 该函数接收一个 Msg，然后根据节点的角色和 Msg 的类型调用不同的处理函数。
// raft层都是通过Step函数和Tick函数驱动的
func (r *raft) Step(m *pb.Message) error {
	return r.stepFunc(r, m)
}

type stepFunc func(r *raft, m *pb.Message) error

func stepLeader(r *raft, m *pb.Message) error {
	switch m.Type {
	case pb.MsgBeat:
		r.bcastHeartbeat()
	case pb.MsgHeartbeatResp:
		r.handleHeartbeatResponse(m)
	case pb.MsgProp:
		r.handlePropMsg(m)
	case pb.MsgAppResp:
		r.handleAppendResponse(m)
	}
	return nil
}

func stepFollower(r *raft, m *pb.Message) error {
	switch m.Type {
	case pb.MsgHup:
		r.campaign()
	case pb.MsgVote:
		r.handleVoteRequest(m)
	case pb.MsgHeartbeat:
		r.handleHeartbeat(*m)
	case pb.MsgApp:
		r.handleAppendEntries(*m)
	}
	return nil
}

func stepCandidate(r *raft, m *pb.Message) error {
	switch m.Type {
	case pb.MsgHup:
		r.campaign()
	case pb.MsgVote:
		r.handleVoteRequest(m)
	case pb.MsgVoteResp:
		r.handleVoteResponse(*m)
	}
	return nil
}

func (r *raft) becomeFollower(term uint64, lead uint64) {
	log.Infof("%v become follower", r.id)
	r.Role = Follower
	r.Term = term
	r.LeaderID = lead
	r.votes = nil
	r.voteCount = 0
	r.rejectCount = 0
	r.stepFunc = stepFollower
	r.tick = r.tickElection
	r.resetTick()
}

func (r *raft) becomeCandidate() {
	log.Infof("%v become candidate", r.id)
	r.Role = Candidate
	r.Term += 1
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.voteCount = 1
	r.tick = r.tickElection
	r.stepFunc = stepCandidate
	r.resetTick()
}

func (r *raft) becomeLeader() {
	log.Infof("%v become leader", r.id)
	r.Role = Leader
	r.LeaderID = r.id
	r.stepFunc = stepLeader
	r.tick = r.tickHeartbeat
	for _, v := range r.Progress {
		v.Match = 0
		v.Next = r.raftLog.lastIndex() + 1 //因为不知道其他节点的目前日志的一个状态，默认同步
	}
	entries := make([]pb.Entry, 1)
	entries = append(entries, pb.Entry{
		Type:  pb.EntryNormal,
		Term:  r.Term,
		Index: r.raftLog.LastIndex() + 1,
		Data:  nil})

	r.raftLog.AppendEntries(entries)
	r.bcastAppendEntries()
	r.resetTick()
}

func (r *raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.realElectionTimeout {
		r.electionElapsed = 0
		err := r.Step(&pb.Message{From: r.id, Type: pb.MsgHup})
		if err != nil {
			log.Error("msg").Err(code.TickErr, err).Record()
			return
		}
	}
}

func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		err := r.Step(&pb.Message{From: r.id, Type: pb.MsgBeat})
		if err != nil {
			log.Error("msg").Err(code.TickErr, err).Record()
			return
		}
	}
}

func (r *raft) campaign() {
	r.becomeCandidate()
	r.bcastVoteRequest()
}

// ------------------- leader behavior -------------------
func (r *raft) bcastHeartbeat() {
	for peer := range r.Progress {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

func (r *raft) sendHeartbeat(to uint64) {
	msg := &pb.Message{
		Type: pb.MsgBeat,
		To:   to,
		From: r.id,
		Term: r.Term,
		//通过携带的committed信息，follower节点可以知道哪些entry已经被committed
		Commit: r.raftLog.CommittedIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

func (r *raft) handleHeartbeatResponse(m *pb.Message) {
}

func (r *raft) handlePropMsg(m *pb.Message) error {
	if len(m.Entries) == 0 {
		log.Panicf("%x stepped empty MsgProp", r.id)
	}
	//首先leader会检查自己的Progress结构是否还存在，以判断自己是否已经被ConfChange操作移出了集群，如果该leader被移出了集群，则不会处理该提议。
	if r.Progress[r.id] == nil {
		return code.ErrProposalDropped
	}
	if r.leadTransferee {
		log.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
		return code.ErrProposalDropped
	}

	r.handleConfigEntry(m.Entries)


	if !r.appendEnts(m.Entries...) {
		return code.ErrProposalDropped
	}

	r.bcastAppendEnts()
	return nil
}

func (r *raft) handleConfigEntry(ents []pb.Entry) {
	e := &m.Entries[i]
	var cc pb.ConfChangeI
	if e.Type == pb.EntryConfChange {
		var ccc pb.ConfChange
		if err := ccc.Unmarshal(e.Data); err != nil {
			panic(err)
		}
		cc = ccc
	} else if e.Type == pb.EntryConfChangeV2 {
		var ccc pb.ConfChangeV2
		if err := ccc.Unmarshal(e.Data); err != nil {
			panic(err)
		}
		cc = ccc
	}
	if cc != nil {
		alreadyPending := r.pendingConfIndex > r.raftLog.applied
		alreadyJoint := len(r.trk.Config.Voters[1]) > 0
		wantsLeaveJoint := len(cc.AsV2().Changes) == 0

		var refused string
		if alreadyPending {
			refused = fmt.Sprintf("possible unapplied conf change at index %d (applied to %d)", r.pendingConfIndex, r.raftLog.applied)
		} else if alreadyJoint && !wantsLeaveJoint {
			refused = "must transition out of joint config first"
		} else if !alreadyJoint && wantsLeaveJoint {
			refused = "not in joint state; refusing empty conf change"
		}

		if refused != "" {
			log.Infof("%x ignoring conf change %v at config %s: %s", r.id, cc, r.trk.Config, refused)
			m.Entries[i] = pb.Entry{Type: pb.EntryNormal}
		} else {
			r.pendingConfIndex = r.raftLog.lastIndex() + uint64(i) + 1
		}
	}
}
}

func (r *raft) appendEnts(es ...pb.Entry) (accepted bool) {
	li := r.raftLog.lastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + uint64(i)
	}

	after := es[0].Index - 1
	if after < r.raftLog.committed {
		log.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}

	// 限流
	if !r.increaseUncommittedSize(es) {
		log.Debugf(
			"%x appending new entries to log would exceed uncommitted entry size limit; dropping proposal",
			r.id,
		)
		return false
	}

	r.raftLog.unstableEnts = append(r.raftLog.unstableEnts, es...)
	return true
}

func (r *raft) bcastAppendEnts() {
	for peer := range r.Progress {
		if peer != r.id {
			r.sendAppendEntries(peer, SendEmptyMessage)
		}
	}
}

// 当需要发送包含新条目的附加RPC时，如果需要发送空消息以传达更新的提交索引，则将sendIfEmpty标记为true。
func (r *raft) sendAppendEntries(to uint64, sendIfEmpty bool) error {
	lastIndex := r.raftLog.lastIndex()
	pr := r.Progress[to]

	//判断是否可以发送entries给该节点
	//1、该节点处于snapshot状态不应该发送
	if pr.IsPaused() {
		return false
	}

	m := pb.Message{}
	m.To = to

	term, errt := r.raftLog.Term(pr.Next - 1)
	ents, erre := r.raftLog.Entries(pr.Next, r.maxMsgSize)
	if len(ents) == 0 && !sendIfEmpty {
		return false
	}

	// 如果最后一条日志索引>=追随者的nextIndex，才会发送entries
	if lastIndex < r.Progress[to].Next {
		return errors.New("dont need to send append message")
	}

	// 目标节点所期望的下一条日志的上一条日志
	preLogIndex := r.Progress[to].Next - 1

	// 目标节点所期望的下一条日志的上一条日志
	preLogTerm, err := r.raftLog.Term(preLogIndex)
	if err != nil {
		// 日志如果被压缩那么需要发送快照,说明此时需要的index已经小于applied index了
		if err == code.ErrCompacted {
			//todo 开始发送快照
		}
		return err
	}

	// 发送节点需要的entry nextLog ----- lastLog
	entries, err := r.raftLog.Entries(r.Progress[to].Next, lastIndex)
	if err != nil {
		return err
	}

	sendEntries := make([]pb.Entry, 0)
	for _, en := range entries {
		sendEntries = append(sendEntries, *en)
	}

	msg := &pb.Message{
		Type:    pb.MsgApp,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: preLogTerm,
		Index:   preLogIndex,
		Entries: sendEntries,
		Commit:  r.raftLog.CommittedIndex(),
	}

	r.msgs = append(r.msgs, msg)

	return nil
}

func (r *raft) handleAppendResponse(m *pb.Message) {
	if m.Reject {
		log.Debugf("%x received MsgAppResp(rejected, hint: (index %d, term %d)) from %x for index %d", r.id, m.RejectHint, m.LogTerm, m.From, m.Index)
		//todo 设计一个算法快速找到冲突的日志
		r.sendAppendEntries(m.From)
	} else {

	}
	r.updateCommittedIndex()
}

// 根据progress中大部分peer的match情况更新committedIndex
func (r *raft) updateCommittedIndex() {
	commitUpdated := false
	for i := r.raftLog.CommittedIndex(); i <= r.raftLog.LastIndex(); i += 1 {
		if i <= r.raftLog.CommittedIndex() {
			continue
		}
		matchCnt := 0
		for _, p := range r.Progress {
			if p.Match >= i {
				matchCnt += 1
			}
		}
		// leader不能直接提交之前leader的日志只能间接提交
		term, _ := r.raftLog.Term(i)
		if matchCnt > len(r.Progress)/2 && term == r.Term && r.raftLog.CommittedIndex() != i {
			r.raftLog.SetCommittedIndex(i)
			commitUpdated = true
		}
	}
	if commitUpdated {
		r.bcastAppendEntries()
	}
}

// ------------------ follower behavior ------------------

func (r *raft) handleHeartbeat(m pb.Message) {
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	if r.Role != Follower {
		r.becomeFollower(m.Term, m.From)
	}
	r.resetTick()
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	term, err := r.raftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	if m.Commit > r.raftLog.CommittedIndex() {
		r.raftLog.SetCommittedIndex(min(m.Commit, r.raftLog.LastIndex()))
	}
	r.sendHeartbeatResponse(m.From, false)
	if r.LeaderID == m.From {
		r.electionElapsed = 0
		r.sendHeartbeatResponse(m.From, false)
		r.updateCommitIndexEntry(m.Commit)
	}
}

func (r *raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := &pb.Message{
		Type:    pb.MsgHeartbeatResp,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Last:    r.raftLog.LastIndex(),
		Applied: r.raftLog.AppliedIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

func (r *raft) handleAppendEntries(m pb.Message) {
	r.electionElapsed = 0
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true)
		return
	}

	//检查日志是否匹配,不匹配需要指出期望的日志
	term, err := r.raftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendAppendResponse(m.From, true)
		return
	}

	if len(m.Entries) > 0 {
		r.raftLog.AppendEntries(m.Entries)
	}

	r.sendAppendResponse(m.From, false)
	r.updateCommitIndexEntry(m.Commit)
}

func (r *raft) sendAppendResponse(to uint64, reject bool) {
	term, _ := r.raftLog.Term(r.raftLog.LastIndex())
	msg := &pb.Message{
		Type:    pb.MsgAppResp,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Applied: r.raftLog.AppliedIndex(),
		Index:   r.raftLog.LastIndex(),
		LogTerm: term,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *raft) updateCommitIndexEntry(leaderCommitIndex uint64) {
	if leaderCommitIndex > r.raftLog.CommittedIndex() {
		r.raftLog.SetCommittedIndex(leaderCommitIndex)
	}
}

// ------------------ candidate behavior ------------------

func (r *raft) bcastVoteRequest() {
	appliedTerm, _ := r.raftLog.Term(r.raftLog.AppliedIndex())
	for peer := range r.Progress {
		if peer != r.id {
			msg := &pb.Message{
				Type:    pb.MsgVote,
				From:    r.id,
				To:      peer,
				Term:    r.Term,
				LogTerm: appliedTerm,
				Applied: r.raftLog.AppliedIndex(),
			}
			r.msgs = append(r.msgs, msg)
		}
	}
}

func (r *raft) handleVoteRequest(m *pb.Message) {
	//如果票已经投给别的节点则拒绝
	if r.VoteFor != None && r.VoteFor != m.From {
		r.sendVoteResponse(m.From, true)
	} else if r.VoteFor != None && r.VoteFor == m.From {
		r.sendVoteResponse(m.From, false)
	}

	//applied index是否更新是判断能否成为leader的重要依据
	appliedIndex := r.raftLog.AppliedIndex()
	if appliedIndex > m.Applied {
		r.sendVoteResponse(m.From, true)
		return
	} else if r.Term >= m.Term {
		r.sendVoteResponse(m.From, true)
		return
	}

	r.becomeFollower(m.Term, m.From)
	r.sendVoteResponse(m.From, false)
}

func (r *raft) handleVoteResponse(m pb.Message) {
	//需要过滤掉重复的投票信息
	if _, ok := r.votes[m.From]; !ok {
		return
	}

	if !m.Reject && !r.votes[m.From] {
		r.votes[m.From] = true
		r.voteCount += 1
	} else {
		r.votes[m.From] = false
		r.rejectCount += 1
	}
	if r.voteCount > len(r.Progress)/2 {
		r.becomeLeader()
	} else if r.rejectCount > len(r.Progress)/2 {
		r.becomeFollower(r.Term, r.LeaderID)
	}
}

func (r *raft) sendVoteResponse(candidate uint64, reject bool) {
	msg := &pb.Message{
		Type:   pb.MsgVoteResp,
		From:   r.id,
		To:     candidate,
		Term:   r.Term,
		Reject: reject,
	}
	r.msgs = append(r.msgs, msg)
}

// ------------------ public behavior ------------------------

func (r *raft) isLeader() bool {
	return r.Role == Leader
}

func (r *raft) isFollower() bool {
	return r.Role == Follower
}

func (r *raft) isCandidate() bool {
	return r.Role == Candidate
}

func (r *raft) resetTick() {
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.resetRealElectionTimeout()
}

func (r *raft) resetRealElectionTimeout() {
	r.realElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// increaseUncommittedSize computes the size of the proposed entries and
// determines whether they would push leader over its maxUncommittedSize limit.
// If the new entries would exceed the limit, the method returns false. If not,
// the increase in uncommitted entry size is recorded and the method returns
// true.
//
// Empty payloads are never refused. This is used both for appending an empty
// entry at a new leader's term, as well as leaving a joint configuration.
func (r *raft) increaseUncommittedSize(ents []pb.Entry) bool {
	var s uint64
	for _, e := range ents {
		s += uint64(PayloadSize(e))
	}

	if r.uncommittedSize > 0 && s > 0 && r.uncommittedSize+s > r.maxUncommittedSize {
		// If the uncommitted tail of the raft log is empty, allow any size
		// proposal. Otherwise, limit the size of the uncommitted tail of the
		// log and drop any proposal that would push the size over the limit.
		// Note the added requirement s>0 which is used to make sure that
		// appending single empty entries to the log always succeeds, used both
		// for replicating a new leader's initial empty entry, and for
		// auto-leaving joint configurations.
		return false
	}
	r.uncommittedSize += s
	return true
}

// reduceUncommittedSize accounts for the newly committed entries by decreasing
// the uncommitted entry size limit.
func (r *raft) reduceUncommittedSize(ents []pb.Entry) {
	if r.uncommittedSize == 0 {
		// Fast-path for followers, who do not track or enforce the limit.
		return
	}

	var s uint64
	for _, e := range ents {
		s += uint64(PayloadSize(e))
	}
	if s > r.uncommittedSize {
		// uncommittedSize may underestimate the size of the uncommitted raft
		// log tail but will never overestimate it. Saturate at 0 instead of
		// allowing overflow.
		r.uncommittedSize = 0
	} else {
		r.uncommittedSize -= s
	}
}


// switchToConfig reconfigures this node to use the provided configuration. It
// updates the in-memory state and, when necessary, carries out additional
// actions such as reacting to the removal of nodes or changed quorum
// requirements.
//
// The inputs usually result from restoring a ConfState or applying a ConfChange.
func (r *raft) switchToConfig(cfg tracker.Config, trk tracker.ProgressMap) pb.ConfState {
	r.trk.Config = cfg
	r.trk.Progress = trk

	log.Infof("%x switched to configuration %s", r.id, r.trk.Config)
	cs := r.trk.ConfState()
	pr, ok := r.trk.Progress[r.id]

	// Update whether the node itself is a learner, resetting to false when the
	// node is removed.
	r.isLearner = ok && pr.IsLearner

	if (!ok || r.isLearner) && r.state == StateLeader {
		// This node is leader and was removed or demoted. We prevent demotions
		// at the time writing but hypothetically we handle them the same way as
		// removing the leader: stepping down into the next Term.
		//
		// TODO(tbg): step down (for sanity) and ask follower with largest Match
		// to TimeoutNow (to avoid interruption). This might still drop some
		// proposals but it's better than nothing.
		//
		// TODO(tbg): test this branch. It is untested at the time of writing.
		return cs
	}

	// The remaining steps only make sense if this node is the leader and there
	// are other nodes.
	if r.state != StateLeader || len(cs.Voters) == 0 {
		return cs
	}

	if r.maybeCommit() {
		// If the configuration change means that more entries are committed now,
		// broadcast/append to everyone in the updated config.
		r.bcastAppend()
	} else {
		// Otherwise, still probe the newly added replicas; there's no reason to
		// let them wait out a heartbeat interval (or the next incoming
		// proposal).
		r.trk.Visit(func(id uint64, pr *tracker.Progress) {
			r.maybeSendAppend(id, false /* sendIfEmpty */)
		})
	}
	// If the the leadTransferee was removed or demoted, abort the leadership transfer.
	if _, tOK := r.trk.Config.Voters.IDs()[r.leadTransferee]; !tOK && r.leadTransferee != 0 {
		r.abortLeaderTransfer()
	}

	return cs
}