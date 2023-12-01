package raft

import (
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/raft/tracker"
	"math/rand"
)

type Role uint8

const None uint64 = 0
var SendEmptyMessage bool = true

const (
	Leader Role = iota
	Follower
	Candidate
)

type RaftOpts struct {
	ID uint64

	Storage db.Storage

	ElectionTick int

	HeartbeatTick int

	maxNextEntSize uint64

	Peers []config.Node
}
type Raft struct {
	id uint64

	LeaderID uint64

	Term uint64

	// max msg can send to follower
	maxMsgSize uint64

	maxUncommittedSize uint64
	// votes records
	VoteFor     uint64
	votes       map[uint64]bool
	voteCount   int //获取的认可票数
	rejectCount int //获取的拒绝票数

	RaftLog RaftLog

	Progress map[uint64]*tracker.Progress

	Role Role

	// 需要发送给其他节点的的消息
	msgs []*pb.Message

	//不同角色指向不同的stepFunc
	stepFunc stepFunc

	//不同角色指向不同的tick驱动函数
	tick func()

	//heartbeat interval是leader发送心跳的间隔时间。
	//election timeout是follower多久没收到心跳要重新选举的时间。
	//etcd默认heartbeat interval是100ms，election timeout是[1000,2000]ms。
	//heartbeat interval一般小于election timeout。
	heartbeatTimeout int

	electionTimeout int

	realElectionTimeout int

	heartbeatElapsed int

	electionElapsed int

	leadTransferee bool
}

type Progress struct {
	Match, Next uint64
}

func NewRaft(c *RaftOpts) (raft *Raft, err error) {
	raft = new(Raft)
	raft.id = c.ID
	raft.electionTimeout = c.ElectionTick
	raft.heartbeatTimeout = c.HeartbeatTick
	raft.RaftLog = newRaftLog(c.Storage)
	raft.becomeFollower(raft.Term, 0)
	progress := make(map[uint64]Progress)
	for _, peer := range c.Peers {
		progress[peer.ID] = Progress{}
	}
	raft.Progress = progress
	return
}

// Step 该函数接收一个 Msg，然后根据节点的角色和 Msg 的类型调用不同的处理函数。
// raft层都是通过Step函数和Tick函数驱动的
func (r *Raft) Step(m *pb.Message) error {
	return r.stepFunc(r, m)
}

type stepFunc func(r *Raft, m *pb.Message) error

func stepLeader(r *Raft, m *pb.Message) error {
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

func stepFollower(r *Raft, m *pb.Message) error {
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

func stepCandidate(r *Raft, m *pb.Message) error {
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

func (r *Raft) becomeFollower(term uint64, lead uint64) {
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

func (r *Raft) becomeCandidate() {
	log.Infof("%v become candidate", r.id)
	r.Role = Candidate
	r.Term += 1
	r.VoteFor = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.voteCount = 1
	r.tick = r.tickElection
	r.stepFunc = stepCandidate
	r.resetTick()
}

func (r *Raft) becomeLeader() {
	log.Infof("%v become leader", r.id)
	r.Role = Leader
	r.LeaderID = r.id
	r.stepFunc = stepLeader
	r.tick = r.tickHeartbeat
	for _, v := range r.Progress {
		v.Match = 0
		v.Next = r.RaftLog.lastIndex() + 1 //因为不知道其他节点的目前日志的一个状态，默认同步
	}
	entries := make([]pb.Entry, 1)
	entries = append(entries, pb.Entry{
		Type:  pb.EntryNormal,
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
		Data:  nil})

	r.RaftLog.AppendEntries(entries)
	r.bcastAppendEntries()
	r.resetTick()
}

func (r *Raft) tickElection() {
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

func (r *Raft) tickHeartbeat() {
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

func (r *Raft) campaign() {
	r.becomeCandidate()
	r.bcastVoteRequest()
}

// ------------------- leader behavior -------------------
func (r *Raft) bcastHeartbeat() {
	for peer := range r.Progress {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

func (r *Raft) sendHeartbeat(to uint64) {
	msg := &pb.Message{
		Type: pb.MsgBeat,
		To:   to,
		From: r.id,
		Term: r.Term,
		//通过携带的committed信息，follower节点可以知道哪些entry已经被committed
		Commit: r.RaftLog.CommittedIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleHeartbeatResponse(m *pb.Message) {
}

func (r *Raft) handlePropMsg(m *pb.Message) error {
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

func (r *Raft) handleConfigEntry(ents []pb.Entry) {
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
		alreadyJoint := len(r.prs.Config.Voters[1]) > 0
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
			r.logger.Infof("%x ignoring conf change %v at config %s: %s", r.id, cc, r.prs.Config, refused)
			m.Entries[i] = pb.Entry{Type: pb.EntryNormal}
		} else {
			r.pendingConfIndex = r.raftLog.lastIndex() + uint64(i) + 1
		}
	}
}
}

func (r *Raft) appendEnts(es ...pb.Entry) (accepted bool) {
	li := r.RaftLog.lastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + uint64(i)
	}

	after := es[0].Index - 1
	if after < r.RaftLog.committed {
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

	r.RaftLog.unstableEnts = append(r.RaftLog.unstableEnts, es...)
	return true
}

func (r *Raft) bcastAppendEnts() {
	for peer := range r.Progress {
		if peer != r.id {
			r.sendAppendEntries(peer, SendEmptyMessage)
		}
	}
}

// 当需要发送包含新条目的附加RPC时，如果需要发送空消息以传达更新的提交索引，则将sendIfEmpty标记为true。
func (r *Raft) sendAppendEntries(to uint64, sendIfEmpty bool) error {
	lastIndex := r.RaftLog.lastIndex()
	pr := r.Progress[to]

	//判断是否可以发送entries给该节点
	//1、该节点处于snapshot状态不应该发送
	if pr.IsPaused() {
		return false
	}

	m := pb.Message{}
	m.To = to

	term, errt := r.RaftLog.Term(pr.Next - 1)
	ents, erre := r.RaftLog.Entries(pr.Next, r.maxMsgSize)
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
	preLogTerm, err := r.RaftLog.Term(preLogIndex)
	if err != nil {
		// 日志如果被压缩那么需要发送快照,说明此时需要的index已经小于applied index了
		if err == code.ErrCompacted {
			//todo 开始发送快照
		}
		return err
	}

	// 发送节点需要的entry nextLog ----- lastLog
	entries, err := r.RaftLog.Entries(r.Progress[to].Next, lastIndex)
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
		Commit:  r.RaftLog.CommittedIndex(),
	}

	r.msgs = append(r.msgs, msg)

	return nil
}

func (r *Raft) handleAppendResponse(m *pb.Message) {
	if m.Reject {
		log.Debugf("%x received MsgAppResp(rejected, hint: (index %d, term %d)) from %x for index %d", r.id, m.RejectHint, m.LogTerm, m.From, m.Index)
		//todo 设计一个算法快速找到冲突的日志
		r.sendAppendEntries(m.From)
	} else {

	}
	r.updateCommittedIndex()
}

// 根据progress中大部分peer的match情况更新committedIndex
func (r *Raft) updateCommittedIndex() {
	commitUpdated := false
	for i := r.RaftLog.CommittedIndex(); i <= r.RaftLog.LastIndex(); i += 1 {
		if i <= r.RaftLog.CommittedIndex() {
			continue
		}
		matchCnt := 0
		for _, p := range r.Progress {
			if p.Match >= i {
				matchCnt += 1
			}
		}
		// leader不能直接提交之前leader的日志只能间接提交
		term, _ := r.RaftLog.Term(i)
		if matchCnt > len(r.Progress)/2 && term == r.Term && r.RaftLog.CommittedIndex() != i {
			r.RaftLog.SetCommittedIndex(i)
			commitUpdated = true
		}
	}
	if commitUpdated {
		r.bcastAppendEntries()
	}
}

// ------------------ follower behavior ------------------

func (r *Raft) handleHeartbeat(m pb.Message) {
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
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	if m.Commit > r.RaftLog.CommittedIndex() {
		r.RaftLog.SetCommittedIndex(min(m.Commit, r.RaftLog.LastIndex()))
	}
	r.sendHeartbeatResponse(m.From, false)
	if r.LeaderID == m.From {
		r.electionElapsed = 0
		r.sendHeartbeatResponse(m.From, false)
		r.updateCommitIndexEntry(m.Commit)
	}
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := &pb.Message{
		Type:    pb.MsgHeartbeatResp,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Last:    r.RaftLog.LastIndex(),
		Applied: r.RaftLog.AppliedIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleAppendEntries(m pb.Message) {
	r.electionElapsed = 0
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true)
		return
	}

	//检查日志是否匹配,不匹配需要指出期望的日志
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendAppendResponse(m.From, true)
		return
	}

	if len(m.Entries) > 0 {
		r.RaftLog.AppendEntries(m.Entries)
	}

	r.sendAppendResponse(m.From, false)
	r.updateCommitIndexEntry(m.Commit)
}

func (r *Raft) sendAppendResponse(to uint64, reject bool) {
	term, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	msg := &pb.Message{
		Type:    pb.MsgAppResp,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Applied: r.RaftLog.AppliedIndex(),
		Index:   r.RaftLog.LastIndex(),
		LogTerm: term,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) updateCommitIndexEntry(leaderCommitIndex uint64) {
	if leaderCommitIndex > r.RaftLog.CommittedIndex() {
		r.RaftLog.SetCommittedIndex(leaderCommitIndex)
	}
}

// ------------------ candidate behavior ------------------

func (r *Raft) bcastVoteRequest() {
	appliedTerm, _ := r.RaftLog.Term(r.RaftLog.AppliedIndex())
	for peer := range r.Progress {
		if peer != r.id {
			msg := &pb.Message{
				Type:    pb.MsgVote,
				From:    r.id,
				To:      peer,
				Term:    r.Term,
				LogTerm: appliedTerm,
				Applied: r.RaftLog.AppliedIndex(),
			}
			r.msgs = append(r.msgs, msg)
		}
	}
}

func (r *Raft) handleVoteRequest(m *pb.Message) {
	//如果票已经投给别的节点则拒绝
	if r.VoteFor != None && r.VoteFor != m.From {
		r.sendVoteResponse(m.From, true)
	} else if r.VoteFor != None && r.VoteFor == m.From {
		r.sendVoteResponse(m.From, false)
	}

	//applied index是否更新是判断能否成为leader的重要依据
	appliedIndex := r.RaftLog.AppliedIndex()
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

func (r *Raft) handleVoteResponse(m pb.Message) {
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

func (r *Raft) sendVoteResponse(candidate uint64, reject bool) {
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

func (r *Raft) isLeader() bool {
	return r.Role == Leader
}

func (r *Raft) isFollower() bool {
	return r.Role == Follower
}

func (r *Raft) isCandidate() bool {
	return r.Role == Candidate
}

func (r *Raft) resetTick() {
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.resetRealElectionTimeout()
}

func (r *Raft) resetRealElectionTimeout() {
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
func (r *Raft) increaseUncommittedSize(ents []pb.Entry) bool {
	var s uint64
	for _, e := range ents {
		s += uint64(PayloadSize(e))
	}

	if r.uncommittedSize > 0 && s > 0 && r.uncommittedSize+s > r.maxUncommittedSize {
		// If the uncommitted tail of the Raft log is empty, allow any size
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
func (r *Raft) reduceUncommittedSize(ents []pb.Entry) {
	if r.uncommittedSize == 0 {
		// Fast-path for followers, who do not track or enforce the limit.
		return
	}

	var s uint64
	for _, e := range ents {
		s += uint64(PayloadSize(e))
	}
	if s > r.uncommittedSize {
		// uncommittedSize may underestimate the size of the uncommitted Raft
		// log tail but will never overestimate it. Saturate at 0 instead of
		// allowing overflow.
		r.uncommittedSize = 0
	} else {
		r.uncommittedSize -= s
	}
}
