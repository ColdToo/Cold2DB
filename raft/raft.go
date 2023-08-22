package raft

import (
	"errors"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"go.etcd.io/etcd/raft/tracker"
	"math/rand"
)

type Role uint8

const None uint64 = 0

const (
	Leader Role = iota
	Follower
	Candidate
)

type RaftOpts struct {
	// local raft id
	ID uint64

	//peers []uint64 //peers ip

	Storage Storage

	ElectionTick int

	HeartbeatTick int
}

type Raft struct {
	id uint64

	Term uint64

	VoteFor uint64

	// votes records
	votes map[uint64]bool

	voteCount int

	rejectCount int

	RaftLog *RaftLog

	trk tracker.ProgressTracker

	Progress map[uint64]*Progress

	Role Role

	// 需要发送给其他节点的msgs
	msgs []*pb.Message

	LeaderID uint64

	heartbeatTimeout int

	electionTimeout int

	realElectionTimeout int

	heartbeatElapsed int

	electionElapsed int

	// leader要维护或者从集群中删除。
	// 有更适合当server的节点，比如说有更高的负载，更好的网络条件。
	leadTransferee uint64

	//不同的角色指向不同的stepFunc
	stepFunc stepFunc

	//不同的角色指向不同的tick驱动函数
	tick func()

	//Check Quorum 是针对这种情况：当 Leader 被网络分区的时，其他实例已经选举出了新的 Leader，旧 Leader 不能收到新 Leader 的消息，
	//这时它自己不能发现自己已不是 Leader。Check Quorum 机制可以帮助 Leader 主动发现这种情况：在electionTimeOut过期的时候检查Quorum，
	//发现自己不能与多数节点保持正常通信时，及时退为 Follower
	checkQuorum bool
}

// match:当前log lastindex
// next:当前log lastindex+1
type Progress struct {
	Match, Next uint64
}

func NewRaft(c *RaftOpts) (raft *Raft, err error) {
	if err != nil {
		return
	}
	raft = new(Raft)
	raft.RaftLog, err = newRaftLog(c.Storage)
	raft.id = c.ID
	raft.stepFunc = stepFollower
	raft.Role = Follower
	raft.electionTimeout = c.ElectionTick
	raft.heartbeatTimeout = c.HeartbeatTick
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
	case pb.MsgProp:
		r.handlePropMsg(m)
	case pb.MsgBeat:
		r.bcastHeartbeat()
	case pb.MsgHeartbeatResp:
		r.handleHeartbeatResponse(m)
	case pb.MsgAppResp:
		r.handleAppendResponse(m)
	case pb.MsgVote:
		r.handleVoteRequest(m)

		//todo 剩下逻辑
	case pb.MsgSnapStatus:
	case pb.MsgUnreachable:
	case pb.MsgTransferLeader:
	}
	return nil
}

func stepFollower(r *Raft, m *pb.Message) error {
	switch m.Type {
	// todo 应该设计一个负载均衡的组件直接将提议发送给leader，而不是在follower这里转发给leader？
	case pb.MsgProp:
		if r.LeaderID == None {
			log.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return errors.New(code.ErrProposalDropped)
		}
		m.To = r.LeaderID
	case pb.MsgApp:
		r.handleAppendEntries(*m)
	case pb.MsgHeartbeat:
		r.electionElapsed = 0
		r.LeaderID = m.From
		r.handleHeartbeat(*m)
	case pb.MsgVote:
		r.handleVoteRequest(m)
	case pb.MsgSnap:

	}
	return nil
}

func stepCandidate(r *Raft, m *pb.Message) error {
	switch m.Type {
	case pb.MsgVoteResp:
		r.handleVoteResponse(*m)
	case pb.MsgVote:
		r.handleVoteRequest(m)
	case pb.MsgApp:
		r.handleAppendEntries(*m)
	}
	return nil
}

func (r *Raft) Tick() {
	r.tick()
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		err := r.Step(&pb.Message{From: r.id, Type: pb.MsgHup})
		if err != nil {
			log.Error("msg").Err(code.TickErr, err).Record()
			return
		}
	}
}

func (r *Raft) tickHeartbeat() {
	if r.Role != Leader {
		log.Panic("only leader can increase beat").Record()
		return
	}
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

func (r *Raft) becomeFollower(term uint64, lead uint64) {
	log.Debugf("%v become follower", r.id)
	r.Role = Follower
	r.Term = term
	r.stepFunc = stepFollower
	r.LeaderID = lead
	r.votes = nil
	r.voteCount = 0
	r.rejectCount = 0
	r.leadTransferee = None
	r.resetTick()
}

func (r *Raft) becomeCandidate() {
	log.Debugf("%v become candidate", r.id)
	r.Role = Candidate
	r.Term += 1
	r.VoteFor = r.id
	r.stepFunc = stepCandidate
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.voteCount = 1
	r.resetTick()
}

func (r *Raft) becomeLeader() {
	r.Role = Leader
	r.LeaderID = r.id
	r.stepFunc = stepLeader
	r.RaftLog.committed = 0
	for _, v := range r.Progress {
		v.Match = 0
		v.Next = r.RaftLog.LastIndex() + 1 //因为不知道其他节点的目前日志的一个状态，默认同步
	}
	// NOTE: Leader should propose a noop entry on its term for sync un applied entries
	r.resetTick()
	entries := make([]*pb.Entry, 1)

	// todo 更换leader后是否应该强制让follower节点将raft log memory段空间情况，然后同步applied段的entry
	entries = append(entries, &pb.Entry{
		Type:  pb.EntryNormal,
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
		Data:  nil})
	r.RaftLog.AppendEntries(entries)
	r.bcastAppendEntries()
	r.updateCommit()
}

// ------------------- leader behavior -------------------

func (r *Raft) handlePropMsg(m *pb.Message) {
	lastIndex := r.RaftLog.LastIndex()
	ents := make([]*pb.Entry, 0)
	for _, e := range m.Entries {
		ents = append(ents, &pb.Entry{
			Type:  e.Type,
			Term:  r.Term,
			Index: lastIndex + 1,
			Data:  e.Data,
		})
		lastIndex += 1
	}

	r.RaftLog.AppendEntries(ents)

	r.bcastAppendEntries()
	return
}

func (r *Raft) bcastAppendEntries() {
	for peer := range r.Progress {
		if peer != r.id {
			r.sendAppendEntries(peer)
		}
	}
}

func (r *Raft) sendAppendEntries(to uint64) error {
	lastIndex := r.RaftLog.LastIndex()

	// 目标节点所期望的下一条日志的上一条日志
	preLogIndex := r.Progress[to].Next - 1

	// 如果最后一条日志索引>=追随者的nextIndex，才会发送entries
	if lastIndex < preLogIndex {
		return errors.New("dont need  to send")
	}

	// 目标节点所期望的下一条日志的上一条日志
	preLogTerm, err := r.RaftLog.Term(preLogIndex)
	if err != nil {
		if err == ErrCompacted {
			r.sendSnapshot(to)
			return err
		}
		return err
	}

	// 发送节点需要的entry
	// nextLog ----- lastLog
	entries, err := r.RaftLog.Entries(preLogIndex+1, lastIndex)
	if err != nil {
		return err
	}

	sendEntries := make([]pb.Entry, 0)
	for _, en := range entries {
		sendEntries = append(sendEntries, pb.Entry{
			Type:  en.Type,
			Term:  en.Term,
			Index: en.Index,
			Data:  en.Data,
		})
	}

	msg := &pb.Message{
		Type:    pb.MsgApp,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: preLogTerm,
		Index:   preLogIndex,
		Entries: sendEntries,
		Commit:  r.RaftLog.committed,
	}

	r.msgs = append(r.msgs, msg)

	return nil
}

func (r *Raft) bcastHeartbeat() {
	for peer := range r.Progress {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

// todo 发送心跳的时候是否也可以将commited信息同步给fowller节点？
func (r *Raft) sendHeartbeat(to uint64) {
	msg := &pb.Message{
		Type: pb.MsgBeat,
		To:   to,
		From: r.id,
		Term: r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleAppendResponse(m *pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}

	//当被follower拒绝时，与follower对齐剩下日志
	if m.Reject {
		log.Debugf("%x received MsgAppResp(rejected, hint: (index %d, term %d)) from %x for index %d",
			r.id, m.RejectHint, m.LogTerm, m.From, m.Index)
		r.trk.Progress[m.From].Next = m.RejectHint
		r.sendAppendEntries(m.From)
	} else {
		r.trk.Progress[m.From].Next = m.Index
	}

	//todo 当大多数节点认可了一个日志后将该日志置为commited
	r.updateCommit()
}

// todo hearbeat response是否应该也携带一部分消息传递给follower
func (r *Raft) handleHeartbeatResponse(m *pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
}

func (r *Raft) sendSnapshot(to uint64) {
	snap, err := r.RaftLog.storage.GetSnapshot()
	if err != nil {
		return
	}
	r.msgs = append(r.msgs, &pb.Message{
		Type:     pb.MsgSnap,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: snap,
	})
	r.Progress[to].Next = snap.Metadata.Index + 1
}

func (r *Raft) updateCommit() {
	commitUpdated := false
	for i := r.RaftLog.committed; i <= r.RaftLog.LastIndex(); i += 1 {
		matchCnt := 0
		for _, p := range r.Progress {
			if p.Match >= i {
				matchCnt += 1
			}
		}

		term, _ := r.RaftLog.Term(i)
		if matchCnt > len(r.Progress)/2 && term == r.Term && r.RaftLog.committed != i {
			r.RaftLog.committed = i
			commitUpdated = true
		}
	}
	if commitUpdated {
		r.bcastAppendEntries()
	}
}

// ------------------ candidate behavior ------------------

func (r *Raft) bcastVoteRequest() {
	appliedTerm, _ := r.RaftLog.Term(r.RaftLog.applied)
	for peer := range r.Progress {
		if peer != r.id {
			msg := &pb.Message{
				Type:    pb.MsgVote,
				From:    r.id,
				To:      peer,
				Term:    r.Term,
				LogTerm: appliedTerm,
				Index:   r.RaftLog.applied,
			}
			r.msgs = append(r.msgs, msg)
		}
	}
}

func (r *Raft) handleVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, r.LeaderID)
		r.VoteFor = m.From
		return
	}
	if !m.Reject {
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

func (r *Raft) handleVoteRequest(m *pb.Message) {
	if r.Term > m.Term {
		r.sendVoteResponse(m.From, true)
		return
	}

	//若不是follower变为follower再进行处理
	if !r.isFollower() && m.Term > r.Term {
		r.becomeFollower(r.Term, r.LeaderID)
	}

	//如果已经投给别的节点则拒绝
	if r.VoteFor != None && r.VoteFor != m.From {
		r.sendVoteResponse(m.From, true)
	}

	if r.VoteFor == m.From {
		r.sendVoteResponse(m.From, true)
	}

	if r.isMoreUpToDateThan(m.LogTerm, m.Index) {
		if r.Term < m.Term {
			r.becomeFollower(m.Term, None)
		}
		r.sendVoteResponse(m.From, true)
		return
	}

	appliedTerm, _ := r.RaftLog.Term(r.RaftLog.applied)
	//比较applied index 的大小 todo  是否还需要比较其他东西？
	if r.VoteFor == None && (appliedTerm > m.LogTerm) {
		r.sendVoteResponse(m.From, false)
		return
	}

	r.resetRealElectionTimeout()
	r.sendVoteResponse(m.From, true)
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

// ------------------ follower behavior ------------------

func (r *Raft) handleAppendEntries(m pb.Message) {
	r.electionElapsed = 0
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true)
		return
	}
	r.LeaderID = m.From

	//检查日志是否匹配
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendAppendResponse(m.From, true)
		return
	}

	if len(m.Entries) > 0 {
		r.RaftLog.AppendEntries(m.Entries)
	}

	r.sendAppendResponse(m.From, false)
}

func (r *Raft) sendAppendResponse(to uint64, reject bool) {
	msg := &pb.Message{
		Type:   pb.MsgAppResp,
		From:   r.id,
		To:     to,
		Term:   r.Term,
		Reject: reject,
		Index:  r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleHeartbeat(m pb.Message) {
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}

	r.becomeFollower(m.Term, m.From)
	// todo 根据leader的committed位置挪动本地节点committed的位置
	if r.RaftLog.committed > m.Commit {
		r.RaftLog.committed = m.Commit
	}

	r.sendHeartbeatResponse(m.From, false)
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := &pb.Message{
		Type:   pb.MsgHeartbeatResp,
		From:   r.id,
		To:     to,
		Term:   r.Term,
		Reject: reject,
		Index:  r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleSnapshot(m pb.Message) {
	r.electionElapsed = 0
	r.LeaderID = m.From
	return
}

func (r *Raft) handleLeaderTransfer(m pb.Message) {
	return
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

func (r *Raft) isMoreUpToDateThan(logTerm, index uint64) bool {
	lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if lastTerm > logTerm || (lastTerm == logTerm && r.RaftLog.LastIndex() > index) {
		return true
	}
	return false
}
