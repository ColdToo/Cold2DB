package raft

import (
	"errors"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
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
	ID uint64

	Storage Storage

	ElectionTick int

	HeartbeatTick int

	Peers []config.Node
}

type Raft struct {
	id uint64

	LeaderID uint64

	Term uint64

	// votes records
	VoteFor     uint64
	votes       map[uint64]bool
	voteCount   int //获取的认可票数
	rejectCount int //获取的拒绝票数

	RaftLog Log

	Progress map[uint64]Progress

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
		v.Next = r.RaftLog.LastIndex() + 1 //因为不知道其他节点的目前日志的一个状态，默认同步
	}

	// todo 新leader需要清除其他follower节点的memory区域的entries,以及和其他节点同步wal memory段的applied群
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
	//通过携带的committed信息，follower节点可以知道哪些entry已经被committed
	msg := &pb.Message{
		Type:   pb.MsgBeat,
		To:     to,
		From:   r.id,
		Term:   r.Term,
		Commit: r.RaftLog.CommittedIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleHeartbeatResponse(m *pb.Message) {
	//通过follower的applied index,leader可以将部分满足要求的 committed entry apply 置为pre applied index
	r.updatePreAppliedIndex(m.Last, m.Applied)
}

func (r *Raft) handlePropMsg(m *pb.Message) {
	lastIndex := r.RaftLog.LastIndex()
	ents := make([]pb.Entry, 0)
	for _, e := range m.Entries {
		ents = append(ents, pb.Entry{
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
		if err == ErrCompacted {
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

	//todo 当大多数节点append某个日志后将该日志置为commited,根据follower节点返回的applied last将部分commited index转为preApplied index
	r.updatePreAppliedIndex(m.Last, m.Applied)
	r.updateCommittedIndex()
}

// 根据progress中大部分peer的next情况推进committed的进度
func (r *Raft) updateCommittedIndex() {
	//                      commit
	//  index 1   2   3   4   5   6   7   8
	//  node1 1   1   1   1   1
	//  node2 1   1   1   1   1   1
	//  node3 1   1   1   1   1   1   1
	//  node4 1   1   1   1   1   1   1   1

	var count int
	for _, peer := range r.Progress {
		if peer.Next > r.RaftLog.CommittedIndex() {
			count++
		}
	}
	for _, peer := range r.Progress {
		if peer.Next > r.RaftLog.CommittedIndex() {
			count++
		}
	}
	if count > len(r.Progress)/2 {
		r.RaftLog.SetCommittedIndex()
	}
}

func (r *Raft) updatePreAppliedIndex(applied uint64) {
	commitUpdated := false
	for i := r.RaftLog.CommittedIndex(); i <= r.RaftLog.LastIndex(); i += 1 {
		matchCnt := 0
		for _, p := range r.Progress {
			if p.Match >= i {
				matchCnt += 1
			}
		}

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
