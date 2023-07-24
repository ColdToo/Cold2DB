package raft

import (
	"errors"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
)

type Role uint8

const None uint64 = 0

const (
	Leader Role = iota
	Follower
	Candidate
)

type RaftConfig struct {
	ElectionTick  int
	HeartbeatTick int
}

type Opts struct {
	// local raft id
	ID uint64

	peers []uint64 //peers ip

	Storage Storage

	Applied uint64

	ElectionTick int

	HeartbeatTick int
}

//raft配置文件前置检查
func (c *Opts) validate() error {
	if c.ID == 0 {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

type Raft struct {
	id uint64

	Term uint64

	//记录所投票给的节点id
	VoteFor uint64

	RaftLog *RaftLog

	Progress    map[uint64]*Progress
	voteCount   int
	rejectCount int

	Role Role

	// votes records
	votes map[uint64]bool

	// 需要发送给其他节点的msgs
	msgs []pb.Message

	LeaderID uint64

	heartbeatTimeout int

	electionTimeout int

	heartbeatElapsed int

	electionElapsed int

	// leader要维护或者从集群中删除。
	// 有更适合当server的节点，比如说有更高的负载，更好的网络条件。
	leadTransferee uint64

	PendingConfIndex uint64

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

func NewRaft(c *Opts) (raft *Raft, err error) {
	err = c.validate()
	if err != nil {
		return
	}
	raft = new(Raft)
	raft.id = c.ID
	raft.electionTimeout = c.ElectionTick
	raft.heartbeatTimeout = c.HeartbeatTick
	return
}

// Step 该函数接收一个 Msg，然后根据节点的角色和 Msg 的类型调用不同的处理函数。
// raft层都是通过Step函数和Tick函数驱动的
func (r *Raft) Step(m *pb.Message) error {
	return r.stepFunc(r, m)
}
func (r *Raft) Tick() {
	r.tick()
}

type stepFunc func(r *Raft, m *pb.Message) error

func stepLeader(r *Raft, m *pb.Message) error {
	//pr := r.Progress[m.From]
	switch m.Type {
	// send msg
	case pb.MsgBeat:
		r.bcastHeartbeat()
		return nil
	case pb.MsgProp:
		if len(m.Entries) == 0 {
			log.Panic("%x stepped empty MsgProp").Record()
		}
		if r.Progress[r.id] == nil {
			return errors.New(code.ErrProposalDropped)
		}
		if r.leadTransferee != None {
			return errors.New(code.ErrProposalDropped)
		}
		r.handlePropMsg(m)

	case pb.MsgAppResp:
		// todo 如果append被拒绝那么应该有probe行为 这段代码后续补充
	case pb.MsgHeartbeatResp:
		r.handleHeartbeatResponse(*m)
	case pb.MsgSnapStatus:
		// todo 获取snap是否传输成功后续需要补充这段逻辑
	case pb.MsgUnreachable:
		// During optimistic replication, if the remote becomes unreachable,
		// there is huge probability that a MsgApp is lost.
	case pb.MsgTransferLeader:
		// 暂时不做leader转移
	}
	return nil
}

func stepFollower(r *Raft, m *pb.Message) error {
	switch m.Type {
	// 日志提议直接转发给leader节点
	case pb.MsgProp:
		if r.LeaderID == None {
			// todo zap应该格式化的就格式化，不应该所有的使用json方法格式
			// r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return errors.New(code.ErrProposalDropped)
		}
		m.To = r.LeaderID
		//r.send(m)
	case pb.MsgApp:
		r.electionElapsed = 0
		r.LeaderID = m.From
		r.handleAppendEntries(*m)
	case pb.MsgHeartbeat:
		r.electionElapsed = 0
		r.LeaderID = m.From
		r.handleHeartbeat(*m)
	case pb.MsgSnap:
		r.electionElapsed = 0
		r.LeaderID = m.From
		r.handleSnapshot(*m)
	}
	return nil
}

func stepCandidate(r *Raft, m *pb.Message) error {
	switch m.Type {
	case pb.MsgHup:
		r.campaign()
	case pb.MsgVoteResp:
		r.handleVoteResponse(*m)
	case pb.MsgVote:
		r.handleVoteRequest(*m)
	case pb.MsgApp:
		r.handleAppendEntries(*m)
	case pb.MsgSnap:
		r.handleSnapshot(*m)
	}
	return nil
}

// tickElection is run by followers and candidates after r.electionTimeout.
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

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
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

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.id = lead
	r.Role = Follower
	r.tick = r.tickHeartbeat
	r.stepFunc = stepFollower
	r.Term = term
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.LeaderID = 0
	r.Role = Candidate
	r.Term++
	r.stepFunc = stepCandidate
	r.tick = r.tickElection
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.Role = Leader
	r.LeaderID = r.id
	r.stepFunc = stepLeader
	r.tick = r.tickHeartbeat
}

// ------------------- leader behavior -------------------

func (r *Raft) handlePropMsg(m *pb.Message) (err error) {
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
	r.appendEntries(ents...)
	r.bcastAppendEntries()
	r.updateCommit()
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
	preLogIndex := r.Progress[to].Next - 1

	// 这种情况不用追加日志
	if lastIndex < preLogIndex {
		log.Info("do not need send app msg").Record()
		return errors.New("dont need  to send")
	}

	preLogTerm, err := r.RaftLog.getTermByEntryIndex(preLogIndex)
	if err != nil {
		//如果这条日志已经被compacted那么发送日志
		if err == ErrCompacted {
			r.sendSnapshot(to)
			return err
		}
		return err
	}

	entries, err := r.RaftLog.storage.Entries(preLogIndex+1, lastIndex+1)
	if err != nil {
		return err
	}

	sendEntries := make([]*pb.Entry, 0)
	for _, en := range entries {
		sendEntries = append(sendEntries, &pb.Entry{
			Type:  en.Type,
			Term:  en.Term,
			Index: en.Index,
			Data:  en.Data,
		})
	}

	msg := pb.Message{
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

func (r *Raft) sendHeartbeat(to uint64) {
	msg := pb.Message{
		Type: pb.MsgBeat,
		To:   to,
		From: r.id,
		Term: r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendSnapshot(to uint64) {
	snap, err := r.RaftLog.storage.GetSnapshot()
	if err != nil {
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		Type:     pb.MsgSnap,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: snap,
	})
	r.Progress[to].Next = snap.Metadata.Index + 1
}

// appendEntries append entry to raft log
func (r *Raft) appendEntries(entries ...*pb.Entry) {
	ents := make([]pb.Entry, 0)
	for _, e := range entries {
		// todo
		// 配置变更的提议是不是应该单独处理
		if e.Type == pb.EntryConfChange {
			if r.PendingConfIndex != None {
				continue
			}
			r.PendingConfIndex = e.Index
		}
		ents = append(ents, pb.Entry{
			Type:  e.Type,
			Term:  e.Term,
			Index: e.Index,
			Data:  e.Data,
		})
	}
	r.RaftLog.AppendEntries(ents)
	r.Progress[r.id].Match = r.RaftLog.LastIndex()
	r.Progress[r.id].Next = r.RaftLog.LastIndex() + 1
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if !m.Reject {
		r.Prs[m.From].Next = m.Index + 1
		r.Prs[m.From].Match = m.Index
	} else if r.Prs[m.From].Next > 0 {
		r.Prs[m.From].Next -= 1
		r.sendAppend(m.From)
		return
	}
	r.updateCommit()
	if m.From == r.leadTransferee && m.Index == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(m.From)
	}
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	// leader can send log to follower when
	// it received a heartbeat response which
	// indicate it doesn't have update-to-date log
	if r.isMoreUpToDateThan(m.LogTerm, m.Index) {
		r.sendAppend(m.From)
	}
}

// ------------------ candidate behavior ------------------

// campaign becomes a candidate and start to request vote
func (r *Raft) campaign() {
	r.becomeCandidate()
	r.bcastVoteRequest()
}

// bcastVoteRequest is used by candidate to send vote request
func (r *Raft) bcastVoteRequest() {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.getTermByEntryIndex(lastIndex)
	for peer := range r.Progress {
		if peer != r.id {
			msg := pb.Message{
				Type:    pb.MsgVote,
				From:    r.id,
				To:      peer,
				Term:    r.Term,
				LogTerm: lastTerm,
				Index:   lastIndex,
			}
			r.msgs = append(r.msgs, msg)
		}
	}
}

// handleVoteResponse handle vote response
func (r *Raft) handleVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, r.Lead)
		r.Vote = m.From
		return
	}
	if !m.Reject {
		r.votes[m.From] = true
		r.voteCount += 1
	} else {
		r.votes[m.From] = false
		r.denialCount += 1
	}
	if r.voteCount > len(r.Prs)/2 {
		r.becomeLeader()
	} else if r.denialCount > len(r.Prs)/2 {
		r.becomeFollower(r.Term, r.Lead)
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {

}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {

}

// special condition to abort leader transfer
func (r *Raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

// ------------------ follower behavior ------------------

// sendVoteResponse send vote response
func (r *Raft) sendVoteResponse(nvote uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      nvote,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// handleAppendEntries handle AppendEntries  request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true)
		return
	}

	term, err := r.RaftLog.getTermByEntryIndex(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendAppendResponse(m.From, true)
		return
	}

	if len(m.Entries) > 0 {
		appendStart := 0

		// check is has existing entry conflicts with a new one delete the existing entry and all that
		//	// follow it
		for i, ent := range m.Entries {
			if ent.Index > r.RaftLog.LastIndex() {
				appendStart = i
				break
			}
			validTerm, _ := r.RaftLog.getTermByEntryIndex(ent.Index)
			if validTerm != ent.Term {
				r.RaftLog.RemoveEntriesAfter(ent.Index)
				break
			}
			appendStart = i
		}

		if m.Entries[appendStart].Index > r.RaftLog.LastIndex() {
			for _, e := range m.Entries[appendStart:] {
				r.RaftLog.entries = append(r.RaftLog.entries, *e)
			}
		}
	}
	//  If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)
	// 更新commit
	if m.Commit > r.RaftLog.committed {
		lastNewEntry := m.Index
		if len(m.Entries) > 0 {
			lastNewEntry = m.Entries[len(m.Entries)-1].Index
		}
		r.RaftLog.committed = min(m.Commit, lastNewEntry)
	}

	r.sendAppendResponse(m.From, false)
}

// sendAppendResponse send append response
func (r *Raft) sendAppendResponse(to uint64, reject bool) {
	msg := pb.Message{
		Type:   pb.MsgAppResp,
		From:   r.id,
		To:     to,
		Term:   r.Term,
		Reject: reject,
		Index:  r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

// sendHeartbeatResponse send heartbeat response
func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// most logic is same as `AppendEntries`
	// Reply false if term < currentTerm (§5.1)
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	r.becomeFollower(m.Term, m.From)
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	r.sendHeartbeatResponse(m.From, false)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	meta := m.Snapshot.Metadata
	if meta.Index <= r.RaftLog.committed {
		r.sendAppendResponse(m.From, false)
		return
	}
	r.becomeFollower(max(r.Term, m.Term), m.From)
	// clear log
	r.RaftLog.entries = nil
	// install snapshot
	r.RaftLog.firstIndex = meta.Index + 1
	r.RaftLog.applied = meta.Index
	r.RaftLog.committed = meta.Index
	r.RaftLog.stabled = meta.Index
	r.RaftLog.pendingSnapshot = m.Snapshot
	// update conf
	r.Prs = make(map[uint64]*Progress)
	for _, p := range meta.ConfState.Nodes {
		r.Prs[p] = &Progress{}
	}
	r.sendAppendResponse(m.From, false)
}

// Only leader can recive vote handleVoteRequest handle vote request
func (r *Raft) handleVoteRequest(m pb.Message) {
	lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if r.Term > m.Term {
		r.sendVoteResponse(m.From, true)
		return
	}
	if r.isMoreUpToDateThan(m.LogTerm, m.Index) {
		if r.Term < m.Term {
			r.becomeFollower(m.Term, None)
		}
		r.sendVoteResponse(m.From, true)
		return
	}
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
		r.sendVoteResponse(m.From, false)
		return
	}
	if r.Vote == m.From {
		r.sendVoteResponse(m.From, false)
		return
	}
	if r.isFollower() &&
		r.Vote == None &&
		(lastTerm < m.LogTerm || (lastTerm == m.LogTerm && m.Index >= r.RaftLog.LastIndex())) {
		r.sendVoteResponse(m.From, false)
		return
	}
	r.resetRealElectionTimeout()
	r.sendVoteResponse(m.From, true)
}

// RemoveEntriesAfter remove entries from index lo to the last
func (l *RaftLog) RemoveEntriesAfter(lo uint64) {
	l.stabled = min(l.stabled, lo-1)
	if lo-l.firstIndex >= uint64(len(l.entries)) {
		return
	}
	l.entries = l.entries[:lo-l.firstIndex]
}

func (r *Raft) handleLeaderTransfer(m pb.Message) {
	if m.From == r.id {
		return
	}
	if r.leadTransferee == m.From {
		return
	}
	if _, ok := r.Prs[m.From]; !ok {
		return
	}
	r.leadTransferee = m.From
	if r.Prs[m.From].Match != r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	} else {
		r.sendTimeoutNow(m.From)
	}
}

//当Follower节点接收到MsgTimeoutNow消息时，会立刻切换成Candidate状态，然后创建MsgHup消息并发起选举。
func (r *Raft) sendTimeoutNow(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		Type: pb.MsgTimeoutNow,
		To:   to,
		From: r.id,
	})
}

// ------------------ public behavior ------------------
func (r *Raft) updateCommit() {
	commitUpdated := false
	for i := r.RaftLog.committed; i <= r.RaftLog.LastIndex(); i += 1 {
		if i <= r.RaftLog.committed {
			continue
		}
		matchCnt := 0
		for _, p := range r.Progress {
			if p.Match >= i {
				matchCnt += 1
			}
		}
		// leader only commit on it's current term (§5.4.2)
		term, _ := r.RaftLog.getTermByEntryIndex(i)
		if matchCnt > len(r.Progress)/2 && term == r.Term && r.RaftLog.committed != i {
			r.RaftLog.committed = i
			commitUpdated = true
		}
	}
	if commitUpdated {
		r.bcastAppendEntries()
	}
}
