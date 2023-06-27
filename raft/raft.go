package raft

import (
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"go.etcd.io/etcd/raft/quorum"
	"go.etcd.io/etcd/raft/tracker"
)

type Role uint8

const None uint64 = 0

const (
	Leader Role = iota
	Follower
	Candidate
)

type Config struct {
	// local raft id
	ID uint64

	peers []uint64 //peers ip

	ElectionTick int

	HeartbeatTick int

	Storage Storage

	Applied uint64
}

func (c *Config) validate() error {
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

func NewRaft(c *Config) (raft *Raft, err error) {
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
func (r *Raft) Step(m *pb.Message) error {
	return r.stepFunc(r, m)
}

type stepFunc func(r *Raft, m *pb.Message) error

func stepLeader(r *Raft, m *pb.Message) error {
	pr := r.Progress[m.From]
	switch m.Type {
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
		pr.RecentActive = true

		if m.Reject {
			// RejectHint is the suggested next base entry for appending (i.e.
			// we try to append entry RejectHint+1 next), and LogTerm is the
			// term that the follower has at index RejectHint. Older versions
			// of this library did not populate LogTerm for rejections and it
			// is zero for followers with an empty log.
			//
			// Under normal circumstances, the leader's log is longer than the
			// follower's and the follower's log is a prefix of the leader's
			// (i.e. there is no divergent uncommitted suffix of the log on the
			// follower). In that case, the first probe reveals where the
			// follower's log ends (RejectHint=follower's last index) and the
			// subsequent probe succeeds.
			//
			// However, when networks are partitioned or systems overloaded,
			// large divergent log tails can occur. The naive attempt, probing
			// entry by entry in decreasing order, will be the product of the
			// length of the diverging tails and the network round-trip latency,
			// which can easily result in hours of time spent probing and can
			// even cause outright outages. The probes are thus optimized as
			// described below.
			r.logger.Debugf("%x received MsgAppResp(rejected, hint: (index %d, term %d)) from %x for index %d",
				r.id, m.RejectHint, m.LogTerm, m.From, m.Index)
			nextProbeIdx := m.RejectHint
			if m.LogTerm > 0 {
				// If the follower has an uncommitted log tail, we would end up
				// probing one by one until we hit the common prefix.
				//
				// For example, if the leader has:
				//
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 3 3 3 5 5 5 5 5
				//   term (F)   1 1 1 1 2 2
				//
				// Then, after sending an append anchored at (idx=9,term=5) we
				// would receive a RejectHint of 6 and LogTerm of 2. Without the
				// code below, we would try an append at index 6, which would
				// fail again.
				//
				// However, looking only at what the leader knows about its own
				// log and the rejection hint, it is clear that a probe at index
				// 6, 5, 4, 3, and 2 must fail as well:
				//
				// For all of these indexes, the leader's log term is larger than
				// the rejection's log term. If a probe at one of these indexes
				// succeeded, its log term at that index would match the leader's,
				// i.e. 3 or 5 in this example. But the follower already told the
				// leader that it is still at term 2 at index 9, and since the
				// log term only ever goes up (within a log), this is a contradiction.
				//
				// At index 1, however, the leader can draw no such conclusion,
				// as its term 1 is not larger than the term 2 from the
				// follower's rejection. We thus probe at 1, which will succeed
				// in this example. In general, with this approach we probe at
				// most once per term found in the leader's log.
				//
				// There is a similar mechanism on the follower (implemented in
				// handleAppendEntries via a call to findConflictByTerm) that is
				// useful if the follower has a large divergent uncommitted log
				// tail[1], as in this example:
				//
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 3 3 3 3 3 3 3 7
				//   term (F)   1 3 3 4 4 5 5 5 6
				//
				// Naively, the leader would probe at idx=9, receive a rejection
				// revealing the log term of 6 at the follower. Since the leader's
				// term at the previous index is already smaller than 6, the leader-
				// side optimization discussed above is ineffective. The leader thus
				// probes at index 8 and, naively, receives a rejection for the same
				// index and log term 5. Again, the leader optimization does not improve
				// over linear probing as term 5 is above the leader's term 3 for that
				// and many preceding indexes; the leader would have to probe linearly
				// until it would finally hit index 3, where the probe would succeed.
				//
				// Instead, we apply a similar optimization on the follower. When the
				// follower receives the probe at index 8 (log term 3), it concludes
				// that all of the leader's log preceding that index has log terms of
				// 3 or below. The largest index in the follower's log with a log term
				// of 3 or below is index 3. The follower will thus return a rejection
				// for index=3, log term=3 instead. The leader's next probe will then
				// succeed at that index.
				//
				// [1]: more precisely, if the log terms in the large uncommitted
				// tail on the follower are larger than the leader's. At first,
				// it may seem unintuitive that a follower could even have such
				// a large tail, but it can happen:
				//
				// 1. Leader appends (but does not commit) entries 2 and 3, crashes.
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 2 2     [crashes]
				//   term (F)   1
				//   term (F)   1
				//
				// 2. a follower becomes leader and appends entries at term 3.
				//              -----------------
				//   term (x)   1 2 2     [down]
				//   term (F)   1 3 3 3 3
				//   term (F)   1
				//
				// 3. term 3 leader goes down, term 2 leader returns as term 4
				//    leader. It commits the log & entries at term 4.
				//
				//              -----------------
				//   term (L)   1 2 2 2
				//   term (x)   1 3 3 3 3 [down]
				//   term (F)   1
				//              -----------------
				//   term (L)   1 2 2 2 4 4 4
				//   term (F)   1 3 3 3 3 [gets probed]
				//   term (F)   1 2 2 2 4 4 4
				//
				// 4. the leader will now probe the returning follower at index
				//    7, the rejection points it at the end of the follower's log
				//    which is at a higher log term than the actually committed
				//    log.
				nextProbeIdx = r.raftLog.findConflictByTerm(m.RejectHint, m.LogTerm)
			}
			if pr.MaybeDecrTo(m.Index, nextProbeIdx) {
				r.logger.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
				if pr.State == tracker.StateReplicate {
					pr.BecomeProbe()
				}
				r.sendAppend(m.From)
			}
		} else {
			oldPaused := pr.IsPaused()
			if pr.MaybeUpdate(m.Index) {
				switch {
				case pr.State == tracker.StateProbe:
					pr.BecomeReplicate()
				case pr.State == tracker.StateSnapshot && pr.Match >= pr.PendingSnapshot:
					// TODO(tbg): we should also enter this branch if a snapshot is
					// received that is below pr.PendingSnapshot but which makes it
					// possible to use the log again.
					r.logger.Debugf("%x recovered from needing snapshot, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
					// Transition back to replicating state via probing state
					// (which takes the snapshot into account). If we didn't
					// move to replicating state, that would only happen with
					// the next round of appends (but there may not be a next
					// round for a while, exposing an inconsistent RaftStatus).
					pr.BecomeProbe()
					pr.BecomeReplicate()
				case pr.State == tracker.StateReplicate:
					pr.Inflights.FreeLE(m.Index)
				}

				if r.maybeCommit() {
					// committed index has progressed for the term, so it is safe
					// to respond to pending read index requests
					releasePendingReadIndexMessages(r)
					r.bcastAppend()
				} else if oldPaused {
					// If we were paused before, this node may be missing the
					// latest commit index, so send it.
					r.sendAppend(m.From)
				}
				// We've updated flow control information above, which may
				// allow us to send multiple (size-limited) in-flight messages
				// at once (such as when transitioning from probe to
				// replicate, or when freeTo() covers multiple messages). If
				// we have more entries to send, send as many messages as we
				// can (without sending empty messages for the commit index)
				for r.maybeSendAppend(m.From, false) {
				}
				// Transfer leadership is in progress.
				if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() {
					r.logger.Infof("%x sent MsgTimeoutNow to %x after received MsgAppResp", r.id, m.From)
					r.sendTimeoutNow(m.From)
				}
			}
		}
	case pb.MsgHeartbeatResp:
		pr.RecentActive = true
		pr.ProbeSent = false

		// free one slot for the full inflights window to allow progress.
		if pr.State == tracker.StateReplicate && pr.Inflights.Full() {
			pr.Inflights.FreeFirstOne()
		}
		if pr.Match < r.raftLog.lastIndex() {
			r.sendAppend(m.From)
		}

		if r.readOnly.option != ReadOnlySafe || len(m.Context) == 0 {
			return nil
		}

		if r.prs.Voters.VoteResult(r.readOnly.recvAck(m.From, m.Context)) != quorum.VoteWon {
			return nil
		}

		rss := r.readOnly.advance(m)
		for _, rs := range rss {
			if resp := r.responseToReadIndexReq(rs.req, rs.index); resp.To != None {
				r.send(resp)
			}
		}
	case pb.MsgSnapStatus:
		if pr.State != tracker.StateSnapshot {
			return nil
		}
		// TODO(tbg): this code is very similar to the snapshot handling in
		// MsgAppResp above. In fact, the code there is more correct than the
		// code here and should likely be updated to match (or even better, the
		// logic pulled into a newly created Progress state machine handler).
		if !m.Reject {
			pr.BecomeProbe()
			r.logger.Debugf("%x snapshot succeeded, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		} else {
			// NB: the order here matters or we'll be probing erroneously from
			// the snapshot index, but the snapshot never applied.
			pr.PendingSnapshot = 0
			pr.BecomeProbe()
			r.logger.Debugf("%x snapshot failed, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		}
		// If snapshot finish, wait for the MsgAppResp from the remote node before sending
		// out the next MsgApp.
		// If snapshot failure, wait for a heartbeat interval before next try
		pr.ProbeSent = true
	case pb.MsgUnreachable:
		// During optimistic replication, if the remote becomes unreachable,
		// there is huge probability that a MsgApp is lost.
		if pr.State == tracker.StateReplicate {
			pr.BecomeProbe()
		}
		r.logger.Debugf("%x failed to send message to %x because it is unreachable [%s]", r.id, m.From, pr)
	case pb.MsgTransferLeader:
		if pr.IsLearner {
			r.logger.Debugf("%x is learner. Ignored transferring leadership", r.id)
			return nil
		}
		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			if lastLeadTransferee == leadTransferee {
				r.logger.Infof("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
					r.id, r.Term, leadTransferee, leadTransferee)
				return nil
			}
			r.abortLeaderTransfer()
			r.logger.Infof("%x [term %d] abort previous transferring leadership to %x", r.id, r.Term, lastLeadTransferee)
		}
		if leadTransferee == r.id {
			r.logger.Debugf("%x is already leader. Ignored transferring leadership to self", r.id)
			return nil
		}
		// Transfer leadership to third party.
		r.logger.Infof("%x [term %d] starts to transfer leadership to %x", r.id, r.Term, leadTransferee)
		// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
		r.electionElapsed = 0
		r.leadTransferee = leadTransferee
		if pr.Match == r.raftLog.lastIndex() {
			r.sendTimeoutNow(leadTransferee)
			r.logger.Infof("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, leadTransferee, leadTransferee)
		} else {
			r.sendAppend(leadTransferee)
		}
	}
	return nil
}

func stepCandidate(r *Raft, m *pb.Message) error {
	// Only handle vote responses corresponding to our candidacy (while in
	// StateCandidate, we may get stale MsgPreVoteResp messages in this term from
	// our pre-candidate state).
	var myVoteRespType pb.MessageType
	if r.state == StatePreCandidate {
		myVoteRespType = pb.MsgPreVoteResp
	} else {
		myVoteRespType = pb.MsgVoteResp
	}
	switch m.Type {
	case pb.MsgProp:
		r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case pb.MsgApp:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleSnapshot(m)
	case myVoteRespType:
		gr, rj, res := r.poll(m.From, m.Type, !m.Reject)
		r.logger.Infof("%x has received %d %s votes and %d vote rejections", r.id, gr, m.Type, rj)
		switch res {
		case quorum.VoteWon:
			if r.state == StatePreCandidate {
				r.campaign(campaignElection)
			} else {
				r.becomeLeader()
				r.bcastAppend()
			}
		case quorum.VoteLost:
			// pb.MsgPreVoteResp contains future term of pre-candidate
			// m.Term > r.Term; reuse r.Term
			r.becomeFollower(r.Term, None)
		}
	case pb.MsgTimeoutNow:
		r.logger.Debugf("%x [term %d state %v] ignored MsgTimeoutNow from %x", r.id, r.Term, r.state, m.From)
	}
	return nil
}

func stepFollower(r *Raft, m *pb.Message) error {
	switch m.Type {
	case pb.MsgProp:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return ErrProposalDropped
		} else if r.disableProposalForwarding {
			r.logger.Infof("%x not forwarding to leader %x at term %d; dropping proposal", r.id, r.lead, r.Term)
			return ErrProposalDropped
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgApp:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	case pb.MsgTransferLeader:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return nil
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgTimeoutNow:
		r.logger.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.From)
		// Leadership transfers never use pre-vote even if r.preVote is true; we
		// know we are not recovering from a partition so there is no need for the
		// extra round trip.
		r.hup(campaignTransfer)
	case pb.MsgReadIndex:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping index reading msg", r.id, r.Term)
			return nil
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgReadIndexResp:
		if len(m.Entries) != 1 {
			r.logger.Errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d", r.id, m.From, len(m.Entries))
			return nil
		}
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
	}
	return nil
}

func (r *Raft) Tick(){
	r.tick()
}

// tickElection is run by followers and candidates after r.electionTimeout.
func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		err := r.Step(&pb.Message{From: r.id, Type: pb.MsgHup})
		if err != nil {
			log.Error("msg").Err(code.TickErr,err).Record()
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
			log.Error("msg").Err(code.TickErr,err).Record()
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

// ------------------- leader methods -------------------

func (r *Raft) handlePropMsg(m *pb.Message)(err error){
	lastIndex := r.RaftLog.LastIndex()
	ents := make([]*pb.Entry, 0)
	for _, e := range m.Entries {
		ents = append(ents, &pb.Entry{
			Type:      e.Type,
			Term:      r.Term,
			Index:     lastIndex + 1,
			Data:      e.Data,
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
		Type:     pb.,
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
			Type: 	   e.Type,
			Term:      e.Term,
			Index:     e.Index,
			Data:      e.Data,
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










// ------------------ candidate methods ------------------

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

// updateCommit ...
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

func (r *Raft) sendTimeoutNow(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      to,
		From:    r.id,
	})
}

// ------------------ follower methods ------------------

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
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   r.RaftLog.LastIndex(),
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