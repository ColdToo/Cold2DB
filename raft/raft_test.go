package raft

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/db/mocks"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/golang/mock/gomock"
	"reflect"
	"sort"
	"testing"
)

type messageSlice []pb.Message

func (s messageSlice) Len() int           { return len(s) }
func (s messageSlice) Less(i, j int) bool { return fmt.Sprint(s[i]) < fmt.Sprint(s[j]) }
func (s messageSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func newTestRaftOpts(id uint64, peers []uint64, election, heartbeat int, storage db.Storage) *raftOpts {
	return &raftOpts{
		Id:               id,
		peers:            peers,
		electionTimeout:  election,
		heartbeatTimeout: heartbeat,
		storage:          storage,
	}
}

func newTestRaft(id uint64, peers []uint64, election, heartbeat int, storage db.Storage) *raft {
	r, err := newRaft(newTestRaftOpts(id, peers, election, heartbeat, storage))
	if err != nil {
		return nil
	}
	return r
}

func MockNewStorage(t *testing.T, fistIndex, lastIndex, expIdx, expTerm uint64, hs pb.HardState, cs pb.ConfState) db.Storage {
	mockCtl := gomock.NewController(t)
	storage := mocks.NewMockStorage(mockCtl)
	storage.EXPECT().FirstIndex().Return(fistIndex).AnyTimes()
	storage.EXPECT().LastIndex().Return(lastIndex).AnyTimes()
	storage.EXPECT().Term(expIdx).Return(expTerm, nil).AnyTimes()
	storage.EXPECT().InitialState().Return(hs, cs, nil)
	return storage
}

// TestStartAsFollower tests that when servers start up, they begin as followers.
// Reference: section 5.2
func TestStartAsFollower2AA(t *testing.T) {
	InitLog()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockNewStorage(t, 0, 0, ignore, ignore, pb.HardState{}, pb.ConfState{}))
	if r.state != StateFollower {
		t.Errorf("state = %s, want %s", r.state, StateFollower)
	}
}

func TestFollowerUpdateTermFromMessage2AA(t *testing.T) {
	InitLog()
	testUpdateTermFromMessage(t, StateFollower)
}
func TestCandidateUpdateTermFromMessage2AA(t *testing.T) {
	InitLog()
	testUpdateTermFromMessage(t, StateCandidate)
}

// testUpdateTermFromMessage tests that if one server’s current term is
// smaller than the other’s, then it updates its current term to the larger
// value. If a candidate or leader discovers that its term is out of date,
// it immediately reverts to follower state.
// Reference: section 5.1
func testUpdateTermFromMessage(t *testing.T, state StateType) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, MockNewStorage(t, 0, 0, ignore, ignore, pb.HardState{}, pb.ConfState{}))
	switch state {
	case StateFollower:
		r.becomeFollower(1, 2)
	case StateCandidate:
		r.becomeCandidate()
	}
	r.Step(pb.Message{Type: pb.MsgApp, Term: 2})

	if r.Term != 2 {
		t.Errorf("term = %d, want %d", r.Term, 2)
	}
	if r.state != StateFollower {
		t.Errorf("state = %v, want %v", r.state, StateFollower)
	}
}

// TestLeaderBcastBeat tests that if the leader receives a heartbeat tick,
// it will send a MessageType_MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries
// as heartbeat to all followers.
// Reference: section 5.2
func TestLeaderBcastBeat2AA(t *testing.T) {
	InitLog()
	// heartbeat interval
	hi := 1
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, hi, MockNewStorage(t, 0, 0, ignore, ignore, pb.HardState{}, pb.ConfState{}))
	r.becomeCandidate()
	r.becomeLeader()
	r.readMessages() // clear message
	r.tick()
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	wmsgs := []pb.Message{
		{From: 1, To: 2, Term: 1, Type: pb.MsgHeartbeat},
		{From: 1, To: 3, Term: 1, Type: pb.MsgHeartbeat},
	}
	if !reflect.DeepEqual(msgs, wmsgs) {
		t.Errorf("msgs = %v, want %v", msgs, wmsgs)
	}
}

func (r *raft) readMessages() []pb.Message {
	msgs := r.msgs
	r.msgs = make([]pb.Message, 0)
	return msgs
}
