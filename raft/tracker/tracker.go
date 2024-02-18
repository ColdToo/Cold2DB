// Copyright 2019 The etcd Authors
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

package tracker

import (
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/raft/quorum"
	"sort"
)

// ProgressTracker tracks the currently active configuration and the information
// known about the nodes and learners in it. In particular, it tracks the match
// index for each peer which in turn allows reasoning about the committed index.
type ProgressTracker struct {
	Voters quorum.MajorityConfig

	Progress ProgressMap

	Votes map[uint64]bool

	MaxInflight int
}

// MakeProgressTracker initializes a ProgressTracker.
func MakeProgressTracker(maxInflight int) ProgressTracker {
	p := ProgressTracker{
		MaxInflight: maxInflight,
		Votes:       map[uint64]bool{},
		Progress:    map[uint64]*Progress{},
	}
	return p
}

// ConfState returns a ConfState representing the active configuration.
func (p *ProgressTracker) ConfState() pb.ConfState {
	return pb.ConfState{
		Voters: p.Voters.Slice(),
	}
}

// IsSingleton returns true if (and only if) there is only one voting member
// (i.e. the leader) in the current configuration.
func (p *ProgressTracker) IsSingleton() bool {
	return len(p.Voters) == 1
}

type matchAckIndexer map[uint64]*Progress

var _ quorum.AckedIndexer = matchAckIndexer(nil)

// AckedIndex implements IndexLookuper.
func (l matchAckIndexer) AckedIndex(id uint64) (quorum.Index, bool) {
	pr, ok := l[id]
	if !ok {
		return 0, false
	}
	return quorum.Index(pr.Match), true
}

// Committed returns the largest log index known to be committed based on what
// the voting members of the group have acknowledged.
func (p *ProgressTracker) Committed() uint64 {
	return uint64(p.Voters.CommittedIndex(matchAckIndexer(p.Progress)))
}

func insertionSort(sl []uint64) {
	a, b := 0, len(sl)
	for i := a + 1; i < b; i++ {
		for j := i; j > a && sl[j] < sl[j-1]; j-- {
			sl[j], sl[j-1] = sl[j-1], sl[j]
		}
	}
}

// Visit invokes the supplied closure for all tracked progresses in stable order.
func (p *ProgressTracker) Visit(f func(id uint64, pr *Progress)) {
	n := len(p.Progress)
	// We need to sort the IDs and don't want to allocate since this is hot code.
	// The optimization here mirrors that in `(MajorityConfig).CommittedIndex`,
	// see there for details.
	var sl [7]uint64
	ids := sl[:]
	if len(sl) >= n {
		ids = sl[:n]
	} else {
		ids = make([]uint64, n)
	}
	for id := range p.Progress {
		n--
		ids[n] = id
	}
	insertionSort(ids)
	for _, id := range ids {
		f(id, p.Progress[id])
	}
}

// QuorumActive returns true if the quorum is active from the view of the local
// raft state machine. Otherwise, it returns false.
func (p *ProgressTracker) QuorumActive() bool {
	votes := map[uint64]bool{}
	p.Visit(func(id uint64, pr *Progress) {
		votes[id] = pr.RecentActive
	})

	return p.Voters.VoteResult(votes) == quorum.VoteWon
}

// VoterNodes returns a sorted slice of voters.
func (p *ProgressTracker) VoterNodes() []uint64 {
	m := p.Voters.IDs()
	nodes := make([]uint64, 0, len(m))
	for id := range m {
		nodes = append(nodes, id)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	return nodes
}

// ResetVotes prepares for a new round of vote counting via recordVote.
func (p *ProgressTracker) ResetVotes() {
	p.Votes = map[uint64]bool{}
}

// RecordVote records that the node with the given id voted for this Raft
func (p *ProgressTracker) RecordVote(id uint64, v bool) {
	_, ok := p.Votes[id]
	if !ok {
		p.Votes[id] = v
	}
}

// TallyVotes returns the number of granted and rejected Votes, and whether the election outcome is known.
func (p *ProgressTracker) TallyVotes() (granted int, rejected int, _ quorum.VoteResult) {
	for id, _ := range p.Progress {
		v, voted := p.Votes[id]
		if !voted {
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}
	result := p.Voters.VoteResult(p.Votes)
	return granted, rejected, result
}
