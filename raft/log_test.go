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
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/db/mocks"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"reflect"
	"testing"
)

func initLog() {
	cfg := &config.ZapConfig{
		Level:         "debug",
		Format:        "console",
		Prefix:        "[Cold2DB]",
		Director:      "./log",
		ShowLine:      true,
		EncodeLevel:   "LowercaseColorLevelEncoder",
		StacktraceKey: "stacktrace",
		LogInConsole:  true,
	}
	log.InitLog(cfg)
}

// todo
func TestFindConflict(t *testing.T) {
	initLog()
	previousEnts := []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}
	tests := []struct {
		ents      []pb.Entry
		wconflict uint64
	}{
		// no conflict, empty ent
		{[]pb.Entry{}, 0},
		// no conflict
		{[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}, 0},
		{[]pb.Entry{{Index: 2, Term: 2}, {Index: 3, Term: 3}}, 0},
		{[]pb.Entry{{Index: 3, Term: 3}}, 0},
		// no conflict, but has new entries
		{[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		{[]pb.Entry{{Index: 2, Term: 2}, {Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		{[]pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		{[]pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		// conflicts with existing entries
		{[]pb.Entry{{Index: 1, Term: 4}, {Index: 2, Term: 4}}, 1},
		{[]pb.Entry{{Index: 2, Term: 1}, {Index: 3, Term: 4}, {Index: 4, Term: 4}}, 2},
		{[]pb.Entry{{Index: 3, Term: 1}, {Index: 4, Term: 2}, {Index: 5, Term: 4}, {Index: 6, Term: 4}}, 3},
	}

	for i, tt := range tests {
		mockCtl := gomock.NewController(t)
		storage := mocks.NewMockStorage(mockCtl)
		storage.EXPECT().FirstIndex().Return(uint64(1))
		storage.EXPECT().LastIndex().Return(uint64(3))
		storage.EXPECT().Truncate(mock.AnythingOfType("uint64")).Return(nil)
		raftLog := newRaftLog(storage, noLimit)
		raftLog.append(previousEnts...)

		gconflict := raftLog.findConflict(tt.ents)
		if gconflict != tt.wconflict {
			t.Errorf("#%d: conflict = %d, want %d", i, gconflict, tt.wconflict)
		}
		mockCtl.Finish()
	}
}

func TestUnstableTruncateAndAppend(t *testing.T) {
	initLog()
	tests := []struct {
		entries  []pb.Entry
		offset   uint64
		snap     *pb.Snapshot
		toappend []pb.Entry

		woffset          uint64
		wunstableentries []pb.Entry
		willTruncStable  bool
	}{
		// append to the end
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, nil,
			[]pb.Entry{{Index: 6, Term: 1}, {Index: 7, Term: 1}},
			5, []pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}}, false,
		},
		// truncate the stable entries  and replace the unstable entries
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, nil,
			[]pb.Entry{{Index: 4, Term: 2}, {Index: 5, Term: 2}, {Index: 6, Term: 2}},
			4, []pb.Entry{{Index: 4, Term: 2}, {Index: 5, Term: 2}, {Index: 6, Term: 2}}, true,
		},
		// truncate the unstable entries
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}}, 5, nil,
			[]pb.Entry{{Index: 6, Term: 2}},
			5, []pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 2}}, false,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}}, 5, nil,
			[]pb.Entry{{Index: 7, Term: 2}, {Index: 8, Term: 2}},
			5, []pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 2}, {Index: 8, Term: 2}}, false,
		},
	}

	mockCtl := gomock.NewController(t)
	storage := mocks.NewMockStorage(mockCtl)
	for i, tt := range tests {
		if tt.willTruncStable {
			storage.EXPECT().Truncate(tt.toappend[0].Index).Return(nil).AnyTimes()
		}
		u := raftLog{
			unstableEnts: tt.entries,
			offset:       tt.offset,
			storage:      storage,
		}
		u.truncateAndAppend(tt.toappend)
		if u.offset != tt.woffset {
			t.Errorf("#%d: offset = %d, want %d", i, u.offset, tt.woffset)
		}
		if !reflect.DeepEqual(u.unstableEnts, tt.wunstableentries) {
			t.Errorf("#%d: entries = %v, want %v", i, u.unstableEnts, tt.wunstableentries)
		}
	}
}

// todo
func TestTerm(t *testing.T) {
	initLog()
	num := uint64(100)
	mockCtl := gomock.NewController(t)
	storage := mocks.NewMockStorage(mockCtl)
	u := raftLog{
		stabled: num,
		offset:  num + 1,
		storage: storage,
	}

	var i uint64
	for i = 1; i < num; i++ {
		u.append(pb.Entry{Index: num + i, Term: i})
	}

	tests := []struct {
		index         uint64
		w             uint64
		stableStorage bool
	}{
		//访问稳定存储
		{1, 0, true},
		{u.stabled, 999, true},
		//unstable entries
		{u.offset, 1, false},
		{u.offset + num/2, num/2 + 1, false},
		{u.offset + num - 2, num, false},
		//invalid index
		{u.offset + num - 1, 0, false},
	}

	storage.EXPECT().FirstIndex().Return(uint64(1)).AnyTimes()
	storage.EXPECT().LastIndex().Return(num + num).AnyTimes()
	for j, tt := range tests {
		if tt.stableStorage {
			switch tt.index {
			case 1:
				storage.EXPECT().Term(uint64(1)).Return(uint64(0), ErrCompacted).AnyTimes()
				if _, err := u.term(tt.index); err != ErrCompacted {
					t.Log("can not get compacted index")
				}
				continue
			case u.stabled:
				storage.EXPECT().Term(u.stabled).Return(uint64(999), nil).AnyTimes()
			}
		}
		term, _ := u.term(tt.index)
		if term != tt.w {
			t.Errorf("#%d: at = %d, want %d", j, term, tt.w)
		}
	}
}

func TestSlice(t *testing.T) {
	initLog()
	var i uint64
	offset := uint64(100)
	num := uint64(100)
	last := offset + num
	half := offset + num/2
	halfe := pb.Entry{Index: half, Term: half}

	persistEnts := make([]pb.Entry, 0)
	for i = 1; i < num/2; i++ {
		persistEnts = append(persistEnts, pb.Entry{Term: offset + i, Index: offset + i})
	}
	storage := &db.C2KV{ent}
	l := newRaftLog(storage, noLimit)
	for i = num / 2; i < num; i++ {
		l.append(pb.Entry{Index: offset + i, Term: offset + i})
	}

	tests := []struct {
		from  uint64
		to    uint64
		limit uint64

		w      []pb.Entry
		wpanic bool
	}{
		// test no limit
		{offset - 1, offset + 1, noLimit, nil, false},
		{offset, offset + 1, noLimit, nil, false},
		{half - 1, half + 1, noLimit, []pb.Entry{{Index: half - 1, Term: half - 1}, {Index: half, Term: half}}, false},
		{half, half + 1, noLimit, []pb.Entry{{Index: half, Term: half}}, false},
		{last - 1, last, noLimit, []pb.Entry{{Index: last - 1, Term: last - 1}}, false},
		{last, last + 1, noLimit, nil, true},

		// test limit
		{half - 1, half + 1, 0, []pb.Entry{{Index: half - 1, Term: half - 1}}, false},
		{half - 1, half + 1, uint64(halfe.Size() + 1), []pb.Entry{{Index: half - 1, Term: half - 1}}, false},
		{half - 2, half + 1, uint64(halfe.Size() + 1), []pb.Entry{{Index: half - 2, Term: half - 2}}, false},
		{half - 1, half + 1, uint64(halfe.Size() * 2), []pb.Entry{{Index: half - 1, Term: half - 1}, {Index: half, Term: half}}, false},
		{half - 1, half + 2, uint64(halfe.Size() * 3), []pb.Entry{{Index: half - 1, Term: half - 1}, {Index: half, Term: half}, {Index: half + 1, Term: half + 1}}, false},
		{half, half + 2, uint64(halfe.Size()), []pb.Entry{{Index: half, Term: half}}, false},
		{half, half + 2, uint64(halfe.Size() * 2), []pb.Entry{{Index: half, Term: half}, {Index: half + 1, Term: half + 1}}, false},
	}

	for j, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v: %v", j, true, false, r)
					}
				}
			}()
			g, err := l.slice(tt.from, tt.to, tt.limit)
			if tt.from <= offset && err != ErrCompacted {
				t.Fatalf("#%d: err = %v, want %v", j, err, ErrCompacted)
			}
			if tt.from > offset && err != nil {
				t.Fatalf("#%d: unexpected error %v", j, err)
			}
			if !reflect.DeepEqual(g, tt.w) {
				t.Errorf("#%d: from %d to %d = %v, want %v", j, tt.from, tt.to, g, tt.w)
			}
		}()
	}
}
