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
	"reflect"
	"testing"
)

const ignore = 0

func InitLog() {
	cfg := &config.ZapConfig{
		Level:         "debug",
		Format:        "console",
		Prefix:        "[C2KV]",
		Director:      "./log",
		ShowLine:      true,
		EncodeLevel:   "LowercaseColorLevelEncoder",
		StacktraceKey: "stacktrace",
		LogInConsole:  true,
	}
	log.InitLog(cfg)
}

func MockSpecStorage(t *testing.T, fistIndex, lastIndex, expIdx, expTerm uint64) db.Storage {
	mockCtl := gomock.NewController(t)
	storage := mocks.NewMockStorage(mockCtl)
	storage.EXPECT().FirstIndex().Return(fistIndex).AnyTimes()
	storage.EXPECT().LastIndex().Return(lastIndex).AnyTimes()
	storage.EXPECT().Term(expIdx).Return(expTerm, nil).AnyTimes()
	return storage
}

func MockEntriesStorage(t *testing.T, fistIndex, lastIndex, from, to uint64, entries []pb.Entry) db.Storage {
	mockCtl := gomock.NewController(t)
	storage := mocks.NewMockStorage(mockCtl)
	storage.EXPECT().FirstIndex().Return(fistIndex).AnyTimes()
	storage.EXPECT().LastIndex().Return(lastIndex).AnyTimes()
	storage.EXPECT().Entries(from, to).Return(entries, nil).AnyTimes()
	return storage
}

func MockTruncateStorage(t *testing.T, fistIndex, lastIndex, expIdx, expTerm, truncateIndex uint64) db.Storage {
	mockCtl := gomock.NewController(t)
	storage := mocks.NewMockStorage(mockCtl)
	storage.EXPECT().Truncate(truncateIndex).Return(nil).AnyTimes()
	storage.EXPECT().FirstIndex().Return(fistIndex).AnyTimes()
	storage.EXPECT().LastIndex().Return(lastIndex).AnyTimes()
	storage.EXPECT().Term(expIdx).Return(expTerm, nil).AnyTimes()
	return storage
}

func MockEntries(from, to uint64) (ents []pb.Entry) {
	for i := from; i < to; i++ {
		ents = append(ents, pb.Entry{Index: i, Term: i})
	}
	return
}

func TestFirstIndex(t *testing.T) {
	InitLog()
	tests := []struct {
		raftLog *raftLog
		entries []pb.Entry
		want    uint64
	}{
		{
			raftLog: newRaftLog(MockSpecStorage(t, 1, 10, 0, 0)),
			entries: []pb.Entry{{Index: 11, Term: 1}, {Index: 12, Term: 1}, {Index: 13, Term: 1}},
			want:    1,
		},
		{
			raftLog: newRaftLog(MockSpecStorage(t, 0, 0, 0, 0)),
			entries: []pb.Entry{{Index: 11, Term: 1}, {Index: 12, Term: 1}, {Index: 13, Term: 1}},
			want:    11,
		},
	}

	for i, tt := range tests {
		u := tt.raftLog
		u.unstableEnts = tt.entries
		index := u.firstIndex()
		if index != tt.want {
			t.Errorf("#%d: index = %d, want %d", i, index, tt.want)
		}
	}
}

func TestLastIndex(t *testing.T) {
	InitLog()
	tests := []struct {
		raftLog *raftLog
		entries []pb.Entry
		want    uint64
	}{
		{
			raftLog: newRaftLog(MockSpecStorage(t, 1, 10, 0, 0)),
			entries: []pb.Entry{{Index: 11, Term: 1}, {Index: 12, Term: 1}, {Index: 13, Term: 1}},
			want:    13,
		},
		{
			raftLog: newRaftLog(MockSpecStorage(t, 1, 10, 0, 0)),
			entries: make([]pb.Entry, 0),
			want:    10,
		},
	}

	for i, tt := range tests {
		u := tt.raftLog
		u.unstableEnts = tt.entries
		index := u.lastIndex()
		if index != tt.want {
			t.Errorf("#%d: index = %d, want %d", i, index, tt.want)
		}
	}
}

func TestTerm(t *testing.T) {
	InitLog()
	tests := []struct {
		raftLog *raftLog
		entries []pb.Entry
		exptidx uint64
		want    uint64
	}{
		{
			raftLog: newRaftLog(MockSpecStorage(t, 1, 10, 8, 8)),
			entries: []pb.Entry{{Index: 11, Term: 11}, {Index: 12, Term: 12}, {Index: 13, Term: 13}},
			exptidx: 8,
			want:    8,
		},
		{
			raftLog: newRaftLog(MockSpecStorage(t, 1, 10, 0, 0)),
			entries: []pb.Entry{{Index: 11, Term: 11}, {Index: 12, Term: 12}, {Index: 13, Term: 13}},
			exptidx: 12,
			want:    12,
		},
	}

	for i, tt := range tests {
		u := tt.raftLog
		u.unstableEnts = tt.entries
		term, _ := u.term(tt.exptidx)
		if term != tt.want {
			t.Errorf("#%d: term = %d, want %d", i, term, tt.want)
		}
	}
}

func TestStableTo(t *testing.T) {
	tests := []struct {
		entries     []pb.Entry
		offset      uint64
		index, term uint64

		woffset uint64
		wlen    int
	}{
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5,
			5, 1, // stable to the first entry
			6, 0,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, 5,
			5, 1, // stable to the first entry
			6, 1,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5,
			4, 1, // stable to old entry
			5, 1,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5,
			4, 2, // stable to old entry
			5, 1,
		},
	}

	for i, tt := range tests {
		u := raftLog{
			unstableEnts: tt.entries,
			offset:       tt.offset,
		}
		u.stableTo(tt.index)
		if u.offset != tt.woffset {
			t.Errorf("#%d: offset = %d, want %d", i, u.offset, tt.woffset)
		}
		if len(u.unstableEnts) != tt.wlen {
			t.Errorf("#%d: len = %d, want %d", i, len(u.unstableEnts), tt.wlen)
		}
	}
}

func TestSlice(t *testing.T) {
	var i uint64
	InitLog()
	firstIndex := uint64(1)
	offset := uint64(100)
	num := uint64(100)

	tests := []struct {
		name    string
		raftLog *raftLog
		from    uint64
		to      uint64

		w      []pb.Entry
		wpanic bool
	}{
		// only storage
		{"only storage",
			newRaftLog(MockEntriesStorage(t, firstIndex, offset-1, 50, offset, MockEntries(50, offset))),
			50, offset, MockEntries(50, offset), false},

		// cross storage and raft log
		{"cross storage and raft log",
			newRaftLog(MockEntriesStorage(t, firstIndex, offset-1, 60, offset, MockEntries(60, offset))),
			60, 120, MockEntries(60, 120), false},

		// only raft log
		{"only raft log",
			newRaftLog(MockSpecStorage(t, firstIndex, offset-1, ignore, ignore)),
			offset, 110, MockEntries(offset, 110), false},
		{"only raft log",
			newRaftLog(MockSpecStorage(t, firstIndex, offset-1, ignore, ignore)),
			110, 130, MockEntries(110, 130), false},

		// err compacted
		{"err compacted",
			newRaftLog(MockSpecStorage(t, firstIndex, offset-1, ignore, ignore)),
			0, 100, nil, false},

		// panic out bounds
		{"panic out bounds",
			newRaftLog(MockSpecStorage(t, firstIndex, offset-1, ignore, ignore)),
			1, 200, nil, true},
	}

	for j, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			func() {
				defer func() {
					if r := recover(); r != nil {
						if !tt.wpanic {
							t.Errorf("%d: panic = %v, want %v: %v", j, true, false, r)
						}
					}
				}()

				for i = 0; i < num/2; i++ {
					tt.raftLog.truncateAndAppend([]pb.Entry{{Index: offset + i, Term: offset + i}})
				}
				g, err := tt.raftLog.slice(tt.from, tt.to)
				if tt.from < firstIndex && err != ErrCompacted {
					t.Fatalf("#%d: err = %v, want %v", j, err, ErrCompacted)
				}
				if !reflect.DeepEqual(g, tt.w) {
					t.Errorf("#%d: from %d to %d = %v, want %v", j, tt.from, tt.to, g, tt.w)
				}
			}()
		})
	}
}

func TestTruncateAndAppend(t *testing.T) {
	InitLog()
	tests := []struct {
		raftLog *raftLog
		entries []pb.Entry

		toappend         []pb.Entry
		woffset          uint64
		wunstableentries []pb.Entry
	}{
		// append to the end
		{
			newRaftLog(MockSpecStorage(t, 0, 0, ignore, ignore)),
			[]pb.Entry{},
			[]pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}},
			1, []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}},
		},
		{
			newRaftLog(MockSpecStorage(t, 1, 4, ignore, ignore)),
			[]pb.Entry{{Index: 5, Term: 1}},
			[]pb.Entry{{Index: 6, Term: 1}, {Index: 7, Term: 1}},
			5, []pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}},
		},
		// truncate the stable entries  and replace the unstable entries
		{
			newRaftLog(MockTruncateStorage(t, 1, 4, ignore, ignore, 4)),
			[]pb.Entry{{Index: 5, Term: 1}},
			[]pb.Entry{{Index: 4, Term: 2}, {Index: 5, Term: 2}, {Index: 6, Term: 2}},
			4, []pb.Entry{{Index: 4, Term: 2}, {Index: 5, Term: 2}, {Index: 6, Term: 2}},
		},
		// truncate the unstable entries
		{
			newRaftLog(MockSpecStorage(t, 1, 4, ignore, ignore)),
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}},
			[]pb.Entry{{Index: 6, Term: 2}},
			5, []pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 2}},
		},
		{
			newRaftLog(MockSpecStorage(t, 1, 4, ignore, ignore)),
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}},
			[]pb.Entry{{Index: 7, Term: 2}, {Index: 8, Term: 2}},
			5, []pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 2}, {Index: 8, Term: 2}},
		},
	}

	for i, tt := range tests {
		tt.raftLog.committed = 0
		tt.raftLog.unstableEnts = tt.entries
		tt.raftLog.truncateAndAppend(tt.toappend)
		if tt.raftLog.offset != tt.woffset {
			t.Errorf("#%d: offset = %d, want %d", i, tt.raftLog.offset, tt.woffset)
		}
		if !reflect.DeepEqual(tt.raftLog.unstableEnts, tt.wunstableentries) {
			t.Errorf("#%d: entries = %v, want %v", i, tt.raftLog.unstableEnts, tt.wunstableentries)
		}
	}
}

// TestLogMaybeAppend ensures:
// If the given (index, term) matches with the existing log:
//  1. If an existing entry conflicts with a new one (same index
//     but different terms), delete the existing entry and all that
//     follow it
//  2. Append any new entries not already in the log
//
// If the given (index, term) does not match with the existing log:
//
//	return false
func TestLogMaybeAppend(t *testing.T) {
	previousEnts := []pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}
	lastindex := uint64(3)
	lastterm := uint64(3)
	commit := uint64(1)

	tests := []struct {
		logTerm   uint64
		index     uint64
		committed uint64
		ents      []pb.Entry

		wlasti  uint64
		wappend bool
		wcommit uint64
		wpanic  bool
	}{
		// not match: term is different
		{
			lastterm - 1, lastindex, lastindex, []pb.Entry{{Index: lastindex + 1, Term: 4}},
			0, false, commit, false,
		},
		// not match: index out of bound
		{
			lastterm, lastindex + 1, lastindex, []pb.Entry{{Index: lastindex + 2, Term: 4}},
			0, false, commit, false,
		},
		// match with the last existing entry
		{
			lastterm, lastindex, lastindex, nil,
			lastindex, true, lastindex, false,
		},
	}

	for i, tt := range tests {
		raftLog := newRaftLog(MockSpecStorage(t, 1, 4, ignore, ignore))
		raftLog.truncateAndAppend(previousEnts)
		raftLog.committed = commit
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v", i, true, tt.wpanic)
					}
				}
			}()
			glasti, gappend := raftLog.maybeAppend(tt.index, tt.logTerm, tt.committed, tt.ents...)
			gcommit := raftLog.committed

			if glasti != tt.wlasti {
				t.Errorf("#%d: lastindex = %d, want %d", i, glasti, tt.wlasti)
			}
			if gappend != tt.wappend {
				t.Errorf("#%d: append = %v, want %v", i, gappend, tt.wappend)
			}
			if gcommit != tt.wcommit {
				t.Errorf("#%d: committed = %d, want %d", i, gcommit, tt.wcommit)
			}
			if gappend && len(tt.ents) != 0 {
				gents, err := raftLog.slice(raftLog.lastIndex()-uint64(len(tt.ents))+1, raftLog.lastIndex()+1)
				if err != nil {
					t.Fatalf("unexpected error %v", err)
				}
				if !reflect.DeepEqual(tt.ents, gents) {
					t.Errorf("#%d: appended entries = %v, want %v", i, gents, tt.ents)
				}
			}
		}()
	}
}
