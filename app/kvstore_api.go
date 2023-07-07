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

package main

import (
	"bytes"
	"encoding/gob"
	"github.com/ColdToo/Cold2DB/db"
	"log"
	"sync"
)

// a key-value store backed by raft
type kvstore struct {
	db       db.DB
	proposeC chan<- bytes.Buffer // channel for proposing updates
	mu       sync.RWMutex
}

type kv struct {
	Key []byte
	Val []byte
}

func NewKVStore(proposeC chan<- bytes.Buffer, commitC <-chan *commit, errorC <-chan error) *kvstore {
	//初始化db
	db.InitDB()
	s := &kvstore{proposeC: proposeC}
	go s.serveCommitC(commitC, errorC)
	return s
}

func (s *kvstore) Lookup(key []byte) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	get, err := s.db.Get(key)
	if err != nil {
		return nil, false
	}
	return get, true
}

// Propose 提议kv对交给raft算法层处理
func (s *kvstore) Propose(kv kv) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- buf
}

//  持久化kv对到db中
func (s *kvstore) serveCommitC(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		for _, data := range commit.kv {
			//var dataKv kv
			//dec := gob.NewDecoder(bytes.NewBufferString(data))
			//if err := dec.Decode(&dataKv); err != nil {
			//log.Fatalf("raftexample: could not decode message (%v)", err)
			//}
			s.db.Put(data.Key, data.Val)
		}
	}

	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
