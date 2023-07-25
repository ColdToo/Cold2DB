package main

import (
	"bytes"
	"encoding/gob"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/domain"
	"github.com/ColdToo/Cold2DB/log"
)

type KV domain.KV

type KvStore struct {
	db       db.DB
	proposeC chan<- bytes.Buffer // channel for proposing updates
}

func NewKVStore(proposeC chan<- bytes.Buffer, commitC <-chan *commit, errorC <-chan error) *KvStore {
	cold2DB, err := db.GetDB()
	if err != nil {
		log.Panicf("get db failed", err)
	}
	s := &KvStore{db: cold2DB, proposeC: proposeC}
	go s.serveCommitC(commitC, errorC)
	return s
}

func (s *KvStore) Lookup(key []byte) ([]byte, error) {
	val, err := s.db.Get(key)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// Propose 提议kv对交给raft算法层处理
func (s *KvStore) Propose(key, val []byte, delete bool, expiredAt int64) (bool, error) {
	//针对put请求和delete请求分别做对应处理
	kv := &KV{
		Key:       key,
		Val:       val,
		ExpiredAt: expiredAt,
		Type:      1,
	}
	if delete {
		kv.Type = 0
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv); err != nil {
		return false, err
	}
	// todo 监听该key
	s.proposeC <- buf
	return false, nil
}

//  持久化kv对到db中
func (s *KvStore) serveCommitC(commitC <-chan *bytes.Buffer, errorC <-chan error) {
	for commit := range commitC {
		for _, data := range commit.kv {
			s.db.Put(data.Key, data.Val)
		}
	}
	// inform

	if _, ok := <-errorC; ok {
	}
}
