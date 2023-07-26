package main

import (
	"bytes"
	"encoding/gob"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/db/logfile"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"time"
)

type entry logfile.WalEntry

type KV struct {
	id        int64
	Key       []byte
	Value     []byte
	Type      logfile.EntryType
	ExpiredAt int64
}

type KvStore struct {
	db       db.DB
	proposeC chan<- bytes.Buffer // channel for proposing updates
	monitorC []chan int64        // channel for monitor key is commit
}

func NewKVStore(proposeC chan<- bytes.Buffer, commitC <-chan []pb.Entry, errorC <-chan error) *KvStore {
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
		id:        time.Now().UnixNano(),
		Key:       key,
		Value:     val,
		ExpiredAt: expiredAt,
		Type:      logfile.TypeDelete,
	}
	if delete {
		kv.Type = 0
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv); err != nil {
		return false, err
	}
	// todo 如何监听该key
	s.proposeC <- buf
	return false, nil
}

//  持久化kv对到db中
func (s *KvStore) serveCommitC(commitC <-chan []pb.Entry, errorC <-chan error) {
	for entries := range commitC {
		err := s.db.Put(entries)
		//唤醒部分
		if err != nil {
			return
		}
	}

	if _, ok := <-errorC; ok {
	}
}
