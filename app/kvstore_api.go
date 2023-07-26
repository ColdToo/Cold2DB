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
	// 线性一致性读
	val, err := s.db.Get(key)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (s *KvStore) Propose(key, val []byte, delete bool, expiredAt int64) (bool, error) {
	uid := time.Now().UnixNano()
	kv := &KV{
		id:        uid,
		Key:       key,
		Value:     val,
		ExpiredAt: expiredAt,
	}
	if delete {
		kv.Type = logfile.TypeDelete
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv); err != nil {
		return false, err
	}
	// 开始监听uid
	s.proposeC <- buf
	return false, nil
}

func (s *KvStore) serveCommitC(commitC <-chan []pb.Entry, errorC <-chan error) {
	var buf *bytes.Buffer
	var kv KV
	for entries := range commitC {
		for _, entry := range entries {
			err := gob.NewDecoder(buf).Decode(&kv)
			if err != nil {
				log.Errorf("decode err:", err)
				continue
			}
			walEntry := logfile.WalEntry{
				Index:     entry.Index,
				Term:      entry.Term,
				Key:       kv.Key,
				Value:     kv.Value,
				ExpiredAt: kv.ExpiredAt,
				Type:      kv.Type,
			}
			err = s.db.Put(walEntry)
			if err != nil {
				return
			}
			//唤醒应用层返回客户端ok
		}
	}

	if err, ok := <-errorC; ok {
		log.Errorf("found err exit serveCommitC:", err)
		return
	}
}
