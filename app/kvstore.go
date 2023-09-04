package main

import (
	"errors"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/db/logfile"
	"github.com/ColdToo/Cold2DB/log"
	"time"
)

type KV = logfile.KV

type KvStore struct {
	db         db.DB
	proposeC   chan<- []byte
	monitorKV  map[uint64]chan struct{}
	ReqTimeout time.Duration
}

func NewKVStore(proposeC chan<- []byte, requestTimeOut int) *KvStore {
	cold2DB, err := db.GetDB()
	if err != nil {
		log.Panicf("get db failed %s", err.Error())
	}
	s := &KvStore{
		db:         cold2DB,
		proposeC:   proposeC,
		monitorKV:  make(map[uint64]chan struct{}),
		ReqTimeout: time.Duration(requestTimeOut) * time.Second,
	}
	return s
}

func (s *KvStore) Lookup(key []byte) ([]byte, error) {
	return s.db.Get(key)
}

func (s *KvStore) Propose(key, val []byte, delete bool, expiredAt int64) (bool, error) {
	timeOutC := time.NewTimer(s.ReqTimeout)
	uid := uint64(time.Now().UnixNano())
	kv := &KV{
		Id:        uid,
		Key:       key,
		Value:     val,
		ExpiredAt: expiredAt,
	}
	if delete {
		kv.Type = logfile.TypeDelete
	}
	buf, _ := logfile.GobEncode(kv)
	s.proposeC <- buf

	sig := make(chan struct{})
	//监听该kv，当该kv被applied时返回客户端
	s.monitorKV[uid] = sig

	select {
	case <-sig:
		return true, nil
	case <-timeOutC.C:
		return false, errors.New("request time out")
	}
}

func (s *KvStore) BatchPropose() {
	return
}

func (s *KvStore) Scan(lowKey, highKey []byte) ([][]byte, error) {
	return nil, nil
}
