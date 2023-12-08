package main

import (
	"errors"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/log"
	"time"
)

type KV = marshal.KV

type KvStore struct {
	storage    db.Storage
	proposeC   chan<- []byte
	monitorKV  map[uint64]chan struct{}
	ReqTimeout time.Duration
}

func NewKVStore(proposeC chan<- []byte, requestTimeOut int) *KvStore {
	storage, err := db.GetStorage()
	if err != nil {
		log.Panicf("get db failed %s", err.Error())
	}
	s := &KvStore{
		storage:    storage,
		proposeC:   proposeC,
		monitorKV:  make(map[uint64]chan struct{}),
		ReqTimeout: time.Duration(requestTimeOut) * time.Second,
	}
	return s
}

// todo 线性一致性读
func (s *KvStore) Lookup(key []byte) ([]byte, error) {
	return s.storage.Get(key)
}

func (s *KvStore) Scan(lowKey, highKey []byte) ([][]byte, error) {
	return nil, nil
}

func (s *KvStore) Propose(key, val []byte, delete bool, expiredAt int64) (bool, error) {
	timeOutC := time.NewTimer(s.ReqTimeout)
	uid := uint64(time.Now().UnixNano())
	kv := new(KV)
	kv.Key = key
	kv.Id = uid
	kv.Value = val
	kv.ExpiredAt = expiredAt
	if delete {
		kv.Type = valuefile.TypeDelete
	}
	buf, _ := valuefile.GobEncode(kv)
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
