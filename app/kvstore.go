package main

import (
	"errors"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"time"
)

type KvStore struct {
	storage    db.Storage
	proposeC   chan<- []byte
	monitorKV  map[int64]chan struct{}
	ReqTimeout time.Duration
}

func NewKVStore(proposeC chan<- []byte, requestTimeOut int, kvStorage *db.C2KV) *KvStore {
	s := &KvStore{
		storage:    kvStorage,
		proposeC:   proposeC,
		monitorKV:  make(map[int64]chan struct{}),
		ReqTimeout: time.Duration(requestTimeOut) * time.Second,
	}
	return s
}

func (s *KvStore) Propose(key, val []byte, delete bool, expiredAt int64) (bool, error) {
	timeOutC := time.NewTimer(s.ReqTimeout)
	// todo 重写一个获取全局递增的ID函数
	uid := time.Now().UnixNano()
	kv := new(marshal.KV)
	kv.Key = key
	kv.Data.Value = val
	kv.Data.TimeStamp = time.Now().Unix()
	kv.ApplySig = uid
	if delete {
		kv.Data.Type = marshal.TypeDelete
	}
	buf := marshal.EncodeKV(kv)
	s.proposeC <- buf

	//监听该kv，当该kv被applied时返回客户端
	sig := make(chan struct{})
	s.monitorKV[uid] = sig

	select {
	case <-sig:
		return true, nil
	case <-timeOutC.C:
		return false, errors.New("request time out")
	}
}

func (s *KvStore) Lookup(key []byte) (*marshal.BytesKV, error) {
	kv, err := s.storage.Get(key)

	//handle error
	if kv.Data.Type == marshal.TypeDelete {
		return true, nil
	}

	return
}

func (s *KvStore) Scan(lowKey, highKey []byte) ([]*marshal.BytesKV, error) {
	kvs, err := s.storage.Scan(lowKey, highKey)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *KvStore) Apply(kvs []*marshal.KV) error {
	return s.storage.Put(kvs)
}
