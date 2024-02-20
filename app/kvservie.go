package main

import (
	"errors"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/pb"
	"time"
)

type ConfChangeInfo struct {
	NodeIP string            `json:"node_ip"`
	NodeId uint64            `json:"node_id"`
	NodeOp pb.ConfChangeType `json:"node_op"`
}

type KvService struct {
	storage     db.Storage
	proposeC    chan<- []byte
	confChangeC chan pb.ConfChange
	monitorKV   map[int64]chan struct{}
	ReqTimeout  time.Duration
}

func NewKVService(proposeC chan<- []byte, confChangeC chan pb.ConfChange, requestTimeOut int, kvStorage db.Storage, monitorKV map[int64]chan struct{}) *KvService {
	s := &KvService{
		storage:     kvStorage,
		proposeC:    proposeC,
		monitorKV:   make(map[int64]chan struct{}),
		ReqTimeout:  time.Duration(requestTimeOut) * time.Second,
		confChangeC: confChangeC,
	}
	return s
}

func (s *KvService) Propose(key, val []byte, delete bool, expiredAt int64) (bool, error) {
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

	for {
		select {
		case <-sig:
			return true, nil
		case <-timeOutC.C:
			return false, errors.New("request time out")
		}
	}
}

func (s *KvService) Lookup(key []byte) (*marshal.BytesKV, error) {
	kv, err := s.storage.Get(key)

	//handle error
	if kv.Data.Type == marshal.TypeDelete {
		return true, nil
	}

	return
}

func (s *KvService) Scan(lowKey, highKey []byte) ([]*marshal.BytesKV, error) {
	kvs, err := s.storage.Scan(lowKey, highKey)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *KvService) ConfChangePropose(nodeOp pb.ConfChangeType, nodeId uint64) (bool, error) {
	return false, nil
}
