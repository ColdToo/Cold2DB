package main

import (
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db"
	"github.com/ColdToo/Cold2DB/db/logfile"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/raft"
	"github.com/ColdToo/Cold2DB/transport"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"time"
)

type AppNode struct {
	localId  int
	localIp  string
	peersUrl []string

	kvStore   *KvStore
	raftNode  raft.RaftLayer
	transport transport.Transporter

	proposeC    chan []byte        // 提议 (k,v) channel
	confChangeC chan pb.ConfChange // 提议更改配置文件 channel
	kvHTTPStopC chan struct{}      // 关闭http服务器的信号 channel
}

func StartAppNode(localId int, peersUrl []string, proposeC chan []byte, confChangeC chan pb.ConfChange,
	kvHTTPStopC chan struct{}, kvStore *KvStore, config *config.RaftConfig, localIp string) {
	an := &AppNode{
		localId:     localId,
		localIp:     localIp,
		peersUrl:    peersUrl,
		kvStore:     kvStore,
		proposeC:    proposeC,
		confChangeC: confChangeC,
		kvHTTPStopC: kvHTTPStopC,
	}

	// 完成当前节点与集群中其他节点之间的网络连接
	an.servePeerRaft()
	// 启动Raft算法层
	an.startRaftNode(config)
	// 启动一个goroutine,处理appLayer与raftLayer的交互
	go an.serveRaftNode()
	// 启动一个goroutine,处理节点变更以及日志提议
	go an.servePropCAndConfC()

	return
}

func (an *AppNode) startRaftNode(config *config.RaftConfig) {
	rpeers := make([]raft.Peer, len(an.peersUrl))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	opts := &raft.RaftOpts{
		ID:            uint64(an.localId),
		Storage:       an.kvStore.db.(*db.Cold2DB),
		ElectionTick:  config.ElectionTick,
		HeartbeatTick: config.HeartbeatTick}

	an.raftNode = raft.StartRaftNode(opts, rpeers)
}

func (an *AppNode) IsRestartNode() (flag bool) {
	return an.kvStore.db.IsRestartNode()
}

func (an *AppNode) servePeerRaft() {
	an.transport = &transport.Transport{
		LocalID:   types.ID(an.localId),
		ClusterID: 0x1000,
		Raft:      an,
		ErrorC:    make(chan error),
		Peers:     make(map[types.ID]transport.Peer),
		StopC:     make(chan struct{}),
	}

	go an.transport.ListenPeerAttachConn(an.localIp)

	for i := range an.peersUrl {
		if i+1 != an.localId {
			an.transport.AddPeer(types.ID(i+1), an.peersUrl[i])
		}
	}
}

func (an *AppNode) servePropCAndConfC() {
	//todo 用于 prometheus 指标
	confChangeCount := uint64(0)

	//当proposeC和confChangeC关闭后退出该goroutine,并停止raft服务
	for an.proposeC != nil && an.confChangeC != nil {
		select {
		case prop := <-an.proposeC:
			err := an.raftNode.Propose(prop)
			if err != nil {
				log.Errorf("propose err", err)
			}
		case cc := <-an.confChangeC:
			confChangeCount++
			cc.ID = confChangeCount
			err := an.raftNode.ProposeConfChange(cc)
			if err != nil {
				log.Errorf("propose conf err", err)
			}
		}
	}
}

func (an *AppNode) serveRaftNode() {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			an.raftNode.Tick()

		case rd := <-an.raftNode.GetReadyC():
			log.Infof("start handle ready %v", rd.HardState)
			if len(rd.CommittedEntries) > 0 {
				err := an.applyEntries(rd.CommittedEntries)
				if err != nil {
					log.Errorf("apply entries failed", err)
				}
			}

			if !raft.IsEmptyHardState(rd.HardState) {
				err := an.saveHardState(rd.HardState)
				if err != nil {
					log.Errorf("", err)
				}
			}

			if len(rd.Messages) > 0 {
				an.transport.Send(rd.Messages)
			}

			//通知raftNode本轮ready已经处理完可以进行下一轮处理
			an.raftNode.Advance()
			log.Infof("handle ready success %v", rd.HardState)

			//如果网络层发现致命错误需要停止服务

		case err := <-an.transport.GetErrorC():
			log.Panicf("transport get critical err", err)
			an.stop()
			return

			//如果raft层发现致命错误需要停止服务

		case err := <-an.raftNode.GetErrorC():
			log.Panicf("raftNode get critical err", err)
			an.stop()
			return
		}
	}
}

func (an *AppNode) applyEntries(ents []*pb.Entry) (err error) {
	entries := make([]*pb.Entry, 0)

	//apply entries
	for i, entry := range ents {
		switch ents[i].Type {
		case pb.EntryNormal:
			if len(ents[i].Data) == 0 {
				continue
			}
			entries = append(entries, entry)

		case pb.EntryConfChange:
			var cc pb.ConfChange
			cc.Unmarshal(ents[i].Data)
			an.raftNode.ApplyConfChange(cc)
			switch cc.Type {
			/*case pb.ConfChangeAddNode:
			if len(cc.Context) > 0 {
				an.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
			}*/
			case pb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(an.localId) {
					return
				}
				an.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}
	}

	var kv KV
	walEntries := make([]logfile.WalEntry, len(entries))
	walEntriesId := make([]uint64, 0)
	for _, entry := range entries {
		kv, err = logfile.GobDecode(entry.Data)
		if err != nil {
			return err
		}
		walEntry := logfile.WalEntry{
			Index:     entry.Index,
			Term:      entry.Term,
			Key:       kv.Key,
			Value:     kv.Value,
			ExpiredAt: kv.ExpiredAt,
			Type:      kv.Type,
		}
		walEntries = append(walEntries, walEntry)
		walEntriesId = append(walEntriesId, kv.Id)
	}

	err = an.kvStore.db.Put(walEntries)
	if err != nil {
		log.Errorf("", err)
		return
	}

	for _, id := range walEntriesId {
		close(an.kvStore.monitorKV[id])
		delete(an.kvStore.monitorKV, id)
	}
	return nil
}

func (an *AppNode) saveHardState(state pb.HardState) error {
	return an.kvStore.db.SaveHardState(state)
}

// Process Rat网络层接口,网络层通过该接口与RaftNode交互
func (an *AppNode) Process(m *pb.Message) error {
	return an.raftNode.Step(m)
}

func (an *AppNode) ReportUnreachable(id uint64) { an.raftNode.ReportUnreachable(id) }

func (an *AppNode) ReportSnapshotStatus(id uint64, status raft.SnapshotStatus) {
	an.raftNode.ReportSnapshot(id, status)
}

// todo 关闭kv存储服务,回收相关资源
func (an *AppNode) stop() {
	an.transport.Stop()
	an.raftNode.Stop()
	an.kvStore.db.Close()
	close(an.proposeC)
	close(an.confChangeC)
	close(an.kvHTTPStopC)
}
