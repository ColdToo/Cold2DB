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
	raftNode  *raft.RaftNode
	transport transport.Transporter

	proposeC    chan []byte        // 提议 (k,v)
	confChangeC chan pb.ConfChange // 提议更改配置文件
	kvApiStopC  chan struct{}      // 关闭http服务器的信号
}

func StartAppNode(localId int, peersUrl []string, proposeC chan []byte, confChangeC chan pb.ConfChange,
	kvApiStopC chan struct{}, kvStore *KvStore, config *config.RaftConfig, localIp string) {
	an := &AppNode{
		localId:     localId,
		localIp:     localIp,
		peersUrl:    peersUrl,
		kvStore:     kvStore,
		proposeC:    proposeC,
		confChangeC: confChangeC,
		kvApiStopC:  kvApiStopC,
	}

	an.startRaftNode(config)

	// 启动一个goroutine,处理节点变更以及日志提议
	go an.servePropCAndConfC()
	// 启动一个goroutine,处理appLayer与raftLayer的交互
	go an.serveRaftNode(config.HeartbeatTick)
	// 启动一个goroutine,监听当前节点与集群中其他节点之间的网络连接
	go an.servePeerRaft()

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
	if an.IsRestartNode() {
		an.raftNode = raft.RestartRaftNode(opts)
	} else {
		an.raftNode = raft.StartRaftNode(opts, rpeers)
	}
}

func (an *AppNode) IsRestartNode() (flag bool) {
	return an.kvStore.db.IsRestartNode()
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
				return
			}
		case cc := <-an.confChangeC:
			confChangeCount++
			cc.ID = confChangeCount
			err := an.raftNode.ProposeConfChange(cc)
			if err != nil {
				return
			}
		}
	}
}

func (an *AppNode) serveRaftNode(heartbeatTick int) {
	ticker := time.NewTicker(time.Duration(heartbeatTick) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			an.raftNode.Tick()

		case rd := <-an.raftNode.ReadyC:
			err := an.handleReady(rd)
			if err != nil {
				log.Errorf("", err)
			}

			an.transport.Send(rd.Messages)

			//通知raftNode本轮ready已经处理完可以进行下一轮处理
			an.raftNode.Advance()

			//如果发现致命错误需要停止服务
		case err := <-an.transport.GetErrorC():
			log.Panicf("transport get critical err", err)
			an.stop()
			return

		case err := <-an.raftNode.ErrorC:
			log.Panicf("raftNode get critical err", err)
			an.stop()
			return
		}
	}
}

func (an *AppNode) handleReady(rd raft.Ready) (err error) {
	ents := rd.CommittedEntries
	entries := make([]*pb.Entry, len(ents))

	//apply entries
	for i, entry := range ents {
		switch ents[i].Type {
		case pb.EntryNormal:
			if len(ents[i].Data) == 0 {
				continue
			}
			entries = append(entries, entry)

		case pb.EntryConfChange:
			var cc *pb.ConfChange
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
	walEntriesId := make([]int64, 0)
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
	}
	return nil
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

	for i := range an.peersUrl {
		if i+1 != an.localId {
			an.transport.AddPeer(types.ID(i+1), an.peersUrl[i])
		}
	}

	go an.transport.ListenPeerAttachConn(an.localIp)
}

//  Rat网络层接口,网络层通过该接口与RaftNode交互

func (an *AppNode) Process(m *pb.Message) error {
	return an.raftNode.Step(m)
}

func (an *AppNode) ReportUnreachable(id uint64) { an.raftNode.ReportUnreachable(id) }

func (an *AppNode) ReportSnapshotStatus(id uint64, status raft.SnapshotStatus) {
	an.raftNode.ReportSnapshot(id, status)
}

func (an *AppNode) stop() {
	an.transport.Stop()
	an.raftNode.Stop()
	an.kvStore.db.Close()
	close(an.proposeC)
	close(an.confChangeC)
	close(an.kvApiStopC)
}
