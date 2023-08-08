package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/db/logfile"
	log "github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/raft"
	"github.com/ColdToo/Cold2DB/transport"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"net/http"
	"net/url"
	"time"
)

// A key-value stream backed by raft
type AppNode struct {
	localId  int
	peersUrl []string
	join     bool

	KvStore *KvStore
	Storage raft.Storage

	raftNode  *raft.RaftNode
	transport *transport.Transport

	proposeC    <-chan bytes.Buffer  // 提议 (k,v)
	confChangeC <-chan pb.ConfChange // 提议更改配置文件
	commitC     chan<- []*pb.Entry   // 提交 (k,v)
	errorC      chan<- error         // errors from raft session
	stopc       chan struct{}        // signals proposal channel closed
	httpstopc   chan struct{}        // signals http server to shutdown
	httpdonec   chan struct{}        // signals http server shutdown complete

	TickTime int //定时触发定时器的时间
}

func StartAppNode(localId int, peersUrl []string, join bool, proposeC <-chan bytes.Buffer,
	confChangeC <-chan pb.ConfChange, commitC chan<- []*pb.Entry, errorC chan<- error, kvStore *KvStore) {
	an := &AppNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		localId:     localId,
		peersUrl:    peersUrl,
		join:        join,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),
		KvStore:     kvStore,
	}
	an.startRaftNode()

	// 启动一个goroutine,处理appLayer与raftLayer的交互
	go an.serveRaftNode()
	// 启动一个goroutine,监听当前节点与集群中其他节点之间的网络连接
	go an.servePeerRaft()

	return
}

func (an *AppNode) startRaftNode() {
	rpeers := make([]raft.Peer, len(an.peersUrl))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	if an.Storage.IsRestartNode() {
		an.raftNode = raft.RestartRaftNode(c)
	} else {
		an.raftNode = raft.StartRaftNode(c, rpeers)
	}
}

func (an *AppNode) serveRaftNode() {
	//处理配置变更以及日志提议
	go an.servePropCAndConfC()

	//AppNode 会启动一个定时器，每个 tick 默认为 100ms，然后定时调用 Node.Tick 方法驱动算法层执行定时函数：
	//todo 定时器应该做成可配置选项
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			an.raftNode.Tick()

		//当应用层通过 Node.Ready 方法接收到来自算法层的处理结果后，AppNode 需要将待commited的预写日志（Ready.Entries）进行持久化，
		//需要调用通信模块为算法层执行消息发送动作（Ready.Messages），需要与数据状态机应用算法层已确认提交的预写日志.
		//当以上步骤处理完成时，AppNode 会调用 Node.Advance 方法对算法层进行响应.
		case rd := <-an.raftNode.ReadyC:
			applyDoneC, ok := an.handleReady(rd)
			if !ok {
				an.stop()
				return
			}

			an.transport.Send(rd.Messages)

			<-applyDoneC
			//通知算法层进行下一轮
			an.raftNode.Advance(raft.Ready{})

		case err := <-an.transport.ErrorC:
			an.writeError(err)
			return

		case <-an.stopc:
			an.stop()
			return
		}
	}
}

// todo 一定需要proposeC和ConfC吗？为什么 kvstore无法调用app node的方法
func (an *AppNode) servePropCAndConfC() {
	confChangeCount := uint64(0)

	for an.proposeC != nil && an.confChangeC != nil {
		select {
		case prop, ok := <-an.proposeC:
			if !ok {
				an.proposeC = nil
			} else {
				an.raftNode.Propose(prop)
			}

		case cc, ok := <-an.confChangeC:
			if !ok {
				an.confChangeC = nil
			} else {
				confChangeCount++
				cc.ID = confChangeCount
				an.raftNode.ProposeConfChange(cc)
			}
		}
	}

	close(an.stopc)
}

func (an *AppNode) handleReady(rd raft.Ready) (<-chan struct{}, bool) {
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

			//todo 节点变更
		case pb.EntryConfChange:
			var cc *pb.ConfChange
			cc.Unmarshal(ents[i].Data)
			an.confState = an.raftNode.ApplyConfChange(cc)
			switch cc.Type {
			case pb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					an.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case pb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(an.localId) {
					return nil, false
				}
				an.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}
	}

	var kv KV
	walEntries := make([]logfile.WalEntry, len(entries))
	walEntriesid := make([]int64, 0)
	for _, entry := range entries {
		err := gob.NewDecoder(bytes.NewBuffer(entry.Data)).Decode(&kv)
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
		walEntries = append(walEntries, walEntry)
		walEntriesid = append(walEntriesid, kv.id)
	}

	err := an.KvStore.Put(walEntries)
	if err != nil {
		log.Error(err)
	}

	for _, id := range walEntriesid {
		close(an.KvStore.monitorKV[id])
	}
}

func (an *AppNode) servePeerRaft() {
	//transport 实例，负责raft节点之间的网络通信服务
	an.transport = &transport.Transport{
		LocalID:   types.ID(an.localId),
		ClusterID: 0x1000,
		Raft:      an,
		ErrorC:    make(chan error),
	}

	err := an.transport.Initialize()
	if err != nil {
		log.Panic("initialize transport failed").Err(code.NodeInIErr, err).Record()
		return
	}

	//raftexample --id 1 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 12380
	for i := range an.peersUrl {
		if i+1 != an.localId {
			an.transport.AddPeer(types.ID(i+1), []string{an.peersUrl[i]})
		}
	}

	//监听来自其他节点的http请求
	an.listenAndServePeerRaft()
}

func (an *AppNode) listenAndServePeerRaft() {
	localUrl, err := url.Parse(an.peersUrl[an.localId-1])
	if err != nil {
		log.Panic("raftexample: Failed parsing URL (%v)")
	}
	ln, err := transport.NewStoppableListener(localUrl.Host, an.httpstopc)
	if err != nil {
		log.Panic("raftexample: Failed to listen transport (%v)")
	}
	err = (&http.Server{Handler: an.transport.Handler()}).Serve(ln)

	select {
	case <-an.httpstopc:
	default:
		log.Panic("raftexample: Failed to serve transport (%v)")
	}
	close(an.httpdonec)
}

//  Rat网络层接口,网络层通过该接口与RaftNode交互

func (an *AppNode) Process(ctx context.Context, m *pb.Message) error {
	return an.raftNode.Step(m)
}

func (an *AppNode) IsIDRemoved(id uint64) bool { return false }

func (an *AppNode) ReportUnreachable(id uint64) { an.raftNode.ReportUnreachable(id) }

func (an *AppNode) ReportSnapshotStatus(id uint64, status raft.SnapshotStatus) {
	an.raftNode.ReportSnapshot(id, status)
}

// close app node
func (an *AppNode) stopHTTP() {
	an.transport.Stop()
	close(an.httpstopc)
	<-an.httpdonec
}

func (an *AppNode) stop() {
	an.stopHTTP()
	close(an.commitC)
	close(an.errorC)
	an.raftNode.Stop()
}

func (an *AppNode) writeError(err error) {
	an.stopHTTP()
	close(an.commitC)
	an.errorC <- err
	close(an.errorC)
	an.raftNode.Stop()
}
