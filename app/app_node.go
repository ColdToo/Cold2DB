package main

import (
	"context"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/db/logfile"
	"github.com/ColdToo/Cold2DB/domain"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/raft"
	"github.com/ColdToo/Cold2DB/transportHttp"
	types "github.com/ColdToo/Cold2DB/transportHttp/types"
	"net/http"
	"net/url"
	"time"
)

// A key-value stream backed by raft
type AppNode struct {
	localId  int
	peersUrl []string
	join     bool

	kvStore   *KvStore
	raftNode  *raft.RaftNode
	transport *transportHttp.Transport

	proposeC    <-chan []byte        // 提议 (k,v)
	confChangeC <-chan pb.ConfChange // 提议更改配置文件
	errorC      chan<- error         // errors from raft session
	stopc       chan struct{}        // signals proposal channel closed
	httpstopc   chan struct{}        // signals http server to shutdown
	httpdonec   chan struct{}        // signals http server shutdown complete

	TickTime int //定时触发定时器的时间
}

func StartAppNode(localId int, peersUrl []string, join bool, proposeC <-chan []byte,
	confChangeC <-chan pb.ConfChange, errorC chan<- error, kvStore *KvStore) {
	an := &AppNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		errorC:      errorC,
		localId:     localId,
		peersUrl:    peersUrl,
		join:        join,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),
		kvStore:     kvStore,
	}
	an.startRaftNode()

	//处理配置变更以及日志提议
	go an.servePropCAndConfC()
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

	cfg := domain.GetConfig()
	opts := &raft.RaftOpts{ID: uint64(an.localId),
		Storage:       an.kvStore.db,
		ElectionTick:  cfg.RaftConfig.ElectionTick,
		HeartbeatTick: cfg.RaftConfig.ElectionTick}
	if an.IsRestartNode() {
		an.raftNode = raft.RestartRaftNode(opts)
	} else {
		an.raftNode = raft.StartRaftNode(opts, rpeers)
	}
}

func (an *AppNode) serveRaftNode() {
	//todo 定时器应该做成可配置选项
	ticker := time.NewTicker(100 * time.Millisecond)
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

		case err := <-an.transport.ErrorC:
			an.writeError(err)
			return

		case <-an.stopc:
			an.stop()
			return
		}
	}
}

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
	an.transport = &transportHttp.Transport{
		LocalID:   types.ID(an.localId),
		ClusterID: 0x1000,
		Raft:      an,
		ErrorC:    make(chan error),
	}

	err := an.transport.Initialize()
	if err != nil {
		log.Panic("initialize transportHttp failed").Err(code.NodeInIErr, err).Record()
		return
	}

	for i := range an.peersUrl {
		if i+1 != an.localId {
			an.transport.AddPeer(types.ID(i+1), an.peersUrl[i])
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
	ln, err := transportHttp.NewStoppableListener(localUrl.Host, an.httpstopc)
	if err != nil {
		log.Panic("raftexample: Failed to listen transportHttp (%v)")
	}
	err = (&http.Server{Handler: an.transport.Handler()}).Serve(ln)

	select {
	case <-an.httpstopc:
	default:
		log.Panic("raftexample: Failed to serve transportHttp (%v)")
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
	close(an.errorC)
	an.raftNode.Stop()
}

func (an *AppNode) writeError(err error) {
	an.stopHTTP()
	an.errorC <- err
	close(an.errorC)
	an.raftNode.Stop()
}

func (an *AppNode) IsRestartNode() (flag bool) {
	return an.kvStore.db.IsRestartNode()
}
