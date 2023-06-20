package main

import (
	"bytes"
	"context"
	"github.com/ColdToo/Cold2DB/code"
	log "github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/raft"
	"github.com/ColdToo/Cold2DB/transport"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"github.com/ColdToo/Cold2DB/wal"
	"go.uber.org/zap"
	"net/http"
	"net/url"
	"time"
)

type commit struct {
	kv []kv
}

// A key-value stream backed by raft
type AppNode struct {
	localId  int
	peersUrl []string
	join     bool

	confState     *pb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	raftNode    *raft.RaftNode
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL
	transport   *transport.Transport

	proposeC    <-chan bytes.Buffer  // 提议 (k,v)
	confChangeC <-chan pb.ConfChange // 提议更改配置文件
	commitC     chan<- *commit       // 提交 (k,v)
	errorC      chan<- error         // errors from raft session
	stopc       chan struct{}        // signals proposal channel closed
	httpstopc   chan struct{}        // signals http server to shutdown
	httpdonec   chan struct{}        // signals http server shutdown complete

	TickTime int //定时触发定时器的时间

	logger *zap.Logger
}

func StartAppNode(localId int, peersUrl []string, join bool, proposeC <-chan kv, confChangeC <-chan pb.ConfChange, commitC chan<- *commit, errorC chan<- error) {
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
		logger:      zap.NewExample(),
	}
	an.startRaft()
	return
}

func (an *AppNode) startRaft() {
	//是否存在旧的WAL日志,用于判断该节点是首次加入还是重启的节点
	oldwal := wal.Exist(an.wal.Dir)

	//如果wal文件存在那么先回放wal中的文件到内存中
	an.wal = an.openWAL()

	rpeers := make([]raft.Peer, len(an.peersUrl))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	// 初始化raft配置
	// todo 从配置文件获取参数
	c := &raft.Config{
		ID:            uint64(an.localId),
		ElectionTick:  10,
		HeartbeatTick: 1,
		Storage:       an.raftStorage,
	}

	// 初始化底层的 etcd-raft 模块，这里会根据 WAL 日志的回放情况，
	// 判断当前节点是首次启动还是重新启动
	if oldwal || an.join {
		an.raftNode = raft.RestartRaftNode(c)
	} else {
		an.raftNode = raft.StartRaftNode(c, rpeers)
	}

	// 启动一个goroutine,监听当前节点与集群中其他节点之间的网络连接
	go an.servePeerRaft()
	// 启动一个goroutine,处理appLayer与raftLayer的交互
	go an.serveRaftLayer()
}

func (an *AppNode) serveRaftLayer() {
	// 获取快照的一些信息
	snap, err := an.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	//an.confState = snap.Metadata.ConfState
	an.snapshotIndex = snap.Metadata.Index
	an.appliedIndex = snap.Metadata.Index

	//处理配置变更以及日志提议
	go func() {
		confChangeCount := uint64(0)

		for an.proposeC != nil && an.confChangeC != nil {
			select {
			case prop, ok := <-an.proposeC:
				if !ok {
					an.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
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
	}()

	//AppNode 会启动一个定时器，每个 tick 默认为 100ms，然后定时调用 Node.Tick 方法驱动算法层执行定时函数：
	//todo 定时器应该做成可配置选项
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			an.raftNode.Tick()

		//当应用层通过 Node.Ready 方法接收到来自算法层的处理结果后，AppNode 需要将待持久化的预写日志（Ready.Entries）进行持久化，
		//需要调用通信模块为算法层执行消息发送动作（Ready.Messages），需要与数据状态机应用算法层已确认提交的预写日志.
		//当以上步骤处理完成时，AppNode 会调用 Node.Advance 方法对算法层进行响应.
		//处理Ready，应该是个channel,看能否获取到ready如果能获取到ready那么应用层根据该ready进行
		case rd := <-an.raftNode.Ready():
			an.wal.Save(&rd.HardState, rd.Entries)
			an.raftStorage.Append(rd.Entries)
			an.transport.Send(rd.Messages)
			applyDoneC, ok := an.commitEntries(an.checkEntrys(rd.CommittedEntries))
			if !ok {
				an.stop()
				return
			}

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

func (an *AppNode) checkEntrys(ents []pb.Entry) (nents []pb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > an.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, an.appliedIndex)
	}
	if an.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[an.appliedIndex-firstIdx+1:]
	}
	return nents
}

func (an *AppNode) commitEntries(ents []pb.Entry) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

	data := make([]string, 0, len(ents))
	for i := range ents {
		switch ents[i].EntryType {
		case pb.EntryType_EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			data = append(data, s)

		case pb.EntryType_EntryConfChange:
			var cc pb.ConfChange
			cc.Unmarshal(ents[i].Data)
			//通过算法层应用配置更新请求
			an.confState = *an.raftNode.ApplyConfChange(cc)
			switch cc.Type {
			case pb.ConfChangeType_AddNode:
				if len(cc.Context) > 0 {
					an.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case pb.ConfChangeType_RemoveNode:
				if cc.NodeID == uint64(an.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return nil, false
				}
				an.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}
	}

	var applyDoneC chan struct{}

	//将commitedEntry传入commitC中由KVstore完成持久化
	if len(data) > 0 {
		applyDoneC = make(chan struct{}, 1)
		select {
		case an.commitC <- &commit{data}:
		case <-an.stopc:
			return nil, false
		}
	}

	// after commit, update appliedIndex
	an.appliedIndex = ents[len(ents)-1].Index

	return applyDoneC, true
}

func (an *AppNode) openWAL() (w *wal.WAL) {
	return w
}

// 回放可能存在的wal到内存中
func (an *AppNode) replayWAL() {
	log.Info("replaying WAL of member %d").Record()
}

//  实现Rat接口,网络层通过该接口与RaftNode交互
//	当transport模块接收到其他节点的信息时调用如下方法让raft算法层进行处理

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
