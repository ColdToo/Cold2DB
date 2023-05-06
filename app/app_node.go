package main

import (
	"context"
	"github.com/ColdToo/Cold2DB/raft"
	"github.com/ColdToo/Cold2DB/rafthttp"
	"github.com/ColdToo/Cold2DB/raftproto"
	"github.com/ColdToo/Cold2DB/wal"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
	"log"
	"net/http"
	"net/url"
	"time"
)

type commit struct {
	kv []kv
}

// A key-value stream backed by raft
type AppNode struct {
	id    int      // client ID for raft session
	peers []string // raft peer URLs
	join  bool     // node is joining an existing cluster

	confState     *raftproto.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	raftNode    *raft.RaftNode
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	transport *rafthttp.Transport

	proposeC    <-chan kv                   // 提议 (k,v)
	confChangeC <-chan raftproto.ConfChange // 提议更改配置文件
	commitC     chan<- *commit              // 提交 (k,v)
	errorC      chan<- error                // errors from raft session
	stopc       chan struct{}               // signals proposal channel closed
	httpstopc   chan struct{}               // signals http server to shutdown
	httpdonec   chan struct{}               // signals http server shutdown complete

	TickTime int //定时触发定时器的时间

	logger *zap.Logger
}

// StartAppNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read erroan.
func StartAppNode(id int, peers []string, join bool, proposeC <-chan kv, confChangeC <-chan raftproto.ConfChange, commitC chan<- *commit, errorC chan<- error) {
	an := &AppNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),
		logger:      zap.NewExample(),
	}
	go an.startRaft()
	return
}

func (an *AppNode) startRaft() {
	//是否存在旧的WAL日志,用于判断该节点是首次加入还是重启的节点
	oldwal := wal.Exist(an.wal.Dir)

	//如果wal文件存在那么先回放wal中的文件到内存中
	an.wal = an.replayWAL()

	rpeers := make([]raft.Peer, len(an.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	//初始化raft配置
	c := &raft.Config{
		ID:            uint64(an.id),
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
	// 启动一个goroutine,处理appNode与raftNode的交互
	go an.serveRaftNode()
}

func (an *AppNode) servePeerRaft() {
	//Transport 实例，负责raft节点之间的网络通信服务
	an.transport = &rafthttp.Transport{
		Logger:      an.logger,
		ID:          types.ID(an.id),
		ClusterID:   0x1000,
		Raft:        an,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), stanonv.Itoa(an.id)),
		Erroan:      make(chan error),
	}

	an.transport.Start()

	for i := range an.peers {
		if i+1 != an.id {
			an.transport.AddPeer(types.ID(i+1), []string{an.peers[i]})
		}
	}

	url, err := url.Parse(an.peers[an.id-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, an.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: an.transport.Handler()}).Serve(ln)
	select {
	case <-an.httpstopc:
	default:
		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	}
	close(an.httpdonec)
}

func (an *AppNode) serveRaftNode() {
	// 获取快照的一些信息
	snap, err := an.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	an.confState = snap.Metadata.ConfState
	an.snapshotIndex = snap.Metadata.Index
	an.appliedIndex = snap.Metadata.Index

	// send proposals over raft
	go func() {
		confChangeCount := uint64(0)

		for an.proposeC != nil && an.confChangeC != nil {
			select {
			case prop, ok := <-an.proposeC:
				if !ok {
					an.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					an.raftNode.Propose([]byte(prop))
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
		// client closed channel; shutdown raft if not already
		close(an.stopc)
	}()

	//AppNode 会启动一个定时器，每个 tick 默认为 100ms，然后定时调用 Node.Tick 方法驱动算法层执行定时函数：
	//todo 应该做成可配置选项
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			an.raftNode.Tick()

		//当应用层通过 Node.Ready 方法接收到来自算法层的处理结果后，AppNode 需要将待持久化的预写日志（Ready.Entries）进行持久化，
		//需要调用通信模块为算法层执行消息发送动作（Ready.Messages），需要与数据状态机应用算法层已确认提交的预写日志.
		//当以上步骤处理完成时，AppNode 会调用 Node.Advance 方法对算法层进行响应.
		case rd := <-an.raftNode.Ready():
			an.wal.Save(&rd.HardState, rd.Entries)
			an.raftStorage.Append(rd.Entries)
			an.transport.Send(rd.Messages)
			applyDoneC, ok := an.commitEntries(an.CheckEntrys(rd.CommittedEntries))
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

func (an *AppNode) checkEntrys(ents []raftproto.Entry) (nents []raftproto.Entry) {
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

func (an *AppNode) commitEntries(ents []raftproto.Entry) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

	data := make([]string, 0, len(ents))
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			data = append(data, s)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			//通过算法层应用配置更新请求
			an.confState = *an.raftNode.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					an.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
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

// openWAL returns a WAL ready for reading.
func (an *AppNode) openWAL() (w *wal.WAL) {
	return w
}

// replayWAL replays WAL entries into the raft instance.
func (an *AppNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", an.id)
	return nil
}

func (an *AppNode) Process(ctx context.Context, m *raftproto.Message) error {
	return an.raftNode.Step(m)
}

/*
func (an *AppNode) ReportUnreachable(id uint64) { an.raftNode.ReportUnreachable(id) }  用于报告哪个节点不可达，此时算法层可以做一些相应的操作

func (an *AppNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	an.raftNode.ReportSnapshot(id, status)
}
*/

func (an *AppNode) stopHTTP() {
	an.transport.Stop()
	close(an.httpstopc)
	<-an.httpdonec
}

// stop closes http, closes all channels, and stops raft.
func (an *AppNode) stop() {
	an.stopHTTP()
	close(an.commitC)
	close(an.erroan)
	an.raftNode.Stop()
}

func (an *AppNode) writeError(err error) {
	an.stopHTTP()
	close(an.commitC)
	an.erroan <- err
	close(an.erroan)
	an.raftNode.Stop()
}
