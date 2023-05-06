package main

import (
	"context"
	"fmt"
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
	"os"
	"strconv"
	"time"
)

type commit struct {
	kv []kv
}

// A key-value stream backed by raft
type AppNode struct {
	id     int      // client ID for raft session
	peers  []string // raft peer URLs
	join   bool     // node is joining an existing cluster
	waldir string   // path to WAL directory

	confState     *raftproto.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	raftNode    raft.RaftNode
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
// current), then new log entries. To shutdown, close proposeC and read errorC.
func StartAppNode(id int, peers []string, join bool, proposeC <-chan kv, confChangeC <-chan raftproto.ConfChange, commitC chan<- *commit, errorC chan<- error) {
	rc := &AppNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		waldir:      fmt.Sprintf("raftexample-%d", id),
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),
		logger:      zap.NewExample(),
	}
	go rc.startRaft()
	return
}

// openWAL returns a WAL ready for reading.
func (rc *AppNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(zap.NewExample(), rc.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(zap.NewExample(), rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *AppNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rc.id)
	return nil
}

func (rc *AppNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.raftNode.Stop()
}

func (rc *AppNode) startRaft() {
	//是否存在旧的WAL日志,用于判断该节点是首次加入还是重启的节点
	oldwal := wal.Exist(rc.waldir)

	//如果wal文件存在那么先回放wal中的文件到内存中
	rc.wal = rc.replayWAL()

	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	//初始化raft配置
	c := &raft.Config{
		ID:            uint64(rc.id),
		ElectionTick:  10,
		HeartbeatTick: 1,
		Storage:       rc.raftStorage,
	}

	// 初始化底层的 etcd-raft 模块，这里会根据 WAL 日志的回放情况，
	// 判断当前节点是首次启动还是重新启动
	if oldwal || rc.join {
		rc.raftNode = raft.RestartRaftNode(c)
	} else {
		rc.raftNode = raft.StartRaftNode(c, rpeers)
	}

	// 启动一个goroutine,监听当前节点与集群中其他节点之间的网络连接
	go rc.servePeerRaft()
	// 启动一个goroutine,处理appNode与raftNode的交互
	go rc.serveRaftNode()
}

// stop closes http, closes all channels, and stops raft.
func (rc *AppNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.raftNode.Stop()
}

func (rc *AppNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

func (rc *AppNode) Process(ctx context.Context, m *raftproto.Message) error {
	return rc.raftNode.Step(m)
}

func (rc *AppNode) servePeerRaft() {
	//Transport 实例，负责raft节点之间的网络通信服务
	rc.transport = &rafthttp.Transport{
		Logger:      rc.logger,
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}

	rc.transport.Start()

	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}

	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
}

func (rc *AppNode) serveRaftNode() {
	// 获取快照的一些信息
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	//AppNode 会启动一个定时器，每个 tick 默认为 100ms，然后定时调用 Node.Tick 方法驱动算法层执行定时函数：
	//todo 应该做成可配置选项
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		confChangeCount := uint64(0)

		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					rc.raftNode.Propose([]byte(prop))
				}

			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					rc.raftNode.ProposeConfChange(cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rc.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.raftNode.Tick()

		//store raft entries to wal, then publish over commit channel
		//当应用层通过 Node.Ready 方法接收到来自算法层的处理结果后，AppNode 需要将待持久化的预写日志（Ready.Entries）进行持久化，
		//需要调用通信模块为算法层执行消息发送动作（Ready.Messages），需要与数据状态机应用算法层已确认提交的预写日志.
		//当以上步骤处理完成时，AppNode 会调用 Node.Advance 方法对算法层进行响应.
		case rd := <-rc.raftNode.Ready():
			rc.wal.Save(&rd.HardState, rd.Entries)
			rc.raftStorage.Append(rd.Entries)
			rc.transport.Send(rd.Messages)
			applyDoneC, ok := rc.commitEntries(rc.CheckEntrys(rd.CommittedEntries))
			if !ok {
				rc.stop()
				return
			}

			//通知算法层进行下一轮
			rc.raftNode.Advance(raft.Ready{})

		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

func (rc *AppNode) CheckEntrys(ents []raftproto.Entry) (nents []raftproto.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

// writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *AppNode) commitEntries(ents []raftpb.Entry) (<-chan struct{}, bool) {
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
			rc.confState = *rc.raftNode.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return nil, false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}
	}

	var applyDoneC chan struct{}

	//将commitedEntry传入commitC中由KVstore完成持久化
	if len(data) > 0 {
		applyDoneC = make(chan struct{}, 1)
		select {
		case rc.commitC <- &commit{data}:
		case <-rc.stopc:
			return nil, false
		}
	}

	// after commit, update appliedIndex
	rc.appliedIndex = ents[len(ents)-1].Index

	return applyDoneC, true
}

/*
func (rc *AppNode) ReportUnreachable(id uint64) { rc.raftNode.ReportUnreachable(id) }  用于报告哪个节点不可达，此时算法层可以做一些相应的操作

func (rc *AppNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.raftNode.ReportSnapshot(id, status)
}
*/
