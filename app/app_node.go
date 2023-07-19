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
	Storage raft.Storage
	transport   *transport.Transport

	proposeC    <-chan bytes.Buffer  // 提议 (k,v)
	confChangeC <-chan pb.ConfChange // 提议更改配置文件
	commitC     chan<- *commit       // 提交 (k,v)
	errorC      chan<- error         // errors from raft session
	stopc       chan struct{}        // signals proposal channel closed
	httpstopc   chan struct{}        // signals http server to shutdown
	httpdonec   chan struct{}        // signals http server shutdown complete

	TickTime int //定时触发定时器的时间
}

func StartAppNode(localId int, peersUrl []string, join bool, proposeC <-chan bytes.Buffer, confChangeC <-chan pb.ConfChange, commitC chan<- *commit, errorC chan<- error) {
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
	}
	an.startRaftNode()

	// 启动一个goroutine,监听当前节点与集群中其他节点之间的网络连接
	go an.servePeerRaft()
	// 启动一个goroutine,处理appLayer与raftLayer的交互
	go an.serveRaftLayer()

	return
}

func (an *AppNode) startRaftNode() {
	rpeers := make([]raft.Peer, len(an.peersUrl))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	// 初始化raft配置
	// todo 从配置文件获取参数
	c := &raft.RaftConfig{
		ID:            uint64(an.localId),
		ElectionTick:  10,
		HeartbeatTick: 1,
		Storage:       an.Storage,
	}


	// 初始化底层的 raft 模块，这里会根据WAL的回放情况，
	// 判断当前节点是首次启动还是重新启动
	// oldwal 通过 sotrage接口获取wal日志判断
	if oldwal || an.join {
		an.raftNode = raft.RestartRaftNode(c)
	} else {
		an.raftNode = raft.StartRaftNode(c, rpeers)
	}
}

func (an *AppNode)

func (an *AppNode) serveRaftLayer() {
	//处理配置变更以及日志提议
	go func() {
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
		case rd := <-an.raftNode.ReadyC:
			// 先写到预写日志wal
			an.wal.Save(&rd.HardState, rd.Entries)
			// 再写入内存数据库中
			an.Storage.Append(rd.Entries)
			an.transport.Send(rd.Messages)
			applyDoneC, ok := an.commitEntries(an.checkEntries(rd.CommittedEntries))
			if !ok {
				an.stop()
				return
			}

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

func (an *AppNode) checkEntries(ents []pb.Entry) (nents []pb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > an.appliedIndex+1 {
		log.Infof("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, an.appliedIndex)
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
		switch ents[i].Type {
		case pb.EntryNormal:
			if len(ents[i].Data) == 0 {
				break
			}
			s := string(ents[i].Data)
			data = append(data, s)

		case pb.EntryConfChange:
			var cc *pb.ConfChange
			cc.Unmarshal(ents[i].Data)
			//通过算法层应用配置更新请求
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

	var applyDoneC chan struct{}

	//将commitedEntry传入commitC中由KVstore完成持久化
	if len(data) > 0 {
		applyDoneC = make(chan struct{}, 0)
		select {
		//todo 将data序列化后插入
		case an.commitC <- &commit{kv: []kv{}}:
		case <-an.stopc:
			return nil, false
		}
	}

	// after commit, update appliedIndex
	an.appliedIndex = ents[len(ents)-1].Index

	return applyDoneC, true
}

//  实现Rat网络层接口,网络层通过该接口与RaftNode交互

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
