package raftTransport

import (
	"bytes"
	"context"
	"errors"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/log"
	types "github.com/ColdToo/Cold2DB/raftTransport/types"
	"github.com/ColdToo/Cold2DB/raftproto"
	"io/ioutil"
	"sync"
	"time"

	"go.etcd.io/etcd/pkg/pbutil"

	"go.etcd.io/etcd/raft"

	"go.uber.org/zap"
)

const (
	connPerPipeline = 4

	// pipelineBufSize is the size of pipeline buffer, which helps hold the
	// temporary network latency.
	// The size ensures that pipeline does not drop messages when the network
	// is out of work for less than 1 second in good path.
	pipelineBufSize = 64
)

var errStopped = errors.New("stopped")

// pipeline主要用于发送快照
type pipeline struct {
	peerID     types.ID
	tr         *Transport
	picker     *urlPicker
	peerStatus *peerStatus

	//向raft报告发送快照失败或者成功
	raft Raft

	//pipeline实例从这个管道中获取待发送到对端的message，默认缓冲通道是64个
	msgc chan *raftproto.Message

	wg     sync.WaitGroup
	stopC  chan struct{}
	errorC chan error
}

func (p *pipeline) start() {
	p.stopC = make(chan struct{})
	p.msgc = make(chan *raftproto.Message, pipelineBufSize)
	p.wg.Add(connPerPipeline)
	for i := 0; i < connPerPipeline; i++ {
		go p.handle()
	}

	log.Info("started HTTP pipelining with remote peer").Str(code.LocalMemberId, p.tr.LocalID.Str()).Str(code.RemotePeerId, p.peerID.Str()).Record()

}

func (p *pipeline) stop() {
	close(p.stopc)
	p.wg.Wait()
	p.tr.Logger.Info(
		"stopped HTTP pipelining with remote peer",
		zap.String("local-member-id", p.tr.LocalID.String()),
		zap.String("remote-peer-id", p.peerID.String()),
	)
}

func (p *pipeline) handle() {
	defer p.wg.Done()

	for {
		select {
		case m := <-p.msgc:
			err := p.post(pbutil.MustMarshal(m))

			if err != nil {
				p.status.deactivate(failureType{source: pipelineMsg, action: "write"}, err.Error())
				p.raft.ReportUnreachable(m.To)
				if isMsgSnap(m) {
					p.raft.ReportSnapshot(m.To, raft.SnapshotFailure)
				}
				continue
			}
			p.status.activate()

			if isMsgSnap(m) {
				p.raft.ReportSnapshot(m.To, raft.SnapshotFinish)
			}
		case <-p.stopc:
			return
		}
	}
}

// post POSTs a data payload to a url. Returns nil if the POST succeeds, error on any failure.
func (p *pipeline) post(data []byte) (err error) {
	u := p.picker.pick()
	req := createPostRequest(u, RaftPrefix, bytes.NewBuffer(data), "application/protobuf", p.tr.URLs, p.tr.LocalID, p.tr.ClusterID)

	done := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)

	go func() {
		select {
		case <-done:
			//暂时关闭pipeline
		case <-p.stopc:
			waitSchedule()
			cancel()
		}
	}()

	resp, err := p.tr.pipelineRt.RoundTrip(req)
	done <- struct{}{}
	if err != nil {
		p.picker.unreachable(u)
		return err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		p.picker.unreachable(u)
		return err
	}

	err = checkPostResponse(resp, b, req, p.peerID)
	if err != nil {
		p.picker.unreachable(u)
		// errMemberRemoved is a critical error since a removed member should
		// always be stopped. So we use reportCriticalError to report it to errorc.
		if err == errMemberRemoved {
			reportCriticalError(err, p.errorc)
		}
		return err
	}

	return nil
}

// waitSchedule waits other goroutines to be scheduled for a while
func waitSchedule() { time.Sleep(time.Millisecond) }
