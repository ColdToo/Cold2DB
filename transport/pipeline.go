package transport

import (
	"bytes"
	"context"
	"errors"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/raft"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"go.etcd.io/etcd/pkg/pbutil"
	"io/ioutil"
	"sync"
	"time"
)

const (
	connPerPipeline = 4

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
	raft RaftTransport

	//pipeline实例从这个管道中获取待发送到对端的message，默认缓冲通道是64个
	msgc chan *pb.Message

	wg     sync.WaitGroup
	stopC  chan struct{}
	errorC chan error
}

func (p *pipeline) start() {
	p.stopC = make(chan struct{})
	p.msgc = make(chan *pb.Message, pipelineBufSize)
	p.wg.Add(connPerPipeline)
	for i := 0; i < connPerPipeline; i++ {
		go p.handle()
	}

	log.Info("started HTTP pipelining with remote peer").Str(code.LocalId, p.tr.LocalID.Str()).Str(code.RemoteId, p.peerID.Str()).Record()
}

func (p *pipeline) stop() {
	close(p.stopC)
	p.wg.Wait()
}

func (p *pipeline) handle() {
	defer p.wg.Done()

	for {
		select {
		// 若有需要发送的数据则直接发送
		case m := <-p.msgc:
			err := p.post(pbutil.MustMarshal(m))

			if err != nil {
				p.peerStatus.deactivate(failureType{source: pipelineMsg, action: "write"}, err.Error())
				p.raft.ReportUnreachable(m.To)
				p.raft.ReportSnapshotStatus(m.To, raft.SnapshotFailure)
				continue
			}

			p.peerStatus.activate()

			p.raft.ReportSnapshotStatus(m.To, raft.SnapshotFinish)
		case <-p.stopC:
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
		case <-p.stopC:
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
			reportCriticalError(err, p.errorC)
		}
		return err
	}

	return nil
}

// waitSchedule waits other goroutines to be scheduled for a while
func waitSchedule() { time.Sleep(time.Millisecond) }
