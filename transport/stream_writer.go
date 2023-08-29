package transport

import (
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"io"
	"sync"
)

const (
	streamBufSize = 4096
)

type streamWriter struct {
	localID types.ID
	peerID  types.ID
	peerIp  string

	enc     *messageEncoderAndWriter
	status  *peerStatus
	r       RaftTransport
	mu      sync.Mutex // guard field working and enc
	working bool       //working字段指示了enc是否存在
	paused  bool

	msgC   chan *pb.Message    //Peer会将待发送的消息写入到该通道，streamWriter则从该通道中读取消息并发送出去
	connC  chan io.WriteCloser //通过该通道获取当前streamWriter实例关联的底层网络连接
	stopC  chan struct{}
	done   chan struct{}
	errorC chan error
}

func startStreamWriter(local, id types.ID, status *peerStatus, r RaftTransport, errorC chan error) *streamWriter {
	w := &streamWriter{
		localID: local,
		peerID:  id,
		status:  status,
		r:       r,
		msgC:    make(chan *pb.Message, streamBufSize),
		connC:   make(chan io.WriteCloser),
		stopC:   make(chan struct{}),
		done:    make(chan struct{}),
	}
	go w.run()
	return w
}

func (cw *streamWriter) run() {
	var msgC chan *pb.Message
	log.Info("started stream writer with remote peer").Str(code.LocalId, cw.localID.Str()).
		Str(code.RemoteId, cw.peerID.Str()).Record()
	for {
		select {
		case m := <-msgC:
			err := cw.enc.encodeAndWrite(*m)
			if err != nil {
				cw.status.deactivate(failureType{source: cw.localID.Str(), action: "write"}, err.Error())
				cw.close()
				msgC = nil
				cw.r.ReportUnreachable(m.To)
				log.Warn("lost TCP streaming connection with remote peer").Str(code.LocalId, cw.localID.Str()).
					Str(code.RemoteId, cw.peerID.Str()).Record()
			}
		case conn := <-cw.connC:
			cw.mu.Lock()
			closed := cw.closeUnlocked()
			if closed {
				log.Warn("tempt to close existed TCP streaming connection when get a new conn").Str(code.LocalId, cw.localID.Str()).
					Str(code.RemoteId, cw.peerID.Str()).Record()
			}
			cw.enc = &messageEncoderAndWriter{conn}
			cw.status.activate()
			cw.working = true
			cw.mu.Unlock()
			msgC = cw.msgC
			log.Info("established TCP streaming connection with remote peer").Str(code.LocalId, cw.localID.Str()).
				Str(code.RemoteId, cw.peerID.Str()).Record()
		case <-cw.stopC:
			if cw.close() {
				log.Info("closed TCP streaming connection with remote peer").Str(code.RemoteId, cw.peerID.Str()).Record()
			}
			close(cw.done)
			return
		}
	}
}

func (cw *streamWriter) writeC() (chan<- *pb.Message, bool) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.msgC, cw.working
}

func (cw *streamWriter) close() bool {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.closeUnlocked()
}

func (cw *streamWriter) closeUnlocked() bool {
	if !cw.working {
		return false
	}
	if err := cw.enc.w.Close(); err != nil {
		log.Errorf("", err)
		return false
	}
	cw.msgC = make(chan *pb.Message, streamBufSize)
	cw.working = false
	return true
}

func (cw *streamWriter) stop() {
	close(cw.stopC)
	<-cw.done
}

func (cw *streamWriter) pause() {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.paused = true
}

func (cw *streamWriter) resume() {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.paused = false
}
