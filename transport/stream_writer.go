package transport

import (
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"io"
	"net"
	"net/url"
	"sync"
)

const (
	streamBufSize = 4096
)

type streamWriter struct {
	localID types.ID
	peerID  types.ID
	peerUrl url.URL

	status  *peerStatus
	r       RaftTransport
	mu      sync.Mutex // guard field working and closer
	closer  io.Closer
	working bool

	msgC  chan *pb.Message //Peer会将待发送的消息写入到该通道，streamWriter则从该通道中读取消息并发送出去
	connC chan io.Writer   //通过该通道获取当前streamWriter实例关联的底层网络连接
	stopC chan struct{}
	done  chan struct{}
}

func startStreamWriter(local, id types.ID, status *peerStatus, r RaftTransport) *streamWriter {
	w := &streamWriter{
		localID: local,
		peerID:  id,
		status:  status,
		r:       r,
		msgC:    make(chan *pb.Message, streamBufSize),
		connC:   make(chan io.Writer),
		stopC:   make(chan struct{}),
		done:    make(chan struct{}),
	}
	go w.run()
	return w
}

func (cw *streamWriter) run() {
	var (
		msgC    chan *pb.Message
		connTcp io.Writer
	)
	log.Info("started stream writer with remote peer").Str(code.LocalId, cw.localID.Str()).
		Str(code.RemoteId, cw.peerID.Str()).Record()
	for {
		select {
		case m := <-msgC:
			e := &messageEncoderAndWriter{connTcp}
			err := e.encodeAndWrite(m)
			if err != nil {
				cw.status.deactivate(failureType{source: cw.localID.Str(), action: "write"}, err.Error())
				cw.close()
				log.Warn("lost TCP streaming connection with remote peer").Str(code.LocalId, cw.localID.Str()).
					Str(code.RemoteId, cw.peerID.Str()).Record()
				msgC = nil
				cw.r.ReportUnreachable(m.To)
			}
		case conn := <-cw.connC:
			cw.mu.Lock()
			closed := cw.closeUnlocked()
			if closed {
				log.Warn("closed TCP streaming connection with remote peer").Str(code.LocalId, cw.localID.Str()).
					Str(code.RemoteId, cw.peerID.Str()).Record()
			}
			connTcp = conn
			cw.status.activate()
			cw.working = true
			cw.mu.Unlock()
			log.Warn("established TCP streaming connection with remote peer").Str(code.LocalId, cw.localID.Str()).
				Str(code.RemoteId, cw.peerID.Str()).Record()
			msgC = cw.msgC
		case <-cw.stopC:
			if cw.close() {
				log.Warn("closed TCP streaming connection with remote peer").Str(code.RemoteId, cw.peerID.Str()).Record()
			}
			log.Warn("stopped TCP streaming connection with remote peer").Str(code.RemoteId, cw.peerID.Str()).Record()
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
	if err := cw.closer.Close(); err != nil {
	}
	if len(cw.msgC) > 0 {
		cw.r.ReportUnreachable(uint64(cw.peerID))
	}
	cw.msgC = make(chan *pb.Message, streamBufSize)
	cw.working = false
	return true
}

func (cw *streamWriter) attach(conn *net.TCPConn) bool {
	select {
	case cw.connC <- conn:
		return true
	case <-cw.done:
		return false
	}
}

func (cw *streamWriter) stop() {
	close(cw.stopC)
	<-cw.done
}
