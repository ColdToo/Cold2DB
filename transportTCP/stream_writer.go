package transportTCP

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transportHttp/types"
	"io"
	"net"
	"net/url"
	"sync"
	"time"
)

const (
	streamBufSize = 4096
)

var (
	errUnsupportedStreamType = fmt.Errorf("unsupported stream type")
	// 理解为线路的心跳信息
	linkHeartbeatMessage = pb.Message{Type: pb.MsgHeartbeat}
)

func isLinkHeartbeatMessage(m *pb.Message) bool {
	return m.Type == pb.MsgHeartbeat && m.From == 0 && m.To == 0
}

type streamWriter struct {
	localID types.ID
	peerID  types.ID
	peerUrl url.URL

	status  *peerStatus
	r       RaftTransport
	mu      sync.Mutex // guard field working and closer
	closer  io.Closer
	working bool

	msgC  chan *pb.Message  //Peer会将待发送的消息写入到该通道，streamWriter则从该通道中读取消息并发送出去
	connC chan *net.TCPConn //通过该通道获取当前streamWriter实例关联的底层网络连接
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
		connC:   make(chan *net.TCPConn),
		stopC:   make(chan struct{}),
		done:    make(chan struct{}),
	}
	go w.run()
	return w
}

func (cw *streamWriter) run() {
	var (
		msgC       chan *pb.Message
		heartbeatC <-chan time.Time // 定时器会定时向该通道发送信号，触发心跳消息的发送，该心跳消息与后台介绍的Raft的心跳消息有所不同，该心跳的主要目的是为了防止连接长时间不用断开的
		enc        encoder          // 编码器接口，实际实现该接口的会负责将消息序列化并写入连接的缓冲区中
		batched    int              // 统计未flush的批次
	)
	log.Info("started stream writer with remote peer").Str(code.LocalId, cw.localID.Str()).
		Str(code.RemoteId, cw.peerID.Str()).Record()
	var unflushed int //为unflushed的字节数

	tickC := time.NewTicker(ConnReadTimeout / 3)
	defer tickC.Stop()
	for {
		select {
		case <-heartbeatC: //触发心跳信息
			err := enc.encode(&linkHeartbeatMessage)
			unflushed += linkHeartbeatMessage.Size()
			if err == nil {
				flusher.Flush()
				batched = 0
				unflushed = 0
				continue
			}
			//如果有异常，关闭streamWriter
			cw.status.deactivate(failureType{source: cw.peerID.Str(), action: "heartbeat"}, err.Error())
			cw.close()
			log.Warn("lost TCP streaming connection with remote peer").Str(code.LocalId, cw.localID.Str()).
				Str(code.RemoteId, cw.peerID.Str()).Record()
			//将heartbeatc和msgC两个通道清空，后续就不会在发送心跳消息和其他类型的消息了
			heartbeatC, msgC = nil, nil
		case m := <-msgC:
			err := enc.encode(m)
			if err == nil {
				unflushed += m.Size()
				if len(msgC) == 0 || batched > streamBufSize/2 {
					flusher.Flush()
					unflushed = 0
					batched = 0
				} else {
					batched++
				}
				continue
			}
			//异常情况处理
			cw.status.deactivate(failureType{source: cw.localID.Str(), action: "write"}, err.Error())
			cw.close()
			log.Warn("lost TCP streaming connection with remote peer").Str(code.LocalId, cw.localID.Str()).
				Str(code.RemoteId, cw.peerID.Str()).Record()
			heartbeatC, msgC = nil, nil
			cw.r.ReportUnreachable(m.To)
		case conn := <-cw.connC:
			cw.mu.Lock()
			//先关闭之前的连接如果存在
			closed := cw.closeUnlocked()
			if closed {
				log.Warn("closed TCP streaming connection with remote peer").Str(code.LocalId, cw.localID.Str()).
					Str(code.RemoteId, cw.peerID.Str()).Record()
			}
			//重新建立一个新的连接
			enc = &messageEncoderAndWriter{w: conn.Writer}
			flusher = conn.Flusher
			unflushed = 0
			cw.status.activate()
			cw.closer = conn.Closer
			cw.working = true
			cw.mu.Unlock()
			log.Warn("established TCP streaming connection with remote peer").Str(code.LocalId, cw.localID.Str()).
				Str(code.RemoteId, cw.peerID.Str()).Record()
			heartbeatC, msgC = tickC.C, cw.msgC
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
