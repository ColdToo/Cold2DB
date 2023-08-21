package transportHttp

import (
	"context"
	"fmt"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transportHttp/types"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"

	"go.etcd.io/etcd/pkg/httputil"
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

type outgoingConn struct {
	io.Writer
	http.Flusher
	io.Closer

	localID types.ID
	peerID  types.ID
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

	msgC  chan *pb.Message   //Peer会将待发送的消息写入到该通道，streamWriter则从该通道中读取消息并发送出去
	connC chan *outgoingConn //通过该通道获取当前streamWriter实例关联的底层网络连接
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
		connC:   make(chan *outgoingConn),
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
		flusher    http.Flusher     // 负责刷新底层连接，将缓冲区的数据发送出去
		batched    int              // 统计未flush的批次
	)

	tickc := time.NewTicker(ConnReadTimeout / 3)
	defer tickc.Stop()

	var unflushed int //为unflushed的字节数

	log.Info("started stream writer with remote peer").Str(code.LocalId, cw.localID.Str()).
		Str(code.RemoteId, cw.peerID.Str()).Record()

	for {
		select {
		case <-heartbeatC: //触发心跳信息
			err := enc.encode(&linkHeartbeatMessage)
			unflushed += linkHeartbeatMessage.Size()
			//若没有异常，则使用flusher将缓存的消息全部发送出去，并重置batched和unflushed两个统计变量
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

			/*
			   当其他节点(对端)主动与当前节点创建Stream消息通道时，会先通过StreamHandler的处理，
			   StreamHandler会通过attach()方法将连接写入对应的peer.writer.connc通道，
			   而当前的goroutine会通过该通道获取连接，然后开始发送消息
			*/

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
			heartbeatC, msgC = tickc.C, cw.msgC

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

// 提供一个可以获取连接的通道
func (cw *streamWriter) attach(conn *outgoingConn) bool {
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

type streamReader struct {
	localId types.ID
	peerID  types.ID
	peerUrl url.URL

	tr         *Transport
	peerStatus *peerStatus
	recvC      chan<- *pb.Message //从peer中获取对端节点发送过来的消息，然后交给raft算法层进行处理，只接收非prop信息
	propC      chan<- *pb.Message //只接收prop类消息

	errorC chan<- error

	mu     sync.Mutex
	paused bool
	closer io.Closer

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func (cr *streamReader) start() {
	cr.done = make(chan struct{})
	cr.errorC = cr.tr.ErrorC
	cr.ctx, cr.cancel = context.WithCancel(context.Background())
	go cr.run()
}

func startStreamReader(localID, peerId types.ID, status *peerStatus, cancel context.CancelFunc,
	t *Transport, recvC, propC chan *pb.Message, errC chan error) *streamReader {
	r := &streamReader{
		localId:    localID,
		peerID:     peerId,
		tr:         t,
		peerStatus: status,
		recvC:      recvC,
		propC:      propC,
		done:       make(chan struct{}),
		errorC:     errC,
		cancel:     cancel,
	}
	go r.run()
	return r
}

func (cr *streamReader) run() {
	log.Info("started stream reader with remote peer").Str(code.LocalId, cr.tr.LocalID.Str()).
		Str(code.RemoteId, cr.peerID.Str()).Record()

	for {
		//首先调用dial方法给对端发送一个GET请求，从而和对端建立长连接。对端的Header收到该连接后，会封装成outgoingConn结构，并发送给streamWriter，
		rc, err := cr.dial()
		if err != nil {
			cr.peerStatus.deactivate(failureType{source: cr.peerID.Str(), action: "dial"}, err.Error())
		} else { //如果未出现异常，则开始读取对端返回的消息，并将读取到的消息写入streamReader.recvc通道中
			cr.peerStatus.activate()
			log.Info("established TCP streaming connection with remote peer").Str(code.LocalId, cr.tr.LocalID.Str()).
				Str(code.RemoteId, cr.peerID.Str()).Record()

			//连接建立成功之后会调用decodeLoop方法来轮询读取对端节点发送过来的消息，并将收到的字节流转成raftpb.Message消息类型的格式。如果是保持连接的心跳类型则忽略。
			//如果是raftpb.MsgProp类型的消息，则发送到propc通道中，如果是其他消息则发送到recvc通道中
			err = cr.decodeLoop(rc) //轮询读取消息

			switch {
			case err == io.EOF:
			case IsClosedConnError(err):
			default:
				cr.peerStatus.deactivate(failureType{source: cr.peerID.Str(), action: "read"}, err.Error())
			}
		}
	}
}

func (cr *streamReader) decodeLoop(rc io.ReadCloser) error {
	var dec decoder
	cr.mu.Lock()

	dec = &messageDecoder{r: rc}

	select {
	case <-cr.ctx.Done():
		cr.mu.Unlock()
		if err := rc.Close(); err != nil {
			return err
		}
		return io.EOF
	default:
		cr.closer = rc
	}

	cr.mu.Unlock()

	for {
		m, err := dec.decode()
		if err != nil {
			cr.mu.Lock()
			cr.close()
			cr.mu.Unlock()
			return err
		}

		cr.mu.Lock()
		paused := cr.paused
		cr.mu.Unlock()

		if paused {
			continue
		}

		if isLinkHeartbeatMessage(&m) {
			// 忽略掉用于维护长连接的心跳信息
			continue
		}

		recvc := cr.recvC
		if m.Type == pb.MsgProp {
			recvc = cr.propC
		}

		select {
		case recvc <- &m:
		default:
			if cr.peerStatus.isActive() {
				log.Warn("dropped internal Raft message since receiving buffer is full (overloaded network)").
					Str("message-type", m.Type.String()).
					Str("local-member-id", cr.localId.Str()).
					Str("from", types.ID(m.From).Str()).
					Str("remote-peer-id", types.ID(m.To).Str()).
					Bool("remote-peer-active", cr.peerStatus.isActive()).Record()
			}
		}
	}
}

func (cr *streamReader) stop() {
	cr.mu.Lock()
	cr.cancel()
	cr.close()
	cr.mu.Unlock()
	<-cr.done
}

func (cr *streamReader) dial() (io.ReadCloser, error) {
	log.Debug("dial stream reader").
		Str("from", cr.tr.LocalID.Str()).
		Str("to", cr.peerID.Str()).
		Str("address", cr.peerUrl.String()).Record()

	req, err := http.NewRequest("GET", cr.peerUrl.String(), nil)
	req.Header.Set("X-Server-From", cr.tr.LocalID.Str())
	req.Header.Set("X-Etcd-Cluster-ID", cr.tr.ClusterID.Str())
	req.Header.Set("X-Raft-To", cr.peerID.Str())
	req = req.WithContext(cr.ctx)
	setPeerURLsHeader(req, cr.tr.URLs)

	cr.mu.Lock()
	select {
	case <-cr.ctx.Done():
		cr.mu.Unlock()
		return nil, fmt.Errorf("stream reader is stopped")
	default:
	}
	cr.mu.Unlock()

	resp, err := cr.tr.streamRt.RoundTrip(req)
	if err != nil {
	}

	switch resp.StatusCode {
	case http.StatusGone:
		httputil.GracefulClose(resp)
		reportCriticalError(errMemberRemoved, cr.errorC)
		return nil, errMemberRemoved

	case http.StatusOK:
		return resp.Body, nil

	case http.StatusNotFound:
		httputil.GracefulClose(resp)
		return nil, fmt.Errorf("peer %s failed to find local node %s", cr.peerID, cr.tr.LocalID)

	case http.StatusPreconditionFailed:
		_, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		httputil.GracefulClose(resp)

	default:
		httputil.GracefulClose(resp)
		return nil, fmt.Errorf("unhandled http status %d", resp.StatusCode)
	}
	return resp.Body, nil
}

func (cr *streamReader) close() {
	if cr.closer != nil {
		if err := cr.closer.Close(); err != nil {
			log.Warn("failed to close remote peer connection").
				Str("local-member-id", cr.tr.LocalID.Str()).
				Str("remote-peer-id", cr.peerID.Str()).
				Err("", err).Record()
		}
	}
	cr.closer = nil
}

func (cr *streamReader) pause() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = true
}

func (cr *streamReader) resume() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = false
}
