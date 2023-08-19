package transport

import (
	"context"
	"fmt"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/pkg/httputil"
	"go.etcd.io/etcd/version"

	"go.uber.org/zap"
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

	status *peerStatus
	r      RaftTransport

	mu      sync.Mutex // guard field working and closer
	closer  io.Closer
	working bool

	msgC  chan *pb.Message   //Peer会将待发送的消息写入到该通道，streamWriter则从该通道中读取消息并发送出去
	connC chan *outgoingConn //通过该通道获取当前streamWriter实例关联的底层网络连接，  outgoingConn其实是对网络连接的一层封装，其中记录了当前连接使用的协议版本，以及用于关闭连接的Flusher和Closer等信息。
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

// 从管道中获取msg
func (cw *streamWriter) run() {
	var (
		msgc       chan *pb.Message
		heartbeatc <-chan time.Time // 定时器会定时向该通道发送信号，触发心跳消息的发送，该心跳消息与后台介绍的Raft的心跳消息有所不同，该心跳的主要目的是为了防止连接长时间不用断开的
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
		case <-heartbeatc: //触发心跳信息
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
			cw.status.deactivate(failureType{source: t.String(), action: "heartbeat"}, err.Error())

			cw.close()

			log.Warn("lost TCP streaming connection with remote peer").Str(code.LocalId, cw.localID.Str()).
				Str(code.RemoteId, cw.peerID.Str()).Record()

			//将heartbeatc和msgc两个通道清空，后续就不会在发送心跳消息和其他类型的消息了
			heartbeatc, msgc = nil, nil

		case m := <-msgc:
			err := enc.encode(m)
			if err == nil {
				unflushed += m.Size()

				if len(msgc) == 0 || batched > streamBufSize/2 {
					flusher.Flush()
					unflushed = 0
					batched = 0
				} else {
					batched++
				}

				continue
			}

			//异常情况处理
			cw.status.deactivate(failureType{source: t.String(), action: "write"}, err.Error())
			cw.close()
			log.Warn("lost TCP streaming connection with remote peer").Str(code.LocalId, cw.localID.Str()).
				Str(code.RemoteId, cw.peerID.Str()).Record()
			heartbeatc, msgc = nil, nil
			cw.r.ReportUnreachable(m.To)

			/*

			   当其他节点(对端)主动与当前节点创建Stream消息通道时，会先通过StreamHandler的处理，

			   StreamHandler会通过attach()方法将连接写入对应的peer.writer.connc通道，

			   而当前的goroutine会通过该通道获取连接，然后开始发送消息

			*/

		case conn := <-cw.connC:
			cw.mu.Lock()

			closed := cw.closeUnlocked()

			enc = &messageEncoderAndWriter{w: conn.Writer}

			//获取底层连接的flusher
			flusher = conn.Flusher
			unflushed = 0
			cw.status.activate()
			cw.closer = conn.Closer
			cw.working = true
			cw.mu.Unlock()

			if closed {
				log.Warn("closed TCP streaming connection with remote peer").Str(code.LocalId, cw.localID.Str()).
					Str(code.RemoteId, cw.peerID.Str()).Record()
			}

			log.Warn("established TCP streaming connection with remote peer").Str(code.LocalId, cw.localID.Str()).
				Str(code.RemoteId, cw.peerID.Str()).Record()

			heartbeatc, msgc = tickc.C, cw.msgc

		case <-cw.stopc:
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
		cw.lg.Warn(
			"failed to close connection with remote peer",
			zap.String("remote-peer-id", cw.peerID.Str()),
			zap.Error(err),
		)
	}
	if len(cw.msgc) > 0 {
		cw.r.ReportUnreachable(uint64(cw.peerID))
	}
	cw.msgc = make(chan *pb.Message, streamBufSize)
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
	lg *zap.Logger

	peerID types.ID

	tr         *Transport
	picker     *urlPicker
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

func (cr *streamReader) run() {
	log.Info("started stream reader with remote peer").Str(code.LocalId, cr.tr.LocalID.Str()).
		Str(code.RemoteId, cr.peerID.Str()).Record()

	for {
		//首先调用dial方法给对端发送一个GET请求，从而和对端建立长连接。对端的Header收到该连接后，会封装成outgoingConn结构，并发送给streamWriter，
		rc, err := cr.dial()
		if err != nil {
			cr.peerStatus.deactivate(failureType{source: t.String(), action: "dial"}, err.Error())
		} else { //如果未出现异常，则开始读取对端返回的消息，并将读取到的消息写入streamReader.recvc通道中
			cr.peerStatus.activate()
			log.Info("established TCP streaming connection with remote peer").Str(code.LocalId, cr.tr.LocalID.Str()).
				Str(code.RemoteId, cr.peerID.Str()).Record()

			//连接建立成功之后会调用decodeLoop方法来轮询读取对端节点发送过来的消息，并将收到的字节流转成raftpb.Message消息类型的格式。如果是保持连接的心跳类型则忽略。
			//如果是raftpb.MsgProp类型的消息，则发送到propc通道中，如果是其他消息则发送到recvc通道中
			err = cr.decodeLoop(rc) //轮询读取消息
			cr.lg.Warn(
				"lost TCP streaming connection with remote peer",
				zap.String("local-member-id", cr.tr.LocalID.Str()),
				zap.String("remote-peer-id", cr.peerID.Str()),
				zap.Error(err),
			)
			switch {
			case err == io.EOF:
			case IsClosedConnError(err):
			default:
				cr.peerStatus.deactivate(failureType{source: t.String(), action: "read"}, err.Error())
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

	// gofail: labelRaftDropHeartbeat:
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
				cr.lg.Warn(
					"dropped internal Raft message since receiving buffer is full (overloaded network)",
					zap.String("message-type", m.Type.String()),
					zap.String("local-member-id", cr.tr.LocalID.Str()),
					zap.String("from", types.ID(m.From).Str()),
					zap.String("remote-peer-id", types.ID(m.To).Str()),
					zap.Bool("remote-peer-active", cr.peerStatus.isActive()),
				)
			} else {
				cr.lg.Warn(
					"dropped Raft message since receiving buffer is full (overloaded network)",
					zap.String("message-type", m.Type.String()),
					zap.String("local-member-id", cr.tr.LocalID.Str()),
					zap.String("from", types.ID(m.From).Str()),
					zap.String("remote-peer-id", types.ID(m.To).Str()),
					zap.Bool("remote-peer-active", cr.peerStatus.isActive()),
				)
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
	u := cr.picker.pick() //获取对端节点暴露的一个url

	cr.lg.Debug(
		"dial stream reader",
		zap.String("from", cr.tr.LocalID.Str()),
		zap.String("to", cr.peerID.Str()),
		zap.String("address", u.String()),
	)

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("failed to make http request to %v (%v)", u, err)
	}

	req.Header.Set("X-Server-From", cr.tr.LocalID.Str())
	req.Header.Set("X-Server-Version", version.Version)
	req.Header.Set("X-Min-Cluster-Version", version.MinClusterVersion)
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
		cr.picker.unreachable(u)
		return nil, err
	}

	switch resp.StatusCode {
	case http.StatusGone:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		reportCriticalError(errMemberRemoved, cr.errorC)
		return nil, errMemberRemoved

	case http.StatusOK:
		return resp.Body, nil

	case http.StatusNotFound:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("peer %s failed to find local node %s", cr.peerID, cr.tr.LocalID)

	case http.StatusPreconditionFailed:
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			cr.picker.unreachable(u)
			return nil, err
		}
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)

		switch strings.TrimSuffix(string(b), "\n") {
		case errIncompatibleVersion.Error():
			cr.lg.Warn(
				"request sent was ignored by remote peer due to server version incompatibility",
				zap.String("local-member-id", cr.tr.LocalID.Str()),
				zap.String("remote-peer-id", cr.peerID.Str()),
				zap.Error(errIncompatibleVersion),
			)
			return nil, errIncompatibleVersion

		case errClusterIDMismatch.Error():
			cr.lg.Warn(
				"request sent was ignored by remote peer due to cluster ID mismatch",
				zap.String("remote-peer-id", cr.peerID.Str()),
				zap.String("remote-peer-cluster-id", resp.Header.Get("X-Etcd-Cluster-ID")),
				zap.String("local-member-id", cr.tr.LocalID.Str()),
				zap.String("local-member-cluster-id", cr.tr.ClusterID.Str()),
				zap.Error(errClusterIDMismatch),
			)
			return nil, errClusterIDMismatch

		default:
			return nil, fmt.Errorf("unhandled error %q when precondition failed", string(b))
		}

	default:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("unhandled http status %d", resp.StatusCode)
	}
}

func (cr *streamReader) close() {
	if cr.closer != nil {
		if err := cr.closer.Close(); err != nil {
			if cr.lg != nil {
				cr.lg.Warn(
					"failed to close remote peer connection",
					zap.String("local-member-id", cr.tr.LocalID.Str()),
					zap.String("remote-peer-id", cr.peerID.Str()),
					zap.Error(err),
				)
			}
		}
		cr.closer = nil
	}
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
