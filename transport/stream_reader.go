package transport

import (
	"context"
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"io"
	"net"
	"sync"
)

type streamReader struct {
	localId types.ID
	peerID  types.ID
	peerIp  string

	enc        msgDecodeRead
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

func startStreamReader(localID, peerId types.ID, status *peerStatus,
	recvC, propC chan *pb.Message, errC chan error, peerIp string) *streamReader {
	r := &streamReader{
		localId:    localID,
		peerID:     peerId,
		peerIp:     peerIp,
		peerStatus: status,
		recvC:      recvC,
		propC:      propC,
		done:       make(chan struct{}),
		errorC:     errC,
	}
	go r.run()
	return r
}

func (cr *streamReader) run() {
	var err error
	log.Info("started stream reader with remote peer").Str(code.LocalId, cr.localId.Str()).
		Str(code.RemoteId, cr.peerID.Str()).Record()
	for {
		cr.enc, err = cr.dial()
		if err != nil {
			cr.peerStatus.deactivate(failureType{source: cr.peerID.Str(), action: "dial"}, err.Error())
			continue
		}
		cr.peerStatus.activate()
		log.Info("established TCP streaming connection with remote peer start read loop").Str(code.LocalId, cr.localId.Str()).
			Str(code.RemoteId, cr.peerID.Str()).Record()
		cr.decodeLoop() //轮询读取消息
	}
}

func (cr *streamReader) decodeLoop() {
	for {
		m, err := cr.enc.decodeAndRead()
		if err != nil {
			log.Errorf("", err)
		}

		recvC := cr.recvC
		if m.Type == pb.MsgProp {
			recvC = cr.propC
		}

		select {
		case recvC <- &m:
		case <-cr.done:
			return
		default:
			if cr.peerStatus.isActive() {
				log.Warn("dropped internal Raft message since receiving buffer is full (overloaded network)").
					Str("message-type", m.Type.String()).
					Str("local-member-id", cr.localId.Str()).
					Str("remote-peer-id", types.ID(m.From).Str()).
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

func (cr *streamReader) dial() (msgDecodeRead, error) {
	log.Debug("start dial remote peer").Str("from", cr.localId.Str()).Str("to", cr.peerID.Str()).
		Str("address", cr.peerIp).Record()

	dail := net.Dialer{Timeout: 10, KeepAlive: 0}
	Conn, err := dail.Dial("tcp", cr.peerIp)
	if err != nil {
		return nil, err
	}

	log.Debug("dial remote peer success").Str("from", cr.localId.Str()).Str("to", cr.peerID.Str()).
		Str("address", cr.peerIp).Record()

	return &messageDecoderAndReader{Conn}, nil
}

func (cr *streamReader) close() {
	if cr.closer != nil {
		if err := cr.closer.Close(); err != nil {
			log.Warn("failed to close remote peer connection").Str("local-member-id", cr.localId.Str()).
				Str("remote-peer-id", cr.peerID.Str()).Err("", err).Record()
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
