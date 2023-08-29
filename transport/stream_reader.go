package transport

import (
	"github.com/ColdToo/Cold2DB/log"
	"github.com/ColdToo/Cold2DB/pb"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"net"
	"sync"
	"time"
)

type streamReader struct {
	localId    types.ID
	peerID     types.ID
	peerIp     string
	peerStatus *peerStatus
	mu         sync.Mutex
	paused     bool

	enc      *messageDecoderAndReader
	receiveC chan<- *pb.Message //从peer中获取对端节点发送过来的消息，然后交给raft算法层进行处理，只接收非prop信息
	errorC   chan<- error
	stopC    chan struct{}
}

func startStreamReader(localID, peerId types.ID, peerStatus *peerStatus,
	receiveC chan *pb.Message, errC chan error, peerIp string) *streamReader {
	r := &streamReader{
		localId:    localID,
		peerID:     peerId,
		peerIp:     peerIp,
		peerStatus: peerStatus,
		receiveC:   receiveC,
		stopC:      make(chan struct{}),
		errorC:     errC,
	}
	go r.run()
	return r
}

func (cr *streamReader) run() {
	cr.enc = cr.dial()
	for {
		m, err := cr.enc.decodeAndRead()
		if err != nil {
			log.Errorf("failed read from conn", err)
			cr.dial()
			continue
		}
		cr.receiveC <- &m

		select {
		case <-cr.stopC:
			return
		}
	}
}

func (cr *streamReader) dial() *messageDecoderAndReader {
	for {
		time.Sleep(time.Second)
		log.Info("start dial remote peer").Str("from", cr.localId.Str()).Str("to", cr.peerID.Str()).
			Str("address", cr.peerIp).Record()

		Conn, err := net.Dial("tcp", cr.peerIp)
		if err != nil {
			log.Error("start dial remote peer").Str("from", cr.localId.Str()).Str("to", cr.peerID.Str()).
				Str("address", cr.peerIp).Record()
			continue
		}

		log.Info("dial remote peer success").Str("from", cr.localId.Str()).Str("to", cr.peerID.Str()).
			Str("address", cr.peerIp).Record()
		return &messageDecoderAndReader{Conn}
	}
}

func (cr *streamReader) stop() {
	cr.mu.Lock()
	cr.close()
	cr.mu.Unlock()
	<-cr.stopC
}

func (cr *streamReader) close() {
	if cr.enc != nil {
		if err := cr.enc.r.Close(); err != nil {
			log.Warn("failed to close remote peer connection").Str("local-member-id", cr.localId.Str()).
				Str("remote-peer-id", cr.peerID.Str()).Err("", err).Record()
		}
	}
	cr.enc = nil
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
