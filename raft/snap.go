package raft

import (
	"github.com/ColdToo/Cold2DB/pb"
	"io"
)

type SnapshotStatus int

const (
	SnapshotFinish  SnapshotStatus = 1
	SnapshotFailure SnapshotStatus = 2
)

type Message struct {
	pb.Message
	ReadCloser io.ReadCloser
	TotalSize  int64
	closeC     chan bool
}

func NewMessage(rs pb.Message, rc io.ReadCloser, rcSize int64) *Message {
	return &Message{
		Message:   rs,
		TotalSize: int64(rs.Size()) + rcSize,
		closeC:    make(chan bool, 1),
	}
}

func (m Message) CloseNotify() <-chan bool {
	return m.closeC
}

func (m Message) CloseWithError(err error) {
	if cerr := m.ReadCloser.Close(); cerr != nil {
		err = cerr
	}
	if err == nil {
		m.closeC <- true
	} else {
		m.closeC <- false
	}
}
