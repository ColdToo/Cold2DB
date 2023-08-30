package transport

import (
	"github.com/ColdToo/Cold2DB/pb"
	mock "github.com/ColdToo/Cold2DB/transport/mocks"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"github.com/golang/mock/gomock"
	"testing"
	"time"
)

var (
	message = &pb.Message{
		Type:       pb.MsgProp,
		To:         2,
		From:       1,
		Term:       4,
		LogTerm:    4,
		Index:      101,
		Commit:     100,
		Reject:     true,
		RejectHint: 100,
		Entries: []pb.Entry{
			{
				Term:  4,
				Index: 101,
				Type:  pb.EntryNormal,
				Data:  []byte{1, 2},
			},
		},
	}
)

func TestTransport_AddPeer(t *testing.T) {
	initLog()
	mockCtl := gomock.NewController(t)
	tr := mock.NewMockRaftTransport(mockCtl)
	tr.EXPECT().Process(message).Return(nil)
	transport := &Transport{
		LocalID:   types.ID(1),
		ClusterID: 0x1000,
		Raft:      tr,
		ErrorC:    make(chan error),
		Peers:     make(map[types.ID]Peer),
		StopC:     make(chan struct{}),
	}
	localIp := "127.0.0.1:7788"
	peersUrl := []string{localIp}

	go transport.ListenPeerAttachConn(localIp)

	for i := range peersUrl {
		transport.AddPeer(types.ID(2), peersUrl[i])
	}
	transport.Send([]*pb.Message{message})
	time.Sleep(10 * time.Second)
}
