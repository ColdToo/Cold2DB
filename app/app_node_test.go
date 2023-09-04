package main

import (
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/log"
	mock "github.com/ColdToo/Cold2DB/mocks"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/ColdToo/Cold2DB/raft"
	"github.com/golang/mock/gomock"
	"testing"
	"time"
)

var (
	ready1 = raft.Ready{
		SoftState: raft.SoftState{},
		HardState: pb.HardState{},
		Messages:  []*pb.Message{message1, message2},
	}

	entries = []pb.Entry{
		{
			Term:  4,
			Index: 101,
			Type:  pb.EntryNormal,
			Data:  []byte{1, 2, 3},
		},
		{
			Term:  4,
			Index: 101,
			Type:  pb.EntryNormal,
			Data:  []byte{1, 2, 3},
		},
	}

	message1 = &pb.Message{
		Type:       pb.MsgProp,
		To:         1,
		From:       2,
		Term:       4,
		LogTerm:    4,
		Index:      101,
		Commit:     100,
		Reject:     true,
		RejectHint: 100,
		Entries:    entries,
	}

	message2 = &pb.Message{
		Type:       pb.MsgProp,
		To:         11,
		From:       21,
		Term:       41,
		LogTerm:    41,
		Index:      102,
		Commit:     100,
		Reject:     true,
		RejectHint: 100,
		Entries:    entries,
	}
)

func initLog() {
	cfg := &config.ZapConfig{
		Level:         "debug",
		Format:        "console",
		Prefix:        "[Cold2DB]",
		Director:      "./log",
		ShowLine:      true,
		EncodeLevel:   "LowercaseColorLevelEncoder",
		StacktraceKey: "stacktrace",
		LogInConsole:  true,
	}
	log.InitLog(cfg)
}

func TestAppNode_ServePropCAndConfC(t *testing.T) {
	initLog()
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()
	raft := mock.NewMockRaftLayer(mockCtl)
	// 创建测试所需的相关对象和变量
	an := &AppNode{
		proposeC:    make(chan []byte),
		confChangeC: make(chan pb.ConfChange),
		raftNode:    raft,
	}
	raft.EXPECT().ProposeConfChange(pb.ConfChange{ID: 1}).Return(nil)
	raft.EXPECT().Propose([]byte("test propose")).Return(nil)
	// 启动servePropCAndConfC函数作为goroutine
	go an.servePropCAndConfC()

	// 模拟发送proposeC和confChangeC的消息
	an.proposeC <- []byte("test propose")
	an.confChangeC <- pb.ConfChange{ID: 1}

	// 等待一段时间以确保goroutine有足够的时间处理消息
	time.Sleep(time.Millisecond * 100)
}

func TestAppNode_ServeRaftNode(t *testing.T) {
	initLog()
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()
	raftI := mock.NewMockRaftLayer(mockCtl)
	transI := mock.NewMockTransporter(mockCtl)
	readyC := make(chan raft.Ready)
	readyC <- ready1

	raftI.EXPECT().Tick()
	raftI.EXPECT().GetErrorC().Return(make(chan error))
	raftI.EXPECT().GetReadyC().Return(readyC)
	raftI.EXPECT().Advance()
	transI.EXPECT().GetErrorC().Return(make(chan error))
	transI.EXPECT().Send(ready1.Messages)
	an := &AppNode{
		raftNode:  raftI,
		transport: transI,
	}

	an.serveRaftNode(1)
}
