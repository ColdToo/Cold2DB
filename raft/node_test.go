package raft

import (
	"context"
	"github.com/ColdToo/Cold2DB/pb"
	"testing"
)

// TestNodeStep ensures that node.Step sends msgProp to propC chan
// and other kinds of messages to receiveC chan.
func TestNodeStep(t *testing.T) {
	for i, msgn := range pb.MessageType_name {
		n := &raftNode{
			propC:    make(chan msgWithResult, 1),
			receiveC: make(chan pb.Message, 1),
		}
		msgt := pb.MessageType(i)
		n.Step(context.TODO(), pb.Message{Type: msgt})
		// Proposal goes to proc chan. Others go to recvc chan.
		if msgt == pb.MsgProp {
			select {
			case <-n.propC:
			default:
				t.Errorf("%d: cannot receive %s on propc chan", msgt, msgn)
			}
		} else {
			if IsLocalMsg(msgt) {
				select {
				case <-n.receiveC:
					t.Errorf("%d: step should ignore %s", msgt, msgn)
				default:
				}
			} else {
				select {
				case <-n.receiveC:
				default:
					t.Errorf("%d: cannot receive %s on recvc chan", msgt, msgn)
				}
			}
		}
	}
}

// TestNodePropose ensures that node.Propose sends the given proposal to the underlying raft.
func TestNodePropose(t *testing.T) {
}
