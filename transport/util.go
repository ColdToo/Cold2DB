package transport

import (
	"github.com/ColdToo/Cold2DB/pb"
	"strings"
)

func reportCriticalError(err error, errc chan<- error) {
	select {
	case errc <- err:
	default:
	}
}

func IsClosedConnError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "closed")
}

func isMsgApp(m *pb.Message) bool { return m.Type == pb.MsgApp }

func isMsgSnap(m *pb.Message) bool { return m.Type == pb.MsgSnap }
