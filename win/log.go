package win

import "go.uber.org/zap"

type log struct {
}

func (l *log) log() {
	zap.Logger{}
}
