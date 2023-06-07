package domain

import "go.uber.org/zap"

var log *zap.Logger

type LogOp interface {
	Record()
}

type Log struct {
	log *zap.Logger
}

func InitLog() {
	log = new(zap.Logger)
}

func (l Log) Record() {

}
