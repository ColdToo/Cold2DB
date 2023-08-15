package db

import (
	"github.com/ColdToo/Cold2DB/db/logfile"
	"sync"
	"time"
)

type hardStateLog struct {
	sync.RWMutex
	opt       hLogOptions
	hsLogFile *logfile.LogFile
}

type hLogOptions struct {
	path string
}

func initHardStateLog(hardStateLogCfg HardStateLogConfig) (hsLog *hardStateLog, err error) {
	var ioType = logfile.DirectIO
	hLogOptions := hLogOptions{
		path: hardStateLogCfg.HardStateLogDir,
	}

	hsLog.hsLogFile, err = logfile.OpenLogFile(hLogOptions.path, time.Now().Unix(), 102, logfile.HardStateLog, ioType)
	if err != nil {
		return nil, err
	}
	return
}

func (hs *hardStateLog) Read(fid uint32, offset int64) (*logfile.LogEntry, error) {
	return nil, nil
}

func (hs *hardStateLog) Write(ent *logfile.LogEntry) (*valuePos, int, error) {
	return nil, 0, nil
}

func (hs *hardStateLog) Close() error {
	hs.Lock()
	defer hs.Unlock()
	return hs.hsLogFile.Close()
}
