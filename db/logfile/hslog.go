package logfile

import (
	"github.com/ColdToo/Cold2DB/config"
	"sync"
	"time"
)

type hardStateLog struct {
	sync.RWMutex
	opt       hLogOptions
	hsLogFile *LogFile
}

type hLogOptions struct {
	path string
}

func initHardStateLog(hardStateLogCfg config.HardStateLogConfig) (hsLog *hardStateLog, err error) {
	var ioType = DirectIO
	hLogOptions := hLogOptions{
		path: hardStateLogCfg.HardStateLogDir,
	}
	hsLog = new(hardStateLog)
	hsLog.hsLogFile, err = OpenLogFile(hLogOptions.path, time.Now().Unix(), 102, HardStateLog, ioType)
	if err != nil {
		return nil, err
	}
	return
}

func (hs *hardStateLog) Read(fid uint32, offset int64) (*LogEntry, error) {
	return nil, nil
}

func (hs *hardStateLog) Write(ent *LogEntry) (int, error) {
	return 0, nil
}

func (hs *hardStateLog) Close() error {
	hs.Lock()
	defer hs.Unlock()
	return hs.hsLogFile.Close()
}
