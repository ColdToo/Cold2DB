package db

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/db/logfile"
	"sync"
	"sync/atomic"
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
	var logFile *logfile.LogFile
	if fid == hs.activeLogFile.Fid {
		logFile = hs.activeLogFile
	} else {
		hs.RLock()
		logFile = hs.logFiles[fid]
		if logFile != nil && logFile.IoSelector == nil {
			opt := hs.opt
			lf, err := logfile.OpenLogFile(opt.path, fid, opt.blockSize, logfile.ValueLog, opt.ioType)
			if err != nil {
				hs.RUnlock()
				return nil, err
			}
			hs.logFiles[fid] = lf
			logFile = lf
		}
		hs.RUnlock()
	}
	if logFile == nil {
		return nil, fmt.Errorf(ErrLogFileNil.Error(), fid)
	}

	entry, _, err := logFile.ReadLogEntry(offset)
	if err == logfile.ErrEndOfEntry {
		return &logfile.LogEntry{}, nil
	}
	return entry, err
}

func (hs *hardStateLog) Write(ent *logfile.LogEntry) (*valuePos, int, error) {
	hs.Lock()
	defer hs.Unlock()
	buf, eSize := logfile.EncodeEntry(ent)
	// if active is reach to thereshold, close it and open a new one.
	if hs.activeLogFile.WriteAt+int64(eSize) >= hs.opt.blockSize {
		if err := hs.Sync(); err != nil {
			return nil, 0, err
		}
		hs.logFiles[hs.activeLogFile.Fid] = hs.activeLogFile

		logFile, err := hs.createLogFile()
		if err != nil {
			return nil, 0, err
		}
		hs.activeLogFile = logFile
	}

	err := hs.activeLogFile.Write(buf)
	if err != nil {
		return nil, 0, err
	}

	writeAt := atomic.LoadInt64(&hs.activeLogFile.WriteAt)
	return &valuePos{
		Fid:    hs.activeLogFile.Fid,
		Offset: writeAt - int64(eSize),
	}, eSize, nil
}

func (hs *hardStateLog) Close() error {
	hs.Lock()
	defer hs.Unlock()
	return hs.hsLogFile.Close()
}
