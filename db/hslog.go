package db

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/db/logfile"
	"sync"
	"sync/atomic"
)

type hardStateLog struct {
	sync.RWMutex
	opt vlogOptions
}

type hLogOptions struct {
	path string
}

func initHardStateLog(hardStateLogCfg HardStateLogConfig) error {
	var ioType = logfile.DirectIO

	hLogOptions := hLogOptions{
		path: hardStateLogCfg.HardStateLogDir,
	}

	logFile, err := logfile.OpenLogFile(vlogOpt.path, int64(fids[len(fids)-1]), vlogOpt.blockSize, logfile.ValueLog, vlogOpt.ioType)

	vlog := &HardStateLog{
		opt:           vlogOpt,
		activeLogFile: logFile,
		logFiles:      make(map[uint32]*logfile.LogFile),
		discard:       discard,
	}

	// load other log files when reading from it.
	for i := 0; i < len(fids)-1; i++ {
		vlog.logFiles[fids[i]] = &logfile.LogFile{Fid: fids[i]}
	}
	return nil
}

func (vlog *valueLog) Read(fid uint32, offset int64) (*logfile.LogEntry, error) {
	var logFile *logfile.LogFile
	if fid == vlog.activeLogFile.Fid {
		logFile = vlog.activeLogFile
	} else {
		vlog.RLock()
		logFile = vlog.logFiles[fid]
		if logFile != nil && logFile.IoSelector == nil {
			opt := vlog.opt
			lf, err := logfile.OpenLogFile(opt.path, fid, opt.blockSize, logfile.ValueLog, opt.ioType)
			if err != nil {
				vlog.RUnlock()
				return nil, err
			}
			vlog.logFiles[fid] = lf
			logFile = lf
		}
		vlog.RUnlock()
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

func (vlog *valueLog) Write(ent *logfile.LogEntry) (*valuePos, int, error) {
	vlog.Lock()
	defer vlog.Unlock()
	buf, eSize := logfile.EncodeEntry(ent)
	// if active is reach to thereshold, close it and open a new one.
	if vlog.activeLogFile.WriteAt+int64(eSize) >= vlog.opt.blockSize {
		if err := vlog.Sync(); err != nil {
			return nil, 0, err
		}
		vlog.logFiles[vlog.activeLogFile.Fid] = vlog.activeLogFile

		logFile, err := vlog.createLogFile()
		if err != nil {
			return nil, 0, err
		}
		vlog.activeLogFile = logFile
	}

	err := vlog.activeLogFile.Write(buf)
	if err != nil {
		return nil, 0, err
	}

	writeAt := atomic.LoadInt64(&vlog.activeLogFile.WriteAt)
	return &valuePos{
		Fid:    vlog.activeLogFile.Fid,
		Offset: writeAt - int64(eSize),
	}, eSize, nil
}

func (vlog *valueLog) Close() error {
	if vlog.activeLogFile == nil {
		return ErrActiveLogFileNil
	}

	vlog.activeLogFile.Lock()
	defer vlog.activeLogFile.Unlock()
	return vlog.activeLogFile.Close()
}
