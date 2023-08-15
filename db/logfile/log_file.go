package logfile

import (
	"errors"
	"github.com/ColdToo/Cold2DB/db/iooperator"
	"hash/crc32"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
)

var (
	ErrInvalidCrc = errors.New("logfile: invalid crc")

	ErrWriteSizeNotEqual = errors.New("logfile: write size is not equal to entry size")

	ErrEndOfEntry = errors.New("logfile: end of entry in log file")

	ErrUnsupportedIoType = errors.New("unsupported io type")

	ErrUnsupportedLogFileType = errors.New("unsupported log file type")
)

const (
	PathSeparator = string(os.PathSeparator)

	WalSuffixName = ".wal"

	VLogSuffixName = ".vlog"

	RaftHardStateSuffixName = ".raft"
)

// FileType represents different types of log file: wal and value log.
type FileType int8

const (
	// WAL write ahead log.
	WAL FileType = iota

	// ValueLog value log.
	ValueLog

	// HardStateLog persist raft status
	HardStateLog
)

// IOType represents different types of file io
type IOType int8

const (
	// BufferedIO standard file io.
	BufferedIO IOType = iota
	// MMap Memory Map.
	MMap
	// DirectIO bypass system page cache
	DirectIO
)

// LogFile is an abstraction of a disk file, entry`s read and write will go through it.
type LogFile struct {
	sync.RWMutex
	Fid        int64 //timestamp
	WriteAt    int64
	IoOperator iooperator.IoOperator
}

func OpenLogFile(path string, fid int64, fsize int64, ftype FileType, ioType IOType) (lf *LogFile, err error) {
	lf = &LogFile{Fid: fid}
	fileName, err := lf.getLogFileName(path, fid, ftype)
	if err != nil {
		return nil, err
	}

	var operator iooperator.IoOperator
	switch ioType {
	case BufferedIO:
		if operator, err = iooperator.NewFileIoOperator(fileName, fsize); err != nil {
			return
		}
	case MMap:
		if operator, err = iooperator.NewMMapIoOperator(fileName, fsize); err != nil {
			return
		}
	case DirectIO:
		if operator, err = iooperator.NewDirectorIoOperator(fileName, fsize); err != nil {
			return
		}
	default:
		return nil, ErrUnsupportedIoType
	}

	lf.IoOperator = operator
	return
}

func (lf *LogFile) ReadWALEntry(offset int64) (*WalEntry, int64, error) {
	//  todo 重构
	// read entry header.
	headerBuf, err := lf.readBytes(offset, MaxHeaderSize)
	if err != nil {
		return nil, 0, err
	}
	header, size := decodeHeader(headerBuf)
	// the end of entries.
	if header.crc32 == 0 && header.kSize == 0 && header.vSize == 0 {
		return nil, 0, ErrEndOfEntry
	}

	e := &WalEntry{
		ExpiredAt: header.expiredAt,
		Type:      header.typ,
	}
	kSize, vSize := int64(header.kSize), int64(header.vSize)
	var entrySize = size + kSize + vSize

	// read entry key and value.
	if kSize > 0 || vSize > 0 {
		kvBuf, err := lf.readBytes(offset+size, kSize+vSize)
		if err != nil {
			return nil, 0, err
		}
		e.Key = kvBuf[:kSize]
		e.Value = kvBuf[kSize:]
	}

	// crc32 check.
	if crc := getEntryCrc(e, headerBuf[crc32.Size:size]); crc != header.crc32 {
		return nil, 0, ErrInvalidCrc
	}
	return e, entrySize, nil
}

func (lf *LogFile) readBytes(offset, n int64) (buf []byte, err error) {
	buf = make([]byte, n)
	_, err = lf.IoOperator.Read(buf, offset)
	return
}

func (lf *LogFile) Read(offset int64, size uint32) ([]byte, error) {
	if size <= 0 {
		return []byte{}, nil
	}
	buf := make([]byte, size)
	if _, err := lf.IoOperator.Read(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

func (lf *LogFile) Write(buf []byte) error {
	if len(buf) <= 0 {
		return nil
	}
	offset := atomic.LoadInt64(&lf.WriteAt)
	n, err := lf.IoOperator.Write(buf, offset)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return ErrWriteSizeNotEqual
	}

	atomic.AddInt64(&lf.WriteAt, int64(n))
	return nil
}

func (lf *LogFile) Sync() error {
	return lf.IoOperator.Sync()
}

func (lf *LogFile) Close() error {
	return lf.IoOperator.Close()
}

func (lf *LogFile) Delete() error {
	return lf.IoOperator.Delete()
}

func (lf *LogFile) getLogFileName(path string, fid int64, ftype FileType) (fname string, err error) {
	fname = path + PathSeparator + strconv.FormatInt(fid, 10)
	switch ftype {
	case WAL:
		fname = fname + WalSuffixName
	case ValueLog:
		fname = fname + VLogSuffixName
	case HardStateLog:
		fname = fname + RaftHardStateSuffixName
	default:
		err = ErrUnsupportedLogFileType
	}
	return
}
