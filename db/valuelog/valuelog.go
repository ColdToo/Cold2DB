package valuelog

import (
	"errors"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db"
)

var (
	ErrInvalidCrc = errors.New("valuelog: invalid crc")

	ErrWriteSizeNotEqual = errors.New("valuelog: write size is not equal to entry size")

	ErrEndOfEntry = errors.New("valuelog: end of entry in log file")

	ErrUnsupportedIoType = errors.New("unsupported io type")

	ErrUnsupportedValueLogType = errors.New("unsupported log file type")
)

const (
	PathSeparator = "/"

	SSTSuffixName = ".SST"
)

// ValueLog is an abstraction of a disk file, entry`s read and write will go through it.
type ValueLog struct {
	memFlushC chan *db.Memtable //memManger的flushChn
	partition *[]Partition
}

func OpenValueLog(dirPath config.ValueLogConfig) (lf *ValueLog, err error) {
	lf = &ValueLog{Fid: fid}
	fileName, err := lf.getValueLogName(dirPath, fid, ftype)
	if err != nil {
		return nil, err
	}

	var operator iooperator.IoOperator
	switch ioType {
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

func (lf *ValueLog) Read(key []byte) ([]byte, error) {
	//查找key对应所在分区在分区中进行查找
	return nil, nil
}

func (lf *ValueLog) Scan(low, high int64) error {
	//todo 各个partition扫描再聚合结果
	return nil
}

func (lf *ValueLog) Close() error {
	return lf.IoOperator.Close()
}

func (lf *ValueLog) Delete() error {
	return lf.IoOperator.Delete()
}

func (lf *ValueLog) ListenAndFlush() {
	for {
		select {
		case mem := <-lf.memFlushC:
			// 根据hash到不同的partition
			//mem.Flush()
			//lf.partition.activeSST.Append(mem.Entries)
			//lf.partition.activeSST.Flush()
			//lf.partition.activeSST.Close()
			//lf.partition.activeSST = nil
		}
	}
}
