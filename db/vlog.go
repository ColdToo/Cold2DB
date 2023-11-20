package db

import (
	"errors"
	"fmt"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/db/partition"
	"github.com/ColdToo/Cold2DB/db/wal"
	"github.com/ColdToo/Cold2DB/log"
	"os"
)

var (
	ErrInvalidCrc = errors.New("partition: invalid crc")

	ErrWriteSizeNotEqual = errors.New("partition: write size is not equal to entry size")

	ErrEndOfEntry = errors.New("partition: end of entry in log file")

	ErrUnsupportedIoType = errors.New("unsupported io type")

	ErrUnsupportedValueLogType = errors.New("unsupported log file type")
)

const PartitionFormat = "PARTITION_%d"

// ValueLog is an abstraction of a disk file, entry`s read and write will go through it.
type ValueLog struct {
	memFlushC chan *Memtable //memManger的flushChn

	stateSeg *wal.StateSegment

	partition []*partition.Partition

	// dirPath specifies the directory path where the WAL segment files will be stored.
	dirPath string

	// value log are partitioned to several parts for concurrent writing and reading
	partitionNum uint32

	// hash function for sharding
	hashKeyFunction func([]byte) uint64
}

func OpenValueLog(vlogCfg config.ValueLogConfig, tableC chan *Memtable, stateSegment *wal.StateSegment) (lf *ValueLog, err error) {
	dirs, err := os.ReadDir(vlogCfg.ValueLogDir)
	if err != nil {
		log.Panicf("open wal dir failed", err)
	}

	if len(dirs) == 0 {
		for i := 0; i < vlogCfg.PartitionNums; i++ {
			//检查文件夹下的partition重新打开
			//若vlog下有partition文件夹，则打开该文件夹下的partition以及所有sst文件和index文件
			partition.OpenPartition(fmt.Sprintf(PartitionFormat, i))
		}
	} else {
		for _, dir := range dirs {
			if dir.IsDir() {
				//检查文件夹下的partition重新打开
				//若vlog下有partition文件夹，则打开该文件夹下的partition以及所有sst文件和index文件
				partition.OpenPartition(dir.Name())
			}
		}
	}

	lf = &ValueLog{memFlushC: tableC, stateSeg: stateSegment}
	return
}

func (v *ValueLog) Get(key []byte) ([]byte, error) {
	//查找key对应所在分区在分区中进行查找
	p := v.getKeyPartition(key)
	v.partition[p].Get(key)
	return nil, nil
}

func (v *ValueLog) Scan(low, high []byte) error {
	//todo 各个partition按照该范围进行扫描再聚合结果
	for _, p := range v.partition {
		p.Scan(low, high)
	}
	return nil
}

func (v *ValueLog) ListenAndFlush() {
	for {
		mem := <-v.memFlushC
		//获取有序kvs
		kvs := mem.All()

		partitionRecords := make([][]*marshal.KV, v.partitionNum)

		var index int64 //获取到该immtable中的最后一个index，刷新persist index
		for _, record := range kvs {
			p := v.getKeyPartition(record.Key)
			partitionRecords[p] = append(partitionRecords[p], &record)
		}

		for i := 0; i < int(v.partitionNum); i++ {
			if len(partitionRecords[i]) == 0 {
				continue
			}
			part := i
			// todo 并发刷入每个partition
			go v.partition[part].PersistKvs(partitionRecords[part])
		}

		v.stateSeg.PersistIndex = index
		v.stateSeg.Flush()
	}
}

func (v *ValueLog) getKeyPartition(key []byte) int {
	return int(v.hashKeyFunction(key) % uint64(v.partitionNum))
}

func (v *ValueLog) Close() error {
	return nil
}

func (v *ValueLog) Delete() error {
	return nil
}
