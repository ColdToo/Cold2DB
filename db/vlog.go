package db

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/db/partition"
	"github.com/ColdToo/Cold2DB/db/wal"
	"github.com/ColdToo/Cold2DB/log"
	"os"
)

const PartitionFormat = "PARTITION_%d"

// ValueLog is an abstraction of a disk file, entry`s read and write will go through it.
type ValueLog struct {
	vlogCfg config.ValueLogConfig

	memFlushC chan *Memtable //memManger的flushChn

	kvStateSeg *wal.KVStateSegment

	partition []*partition.Partition

	// value log are partitioned to several parts for concurrent writing and reading
	partitionNums int

	// hash function for sharding
	hashKeyFunction func([]byte) uint64
}

func OpenValueLog(vlogCfg config.ValueLogConfig, tableC chan *Memtable, stateSegment *wal.KVStateSegment) (lf *ValueLog, err error) {
	dirs, err := os.ReadDir(vlogCfg.ValueLogDir)
	if err != nil {
		log.Panicf("open wal dir failed", err)
	}

	partitions := make([]*partition.Partition, 0)
	lf = &ValueLog{memFlushC: tableC, kvStateSeg: stateSegment, partition: partitions, partitionNums: vlogCfg.PartitionNums}

	if len(dirs) == 0 {
		for i := 0; i < lf.partitionNums; i++ {
			p := partition.OpenPartition(fmt.Sprintf(PartitionFormat, i))
			partitions = append(partitions, p)
		}
	}

	//若vlog下有partition文件夹，则打开该文件夹下的partition以及所有sst文件和index文件
	if len(dirs) > 0 {
		for _, dir := range dirs {
			if dir.IsDir() {
				//若vlog下有partition文件夹，则打开该文件夹下的partition以及所有sst文件和index文件
				p := partition.OpenPartition(dir.Name())
				partitions = append(partitions, p)
			}
		}
	}

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

		partitionRecords := make([][]*marshal.KV, v.partitionNums)

		var index uint64 //获取到该immtable中的最后一个index，刷新persist index
		for _, record := range kvs {
			p := v.getKeyPartition(record.Key)
			partitionRecords[p] = append(partitionRecords[p], &record)
		}

		for i := 0; i < int(v.partitionNums); i++ {
			if len(partitionRecords[i]) == 0 {
				continue
			}
			part := i

			go v.partition[part].PersistKvs(partitionRecords[part])
		}

		v.kvStateSeg.PersistIndex = index
		err := v.kvStateSeg.Flush()
		if err != nil {
			log.Panicf("can not flush kv state segment file %e", err)
		}
	}
}

func (v *ValueLog) getKeyPartition(key []byte) int {
	return int(v.hashKeyFunction(key) % uint64(v.partitionNums))
}

func (v *ValueLog) Close() error {
	return nil
}

func (v *ValueLog) Delete() error {
	return nil
}
