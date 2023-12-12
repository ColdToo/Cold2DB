package db

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/db/partition"
	"github.com/ColdToo/Cold2DB/db/wal"
	"github.com/ColdToo/Cold2DB/log"
	"os"
	"sync"
)

const PartitionFormat = "PARTITION_%d"

// ValueLog is an abstraction of a disk file, entry`s read and write will go through it.
type ValueLog struct {
	vlogCfg config.ValueLogConfig

	memFlushC chan *Memtable

	kvStateSeg *wal.KVStateSegment

	partition []*partition.Partition

	hashKeyFunction func([]byte) uint64
}

func OpenValueLog(vlogCfg config.ValueLogConfig, tableC chan *Memtable, stateSegment *wal.KVStateSegment) (lf *ValueLog, err error) {
	dirs, err := os.ReadDir(vlogCfg.ValueLogDir)
	if err != nil {
		log.Panicf("open wal dir failed", err)
	}

	partitions := make([]*partition.Partition, 0)
	lf = &ValueLog{memFlushC: tableC, kvStateSeg: stateSegment, partition: partitions, vlogCfg: vlogCfg}

	if len(dirs) == 0 {
		for i := 0; i < lf.vlogCfg.PartitionNums; i++ {
			p := partition.OpenPartition(fmt.Sprintf(PartitionFormat, i))
			partitions = append(partitions, p)
		}
	}

	//若vlog下有partition文件夹，则打开该文件夹下的partition以及所有sst文件和index文件
	if len(dirs) > 0 {
		for _, dir := range dirs {
			if dir.IsDir() {
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
		kvs := mem.All()
		partitionRecords := make([][]*marshal.KV, v.vlogCfg.PartitionNums)
		lastRecords := marshal.DecodeKV(kvs[len(kvs)-1].Value)

		for _, record := range kvs {
			p := v.getKeyPartition(record.Key)
			partitionRecords[p] = append(partitionRecords[p], marshal.DecodeKV(record.Key))
		}

		wg := &sync.WaitGroup{}
		for i := 0; i < v.vlogCfg.PartitionNums; i++ {
			if len(partitionRecords[i]) == 0 {
				continue
			}
			wg.Add(1)
			go v.partition[i].PersistKvs(partitionRecords[i], wg)
		}
		wg.Wait()

		v.kvStateSeg.PersistIndex = lastRecords.Data.Index
		err := v.kvStateSeg.Flush()
		if err != nil {
			log.Panicf("can not flush kv state segment file %e", err)
		}
	}
}

func (v *ValueLog) getKeyPartition(key []byte) int {
	return int(v.hashKeyFunction(key) % uint64(v.vlogCfg.PartitionNums))
}

func (v *ValueLog) Close() error {
	return nil
}

func (v *ValueLog) Delete() error {
	return nil
}
