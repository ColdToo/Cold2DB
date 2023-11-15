package partition

import (
	"github.com/ColdToo/Cold2DB/db/marshal"
	"os"
)

const (
	PathSeparator = "/"

	SSTSuffixName = ".SST"
)

// Partition 一个partition文件由一个index文件和多个sst文件组成
type Partition struct {
	dirPath   string
	activeSST *SST
	olderSST  []*SST
	pipeSST   chan *SST
	index     Indexer
}

type SST struct {
	fd *os.File
}

func (s *SST) Write(kvs []*marshal.KV) {

}

func OpenPartition() (p *Partition) {
	return
}

func (p *Partition) PersistKvs(kvs []*marshal.KV) {
	//bufferd io直接刷盘或者direct io维护一个内存buffer？

	//异步将kvs的index通过chan写入到index文件中，构建一个index

	//将kvs刷入到sst中且通过chanel将index刷入到index文件中
	posChan := make(chan []*IndexerMeta, len(kvs))

	// channel for receiving the positions of the records after writing to the value log
}

func (p *Partition) AutoCompaction() {

}

func (p *Partition) Compaction() {

}

func (p *Partition) Read() {

}

func (p *Partition) Scan(low, high []byte) {

}
