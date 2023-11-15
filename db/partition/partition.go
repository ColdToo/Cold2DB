package partition

import (
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/valyala/bytebufferpool"
	"hash/crc32"
	"os"
)

const (
	SSTSuffixName = ".SST"
	smallValue    = 256
)

// Partition 一个partition文件由一个index文件和多个sst文件组成
type Partition struct {
	dirPath   string
	activeSST *SST
	olderSST  []*SST
	pipeSST   chan *SST
	indexer   Indexer
}

type SST struct {
	fd     *os.File
	offset int64
}

func OpenPartition() (p *Partition) {
	go p.AutoCompaction()
	return
}

func (p *Partition) PersistKvs(kvs []*marshal.KV) {
	//每次都刷新到一个新的SST文件中？还是一个SST文件可以刷新一次两次或者三次？
	buf := bytebufferpool.Get()
	buf.Reset()
	defer func() {
		bytebufferpool.Put(buf)
	}()

	posChan := make(chan *IndexerNode, len(kvs))
	var fileCurrentOffset int
	go func() {
		select {
		case pos := <-posChan:
			p.indexer.Put(pos)
		}
	}()

	for _, kv := range kvs {
		vSize := len(kv.Value)
		meta := &IndexerNode{
			Key: kv.Key,
			Meta: &IndexerMeta{
				Fid:        p.activeSST.fd.Name(),
				Offset:     fileCurrentOffset,
				ValueSize:  vSize,
				valueCrc32: crc32.ChecksumIEEE(kv.Value),
				TimeStamp:  kv.TimeStamp,
				ExpiredAt:  kv.ExpiredAt,
			},
		}

		if vSize <= smallValue {
			meta.Meta.Value = kv.Value
			posChan <- meta
		}

		fileCurrentOffset += len(kv.Value)
		buf.Write(kv.Value)
		posChan <- meta
	}

	p.activeSST.fd.Write(buf.Bytes())
}

func (p *Partition) AutoCompaction() {
	p.Compaction()
}

func (p *Partition) Compaction() {

}

func (p *Partition) Get(key []byte) []byte {
	return nil
}

func (p *Partition) Scan(low, high []byte) {

}
