package partition

import (
	"github.com/ColdToo/Cold2DB/db/iooperator/bufio"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/valyala/bytebufferpool"
	"hash/crc32"
	"log"
	"os"
	"strings"
)

const (
	SSTFileSuffixName = ".SST"
	smallValue        = 128
)

// Partition 一个partition文件由一个index文件和多个sst文件组成
type Partition struct {
	dirPath   string
	frozenSST []*SST
	pipeSST   chan *SST
	indexer   Indexer
	SSTmap    map[string]SST
}
}

type SST struct {
	fd     *os.File
	fName  string
	offset int64
}

func OpenPartition(partitionDir string) (p *Partition) {
	p = &Partition{
		dirPath: partitionDir,
		indexer: nil,
	}

	files, err := os.ReadDir(partitionDir)
	if err != nil {
		log.Panicf("open partition dir failed", err)
	}
	for _, file := range files {
		switch {
		case strings.HasSuffix(file.Name(), indexFileSuffixName):
			p.indexer, err = NewIndexer(file.Name())
		case strings.HasSuffix(file.Name(), SSTFileSuffixName):
			p.frozenSST = append(p.frozenSST, &SST{
				fd:     bufio.OpenBufferedFile(file.Name()),
				fName:  file.Name(),
				offset: 0,
			})
		}
	}

	sstPipe := make(chan *SST, 0)
	go func() {
		sstPipe <- &SST{
			fd:     bufio.OpenBufferedFile(p.dirPath + "/" + p.dirPath + SSTFileSuffixName),
			fName:  p.dirPath + "/" + p.dirPath + SSTFileSuffixName,
			offset: 0,
		}
	}()
	p.pipeSST = sstPipe

	go p.AutoCompaction()
	return
}

func (p *Partition) Get(key []byte) []byte {
	return nil
}

func (p *Partition) Scan(low, high []byte) {

}

func (p *Partition) PersistKvs(kvs []*marshal.KV) {
	//每次都刷新到一个新的SST文件中？还是一个SST文件可以刷新一次两次或者三次？
	buf := bytebufferpool.Get()
	buf.Reset()
	defer func() {
		bytebufferpool.Put(buf)
	}()

	sst := <-p.pipeSST

	indexNodes := make([]*IndexerNode,0)
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
				Fid:        fd.Name(),
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
	}

	p.indexer.Put(pos)

	sst.fd.Write(buf.Bytes())
	p.frozenSST = append(p.frozenSST)
}

func (p *Partition) AutoCompaction() {
	//根据策略进行compation
	p.Compaction()
}

func (p *Partition) Compaction() {

}
