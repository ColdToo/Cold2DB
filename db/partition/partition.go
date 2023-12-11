package partition

import (
	"github.com/ColdToo/Cold2DB/db/iooperator/bufio"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/valyala/bytebufferpool"
	"hash/crc32"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	SSTFileSuffixName = ".SST"
	smallValue        = 128
)

// Partition 一个partition文件由一个index文件和多个sst文件组成
type Partition struct {
	dirPath string
	oldSST  []*SST
	pipeSST chan *SST
	indexer Indexer
	SSTmap  map[int]SST
}

type SST struct {
	fd     *os.File
	fName  string
	offset int64
}

func OpenPartition(partitionDir string) (p *Partition) {
	p = &Partition{
		dirPath: partitionDir,
	}

	files, err := os.ReadDir(partitionDir)
	if err != nil {
		log.Panicf("open partition dir failed %e", err)
	}

	for _, file := range files {
		switch {
		case strings.HasSuffix(file.Name(), indexFileSuffixName):
			p.indexer, err = NewIndexer(file.Name())
		case strings.HasSuffix(file.Name(), SSTFileSuffixName):
			p.oldSST = append(p.oldSST, &SST{
				fd:     bufio.OpenBufferedFile(file.Name()),
				fName:  file.Name(),
				offset: 0,
			})
		}
	}

	//sst pipeline for speed
	sstPipe := make(chan *SST, 1)
	go func() {
		sstPipe <- &SST{
			fd:     bufio.OpenBufferedFile(p.dirPath + time.Now().String() + SSTFileSuffixName),
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

func (p *Partition) PersistKvs(kvs []*marshal.KV, wg *sync.WaitGroup) {
	//todo 每次都刷新到一个新的SST文件中？还是一个SST文件可以刷新一次两次或者三次？
	buf := bytebufferpool.Get()
	buf.Reset()
	defer func() {
		bytebufferpool.Put(buf)
	}()

	sst := <-p.pipeSST
	fid, _ := strconv.Atoi(sst.fd.Name())

	indexNodes := make([]*IndexerNode, 0)

	var fileCurrentOffset int
	for _, kv := range kvs {
		vSize := len(kv.VBytes)
		value := marshal.DecodeV(kv.VBytes)
		meta := &IndexerNode{
			Key: kv.Key,
			Meta: &IndexerMeta{
				Fid:         fid,
				ValueOffset: fileCurrentOffset,
				ValueSize:   vSize,
				valueCrc32:  crc32.ChecksumIEEE(kv.VBytes),
				TimeStamp:   value.TimeStamp,
				ExpiredAt:   value.ExpiredAt,
			},
		}

		if vSize <= smallValue {
			meta.Meta.Value = kv.VBytes
		}

		fileCurrentOffset += len(kv.VBytes)
		buf.Write(kv.VBytes)

		//todo 是否需要实现为异步写入的方式？
		indexNodes = append(indexNodes, meta)
	}

	p.indexer.Put(indexNodes)
	sst.fd.Write(buf.Bytes())
	p.oldSST = append(p.oldSST)
	wg.Done()
}

func (p *Partition) AutoCompaction() {
	//todo compaction策略
	p.Compaction()
}

func (p *Partition) Compaction() {

}
