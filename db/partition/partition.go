package partition

import (
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/db/iooperator/bufio"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/valyala/bytebufferpool"
	"hash/crc32"
	"io"
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

type SST struct {
	fd      *os.File
	fName   string
	offset  int64
	SSTSize int64
}

func NewSST(fileName string) (*SST, error) {
	return &SST{
		fd:     bufio.OpenBufferedFile(fileName),
		fName:  fileName,
		offset: 0,
	}, nil
}

func (s *SST) Write(buf []byte) (err error) {
	_, err = s.fd.Write(buf)
	if err != nil {
		return err
	}
	return
}

func (s *SST) Read(vSize, vOffset int64) (buf []byte, err error) {
	s.fd.Seek(vOffset, io.SeekStart)
	buf = make([]byte, vSize)
	_, err = s.fd.Read(buf)
	if err != nil {
		return nil, err
	}
	return
}

func createSSTFileName() string {
	return time.Now().String() + SSTFileSuffixName
}

// Partition 一个partition文件由一个index文件和多个sst文件组成
type Partition struct {
	dirPath string
	pipeSST chan *SST
	indexer Indexer
	SSTMap  map[int64]*SST
}

func OpenPartition(partitionDir string) (p *Partition) {
	p = &Partition{
		dirPath: partitionDir,
		pipeSST: make(chan *SST, 1),
		SSTMap:  make(map[int64]*SST),
	}

	files, err := os.ReadDir(partitionDir)
	if err != nil {
		log.Panicf("open partition dir failed %e", err)
	}

	for _, file := range files {
		fName := file.Name()
		switch {
		case strings.HasSuffix(fName, indexFileSuffixName):
			p.indexer, err = NewIndexer(file.Name())
		case strings.HasSuffix(fName, SSTFileSuffixName):
			sst, err := NewSST(fName)
			if err != nil {
				return nil
			}
			fid, err := strconv.Atoi(fName)
			p.SSTMap[int64(fid)] = sst
		}
	}

	go func() {
		sst, err := NewSST(createSSTFileName())
		if err != nil {
			return
		}
		p.pipeSST <- sst
	}()

	//go p.AutoCompaction()
	return
}

func (p *Partition) Get(key []byte) (kv *marshal.KV, err error) {
	//todo 根据索引在sst文件中获取值
	indexMeta, err := p.indexer.Get(key)
	if err != nil {
		return nil, err
	}
	//todo check key是否过期
	index := marshal.DecodeIndexMeta(indexMeta.Value)
	if sst, ok := p.SSTMap[index.Fid]; ok {
		value, err := sst.Read(index.ValueSize, index.ValueOffset)
		if err != nil {
			return nil, err
		}
		return &marshal.KV{
			Key:  key,
			Data: marshal.DecodeData(value),
		}, nil
	}

	return nil, code.ErrCanNotFondSSTFile
}

func (p *Partition) Scan(low, high []byte) (kvs []*marshal.KV, err error) {
	//todo 根据索引在sst文件中获取值
	indexMetas, err := p.indexer.Scan(low, high)
	if err != nil {
		return
	}
	for _, indexMeta := range indexMetas {
		index := marshal.DecodeIndexMeta(indexMeta.Value)
		if sst, ok := p.SSTMap[index.Fid]; ok {
			value, err := sst.Read(index.ValueSize, index.ValueOffset)
			if err != nil {
				return nil, err
			}
			kvs = append(kvs, &marshal.KV{
				Key:  indexMeta.Key,
				Data: marshal.DecodeData(value),
			})
		}
	}
	return
}

func (p *Partition) PersistKvs(kvs []*marshal.KV, wg *sync.WaitGroup, errC chan error) {
	buf := bytebufferpool.Get()
	buf.Reset()
	defer func() {
		bytebufferpool.Put(buf)
		wg.Done()
	}()

	//todo 每次刷盘都重新开启一个sst文件还是刷入旧文件控制旧文件的size？
	sst := <-p.pipeSST
	fid, _ := strconv.Atoi(sst.fd.Name())
	indexMetas := make([]*marshal.BytesKV, 0)
	var fileCurrentOffset int64
	for _, kv := range kvs {
		vSize := len(kv.Data.Value)

		meta := &marshal.IndexerMeta{
			Fid:         int64(fid),
			ValueOffset: fileCurrentOffset,
			ValueSize:   int64(vSize),
			ValueCrc32:  crc32.ChecksumIEEE(kv.Data.Value),
			TimeStamp:   kv.Data.TimeStamp,
		}

		if vSize <= smallValue {
			meta.Value = kv.Data.Value
		}

		indexMetas = append(indexMetas, &marshal.BytesKV{
			Key: kv.Key, Value: marshal.EncodeIndexMeta(meta),
		})

		fileCurrentOffset += int64(len(kv.Data.Value))
		if _, err := buf.Write(kv.Data.Value); err != nil {
			errC <- err
		}
	}

	//todo 索引更新和memtable落盘应该是一个原子操作，写入的index信息应该显示提交
	//todo 会有读写冲突
	err := p.indexer.Put(indexMetas)
	if err != nil {
		return
	}

	//todo 一个value如果跨两个block，那么可能需要访问两次硬盘,后续优化vlog的文件布局
	//todo 写入文件操作也必须是一个原子操作
	err = sst.Write(buf.Bytes())
	if err != nil {
		return
	}

	p.SSTMap[int64(fid)] = sst
}

func (p *Partition) AutoCompaction() {
	//todo compaction策略
	p.Compaction()
}

func (p *Partition) Compaction() {

}
