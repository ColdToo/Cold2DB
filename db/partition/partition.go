package partition

import (
	"github.com/ColdToo/Cold2DB/code"
	"github.com/ColdToo/Cold2DB/db/iooperator"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/valyala/bytebufferpool"
	"hash/crc32"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	SSTFileSuffixName    = ".SST"
	SSTFileTmpSuffixName = ".SST-TMP"
	smallValue           = 128
)

type SST struct {
	fd      *os.File
	fName   string
	SSTSize int64
}

// NewSST todo 一个value如果跨两个block，那么可能需要访问两次硬盘,后续优化vlog中数据的对齐
func NewSST(fileName string) (*SST, error) {
	return &SST{
		fd:    iooperator.OpenBufferIOFile(fileName),
		fName: fileName,
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

func (s *SST) Close() {
	if s.fd != nil {
		s.fd.Close()
	}
}

func (s *SST) Remove() {

}

func (s *SST) Rename(fName string) {
	if s.fd != nil {
		s.fd.Close()
		os.Rename(s.fName, fName)
	}
}

func createTmpSSTFileName(dirPath string) string {
	return path.Join(dirPath, time.Now().String()+SSTFileTmpSuffixName)
}

func createSSTFileName(dirPath string) string {
	return path.Join(time.Now().String() + SSTFileTmpSuffixName)
}

// Partition 一个partition文件由一个index文件和多个sst文件组成
type Partition struct {
	dirPath string
	pipeSST chan *SST
	indexer *BtreeIndexer
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
		sst, err := NewSST(createTmpSSTFileName(p.dirPath))
		if err != nil {
			return
		}
		p.pipeSST <- sst
	}()

	//go p.AutoCompaction()
	return
}

func (p *Partition) Get(key []byte) (kv *marshal.KV, err error) {
	indexMeta, err := p.indexer.Get(key)
	if err != nil {
		return nil, err
	}
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
	var err error
	buf := bytebufferpool.Get()
	buf.Reset()
	defer func() {
		bytebufferpool.Put(buf)
		wg.Done()
		if err != nil {
			errC <- err
		}
	}()

	sst := <-p.pipeSST
	fid, _ := strconv.Atoi(sst.fd.Name())
	indexMetas := make([]*marshal.BytesKV, 0)
	var fileCurrentOffset int64
	for _, kv := range kvs {
		if kv.Data.Type == marshal.TypeDelete {
			indexMetas = append(indexMetas, &marshal.BytesKV{Key: kv.Key})
			continue
		}

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

		indexMetas = append(indexMetas, &marshal.BytesKV{Key: kv.Key, Value: marshal.EncodeIndexMeta(meta)})
		fileCurrentOffset += int64(len(kv.Data.Value))
		if _, err = buf.Write(kv.Data.Value); err != nil {
			return
		}
	}

	tx, err := p.indexer.StartTx()
	if tx, err = p.indexer.StartTx(); err != nil {
		log.Errorf("start index transaction failed", err)
		return
	}
	if err = p.indexer.Insert(tx, indexMetas); err != nil {
		tx.Rollback()
		return
	}
	if err = sst.Write(buf.Bytes()); err != nil {
		tx.Rollback()
		sst.Remove()
		log.Errorf("write sst file failed", err)
	}

	if err = tx.Commit(); err != nil {
		tx.Rollback()
		sst.Remove()
		return
	}
	sst.Rename(createSSTFileName(p.dirPath))
	p.SSTMap[int64(fid)] = sst
}

func (p *Partition) AutoCompaction() {
	//todo compaction策略
	p.Compaction()
}

func (p *Partition) Compaction() {

}
