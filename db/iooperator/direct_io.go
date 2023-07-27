package iooperator

import (
	"github.com/ColdToo/Cold2DB/db/iooperator/mmap"
	"io"
	"os"
)

type DirectorIoOperator struct {
	fd     *os.File
	buf    []byte // a buffer of mmap
	bufLen int64
}

func NewDirectorIoOperator(fName string, fsize int64) (IoOperator, error) {
	return &DirectorIoOperator{}, nil
}

func (lm *DirectorIoOperator) Write(b []byte, offset int64) (int, error) {
	length := int64(len(b))
	if length <= 0 {
		return 0, nil
	}
	if offset < 0 || length+offset > lm.bufLen {
		return 0, io.EOF
	}
	return copy(lm.buf[offset:], b), nil
}

func (lm DirectorIoOperator) Read(b []byte, offset int64) (int, error) {
	if offset < 0 || offset >= lm.bufLen {
		return 0, io.EOF
	}
	if offset+int64(len(b)) >= lm.bufLen {
		return 0, io.EOF
	}
	return copy(b, lm.buf[offset:]), nil
}

func (lm DirectorIoOperator) Sync() error {
	return mmap.Msync(lm.buf)
}

func (lm *DirectorIoOperator) Close() error {
	if err := mmap.Msync(lm.buf); err != nil {
		return err
	}
	if err := mmap.Munmap(lm.buf); err != nil {
		return err
	}
	return lm.fd.Close()
}

func (lm *DirectorIoOperator) Delete() error {
	if err := mmap.Munmap(lm.buf); err != nil {
		return err
	}
	lm.buf = nil

	if err := lm.fd.Truncate(0); err != nil {
		return err
	}
	if err := lm.fd.Close(); err != nil {
		return err
	}
	return os.Remove(lm.fd.Name())
}
