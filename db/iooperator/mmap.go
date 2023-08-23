package iooperator

import (
	"github.com/ColdToo/Cold2DB/db/iooperator/mmap"
	"io"
	"os"
)

// MMapIoOperator represents using memory-mapped file I/O.
type MMapIoOperator struct {
	fd     *os.File
	buf    []byte // a buffer of mmap
	bufLen int64
}

func NewMMapIoOperator(fName string, fsize int64) (IoOperator, error) {
	if fsize <= 0 {
		return nil, ErrInvalidFsize
	}
	file, err := openFile(fName, fsize)
	if err != nil {
		return nil, err
	}
	buf, err := mmap.Mmap(file, true, fsize)
	if err != nil {
		return nil, err
	}

	return &MMapIoOperator{fd: file, buf: buf, bufLen: int64(len(buf))}, nil
}

func (lm *MMapIoOperator) Write(b []byte, offset int64) (int, error) {
	length := int64(len(b))
	if length <= 0 {
		return 0, nil
	}
	if offset < 0 || length+offset > lm.bufLen {
		return 0, io.EOF
	}
	return copy(lm.buf[offset:], b), nil
}

func (lm *MMapIoOperator) Read(b []byte, offset int64) (int, error) {
	if offset < 0 || offset >= lm.bufLen {
		return 0, io.EOF
	}
	if offset+int64(len(b)) >= lm.bufLen {
		return 0, io.EOF
	}
	return copy(b, lm.buf[offset:]), nil
}

func (lm *MMapIoOperator) Sync() error {
	return mmap.Msync(lm.buf)
}

func (lm *MMapIoOperator) Close() error {
	if err := mmap.Msync(lm.buf); err != nil {
		return err
	}
	if err := mmap.Munmap(lm.buf); err != nil {
		return err
	}
	return lm.fd.Close()
}

func (lm *MMapIoOperator) Delete() error {
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
