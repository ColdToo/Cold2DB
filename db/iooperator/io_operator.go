package iooperator

import (
	"errors"
	"os"
)

// ErrInvalidFsize invalid file size.
var ErrInvalidFsize = errors.New("fsize can`t be zero or negative")

// FilePerm default permission of the newly created log file.
const FilePerm = 0644

type IoOperator interface {
	Write(b []byte, offset int64) (int, error)

	Read(b []byte, offset int64) (int, error)

	Sync() error

	Close() error

	Delete() error
}

// open file and truncate it if necessary.
func openFile(fName string, fsize int64) (*os.File, error) {
	fd, err := os.OpenFile(fName, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	stat, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	if stat.Size() < fsize {
		if err := fd.Truncate(fsize); err != nil {
			return nil, err
		}
	}
	return fd, nil
}
