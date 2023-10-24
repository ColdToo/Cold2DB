package iooperator

import (
	"github.com/ColdToo/Cold2DB/db/iooperator/directio"
	"os"
)

func OpenDirectIOFile(name string, flag int, perm os.FileMode, fsize int64) (file *os.File, err error) {
	fd, err := directio.OpenDirectFile(name, flag, perm)
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
