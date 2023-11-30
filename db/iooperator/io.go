package iooperator

import (
	"github.com/ColdToo/Cold2DB/db/iooperator/directio"
	"os"
)

func OpenDirectIOFile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	return directio.OpenDirectFile(name, flag, perm)
}
