package directio

import "os"

func OpenDirectIOFile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	return OpenDirectFile(name, flag, perm)
}
