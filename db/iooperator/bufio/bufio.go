package bufio

import (
	"github.com/ColdToo/Cold2DB/log"
	"os"
)

func OpenBufferedFile(path string) *os.File {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Panicf("", err)
	}
	return file
}
