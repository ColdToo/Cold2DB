package wal

import "os"

// Options represents the configuration options for a Write-Ahead Log (WAL).
type Options struct {
	// DirPath specifies the directory path where the WAL segment files will be stored.
	DirPath string

	// SegmentSize specifies the maximum size of each segment file in bytes.
	SegmentSize int64
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var DefaultOptions = Options{
	DirPath:     os.TempDir(),
	SegmentSize: GB,
}
