package valuelog

import "os"

type Partition struct {
	activeSST *SST
	pipeSST   chan *SST
	older     []*SST
	Snapshot  Snapshot
	index     Indexer
}

type SST struct {
	fd os.File
}
