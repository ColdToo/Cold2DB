package partition

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestSSTReadWrite(t *testing.T) {
	sst, err := OpenSST(createSSTFileName(filePath, NewSST, None))
	if err != nil {
		t.Errorf("Failed to open SST file: %v", err)
	}

	testData := []byte("test data")
	if err := sst.Write(testData); err != nil {
		t.Errorf("Failed to write to SST file: %v", err)
	}

	sst, err = OpenSST(sst.fName)
	readData, err := sst.Read(int64(len(testData)), 0)
	if err != nil {
		t.Errorf("Failed to read from SST file: %v", err)
	}

	if string(readData) != string(testData) {
		t.Errorf("Read data does not match written data")
	}
	sst.Close()
	sst.Remove()
}

func TestSSTRename(t *testing.T) {
	sst, err := OpenSST(createSSTFileName(filePath, TmpSST, None))
	if err != nil {
		t.Errorf("Failed to open SST file: %v", err)
	}

	sst.Rename(createSSTFileName(filePath, NewSST, None))
	if _, err := os.Stat(sst.fName); os.IsNotExist(err) {
		t.Errorf("Renamed SST file does not exist")
	}
}

func TestPartition_OpenOld(t *testing.T) {
	partitionDir := partitionDir1
	if _, err := os.Stat(partitionDir); err != nil {
		if err := os.Mkdir(partitionDir, 0755); err != nil {
			t.Fatalf("failed to create partition directory: %v", err)
		}
	}

	indexFilePath := filepath.Join(partitionDir, "test"+indexFileSuffixName)
	sstFile1Path := filepath.Join(partitionDir, "test1"+SSTFileSuffixName)
	sstFile2Path := filepath.Join(partitionDir, "test2"+SSTFileSuffixName)

	indexFile, _ := os.Create(indexFilePath)
	os.Create(sstFile1Path)
	os.Create(sstFile2Path)
	p := OpenPartition(partitionDir)
	if p.dirPath != partitionDir {
		t.Errorf("expected dirPath to be %s, got %s", partitionDir, p.dirPath)
	}

	if indexFile.Name() != p.indexer.Fp {
		t.Errorf("failed open  index file ")
	}
	os.RemoveAll(partitionDir)
}

func TestPartition_PersistKvs(t *testing.T) {
	CreatPartitionDir(partitionDir1)
	errC := make(chan error, 1)
	p := OpenPartition(partitionDir1)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	p.PersistKvs(MB67MB, wg, errC)
	wg.Wait()
	//todo monitor error
}

func TestPartition_Get(t *testing.T) {
	CreatPartitionDir(partitionDir1)
	errC := make(chan error, 1)
	p := OpenPartition(partitionDir1)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	p.AutoCompaction()
	wg.Wait()
	//todo monitor error
}

func TestPartition_Scan(t *testing.T) {
	CreatPartitionDir(partitionDir1)
	errC := make(chan error, 1)
	p := OpenPartition(partitionDir1)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	p.AutoCompaction()
	wg.Wait()
	//todo monitor error
}
