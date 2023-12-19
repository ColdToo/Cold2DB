package partition

import (
	"os"
	"testing"
)

var filePath, _ = os.Getwd()

func TestSSTReadWrite(t *testing.T) {
	sst, err := OpenSST(createSSTFileName(filePath, NewSST, None))
	if err != nil {
		t.Errorf("Failed to open SST file: %v", err)
	}
	defer sst.Close()
	defer sst.Remove()

	testData := []byte("test data")
	if err := sst.Write(testData); err != nil {
		t.Errorf("Failed to write to SST file: %v", err)
	}

	readData, err := sst.Read(int64(len(testData)), 0)
	if err != nil {
		t.Errorf("Failed to read from SST file: %v", err)
	}

	if string(readData) != string(testData) {
		t.Errorf("Read data does not match written data")
	}
}

func TestSSTRename(t *testing.T) {
	var oldFileName, newFileName string
	sst, err := OpenSST(createSSTFileName(filePath, TmpSST, None))
	if err != nil {
		t.Errorf("Failed to open SST file: %v", err)
	}

	sst.Rename(createSSTFileName(filePath, NewSST, None))
	if _, err := os.Stat(newFileName); os.IsNotExist(err) {
		t.Errorf("Renamed SST file does not exist")
	}

	sst.Remove()
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Errorf("Original SST file was not removed")
	}

	os.Rename(newFileName, oldFileName)
}
