package iooperator

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/db/logfile"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestNewFileIoOperator(t *testing.T) {
	testNewIoOperator(t, 0)
}

func TestNewMMapOperator(t *testing.T) {
	testNewIoOperator(t, 1)
}

func TestFileIoOperator_Write(t *testing.T) {
	testIoOperatorWrite(t, 0)
}

func TestMMapOperator_Write(t *testing.T) {
	testIoOperatorWrite(t, 1)
}

func TestFileIoOperator_Read(t *testing.T) {
	testIoOperatorRead(t, 0)
}

func TestMMapOperator_Read(t *testing.T) {
	testIoOperatorRead(t, 1)
}

func TestFileIoOperator_Sync(t *testing.T) {
	testIoOperatorSync(t, 0)
}

func TestMMapOperator_Sync(t *testing.T) {
	testIoOperatorSync(t, 1)
}

func TestFileIoOperator_Close(t *testing.T) {
	testIoOperatorClose(t, 0)
}

func TestMMapOperator_Close(t *testing.T) {
	testIoOperatorClose(t, 1)
}

func TestFileIoOperator_Delete(t *testing.T) {
	testIoOperatorDelete(t, 0)
}

func TestMMapOperator_Delete(t *testing.T) {
	testIoOperatorDelete(t, 1)
}

func testNewIoOperator(t *testing.T, ioType uint8) {
	type args struct {
		fName string
		fsize int64
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"size-zero", args{fName: "000000001.wal", fsize: 0},
		},
		{
			"size-negative", args{fName: "000000002.wal", fsize: -1},
		},
		{
			"size-big", args{fName: "000000003.wal", fsize: 1024 << 20},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			absPath, err := filepath.Abs(filepath.Join("/tmp", tt.args.fName))
			assert.Nil(t, err)

			var got IoOperator
			if ioType == uint8(logfile.BufferedIO) {
				got, err = NewFileIoOperator(absPath, tt.args.fsize)
			}
			if ioType == uint8(logfile.MMap) {
				got, err = NewMMapIoOperator(absPath, tt.args.fsize)
			}
			if ioType == uint8(logfile.DirectIO) {
				got, err = NewDirectorIoOperator(absPath, tt.args.fsize)
			}
			defer func() {
				if got != nil {
					err = got.Delete()
					assert.Nil(t, err)
				}
			}()
			if tt.args.fsize > 0 {
				assert.Nil(t, err)
				assert.NotNil(t, got)
			} else {
				assert.Equal(t, err, ErrInvalidFsize)
			}
		})
	}
}

func testIoOperatorWrite(t *testing.T, ioType uint8) {
	absPath, err := filepath.Abs(filepath.Join("/tmp", "00000001.vlog"))
	assert.Nil(t, err)
	var size int64 = 1048576

	var Operator IoOperator
	if ioType == 0 {
		Operator, err = NewFileIoOperator(absPath, size)
	}
	if ioType == 1 {
		Operator, err = NewMMapIoOperator(absPath, size)
	}
	assert.Nil(t, err)
	defer func() {
		if Operator != nil {
			_ = Operator.Delete()
		}
	}()

	type fields struct {
		Operator IoOperator
	}
	type args struct {
		b      []byte
		offset int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			"nil-byte", fields{Operator: Operator}, args{b: nil, offset: 0}, 0, false,
		},
		{
			"one-byte", fields{Operator: Operator}, args{b: []byte("0"), offset: 0}, 1, false,
		},
		{
			"many-bytes", fields{Operator: Operator}, args{b: []byte("lotusdb"), offset: 0}, 7, false,
		},
		{
			"bigvalue-byte", fields{Operator: Operator}, args{b: []byte(fmt.Sprintf("%01048576d", 123)), offset: 0}, 1048576, false,
		},
		{
			"exceed-size", fields{Operator: Operator}, args{b: []byte(fmt.Sprintf("%01048577d", 123)), offset: 0}, 1048577, false,
		},
		{
			"EOF-error", fields{Operator: Operator}, args{b: []byte("lotusdb"), offset: -1}, 0, true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fields.Operator.Write(tt.args.b, tt.args.offset)
			// io.EOF err in mmmap.
			if tt.want == 1048577 && ioType == 1 {
				tt.wantErr = true
				tt.want = 0
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Write() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func testIoOperatorRead(t *testing.T, ioType uint8) {
	absPath, err := filepath.Abs(filepath.Join("/tmp", "00000001.wal"))
	var Operator IoOperator
	if ioType == 0 {
		Operator, err = NewFileIoOperator(absPath, 100)
	}
	if ioType == 1 {
		Operator, err = NewMMapIoOperator(absPath, 100)
	}
	assert.Nil(t, err)
	defer func() {
		if Operator != nil {
			_ = Operator.Delete()
		}
	}()
	offsets := writeSomeData(Operator, t)
	results := [][]byte{
		[]byte(""),
		[]byte("1"),
		[]byte("lotusdb"),
	}

	type fields struct {
		Operator IoOperator
	}
	type args struct {
		b      []byte
		offset int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			"nil", fields{Operator: Operator}, args{b: make([]byte, 0), offset: offsets[0]}, 0, false,
		},
		{
			"one-byte", fields{Operator: Operator}, args{b: make([]byte, 1), offset: offsets[1]}, 1, false,
		},
		{
			"many-bytes", fields{Operator: Operator}, args{b: make([]byte, 7), offset: offsets[2]}, 7, false,
		},
		{
			"EOF-1", fields{Operator: Operator}, args{b: make([]byte, 100), offset: -1}, 0, true,
		},
		{
			"EOF-2", fields{Operator: Operator}, args{b: make([]byte, 100), offset: 1024}, 0, true,
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fields.Operator.Read(tt.args.b, tt.args.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Read() got = %v, want %v", got, tt.want)
			}
			if !tt.wantErr {
				assert.Equal(t, tt.args.b, results[i])
			}
		})
	}
}

func writeSomeData(Operator IoOperator, t *testing.T) []int64 {
	tests := [][]byte{
		[]byte(""),
		[]byte("1"),
		[]byte("lotusdb"),
	}

	var offsets []int64
	var offset int64
	for _, tt := range tests {
		offsets = append(offsets, offset)
		n, err := Operator.Write(tt, offset)
		assert.Nil(t, err)
		offset += int64(n)
	}
	return offsets
}

func testIoOperatorSync(t *testing.T, ioType uint8) {
	sync := func(id int, fsize int64) {
		absPath, err := filepath.Abs(filepath.Join("/tmp", fmt.Sprintf("0000000%d.wal", id)))
		assert.Nil(t, err)
		var Operator IoOperator
		if ioType == 0 {
			Operator, err = NewFileIoOperator(absPath, fsize)
		}
		if ioType == 1 {
			Operator, err = NewMMapIoOperator(absPath, fsize)
		}
		assert.Nil(t, err)
		defer func() {
			if Operator != nil {
				_ = Operator.Delete()
			}
		}()
		writeSomeData(Operator, t)
		err = Operator.Sync()
		assert.Nil(t, err)
	}

	for i := 1; i < 4; i++ {
		sync(i, int64(i*100))
	}
}

func testIoOperatorClose(t *testing.T, ioType uint8) {
	sync := func(id int, fsize int64) {
		absPath, err := filepath.Abs(filepath.Join("/tmp", fmt.Sprintf("0000000%d.wal", id)))
		defer func() {
			_ = os.Remove(absPath)
		}()
		assert.Nil(t, err)
		var Operator IoOperator
		if ioType == 0 {
			Operator, err = NewFileIoOperator(absPath, fsize)
		}
		if ioType == 1 {
			Operator, err = NewMMapIoOperator(absPath, fsize)
		}
		assert.Nil(t, err)
		defer func() {
			if Operator != nil {
				err := Operator.Close()
				assert.Nil(t, err)
			}
		}()
		writeSomeData(Operator, t)
		assert.Nil(t, err)
	}

	for i := 1; i < 4; i++ {
		sync(i, int64(i*100))
	}
}

func testIoOperatorDelete(t *testing.T, ioType uint8) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{"1", false},
		{"2", false},
		{"3", false},
		{"4", false},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			absPath, err := filepath.Abs(filepath.Join("/tmp", fmt.Sprintf("0000000%d.wal", i)))
			assert.Nil(t, err)
			var Operator IoOperator
			if ioType == 0 {
				Operator, err = NewFileIoOperator(absPath, int64((i+1)*100))
			}
			if ioType == 1 {
				Operator, err = NewFileIoOperator(absPath, int64((i+1)*100))
			}
			assert.Nil(t, err)

			if err := Operator.Delete(); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
