package partition

import (
	"fmt"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"
)

const CreatKVsFmt = "create KVs nums %d, data length %d, bytes count %s"
const PartitionFormat = "PARTITION_%d"

var filePath, _ = os.Getwd()
var partitionDir1 = path.Join(filePath, fmt.Sprintf(PartitionFormat, 1))
var partitionDir2 = path.Join(filePath, fmt.Sprintf(PartitionFormat, 2))
var partitionDir3 = path.Join(filePath, fmt.Sprintf(PartitionFormat, 3))
var MB67MB = CreateKVs(250000, 250)

func CreatPartitionDir(partitionDir string) {
	if _, err := os.Stat(partitionDir); err != nil {
		if err := os.Mkdir(partitionDir, 0755); err != nil {
			println(err)
		}
	}
}

func TestCreateKvs(t *testing.T) {
	kvs := CreateKVs(250000, 250)
	fmt.Printf(CreatKVsFmt, 1, 10, ConvertSize(marshalKVs(kvs)))
}

func marshalKVs(kvs []*marshal.KV) (byteCount int) {
	for _, kv := range kvs {
		byteCount += len(marshal.EncodeKV(kv))
	}
	return
}

func ConvertSize(size int) string {
	units := []string{"B", "KB", "MB", "GB"}
	if size == 0 {
		return "0" + units[0]
	}
	i := 0
	for size >= 1024 {
		size /= 1024
		i++
	}
	return fmt.Sprintf("%.f", float64(size)) + units[i]
}

func CreateKVs(num int, length int) []*marshal.KV {
	kvs := make([]*marshal.KV, 0)
	for i := 1; i < num; i++ {
		kv := &marshal.KV{
			ApplySig: 1,
			Key:      []byte(fmt.Sprintf("%d", i)),
			Data: &marshal.Data{
				Index:     uint64(i),
				TimeStamp: time.Now().Unix(),
				Type:      0,
				Value:     generateData(length),
			},
		}
		if i%2 == 0 {
			kv.Data.Type = marshal.TypeDelete
		}
		kvs = append(kvs, kv)
	}
	return kvs
}

func generateData(length int) []byte {
	rand.Seed(time.Now().UnixNano())
	data := make([]byte, length)
	for i := 0; i < length; i++ {
		//todo 生成一个随机的26字母
		data[i] = 'a'
	}
	return data
}
