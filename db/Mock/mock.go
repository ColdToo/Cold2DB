package Mock

import (
	"bytes"
	"fmt"
	"github.com/ColdToo/Cold2DB/db/marshal"
	"github.com/ColdToo/Cold2DB/db/partition"
	"github.com/google/uuid"
	"math/rand"
	"os"
	"path"
	"sort"
	"sync"
	"testing"
	"time"
)

const CreatKVsFmt = "create KVs nums %d, data valLen %d, bytes count %s"
const PartitionFormat = "PARTITION_%d"
const minIndex = 0

var filePath, _ = os.Getwd()
var partitionDir1 = path.Join(filePath, fmt.Sprintf(PartitionFormat, 1))
var partitionDir2 = path.Join(filePath, fmt.Sprintf(PartitionFormat, 2))
var partitionDir3 = path.Join(filePath, fmt.Sprintf(PartitionFormat, 3))
var _67MBKVs = CreateSortKVs(250000, 250, true)
var _27KBKVsNoDelOp = CreateSortKVs(100, 250, false)
var _67MBKVsNoDelOp = CreateSortKVs(250000, 250, true)
var KVs67MB = CreateSortKVs(250000, 250, true)
var KVs27KBNoDelOp = CreateSortKVs(100, 250, false)
var OneKV = CreateSortKVs(1, 250, false)

var KVS_RAND_27KB_HASDEL_UQKey = CreateRandKVs(100, 250, true)
var KVS_SORT_27KB_NODEL_UQKey = CreateSortKVs(100, 250, false)
var KVS_RAND_35MB_HASDEL_UQKey = CreateRandKVs(125000, 250, true)

func CreateRandKVs(num int, valLen int, hasDelete bool) []*marshal.KV {
	kvs := make([]*marshal.KV, 0)
	for i := 0; i < num; i++ {
		kv := &marshal.KV{
			ApplySig: 1,
			Key:      genUniqueKey(),
			Data: &marshal.Data{
				Index:     uint64(i),
				TimeStamp: time.Now().Unix(),
				Type:      0,
				Value:     genRanByte(valLen),
			},
		}
		if hasDelete && i%5 == 0 {
			kv.Data.Type = marshal.TypeDelete
		}
		kvs = append(kvs, kv)
	}
	return kvs
}

func CreateSortKVs(num int, valLen int, hasDelete bool) []*marshal.KV {
	return SortKVSByKey(CreateRandKVs(num, valLen, hasDelete))
}

func CreateNotUniqueKey(min, max int) int {
	rand.Seed(time.Now().UnixNano()) // 设置随机数种子
	randomNumber := rand.Intn(max-min+1) + min
	return randomNumber
}

func SortKVSByKey(kvs []*marshal.KV) []*marshal.KV {
	sort.Slice(kvs, func(i, j int) bool {
		return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0
	})
	return kvs
}

func CreatPartitionDirIfNotExist(partitionDir string) {
	if _, err := os.Stat(partitionDir); err != nil {
		if err := os.Mkdir(partitionDir, 0755); err != nil {
			println(err)
		}
	}
}

func marshalKVs(kvs []*marshal.KV) (byteCount int) {
	for _, kv := range kvs {
		byteCount += len(marshal.EncodeKV(kv))
	}
	return
}

func genRanByte(valLen int) []byte {
	rand.Seed(time.Now().UnixNano())
	data := make([]byte, valLen)
	for i := 0; i < valLen; i++ {
		randomLetter := rand.Intn(26) + 97 // 小写字母
		data[i] = byte(randomLetter)
	}
	return data
}

func genUniqueKey() []byte {
	return []byte(uuid.New().String())
}

func CreateRandomIndex(min, max int) int {
	rand.Seed(time.Now().UnixNano()) // 设置随机数种子
	randomNumber := rand.Intn(max-min+1) + min
	return randomNumber
}

func MockPartitionPersistKVs(partitionDir string, persistKVs []*marshal.KV) *partition.Partition {
	CreatPartitionDirIfNotExist(partitionDir)
	errC := make(chan error, 1)
	p := partition.OpenPartition(partitionDir3)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	p.PersistKvs(persistKVs, wg, errC)
	wg.Wait()
	return p
}

func TestCreateKvs(t *testing.T) {
	nums := 100
	valLen := 250
	kvs := CreateRandKVs(nums, valLen, true)
	fmt.Printf(CreatKVsFmt, nums, valLen, ConvertSize(marshalKVs(kvs)))
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
