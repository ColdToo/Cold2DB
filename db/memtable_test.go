package db

import (
	"github.com/ColdToo/Cold2DB/config"
	"github.com/ColdToo/Cold2DB/log"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

var TestMemConfig = config.MemConfig{
	MemtableSize: 64,
	MemtableNums: 10,
	Concurrency:  5,
}

type KVmock struct {
	k []byte
	v []byte
}

func MockKV(size int) (kvList []KVmock) {
	for i := 0; i < size; i++ {
		k := []byte(strconv.Itoa(i))
		v := []byte(strconv.Itoa(i))
		kv := KVmock{k, v}
		kvList = append(kvList, kv)
	}
	return
}

func TestMemtable_WriteRead(t *testing.T) {
	mem, err := NewMemtable(TestMemConfig)
	if err != nil {
		t.Log(err)
	}

	kvs := MockKV(1000)
	verrifyKVs := make([]KVmock, 0)
	for _, kv := range kvs {
		mem.put(kv.k, kv.v)
	}

	for _, kv := range kvs {
		if flag, v := mem.get(kv.k); flag {
			verrifyKVs = append(verrifyKVs, KVmock{k: kv.k, v: v})
		} else {
			t.Error("can not fund k")
		}
	}

	assert.EqualValues(t, kvs, verrifyKVs)
}

func TestMemtable_Scan(t *testing.T) {

}

func InitLog() {
	cfg := &config.ZapConfig{
		Level:         "debug",
		Format:        "console",
		Prefix:        "[C2KV]",
		Director:      "./log",
		ShowLine:      true,
		EncodeLevel:   "LowercaseColorLevelEncoder",
		StacktraceKey: "stacktrace",
		LogInConsole:  true,
	}
	log.InitLog(cfg)
}
