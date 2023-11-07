package db

import (
	"github.com/ColdToo/Cold2DB/db/arenaskl"
	"github.com/ColdToo/Cold2DB/db/valuelog"
	"sync"
	"time"
)

type MemOpt struct {
	fsize   int64
	memSize uint32
}

type Memtable struct {
	CreatAt uint64
	sync.RWMutex
	sklIter  *arenaskl.Iterator
	skl      *arenaskl.Skiplist
	memOpt   MemOpt
	maxIndex uint64
	minIndex uint64
}

func newMemtable(memOpt MemOpt) (*Memtable, error) {
	var sklIter = new(arenaskl.Iterator)
	arena := arenaskl.NewArena(memOpt.memSize + uint32(arenaskl.MaxNodeSize))
	skl := arenaskl.NewSkiplist(arena)
	sklIter.Init(skl)
	table := &Memtable{memOpt: memOpt, skl: skl, sklIter: sklIter}
	return table, nil
}

func (mt *Memtable) put(kv []valuelog.KV) error {
	//判断是否超出当前memtable大小，获取新memtable并将memtable放入刷盘管道中
	return nil
}

func (mt *Memtable) Get(key []byte) (bool, []byte) {
	if found := mt.sklIter.Seek(key); !found {
		return false, nil
	}

	value, err := mt.sklIter.Get(key)
	if err == arenaskl.ErrRecordNotExists {
		return false, nil
	}
	mv := valuelog.DecodeV(value)

	if mv.Type == valuelog.TypeDelete {
		return true, nil
	}

	if mv.ExpiredAt > 0 && mv.ExpiredAt <= time.Now().Unix() {
		return true, nil
	}

	return false, mv.Value
}

func (mt *Memtable) All() []valuelog.KV {
	return nil
}
