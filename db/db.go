package db

import (
	"github.com/ColdToo/Cold2DB/pb"
)

type DB interface {
	Get(key []byte) (val []byte, err error)
	Put(key, val []byte) (ok bool, err error)
}

type Cold2 struct {
}

func InitDB() *Cold2 {
	return new(Cold2)
}

func (db Cold2) Get(key []byte) (val []byte, err error) {
	return
}

func (db Cold2) Put(key, val []byte) (ok bool, err error) {
	return
}

// InitialState implements the Storage interface.
func (db Cold2) InitialState() (pb.HardState, pb.ConfState, error) {
	return ms.hardState, *ms.snapshot.Metadata.ConfState, nil
}

// SetHardState saves the current HardState.
func (db Cold2) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// Entries implements the Storage interface.
func (db Cold2) Entries(lo, hi uint64) ([]pb.Entry, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 {
		log.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}

	ents := ms.ents[lo-offset : hi-offset]
	if len(ms.ents) == 1 && len(ents) != 0 {
		// only contains dummy entries.
		return nil, ErrUnavailable
	}
	return ents, nil
}

// Term implements the Storage interface.
func (db Cold2) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

// LastIndex implements the Storage interface.
func (db Cold2) LastIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(), nil
}

func (db Cold2) lastIndex() uint64 {
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

// FirstIndex implements the Storage interface.
func (db Cold2) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

func (db Cold2) firstIndex() uint64 {
	return ms.ents[0].Index + 1
}

// Snapshot implements the Storage interface.
func (db Cold2) GetSnapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}
