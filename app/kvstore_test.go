package main

import (
	"github.com/ColdToo/Cold2DB/db"
	mock "github.com/ColdToo/Cold2DB/mocks"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/golang/mock/gomock"
	"github.com/magiconair/properties/assert"
	"testing"
	"time"
)

//mac os 使用gomonkey apply func时只能单步调试，否则 permission denied [recovered]

func TestKvStore_Propose_OK(t *testing.T) {
	initLog()
	tests := []struct {
		name string
		want bool
	}{
		{
			name: "ok",
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCtl := gomock.NewController(t)
			defer mockCtl.Finish()
			DB := mock.NewMockDB(mockCtl)
			gomonkey.ApplyFunc(db.GetDB, func() (db.DB, error) {
				return DB, nil
			})
			time.Sleep(time.Second)

			proposeC := make(chan []byte, 100)
			kvStore := NewKVStore(proposeC, 5)
			kvStore.ReqTimeout = time.Second
			key := []byte("testKey")
			val := []byte("testValue")
			delete := true
			expiredAt := time.Now().UnixNano()

			flagC := make(chan bool, 1)
			if tt.name == "ok" {
				go func() {
					result, _ := kvStore.Propose(key, val, delete, expiredAt)
					t.Log(result)
					assert.Equal(t, true, result)
				}()

				//等待协程给map赋值
				time.Sleep(time.Second)
				for _, v := range kvStore.monitorKV {
					close(v)
				}
				time.Sleep(2 * time.Second)
			}
		})
	}
}

func TestKvStore_Propose_TimeOut(t *testing.T) {
	initLog()
	tests := []struct {
		name string
		want bool
	}{
		{
			name: "timeout",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCtl := gomock.NewController(t)
			defer mockCtl.Finish()
			DB := mock.NewMockDB(mockCtl)
			gomonkey.ApplyFunc(db.GetDB, func() (db.DB, error) {
				return DB, nil
			})

			proposeC := make(chan []byte, 100)
			kvStore := NewKVStore(proposeC, 5)
			kvStore.ReqTimeout = time.Second
			key := []byte("testKey")
			val := []byte("testValue")
			delete := true
			expiredAt := time.Now().UnixNano()

			flagC := make(chan bool)
			go func() {
				result, _ := kvStore.Propose(key, val, delete, expiredAt)
				t.Log(result)
				assert.Equal(t, false, result)
				flagC <- result
			}()
			time.Sleep(2 * time.Second)
		})
	}
}
