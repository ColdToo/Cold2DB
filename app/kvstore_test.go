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

func TestKvStore_Propose(t *testing.T) {
	initLog()

	tests := []struct {
		name string
		want bool
	}{
		{
			name: "ok",
			want: true,
		},
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

			proposeC := make(chan []byte)
			kvStore := NewKVStore(proposeC)
			kvStore.ReqTimeout = time.Second
			key := []byte("testKey")
			val := []byte("testValue")
			delete := true
			expiredAt := time.Now().UnixNano()

			flagC := make(chan bool)
			if tt.name == "ok" {
				go func() {
					result, _ := kvStore.Propose(key, val, delete, expiredAt)
					t.Log(result)
					flagC <- result
				}()

				<-flagC
				for _, v := range kvStore.monitorKV {
					close(v)
				}
				assert.Equal(t, true, <-flagC)
			} else {
				go func() {
					result, _ := kvStore.Propose(key, val, delete, expiredAt)
					t.Log(result)
					flagC <- result
				}()
				time.Sleep(2 * time.Second)
				assert.Equal(t, false, <-flagC)
			}
		})
	}
}
