package main

import (
	"bytes"
	mock "github.com/ColdToo/Cold2DB/mocks"
	"github.com/ColdToo/Cold2DB/pb"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"testing"
	"time"
)

const localIpAddr = "127.0.0.1:7878"

func TestServeHttpKVAPI(t *testing.T) {
	initLog()
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()
	DB := mock.NewMockDB(mockCtl)

	// GET PARAMS
	req := []byte{1, 2, 3, 4}
	getBody := bytes.NewReader(req)
	wantBytes := []byte{4, 5, 6, 7}
	DB.EXPECT().Get(req).Return(wantBytes, nil)
	reqGet, _ := http.NewRequest("GET", "http://127.0.0.1:7878/", getBody)

	tests := []struct {
		name      string
		req       *http.Request
		wantBytes []byte
	}{
		{
			name:      GET,
			req:       reqGet,
			wantBytes: wantBytes,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kvStore := &KvStore{db: DB}
			confChangeC := make(chan pb.ConfChange)
			doneC := make(chan struct{})
			go ServeHttpKVAPI(kvStore, localIpAddr, confChangeC, doneC)

			time.Sleep(time.Second)
			response, err := http.DefaultTransport.RoundTrip(tt.req)
			if err != nil {
				return
			}
			defer response.Body.Close()

			respBody, _ := io.ReadAll(response.Body)
			assert.Equal(t, tt.wantBytes, respBody)
		})
	}
}
