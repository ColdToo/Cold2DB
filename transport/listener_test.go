package transport

import (
	"github.com/stretchr/testify/assert"
	"net"
	"strings"
	"testing"
	"time"
)

func TestStoppableListener_Accept(t *testing.T) {
	addr := "localhost:8080"
	stopc := make(chan struct{})

	ln, err := NewStoppableListener(addr, stopc)
	if err != nil {
		t.Errorf("Failed to create stoppable listener: %v", err)
	}

	go func() {
		for {
			_, err := ln.Accept()
			if err != nil {
				if strings.Contains(err.Error(), "server stopped") {
					t.Log("ln exit success")
				}
			}
		}
	}()
	time.Sleep(5 * time.Second)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Errorf("Failed to connect to listener: %v", err)
	}

	if conn != nil {
		t.Log("success dial remote ip ")
		t.Log(conn.RemoteAddr().String())
	}

	close(stopc)

	conn, err = net.Dial("tcp", addr)
	if err != nil {
		t.Errorf("Failed to connect to listener: %v", err)
	}
	assert.NotEqual(t, err, nil)
}
