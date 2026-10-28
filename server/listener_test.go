package main

import (
	"sync"
	"testing"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"
)

func TestServeListenerReturnsAfterClose(t *testing.T) {
	lis, err := kcp.ListenWithOptions("127.0.0.1:0", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go serveListener(lis, nil, &Config{}, &wg)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	lis.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("closed listener kept retrying AcceptKCP")
	}
}
