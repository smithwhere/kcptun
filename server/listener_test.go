package main

import (
	kcp "github.com/xtaci/kcp-go/v5"
	"sync"
	"testing"
	"time"
)

func TestServeListenerReturnsErrorAfterClose(t *testing.T) {
	lis, err := kcp.ListenWithOptions("127.0.0.1:0", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	done := make(chan error, 1)
	go func() { done <- serveListener(lis, nil, &Config{}, &wg) }()
	lis.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("listener failure not reported")
		}
	case <-time.After(time.Second):
		t.Fatal("listener did not stop")
	}
	wg.Wait()
}
