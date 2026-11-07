package main

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtaci/smux"
)

type cancelOnThirdErr struct {
	context.Context
	cancel context.CancelFunc
	calls  atomic.Int32
}

func (c *cancelOnThirdErr) Err() error {
	if c.calls.Add(1) == 3 {
		c.cancel()
	}
	return c.Context.Err()
}

type blockingCloseConn struct {
	net.Conn
	entered chan struct{}
	release chan struct{}
}

func (c *blockingCloseConn) Close() error {
	close(c.entered)
	<-c.release
	return c.Conn.Close()
}

func TestCanceledResultCleanupRetainsDialCapacity(t *testing.T) {
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &cancelOnThirdErr{Context: base, cancel: cancel}
	a, b := net.Pipe()
	defer b.Close()
	conn := &blockingCloseConn{Conn: a, entered: make(chan struct{}), release: make(chan struct{})}
	defer close(conn.release)
	session, err := smux.Client(conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	limiter := make(chan struct{}, 1)
	got, err := createSessionBounded(ctx, limiter, func(context.Context) (*smux.Session, error) { return session, nil })
	if got != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, %v", got, err)
	}
	select {
	case <-conn.entered:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not start")
	}
	if len(limiter) != 1 {
		t.Fatal("dial capacity released while session cleanup remains blocked")
	}
}
