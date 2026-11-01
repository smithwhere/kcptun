package main

import (
	"io"
	"sync"
	"testing"
	"time"
)

type closeProbe struct{ closed chan struct{} }

func (p *closeProbe) Close() error { close(p.closed); return nil }

var _ io.Closer = (*closeProbe)(nil)

func TestDispatchDoesNotBlockOtherSlots(t *testing.T) {
	var first, second sync.Mutex
	blocked := make(chan struct{})
	defer close(blocked)
	started := make(chan struct{})
	dispatchSlot(&first, &closeProbe{make(chan struct{})}, func() { close(started); <-blocked })
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first dispatch blocked")
	}
	rejected := &closeProbe{make(chan struct{})}
	dispatchSlot(&first, rejected, func() { t.Error("concurrent dial on same slot") })
	select {
	case <-rejected.closed:
	case <-time.After(time.Second):
		t.Fatal("busy slot did not close client")
	}
	ready := make(chan struct{})
	dispatchSlot(&second, &closeProbe{make(chan struct{})}, func() { close(ready) })
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("other slot blocked")
	}
}
