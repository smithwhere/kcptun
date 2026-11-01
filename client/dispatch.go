package main

import (
	"io"
	"sync"
)

// dispatchSlot bounds pending setup to one request per tunnel. A slow DNS or
// transport dial must not block Accept or consume unbounded waiting goroutines.
// The next request retries setup after a failed attempt.
func dispatchSlot(slot *sync.Mutex, conn io.Closer, setup func()) {
	if !slot.TryLock() {
		conn.Close()
		return
	}
	go func() {
		defer slot.Unlock()
		setup()
	}()
}
