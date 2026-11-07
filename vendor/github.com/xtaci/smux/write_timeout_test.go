package smux

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestTimedOutUnqueuedV2WriteDoesNotConsumeWindow(t *testing.T) {
	session := &Session{config: &Config{Version: 2}, shaper: make(chan writeRequest), die: make(chan struct{}), chSocketWriteError: make(chan struct{})}
	stream := newStream(1, 1024, session)
	stream.SetWriteDeadline(time.Now().Add(20 * time.Millisecond))
	n, err := stream.Write([]byte("unsent"))
	if n != 0 || !errors.Is(err, ErrTimeout) {
		t.Fatalf("write=%d, %v", n, err)
	}
	if got := atomic.LoadUint32(&stream.numWritten); got != 0 {
		t.Fatalf("unsent bytes consumed window: %d", got)
	}
}
