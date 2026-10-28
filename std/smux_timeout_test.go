package std

import (
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/xtaci/smux"
)

// A silent peer must yield an identifiable timeout, not a generic closed pipe.
// Repeat AcceptStream after closure to cover selection between error and die.
func TestSmuxAcceptReportsKeepaliveTimeout(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	cfg := smux.DefaultConfig()
	cfg.KeepAliveInterval = 10 * time.Millisecond
	cfg.KeepAliveTimeout = 50 * time.Millisecond
	s, err := smux.Server(a, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	go io.Copy(io.Discard, b) // Consume outgoing heartbeats but send none back.
	result := make(chan error, 1)
	go func() { _, err := s.AcceptStream(); result <- err }()
	select {
	case err := <-result:
		assertKeepaliveTimeout(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("silent session failed to expire")
	}
	select {
	case <-s.CloseChan():
	case <-time.After(time.Second):
		t.Fatal("expired session was not closed")
	}
	for i := 0; i < 50; i++ {
		_, err := s.AcceptStream()
		assertKeepaliveTimeout(t, err)
	}
}

func assertKeepaliveTimeout(t *testing.T, err error) {
	t.Helper()
	var timeout net.Error
	if !errors.As(err, &timeout) || !timeout.Timeout() || !strings.Contains(err.Error(), "keepalive") {
		t.Fatalf("want identifiable keepalive timeout, got %T: %v", err, err)
	}
}

func TestSmuxExplicitCloseStillReportsClosedPipe(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	cfg := smux.DefaultConfig()
	cfg.KeepAliveDisabled = true
	s, err := smux.Server(a, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcceptStream(); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("explicit close returned %v", err)
	}
}
