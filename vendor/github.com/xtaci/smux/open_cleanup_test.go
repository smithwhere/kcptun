package smux

import (
	"errors"
	"net"
	"testing"
)

type failSynConn struct{ net.Conn }

func (c failSynConn) Write([]byte) (int, error) { return 0, errors.New("SYN write failed") }

func TestOpenStreamFailureRemovesRegisteredStream(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	cfg := DefaultConfig()
	cfg.KeepAliveDisabled = true
	session, err := Client(failSynConn{a}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if _, err := session.OpenStream(); err == nil {
		t.Fatal("expected SYN failure")
	}
	session.streamLock.Lock()
	n := len(session.streams)
	session.streamLock.Unlock()
	if n != 0 {
		t.Fatalf("failed open left %d registered streams", n)
	}
}
