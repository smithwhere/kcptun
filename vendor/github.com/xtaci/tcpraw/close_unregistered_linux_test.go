//go:build linux

package tcpraw

import "testing"

func TestCloseBeforeRegistration(t *testing.T) {
	conn := &tcpConn{die: make(chan struct{})}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
}
