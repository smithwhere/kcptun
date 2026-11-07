//go:build linux

package tcpraw

import (
	"errors"
	"net"
	"testing"

	"github.com/google/gopacket"
)

func TestWriteToReturnsRawSocketFailure(t *testing.T) {
	handle, err := net.ListenIP("ip4:tcp", &net.IPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Skipf("raw socket unavailable: %v", err)
	}
	handle.Close()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
	c := &tcpConn{listener: listener, die: make(chan struct{}), flowTable: map[string]*tcpFlow{addr.String(): {handle: handle, buf: gopacket.NewSerializeBuffer()}}, tcpFingerPrint: fingerPrintLinux.Clone(), opts: gopacket.SerializeOptions{FixLengths: true, ComputeChecksums: true}}
	n, err := c.WriteTo([]byte("hello"), addr)
	if n != 0 || !errors.Is(err, net.ErrClosed) {
		t.Fatalf("got n=%d err=%v, want closed socket error", n, err)
	}
}
