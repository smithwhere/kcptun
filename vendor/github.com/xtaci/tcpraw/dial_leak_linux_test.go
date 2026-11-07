//go:build linux

package tcpraw

import (
	"net"
	"os"
	"testing"
)

func TestDialFailureDoesNotLeakRawSocket(t *testing.T) {
	probe, err := net.DialIP("ip:tcp", nil, &net.IPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Skipf("raw socket unavailable: %v", err)
	}
	probe.Close()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	// Prime the network poller before counting descriptors.
	if conn, err := Dial("tcp", addr); err == nil {
		conn.Close()
		t.Fatal("expected refused connection")
	}
	before, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skip(err)
	}
	for i := 0; i < 20; i++ {
		if conn, err := Dial("tcp", addr); err == nil {
			conn.Close()
			t.Fatal("expected refused connection")
		}
	}
	after, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	if len(after) > len(before)+2 {
		t.Fatalf("descriptors grew from %d to %d", len(before), len(after))
	}
}
