package main

import (
	"net"
	"testing"
)

func TestDialUsesEachConfiguredRemoteAddress(t *testing.T) {
	for _, address := range []string{"127.0.0.1:29901", "127.0.0.1:29902"} {
		conn, err := dial(&Config{RemoteAddr: address}, nil)
		if err != nil {
			t.Fatalf("dial(%q): %v", address, err)
		}
		want, err := net.ResolveUDPAddr("udp", address)
		if err != nil {
			conn.Close()
			t.Fatal(err)
		}
		got := conn.RemoteAddr().(*net.UDPAddr)
		if !got.IP.Equal(want.IP) || got.Port != want.Port {
			conn.Close()
			t.Fatalf("dial(%q) connected to %s instead of %s", address, got, want)
		}
		conn.Close()
	}
}

func TestDialRejectsNewInvalidRemoteAfterValidRemote(t *testing.T) {
	conn, err := dial(&Config{RemoteAddr: "127.0.0.1:29901"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	if conn, err := dial(&Config{RemoteAddr: "127.0.0.1:70000"}, nil); err == nil {
		conn.Close()
		t.Fatal("invalid remote must not reuse a prior address")
	}
}
