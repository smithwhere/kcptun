package main

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
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

func TestResolveRemoteCancelsBlockedDNS(t *testing.T) {
	started := make(chan struct{}, 1)
	resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err := resolveRemote(ctx, resolver, "blocked.example", 29900)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DNS timeout, got %v", err)
	}
	select {
	case <-started:
	default:
		t.Fatal("resolver dial not called")
	}
}

func TestPreferIPv4Address(t *testing.T) {
	v6 := net.IPAddr{IP: net.ParseIP("2001:db8::1")}
	v4 := net.IPAddr{IP: net.ParseIP("192.0.2.1")}
	if got := preferredIP([]net.IPAddr{v6, v4}); !got.IP.Equal(v4.IP) {
		t.Fatalf("got %v, want IPv4", got)
	}
	if got := preferredIP([]net.IPAddr{v6}); !got.IP.Equal(v6.IP) {
		t.Fatalf("lost IPv6 fallback: %v", got)
	}
}
