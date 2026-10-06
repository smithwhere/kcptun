package main

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtaci/smux"
)

func TestGracefulDrainPreservesAcceptedAndOpenStreams(t *testing.T) {
	a, b := net.Pipe()
	client, err := smux.Client(a, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := smux.Server(b, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	s := timedSession{session: client, expiryDate: time.Now().Add(-time.Hour), clients: new(int64)}
	config := &Config{Graceful: true, ScavengeTTL: 1}
	atomic.AddInt64(s.clients, 1)
	if retiredSessionDone(s, config, time.Now()) {
		t.Fatal("accepted client retired before OpenStream")
	}
	stream, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if retiredSessionDone(s, config, time.Now()) {
		t.Fatal("active stream expired at TTL")
	}
	atomic.AddInt64(s.clients, -1)
	if retiredSessionDone(s, config, time.Now()) {
		t.Fatal("open stream was not protected")
	}
	stream.Close()
	if !retiredSessionDone(s, config, time.Now()) {
		t.Fatal("drained session was leaked")
	}
	config.Graceful = false
	atomic.AddInt64(s.clients, 1)
	if !retiredSessionDone(s, config, time.Now()) {
		t.Fatal("legacy TTL no longer works")
	}
}

func TestPortSelection(t *testing.T) {
	for _, bounds := range [][2]uint64{{1, 2}, {3000, 3010}, {65534, 65535}, {1234, 1234}} {
		var previous uint64
		for i := 0; i < 1000; i++ {
			p, err := choosePort(bounds[0], bounds[1], previous)
			if err != nil || p < bounds[0] || p > bounds[1] {
				t.Fatalf("port %d, error %v", p, err)
			}
			if bounds[0] != bounds[1] && p == previous {
				t.Fatal("consecutive port repeated")
			}
			previous = p
		}
	}
	for _, bounds := range [][2]uint64{{0, 1}, {2, 1}, {1, 65536}} {
		if _, err := choosePort(bounds[0], bounds[1], 0); err == nil {
			t.Fatal("invalid range accepted")
		}
	}
}
