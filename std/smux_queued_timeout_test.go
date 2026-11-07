package std

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/xtaci/smux"
)

type gateFirstData struct {
	net.Conn
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (c *gateFirstData) Write(p []byte) (int, error) {
	if len(p) >= 8 && p[1] == 2 {
		c.once.Do(func() { close(c.entered); <-c.release })
	}
	return c.Conn.Write(p)
}

func TestSmuxTimedOutQueuedDataIsNotSent(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			a, b := net.Pipe()
			gated := &gateFirstData{Conn: a, entered: make(chan struct{}), release: make(chan struct{})}
			cfg := smux.DefaultConfig()
			cfg.Version = version
			cfg.KeepAliveDisabled = true
			client, err := smux.Client(gated, cfg)
			if err != nil {
				t.Fatal(err)
			}
			server, err := smux.Server(b, cfg)
			if err != nil {
				t.Fatal(err)
			}
			var release sync.Once
			defer func() { release.Do(func() { close(gated.release) }); client.Close(); server.Close() }()
			first, err := client.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			peerFirst, err := server.AcceptStream()
			if err != nil {
				t.Fatal(err)
			}
			second, err := client.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			peerSecond, err := server.AcceptStream()
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			defer second.Close()
			defer peerFirst.Close()
			defer peerSecond.Close()
			firstDone := make(chan error, 1)
			go func() { _, err := first.Write([]byte("first")); firstDone <- err }()
			select {
			case <-gated.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("first write not blocked")
			}
			second.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
			payload := []byte("must not be sent")
			if _, err := second.Write(payload); !errors.Is(err, smux.ErrTimeout) {
				t.Fatalf("write error = %v", err)
			}
			copy(payload, bytes.Repeat([]byte{'x'}, len(payload)))
			release.Do(func() { close(gated.release) })
			select {
			case err := <-firstDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("first write stuck")
			}
			peerSecond.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
			buf := make([]byte, 64)
			n, err := peerSecond.Read(buf)
			if n != 0 || !errors.Is(err, smux.ErrTimeout) {
				t.Fatalf("timed-out data was sent: %q, %v", buf[:n], err)
			}
		})
	}
}

type gateVectoredData struct {
	net.Conn
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (c *gateVectoredData) WriteBuffers(v [][]byte) (int, error) {
	if len(v) > 0 && len(v[0]) >= 8 && v[0][1] == 2 {
		c.once.Do(func() { close(c.entered); <-c.release })
	}
	total := 0
	for _, p := range v {
		n, err := c.Conn.Write(p)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
func TestSmuxTimedOutInFlightWriteDoesNotRetainCallerBuffer(t *testing.T) {
	a, b := net.Pipe()
	gated := &gateVectoredData{Conn: a, entered: make(chan struct{}), release: make(chan struct{})}
	cfg := smux.DefaultConfig()
	cfg.Version = 2
	cfg.KeepAliveDisabled = true
	client, err := smux.Client(gated, cfg)
	if err != nil {
		t.Fatal(err)
	}
	server, err := smux.Server(b, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var release sync.Once
	defer func() { release.Do(func() { close(gated.release) }); client.Close(); server.Close() }()
	stream, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	peer, err := server.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	defer peer.Close()
	stream.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	payload := []byte("original data")
	done := make(chan error, 1)
	go func() { _, err := stream.Write(payload); done <- err }()
	select {
	case <-gated.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("data write did not start")
	}
	if err := <-done; !errors.Is(err, smux.ErrTimeout) {
		t.Fatalf("write error %v", err)
	}
	copy(payload, bytes.Repeat([]byte{'x'}, len(payload)))
	release.Do(func() { close(gated.release) })
	peer.SetReadDeadline(time.Now().Add(time.Second))
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(peer, got); err != nil || string(got) != "original data" {
		t.Fatalf("got %q, %v", got, err)
	}
	// Wait for the peer's window update before attempting the next write.
	barrierDone := make(chan error, 1)
	go func() {
		barrier, err := server.OpenStream()
		if err == nil {
			defer barrier.Close()
		}
		barrierDone <- err
	}()
	client.SetDeadline(time.Now().Add(time.Second))
	barrier, err := client.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Close()
	if err := <-barrierDone; err != nil {
		t.Fatal(err)
	}
	stream.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err := stream.Write([]byte("next")); err != nil {
		t.Fatalf("write after late frame: %v", err)
	}
}
