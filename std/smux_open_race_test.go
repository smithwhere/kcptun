package std

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/xtaci/smux"
)

type delayedSynReturn struct {
	net.Conn
	once    sync.Once
	sent    chan struct{}
	release chan struct{}
}

func (c *delayedSynReturn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if err == nil && len(p) >= 8 && p[1] == 0 {
		c.once.Do(func() { close(c.sent); <-c.release })
	}
	return n, err
}

func TestSmuxImmediateResponseDuringOpen(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			a, b := net.Pipe()
			wrapped := &delayedSynReturn{Conn: a, sent: make(chan struct{}), release: make(chan struct{})}
			cfg := smux.DefaultConfig()
			cfg.Version = version
			cfg.KeepAliveDisabled = true
			client, err := smux.Client(wrapped, cfg)
			if err != nil {
				t.Fatal(err)
			}
			server, err := smux.Server(b, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			defer server.Close()
			var release sync.Once
			defer release.Do(func() { close(wrapped.release) })
			type opened struct {
				s   *smux.Stream
				err error
			}
			openedCh := make(chan opened, 1)
			go func() { s, err := client.OpenStream(); openedCh <- opened{s, err} }()
			select {
			case <-wrapped.sent:
			case <-time.After(2 * time.Second):
				t.Fatal("SYN not sent")
			}
			peer, err := server.AcceptStream()
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			payload := []byte("immediate response")
			serverDone := make(chan error, 1)
			go func() {
				_, err := peer.Write(payload)
				if err == nil {
					err = peer.CloseWrite()
				}
				if err == nil {
					var barrier *smux.Stream
					barrier, err = server.OpenStream()
					if err == nil {
						defer barrier.Close()
					}
				}
				serverDone <- err
			}()
			client.SetDeadline(time.Now().Add(2 * time.Second))
			barrier, err := client.AcceptStream()
			if err != nil {
				t.Fatal(err)
			}
			defer barrier.Close()
			if err := <-serverDone; err != nil {
				t.Fatal(err)
			}
			release.Do(func() { close(wrapped.release) })
			var local *smux.Stream
			select {
			case result := <-openedCh:
				if result.err != nil {
					t.Fatal(result.err)
				}
				local = result.s
			case <-time.After(2 * time.Second):
				t.Fatal("open blocked")
			}
			defer local.Close()
			local.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			got, err := io.ReadAll(local)
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("immediate response got %q err %v", got, err)
			}
		})
	}
}
