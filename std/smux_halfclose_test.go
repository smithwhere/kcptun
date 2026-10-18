package std

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/xtaci/smux"
)

// Receiving both FINs must not discard bytes which the application has not read.
// A second stream is a protocol barrier: its SYN follows the response's FIN.
func TestSmuxHalfCloseDrainsBufferedResponse(t *testing.T) {
	for _, version := range []int{1, 2} {
		for _, copyMode := range []bool{false, true} {
			t.Run(fmt.Sprintf("v%d/copy=%t", version, copyMode), func(t *testing.T) {
				a, b := net.Pipe()
				cfg := smux.DefaultConfig()
				cfg.Version = version
				cfg.KeepAliveDisabled = true
				client, err := smux.Client(a, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				server, err := smux.Server(b, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer server.Close()
				c, err := client.OpenStream()
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				s, err := server.AcceptStream()
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				c.SetDeadline(time.Now().Add(3 * time.Second))
				s.SetDeadline(time.Now().Add(3 * time.Second))
				if err := c.CloseWrite(); err != nil {
					t.Fatal(err)
				}
				payload := bytes.Repeat([]byte("buffered response before FIN\n"), 2048)
				if _, err := s.Write(payload); err != nil {
					t.Fatal(err)
				}
				if err := s.CloseWrite(); err != nil {
					t.Fatal(err)
				}
				barrier, err := server.OpenStream()
				if err != nil {
					t.Fatal(err)
				}
				defer barrier.Close()
				client.SetDeadline(time.Now().Add(3 * time.Second))
				accepted, err := client.AcceptStream()
				if err != nil {
					t.Fatal(err)
				}
				defer accepted.Close()
				var received []byte
				if copyMode {
					var dst bytes.Buffer
					_, err = Copy(&dst, c)
					received = dst.Bytes()
				} else {
					received, err = io.ReadAll(c)
				}
				if err != nil && err != io.EOF {
					t.Fatal(err)
				}
				if !bytes.Equal(received, payload) {
					t.Fatalf("buffered response lost after both FINs: got %d bytes, want %d", len(received), len(payload))
				}
			})
		}
	}
}
