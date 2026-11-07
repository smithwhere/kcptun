package std

import (
	"bytes"
	"errors"
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

func TestSmuxV2DrainsAfterSessionFailure(t *testing.T) {
	for _, copyMode := range []bool{false, true} {
		t.Run(fmt.Sprint(copyMode), func(t *testing.T) {
			a, b := net.Pipe()
			cfg := smux.DefaultConfig()
			cfg.Version = 2
			cfg.KeepAliveDisabled = true
			c, err := smux.Client(a, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			s, err := smux.Server(b, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			cs, err := c.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			defer cs.Close()
			ss, err := s.AcceptStream()
			if err != nil {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte("x"), 10000)
			if _, err = ss.Write(payload); err != nil {
				t.Fatal(err)
			}
			barrier, err := s.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			defer barrier.Close()
			c.SetDeadline(time.Now().Add(time.Second))
			accepted, err := c.AcceptStream()
			if err != nil {
				t.Fatal(err)
			}
			defer accepted.Close()
			s.Close()
			select {
			case <-c.CloseChan():
			case <-time.After(time.Second):
				t.Fatal("session did not close")
			}
			var got []byte
			if copyMode {
				var out bytes.Buffer
				_, err = Copy(&out, cs)
				got = out.Bytes()
			} else {
				got, err = io.ReadAll(cs)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("got %d of %d bytes, error: %v", len(got), len(payload), err)
			}
			if err != nil && err != io.EOF && !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("unexpected read error: %v", err)
			}
		})
	}
}

func TestAbruptSmuxFailureIsNotEOF(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			a, b := net.Pipe()
			cfg := smux.DefaultConfig()
			cfg.Version = version
			cfg.KeepAliveDisabled = true
			c, _ := smux.Client(a, cfg)
			defer c.Close()
			s, _ := smux.Server(b, cfg)
			stream, err := c.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			_, err = s.AcceptStream()
			if err != nil {
				t.Fatal(err)
			}
			s.Close()
			select {
			case <-c.CloseChan():
			case <-time.After(time.Second):
				t.Fatal("session not closed")
			}
			_, err = stream.Read(make([]byte, 1))
			if err == nil || errors.Is(err, io.EOF) {
				t.Fatalf("abrupt failure reported as %v", err)
			}
		})
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) {
	if len(p) > 0 {
		return len(p) - 1, nil
	}
	return 0, nil
}
func TestSmuxWriteToRejectsShortWrite(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			a, b := net.Pipe()
			cfg := smux.DefaultConfig()
			cfg.Version = version
			cfg.KeepAliveDisabled = true
			c, _ := smux.Client(a, cfg)
			s, _ := smux.Server(b, cfg)
			defer c.Close()
			defer s.Close()
			stream, err := c.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			peer, err := s.AcceptStream()
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			go func() { peer.Write([]byte("payload")); peer.CloseWrite() }()
			_, err = stream.WriteTo(shortWriter{})
			if !errors.Is(err, io.ErrShortWrite) {
				t.Fatalf("got %v, want short write", err)
			}
		})
	}
}
