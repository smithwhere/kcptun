package std

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/xtaci/qpp"
)

func TestValidateQPPParamsCountBounds(t *testing.T) {
	for _, tt := range []struct {
		count   int
		wantErr bool
	}{
		{-1, true},
		{0, true},
		{1, false},
		{61, false},
		{65535, false},
		{65536, true},
		{65537, true},
		{131072, true},
	} {
		t.Run(fmt.Sprint(tt.count), func(t *testing.T) {
			_, err := ValidateQPPParams(tt.count, "regression-test-key")
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateQPPParams(%d) error = %v, wantErr %t", tt.count, err, tt.wantErr)
			}
		})
	}
}

func TestQPPPortRoundTrip(t *testing.T) {
	pad := qpp.NewQPP([]byte("pad-seed"), 16)
	seed := []byte("session-seed")

	aliceConn, bobConn := net.Pipe()
	alice := NewQPPPort(aliceConn, pad, seed)
	bob := NewQPPPort(bobConn, pad, seed)
	t.Cleanup(func() {
		alice.Close()
		bob.Close()
	})

	t.Run("alice to bob", func(t *testing.T) {
		assertRoundTrip(t, alice, bob, []byte("encrypted hello"))
	})

	t.Run("bob to alice", func(t *testing.T) {
		assertRoundTrip(t, bob, alice, []byte("reply payload"))
	})
}

func assertRoundTrip(t *testing.T, writer io.Writer, reader io.Reader, payload []byte) {
	t.Helper()

	recvErr := make(chan error, 1)
	go func() {
		buf := make([]byte, len(payload))
		if _, err := io.ReadFull(reader, buf); err != nil {
			recvErr <- fmt.Errorf("read encrypted payload: %w", err)
			return
		}
		if !bytes.Equal(buf, payload) {
			recvErr <- fmt.Errorf("payload mismatch: got %q want %q", buf, payload)
			return
		}
		recvErr <- nil
	}()

	msg := append([]byte(nil), payload...)
	if n, err := writer.Write(msg); err != nil {
		t.Fatalf("write failed: %v", err)
	} else if n != len(payload) {
		t.Fatalf("write returned %d, want %d", n, len(payload))
	}

	if err := <-recvErr; err != nil {
		t.Fatalf("round trip error: %v", err)
	}
}

type qppTestSink struct{ bytes.Buffer }

func (s *qppTestSink) Close() error { return nil }

func TestQPPPortWritePreservesInput(t *testing.T) {
	sink := &qppTestSink{}
	pad := qpp.NewQPP([]byte("pad-seed"), 61)
	port := NewQPPPort(sink, pad, []byte("stream-seed"))
	payload := bytes.Repeat([]byte("reusable caller buffer"), 1000)
	original := append([]byte(nil), payload...)
	for i := 0; i < 2; i++ {
		if n, err := port.Write(payload); err != nil || n != len(payload) {
			t.Fatalf("write = %d, %v", n, err)
		}
		if !bytes.Equal(payload, original) {
			t.Fatal("Write modified caller buffer")
		}
	}
	received := append([]byte(nil), sink.Bytes()...)
	pad.DecryptWithPRNG(received, qpp.CreatePRNG([]byte("stream-seed")))
	if !bytes.Equal(received, bytes.Repeat(original, 2)) {
		t.Fatal("repeated writes did not decrypt to the original plaintext")
	}
}
