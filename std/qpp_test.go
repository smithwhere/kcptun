package std

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

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

type partialQPPSink struct {
	bytes.Buffer
	first bool
}

func (s *partialQPPSink) Write(p []byte) (int, error) {
	if !s.first {
		s.first = true
		n, _ := s.Buffer.Write(p[:4])
		return n, io.ErrShortWrite
	}
	return s.Buffer.Write(p)
}
func (s *partialQPPSink) Close() error { return nil }
func TestQPPPortPartialWriteRetryStaysSynchronized(t *testing.T) {
	sink := &partialQPPSink{}
	pad := qpp.NewQPP([]byte("pad-seed"), 61)
	seed := []byte("stream-seed")
	port := NewQPPPort(sink, pad, seed)
	payload := []byte("retry must preserve every byte")
	n, err := port.Write(payload)
	if n != 4 || err != io.ErrShortWrite {
		t.Fatalf("first write: %d %v", n, err)
	}
	if n2, err := port.Write(payload[n:]); err != nil || n2 != len(payload)-n {
		t.Fatalf("retry: %d %v", n2, err)
	}
	got := append([]byte(nil), sink.Bytes()...)
	pad.DecryptWithPRNG(got, qpp.CreatePRNG(seed))
	if !bytes.Equal(got, payload) {
		t.Fatalf("decrypt after partial write: got %q want %q", got, payload)
	}
}

type synchronizedQPPSink struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (s *synchronizedQPPSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data.Write(p)
}
func (s *synchronizedQPPSink) Close() error             { return nil }
func (s *synchronizedQPPSink) Read([]byte) (int, error) { return 0, io.EOF }
func TestQPPPortConcurrentWritesRemainSynchronized(t *testing.T) {
	pad := qpp.NewQPP([]byte("pad-seed"), 61)
	seed := []byte("stream-seed")
	sink := &synchronizedQPPSink{}
	port := NewQPPPort(sink, pad, seed)
	payload := bytes.Repeat([]byte("x"), 512)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := port.Write(payload)
			if err != nil || n != len(payload) {
				t.Errorf("write: %d %v", n, err)
			}
		}()
	}
	wg.Wait()
	got := append([]byte(nil), sink.data.Bytes()...)
	pad.DecryptWithPRNG(got, qpp.CreatePRNG(seed))
	if !bytes.Equal(got, bytes.Repeat(payload, 32)) {
		t.Fatal("concurrent writes desynchronized stream")
	}
}

type synchronizedQPPSource struct {
	mu   sync.Mutex
	data *bytes.Reader
}

func (s *synchronizedQPPSource) Read(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data.Read(p)
}
func (s *synchronizedQPPSource) Write(p []byte) (int, error) { return len(p), nil }
func (s *synchronizedQPPSource) Close() error                { return nil }
func TestQPPPortConcurrentReadsRemainSynchronized(t *testing.T) {
	pad := qpp.NewQPP([]byte("pad-seed"), 61)
	seed := []byte("stream-seed")
	payload := bytes.Repeat([]byte("x"), 512*32)
	encrypted := append([]byte(nil), payload...)
	pad.EncryptWithPRNG(encrypted, qpp.CreatePRNG(seed))
	port := NewQPPPort(&synchronizedQPPSource{data: bytes.NewReader(encrypted)}, pad, seed)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 512)
			n, err := io.ReadFull(port, buf)
			if err != nil || n != len(buf) || !bytes.Equal(buf, bytes.Repeat([]byte("x"), 512)) {
				t.Errorf("read corrupted: %d %v", n, err)
			}
		}()
	}
	wg.Wait()
}

func TestQPPPortBlockedReadDoesNotBlockWrite(t *testing.T) {
	pad := qpp.NewQPP([]byte("pad-seed"), 61)
	seed := []byte("stream-seed")
	a, b := net.Pipe()
	left := NewQPPPort(a, pad, seed)
	right := NewQPPPort(b, pad, seed)
	defer left.Close()
	defer right.Close()
	readStarted := make(chan struct{})
	readDone := make(chan struct{})
	go func() { close(readStarted); left.Read(make([]byte, 1)); close(readDone) }()
	<-readStarted
	got := make(chan error, 1)
	go func() {
		buf := make([]byte, 5)
		_, err := io.ReadFull(right, buf)
		if err == nil && string(buf) != "hello" {
			err = fmt.Errorf("got %q", buf)
		}
		got <- err
	}()
	wrote := make(chan error, 1)
	go func() { _, err := left.Write([]byte("hello")); wrote <- err }()
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked read prevented write")
	}
	select {
	case err := <-got:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("peer did not receive data")
	}
	right.Close()
	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("close did not unblock read")
	}
}
