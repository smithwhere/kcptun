package tcpraw

import (
	"errors"
	"io"
	"testing"
)

func TestWriteRawFrameReportsTransportFailure(t *testing.T) {
	want := errors.New("raw socket write failed")
	n, err := writeRawFrame(12, []byte("frame"), func([]byte) (int, error) { return 0, want })
	if n != 0 || !errors.Is(err, want) {
		t.Fatalf("got %d, %v", n, err)
	}
}
func TestWriteRawFrameRejectsShortWrite(t *testing.T) {
	n, err := writeRawFrame(12, []byte("frame"), func([]byte) (int, error) { return 2, nil })
	if n != 0 || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("got %d, %v", n, err)
	}
}
func TestWriteRawFrameReportsPayloadBytes(t *testing.T) {
	n, err := writeRawFrame(12, []byte("frame"), func(p []byte) (int, error) { return len(p), nil })
	if n != 12 || err != nil {
		t.Fatalf("got %d, %v", n, err)
	}
}
