package std

import (
	"bytes"
	"errors"
	"net"
	"testing"
)

var errCompressedWrite = errors.New("compressed transport failed")

type failSecondCompressedWrite struct {
	net.Conn
	writes int
}

func (c *failSecondCompressedWrite) Write(p []byte) (int, error) {
	c.writes++
	if c.writes == 2 {
		return 0, errCompressedWrite
	}
	return len(p), nil
}

func TestCompStreamReportsCompletedChunksOnWriteFailure(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	stream := NewCompStream(&failSecondCompressedWrite{Conn: a})
	data := bytes.Repeat([]byte{0}, 131072)
	n, err := stream.Write(data)
	if !errors.Is(err, errCompressedWrite) {
		t.Fatalf("error = %v", err)
	}
	if n != 65536 {
		t.Fatalf("written = %d, want completed chunk 65536", n)
	}
}
