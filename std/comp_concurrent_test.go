package std

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestCompStreamConcurrentWrites(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	deadline := time.Now().Add(5 * time.Second)
	left.SetDeadline(deadline)
	right.SetDeadline(deadline)
	writer, reader := NewCompStream(left), NewCompStream(right)
	const count, size = 80, 1024
	got := make(chan error, 1)
	go func() {
		p := make([]byte, 2*count*size)
		_, err := io.ReadFull(reader, p)
		if err == nil && (bytes.Count(p, []byte{'a'}) != count*size || bytes.Count(p, []byte{'b'}) != count*size) {
			err = io.ErrUnexpectedEOF
		}
		got <- err
	}()
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, v := range []byte{'a', 'b'} {
		wg.Add(1)
		go func(v byte) {
			defer wg.Done()
			<-start
			p := bytes.Repeat([]byte{v}, size)
			for i := 0; i < count; i++ {
				if _, err := writer.Write(p); err != nil {
					return
				}
			}
		}(v)
	}
	close(start)
	wg.Wait()
	if err := <-got; err != nil {
		t.Fatalf("concurrent compressed writes: %v", err)
	}
}

func TestCompStreamConcurrentReads(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	deadline := time.Now().Add(5 * time.Second)
	left.SetDeadline(deadline)
	right.SetDeadline(deadline)
	writer, reader := NewCompStream(left), NewCompStream(right)
	const size = 128 * 1024
	writeDone := make(chan error, 1)
	go func() { _, err := writer.Write(bytes.Repeat([]byte{'x'}, 2*size)); writeDone <- err }()
	start := make(chan struct{})
	errors := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			p := make([]byte, size)
			_, err := io.ReadFull(reader, p)
			if err == nil && bytes.Count(p, []byte{'x'}) != size {
				err = io.ErrUnexpectedEOF
			}
			errors <- err
		}()
	}
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errors; err != nil {
			t.Fatalf("concurrent compressed reads: %v", err)
		}
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
}

func TestCompStreamFullDuplex(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	deadline := time.Now().Add(5 * time.Second)
	left.SetDeadline(deadline)
	right.SetDeadline(deadline)
	a, b := NewCompStream(left), NewCompStream(right)
	done := make(chan error, 2)
	for _, pair := range [][2]*CompStream{{a, a}, {b, b}} {
		go func(read, write *CompStream) {
			writeDone := make(chan error, 1)
			go func() { _, err := write.Write([]byte("hello")); writeDone <- err }()
			data := make([]byte, 5)
			_, err := io.ReadFull(read, data)
			if err == nil && string(data) != "hello" {
				err = io.ErrUnexpectedEOF
			}
			if err == nil {
				err = <-writeDone
			}
			done <- err
		}(pair[0], pair[1])
	}
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
