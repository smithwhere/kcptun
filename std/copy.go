// The MIT License (MIT)
//
// # Copyright (c) 2016 xtaci
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package std

import (
	"errors"
	"io"
	"sync"
	"time"
)

const (
	bufSize = 4096
)

// bufPool is a pool of byte slices used for io.Copy operations.
// Reusing buffers reduces GC pressure under high concurrency.
var bufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, bufSize)
		return &buf
	},
}

// Memory optimized io.Copy function specified for this library
func Copy(dst io.Writer, src io.Reader) (written int64, err error) {
	// If the reader has a WriteTo method, use it to do the copy.
	// Avoids an allocation and a copy.
	if wt, ok := src.(io.WriterTo); ok {
		return wt.WriteTo(dst)
	}
	// Similarly, if the writer has a ReadFrom method, use it to do the copy.
	if rt, ok := dst.(io.ReaderFrom); ok {
		return rt.ReadFrom(src)
	}

	// fallback to standard io.CopyBuffer with pooled buffer
	bufPtr := bufPool.Get().(*[]byte)
	defer bufPool.Put(bufPtr)
	return io.CopyBuffer(dst, src, *bufPtr)
}

// closeWriter is an interface for connections that support half-close (CloseWrite).
// Both net.TCPConn and smux.Stream implement this interface.
type closeWriter interface {
	CloseWrite() error
}

// Pipe create a general bidirectional pipe between two streams
// It uses CloseWrite() for half-close to allow graceful shutdown:
// when one direction finishes, it signals the peer that no more data will be sent,
// while still allowing data to be received from the other direction.
// progressWriter reports successful writes without blocking the data path.
type progressWriter struct {
	dst      io.Writer
	progress chan<- struct{}
}

func (w progressWriter) Write(p []byte) (int, error) {
	n, err := w.dst.Write(p)
	if n > 0 {
		select {
		case w.progress <- struct{}{}:
		default:
		}
	}
	return n, err
}

func Pipe(alice, bob io.ReadWriteCloser, closeWait int) (errA, errB error) {
	var wg sync.WaitGroup
	var failureOnce sync.Once
	var closeAOnce, closeBOnce sync.Once
	closedA, closedB := make(chan struct{}), make(chan struct{})
	closeA := func() { closeAOnce.Do(func() { go func() { defer close(closedA); alice.Close() }() }) }
	closeB := func() { closeBOnce.Do(func() { go func() { defer close(closedB); bob.Close() }() }) }
	abort, finished := make(chan struct{}), make(chan struct{})
	failed := make(chan int, 1)
	progress := [2]chan struct{}{make(chan struct{}, 1), make(chan struct{}, 1)}
	// After a failure, interrupt only after the reverse copy makes no write
	// progress for one second. A healthy slow drain may continue indefinitely.
	go func() {
		select {
		case direction := <-failed:
			timer := time.NewTimer(time.Second)
			defer timer.Stop()
			for {
				select {
				case <-finished:
					return
				case <-progress[1-direction]:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(time.Second)
				case <-timer.C:
					closeA()
					closeB()
					return
				}
			}
		case <-finished:
			return
		}
	}()
	wg.Add(2)
	streamCopy := func(direction int, dst, src io.ReadWriteCloser, result *error) {
		defer wg.Done()
		_, *result = Copy(progressWriter{dst: dst, progress: progress[direction]}, src)
		if *result != nil && !errors.Is(*result, io.EOF) {
			failureOnce.Do(func() { close(abort); failed <- direction })
			return
		}
		if closeWait > 0 {
			timer := time.NewTimer(time.Duration(closeWait) * time.Second)
			select {
			case <-timer.C:
			case <-abort:
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		// Half-close may block independently of Close. Never let it prevent
		// shutdown after the opposite direction has failed.
		halfClosed := make(chan error, 1)
		go func() {
			if cw, ok := dst.(closeWriter); ok {
				halfClosed <- cw.CloseWrite()
			} else {
				halfClosed <- dst.Close()
			}
		}()
		select {
		case halfErr := <-halfClosed:
			if halfErr != nil {
				*result = halfErr
				failureOnce.Do(func() { close(abort); failed <- direction })
			}
		case <-abort:
		}
	}
	go streamCopy(0, bob, alice, &errA)
	go streamCopy(1, alice, bob, &errB)
	wg.Wait()
	close(finished)
	closeA()
	closeB()
	// Do not let a stalled Close prevent the other endpoint from being closed.
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	a, b := closedA, closedB
	for a != nil || b != nil {
		select {
		case <-a:
			a = nil
		case <-b:
			b = nil
		case <-timer.C:
			return
		}
	}
	return
}
