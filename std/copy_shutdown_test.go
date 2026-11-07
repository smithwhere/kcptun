package std

import (
	"bytes"
	"errors"
	"github.com/xtaci/smux"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func tcpPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	a, err := net.DialTCP("tcp", nil, l.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })
	b, err := l.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return a, b
}

func TestPipeAbortsBothDirectionsOnReset(t *testing.T) {
	_, a := tcpPair(t)
	reset, b := tcpPair(t)
	done := make(chan [2]error, 1)
	go func() {
		errA, errB := Pipe(a, b, 30)
		done <- [2]error{errA, errB}
	}()
	if err := reset.SetLinger(0); err != nil {
		t.Fatal(err)
	}
	reset.Close()
	select {
	case errs := <-done:
		if errs[0] == nil && errs[1] == nil {
			t.Fatal("TCP reset was not reported")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read failure left the reverse copy blocked or waited for closewait")
	}
}

func TestPipeDrainsBufferedDataAfterSessionFailure(t *testing.T) {
	a, b := net.Pipe()
	cfg := smux.DefaultConfig()
	cfg.Version = 2
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
	stream, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	peer, err := server.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), 10000)
	if _, err := peer.Write(payload); err != nil {
		t.Fatal(err)
	}
	barrier, err := server.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Close()
	accepted, err := client.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()
	server.Close()
	select {
	case <-client.CloseChan():
	case <-time.After(time.Second):
		t.Fatal("session not closed")
	}
	app, proxy := tcpPair(t)
	if _, err := app.Write([]byte("trigger write failure")); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { Pipe(proxy, stream, 0); close(done) }()
	app.SetReadDeadline(time.Now().Add(3 * time.Second))
	got, err := io.ReadAll(app)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("pipe did not finish")
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("received %d/%d bytes: %v", len(got), len(payload), err)
	}
}

// A slow but progressing downstream must not be cut off by a fixed deadline.
type auditSlowDrain struct {
	closed atomic.Bool
	sent   atomic.Int32
}

func (s *auditSlowDrain) Read([]byte) (int, error) { return 0, errors.New("transport failed") }
func (s *auditSlowDrain) Write(p []byte) (int, error) {
	time.Sleep(150 * time.Millisecond)
	if s.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	s.sent.Add(int32(len(p)))
	return len(p), nil
}
func (s *auditSlowDrain) Close() error { s.closed.Store(true); return nil }

type auditBufferedSource struct {
	n      int
	closed atomic.Bool
}

func (s *auditBufferedSource) Read(p []byte) (int, error) {
	if s.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	if s.n == 10 {
		return 0, io.EOF
	}
	s.n++
	p[0] = 1
	return 1, nil
}
func (s *auditBufferedSource) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func (s *auditBufferedSource) Close() error              { s.closed.Store(true); return nil }
func TestPipeAllowsSlowProgress(t *testing.T) {
	a := &auditSlowDrain{}
	b := &auditBufferedSource{}
	Pipe(a, b, 0)
	if a.sent.Load() != 10 {
		t.Fatalf("delivered %d/10 buffered bytes", a.sent.Load())
	}
}

type auditBlockingClose struct{ release chan struct{} }

func (x *auditBlockingClose) Read([]byte) (int, error)    { return 0, errors.New("failed") }
func (x *auditBlockingClose) Write(p []byte) (int, error) { return len(p), nil }
func (x *auditBlockingClose) Close() error                { <-x.release; return nil }

type auditReadUntilClosed struct {
	closed chan struct{}
	once   sync.Once
}

func (x *auditReadUntilClosed) Read([]byte) (int, error)    { <-x.closed; return 0, io.ErrClosedPipe }
func (x *auditReadUntilClosed) Write(p []byte) (int, error) { return len(p), nil }
func (x *auditReadUntilClosed) Close() error                { x.once.Do(func() { close(x.closed) }); return nil }
func TestPipeClosesBothEndsIndependently(t *testing.T) {
	a := &auditBlockingClose{make(chan struct{})}
	b := &auditReadUntilClosed{closed: make(chan struct{})}
	done := make(chan struct{})
	go func() { Pipe(a, b, 0); close(done) }()
	select {
	case <-b.closed:
	case <-time.After(1600 * time.Millisecond):
		close(a.release)
		<-done
		t.Fatal("second Close blocked by first")
	}
	close(a.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pipe did not return")
	}
}

type eofSource struct{}

func (eofSource) Read([]byte) (int, error)    { return 0, io.EOF }
func (eofSource) Write(p []byte) (int, error) { return len(p), nil }
func (eofSource) Close() error                { return nil }

type halfCloseProbe struct {
	entered, release, closed chan struct{}
	once                     sync.Once
	failure                  error
}

func (x *halfCloseProbe) Read([]byte) (int, error) {
	<-x.entered
	if x.failure != nil {
		<-x.closed
	}
	return 0, errors.New("transport failure")
}
func (x *halfCloseProbe) Write(p []byte) (int, error) { return len(p), nil }
func (x *halfCloseProbe) Close() error                { x.once.Do(func() { close(x.closed) }); return nil }
func (x *halfCloseProbe) CloseWrite() error {
	close(x.entered)
	if x.failure != nil {
		return x.failure
	}
	<-x.release
	return nil
}
func TestPipeFailureInterruptsBlockedHalfClose(t *testing.T) {
	b := &halfCloseProbe{entered: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
	done := make(chan struct{})
	go func() { Pipe(eofSource{}, b, 0); close(done) }()
	select {
	case <-done:
		close(b.release)
	case <-time.After(2500 * time.Millisecond):
		close(b.release)
		<-done
		t.Fatal("pipe blocked in CloseWrite after failure")
	}
}
func TestPipeReportsHalfCloseFailure(t *testing.T) {
	sentinel := errors.New("half-close failure")
	b := &halfCloseProbe{entered: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{}), failure: sentinel}
	done := make(chan [2]error, 1)
	go func() { a, bErr := Pipe(eofSource{}, b, 0); done <- [2]error{a, bErr} }()
	select {
	case errs := <-done:
		if !errors.Is(errs[0], sentinel) && !errors.Is(errs[1], sentinel) {
			t.Fatalf("missing half-close error: %v", errs)
		}
	case <-time.After(2500 * time.Millisecond):
		b.Close()
		<-done
		t.Fatal("ignored CloseWrite failure")
	}
}
