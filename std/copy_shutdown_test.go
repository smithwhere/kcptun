package std

import (
	"net"
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
