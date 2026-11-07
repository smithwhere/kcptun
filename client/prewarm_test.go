package main

import (
	"context"
	"errors"
	kcp "github.com/xtaci/kcp-go/v5"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtaci/smux"
)

func testSession(t *testing.T) *smux.Session {
	t.Helper()
	a, b := net.Pipe()
	s, err := smux.Client(a, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close(); b.Close() })
	return s
}

func TestPrewarmInstallsSessionAndKeepsExisting(t *testing.T) {
	var mu sync.Mutex
	var slot timedSession
	first := testSession(t)
	prewarmSlot(&mu, &slot, 0, nil, func() (*smux.Session, error) { return first, nil })
	if slot.session != first {
		t.Fatal("prewarm did not install session")
	}
	second := testSession(t)
	prewarmSlot(&mu, &slot, 0, nil, func() (*smux.Session, error) { return second, nil })
	if slot.session != first || !second.IsClosed() {
		t.Fatal("prewarm replaced existing session or leaked extra session")
	}
}

func TestPrewarmFailureLeavesSlotAvailable(t *testing.T) {
	var mu sync.Mutex
	var slot timedSession
	prewarmSlot(&mu, &slot, 0, nil, func() (*smux.Session, error) { return nil, errors.New("failed") })
	if slot.session != nil {
		t.Fatal("failed prewarm changed slot")
	}
	if !mu.TryLock() {
		t.Fatal("failed prewarm left slot locked")
	}
	mu.Unlock()
}

func TestPrewarmDoesNotHoldSlotDuringDial(t *testing.T) {
	var mu sync.Mutex
	var slot timedSession
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		prewarmSlot(&mu, &slot, 0, nil, func() (*smux.Session, error) { close(entered); <-release; return nil, errors.New("failed") })
	}()
	<-entered
	if !mu.TryLock() {
		close(release)
		t.Fatal("prewarm blocked requests during dial")
	}
	mu.Unlock()
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("prewarm did not finish")
	}
}

func TestPrewarmRetriesAndReplacesClosedSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	locks := make([]sync.Mutex, 1)
	slots := make([]timedSession, 1)
	created := make(chan *smux.Session, 2)
	attempted := make(chan struct{}, 10)
	first, second := testSession(t), testSession(t)
	var calls atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		prewarmSessions(ctx, locks, slots, 0, nil, 10*time.Millisecond, func() (*smux.Session, error) {
			n := calls.Add(1)
			attempted <- struct{}{}
			if n == 1 {
				return nil, errors.New("temporary")
			}
			if n == 2 {
				created <- first
				return first, nil
			}
			created <- second
			return second, nil
		})
	}()
	select {
	case <-created:
	case <-time.After(time.Second):
		t.Fatal("failed prewarm was not retried")
	}
	time.Sleep(40 * time.Millisecond)
	if n := calls.Load(); n != 2 {
		t.Fatalf("healthy session redialed: %d", n)
	}
	first.Close()
	select {
	case <-created:
	case <-time.After(time.Second):
		t.Fatal("closed session was not replaced")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("prewarm did not stop")
	}
}

func TestPrewarmChecksSlotsIndependently(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slots := make([]timedSession, 2)
	locks := make([]sync.Mutex, 2)
	blocked, release, other := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	go prewarmSessions(ctx, locks, slots, 0, nil, time.Second, func() (*smux.Session, error) {
		if calls.Add(1) == 1 {
			close(blocked)
			<-release
			return nil, errors.New("failed")
		}
		close(other)
		return nil, errors.New("failed")
	})
	<-blocked
	select {
	case <-other:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("slow dial blocked other slots")
	}
	cancel()
	close(release)
}

// A UDP dial can succeed before the server exists. Once it starts, the warm
// session must recover and carry real traffic without a local request first.
func TestPrewarmRecoversWhenServerStartsLate(t *testing.T) {
	probe, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.LocalAddr().String()
	probe.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	locks := make([]sync.Mutex, 1)
	slots := make([]timedSession, 1)
	cfg := smux.DefaultConfig()
	cfg.KeepAliveInterval = 30 * time.Millisecond
	cfg.KeepAliveTimeout = 150 * time.Millisecond
	done := make(chan struct{})
	go func() {
		defer close(done)
		prewarmSessions(ctx, locks, slots, 0, nil, 20*time.Millisecond, func() (*smux.Session, error) {
			conn, err := kcp.DialWithOptions(addr, nil, 0, 0)
			if err != nil {
				return nil, err
			}
			conn.SetNoDelay(1, 10, 2, 1)
			s, err := smux.Client(conn, cfg)
			if err != nil {
				conn.Close()
			}
			return s, err
		})
	}()
	defer func() {
		cancel()
		<-done
		locks[0].Lock()
		if slots[0].session != nil {
			slots[0].session.Close()
		}
		locks[0].Unlock()
	}()
	time.Sleep(350 * time.Millisecond)
	lis, err := kcp.ListenWithOptions(addr, nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	accepted := make(chan error, 1)
	go func() {
		conn, err := lis.AcceptKCP()
		if err != nil {
			accepted <- err
			return
		}
		sess, err := smux.Server(conn, cfg)
		if err != nil {
			conn.Close()
			accepted <- err
			return
		}
		defer sess.Close()
		stream, err := sess.AcceptStream()
		if err != nil {
			accepted <- err
			return
		}
		defer stream.Close()
		var b [1]byte
		_, err = io.ReadFull(stream, b[:])
		if err == nil && b[0] != 42 {
			err = errors.New("unexpected payload")
		}
		accepted <- err
	}()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		locks[0].Lock()
		s := slots[0].session
		locks[0].Unlock()
		if s != nil && !s.IsClosed() {
			stream, err := s.OpenStream()
			if err == nil {
				stream.Write([]byte{42})
				select {
				case err := <-accepted:
					stream.Close()
					if err != nil {
						t.Fatal(err)
					}
					return
				case <-time.After(300 * time.Millisecond):
					stream.Close()
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("warm session did not deliver traffic after server started")
}

func TestPrewarmReplacesExpiredSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	locks := make([]sync.Mutex, 1)
	old, replacement := testSession(t), testSession(t)
	slots := []timedSession{{session: old, expiryDate: time.Now().Add(-time.Second)}}
	tracked := make(chan timedSession, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		prewarmSessions(ctx, locks, slots, 10, tracked, time.Hour, func() (*smux.Session, error) { return replacement, nil })
	}()
	select {
	case got := <-tracked:
		if got.session != replacement || !got.expiryDate.After(time.Now()) {
			t.Fatal("replacement was not tracked with a new expiry")
		}
	case <-time.After(time.Second):
		t.Fatal("expired session not replaced")
	}
	cancel()
	<-done
	locks[0].Lock()
	defer locks[0].Unlock()
	if slots[0].session != replacement {
		t.Fatal("slot did not retain replacement")
	}
	if old.IsClosed() {
		t.Fatal("old session was closed before scavenger grace period")
	}
}

func TestPrewarmBlockedSlotDoesNotStopRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	blocked, release := make(chan struct{}), make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		prewarmSessions(ctx, make([]sync.Mutex, 2), make([]timedSession, 2), 0, nil, 10*time.Millisecond, func() (*smux.Session, error) {
			if calls.Add(1) == 1 {
				close(blocked)
				<-release
			}
			return nil, errors.New("failed")
		})
	}()
	<-blocked
	defer close(release)
	deadline := time.After(time.Second)
	for calls.Load() < 3 {
		select {
		case <-deadline:
			t.Fatal("blocked dial stopped other retries")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation waited for blocked dial")
	}
}

func TestSessionReadFailureClosesSession(t *testing.T) {
	a, b := net.Pipe()
	s, err := smux.Client(a, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	b.Close()
	select {
	case <-s.CloseChan():
	case <-time.After(time.Second):
		t.Fatal("read failure left session open")
	}
}

func TestSessionProtocolFailureClosesSession(t *testing.T) {
	a, b := net.Pipe()
	s, err := smux.Client(a, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer b.Close()
	go b.Write([]byte{99, 0, 0, 0, 0, 0, 0, 0})
	select {
	case <-s.CloseChan():
	case <-time.After(time.Second):
		t.Fatal("protocol failure left session open")
	}
}

func TestAllWarmDialsTimeOutAndRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts, active, peak atomic.Int32
	retries := make(chan struct{}, 1)
	go prewarmSessionsWithTimeout(ctx, make([]sync.Mutex, 9), make([]timedSession, 9), 0, nil, 5*time.Millisecond, 30*time.Millisecond, func(dialCtx context.Context) (*smux.Session, error) {
		n := active.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		if attempts.Add(1) > 8 {
			select {
			case retries <- struct{}{}:
			default:
			}
		}
		<-dialCtx.Done()
		active.Add(-1)
		return nil, dialCtx.Err()
	})
	select {
	case <-retries:
	case <-time.After(time.Second):
		t.Fatal("all dial slots remained blocked")
	}
	if peak.Load() > 8 {
		t.Fatalf("too many active dials: %d", peak.Load())
	}
}

func TestDialHonorsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	conn, err := dialContext(ctx, &Config{RemoteAddr: "127.0.0.1:29901"}, nil)
	if conn != nil {
		conn.Close()
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context cancellation", err)
	}
}

func TestPrewarmTimeoutWhileWaitingForSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var mu sync.Mutex
	mu.Lock()
	s := testSession(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		prewarmSlotContext(ctx, &mu, &timedSession{}, 0, nil, func() (*smux.Session, error) { return s, nil })
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		mu.Unlock()
		t.Fatal("timeout did not stop waiting for slot")
	}
	mu.Unlock()
	if !s.IsClosed() {
		t.Fatal("uninstalled session leaked")
	}
}

func TestPrewarmCancelsBlockedScavenger(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var mu sync.Mutex
	var slot timedSession
	session := testSession(t)
	queue := make(chan timedSession)
	done := make(chan struct{})
	go func() {
		defer close(done)
		prewarmSlotContext(ctx, &mu, &slot, 10, queue, func() (*smux.Session, error) { return session, nil })
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scavenger wait ignored timeout")
	}
	mu.Lock()
	defer mu.Unlock()
	if slot.session != nil {
		t.Fatal("untracked session was installed")
	}
	if !session.IsClosed() {
		t.Fatal("untracked session was not closed")
	}
}

func TestPrewarmStartsNinthWithoutRetryDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sessions := make([]*smux.Session, 9)
	for i := range sessions {
		sessions[i] = testSession(t)
	}
	var calls atomic.Int32
	ninth := make(chan struct{}, 1)
	go prewarmSessionsWithTimeout(ctx, make([]sync.Mutex, 9), make([]timedSession, 9), 0, nil, time.Second, time.Second, func(context.Context) (*smux.Session, error) {
		n := calls.Add(1)
		if n == 9 {
			ninth <- struct{}{}
		}
		return sessions[n-1], nil
	})
	select {
	case <-ninth:
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("only %d slots started", calls.Load())
	}
}

func TestPrewarmFailureRespectsRetryInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	first := make(chan struct{}, 1)
	go prewarmSessionsWithTimeout(ctx, make([]sync.Mutex, 1), make([]timedSession, 1), 0, nil, 200*time.Millisecond, time.Second, func(context.Context) (*smux.Session, error) {
		calls.Add(1)
		select {
		case first <- struct{}{}:
		default:
		}
		return nil, errors.New("failed")
	})
	<-first
	time.Sleep(80 * time.Millisecond)
	if n := calls.Load(); n != 1 {
		t.Fatalf("immediate retry loop: %d attempts", n)
	}
}
