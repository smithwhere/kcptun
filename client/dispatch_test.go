package main

import (
	"context"
	"errors"
	"github.com/xtaci/smux"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type closeProbe struct{ closed chan struct{} }

func (p *closeProbe) Close() error { close(p.closed); return nil }

func TestDispatchQueuesWhileSlotIsBusy(t *testing.T) {
	var first, second sync.Mutex
	pending := make(chan struct{}, 3)
	firstPending, secondPending := make(chan struct{}, 3), make(chan struct{}, 3)
	blocked, started, queued, other := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	dispatchSlot(&first, pending, firstPending, time.Second, &closeProbe{make(chan struct{})}, func(_ context.Context) { close(started); <-blocked })
	<-started
	c := &closeProbe{make(chan struct{})}
	dispatchSlot(&first, pending, firstPending, time.Second, c, func(_ context.Context) { close(queued) })
	dispatchSlot(&second, pending, secondPending, time.Second, &closeProbe{make(chan struct{})}, func(_ context.Context) { close(other) })
	select {
	case <-other:
	case <-time.After(time.Second):
		t.Fatal("other slot blocked")
	}
	select {
	case <-c.closed:
		t.Fatal("queued connection closed")
	default:
	}
	close(blocked)
	select {
	case <-queued:
	case <-time.After(time.Second):
		t.Fatal("queued request did not run")
	}
}

func TestDispatchLimitsPendingRequests(t *testing.T) {
	var mu sync.Mutex
	pending := make(chan struct{}, 1)
	slotPending := make(chan struct{}, 1)
	started, release := make(chan struct{}), make(chan struct{})
	dispatchSlot(&mu, pending, slotPending, time.Second, &closeProbe{make(chan struct{})}, func(_ context.Context) { close(started); <-release })
	<-started
	c := &closeProbe{make(chan struct{})}
	dispatchSlot(&mu, pending, slotPending, time.Second, c, func(_ context.Context) { t.Error("accepted more than limit") })
	select {
	case <-c.closed:
	case <-time.After(time.Second):
		t.Fatal("overflow was not closed")
	}
	close(release)
}

func TestDispatchPerSlotLimitDoesNotBlockOtherSlot(t *testing.T) {
	var first, second sync.Mutex
	global := make(chan struct{}, 3)
	firstPending, secondPending := make(chan struct{}, 1), make(chan struct{}, 1)
	started, release, other := make(chan struct{}), make(chan struct{}), make(chan struct{})
	dispatchSlot(&first, global, firstPending, time.Second, &closeProbe{make(chan struct{})}, func(_ context.Context) { close(started); <-release })
	<-started
	rejected := &closeProbe{make(chan struct{})}
	dispatchSlot(&first, global, firstPending, time.Second, rejected, func(_ context.Context) { t.Error("slot limit exceeded") })
	dispatchSlot(&second, global, secondPending, time.Second, &closeProbe{make(chan struct{})}, func(_ context.Context) { close(other) })
	select {
	case <-rejected.closed:
	case <-time.After(time.Second):
		t.Fatal("slot limit not enforced")
	}
	select {
	case <-other:
	case <-time.After(time.Second):
		t.Fatal("other slot starved")
	}
	close(release)
}

func TestDispatchTimesOutWhileWaiting(t *testing.T) {
	var slot sync.Mutex
	slot.Lock()
	c := &closeProbe{make(chan struct{})}
	dispatchSlot(&slot, make(chan struct{}, 1), make(chan struct{}, 1), 20*time.Millisecond, c, func(_ context.Context) { t.Error("expired request ran") })
	select {
	case <-c.closed:
	case <-time.After(time.Second):
		slot.Unlock()
		t.Fatal("waiting request did not time out")
	}
	slot.Unlock()
}

func TestDispatchTimeoutCoversSetup(t *testing.T) {
	var mu sync.Mutex
	ctxDone := make(chan struct{})
	conn := &closeProbe{make(chan struct{})}
	dispatchSlot(&mu, make(chan struct{}, 1), make(chan struct{}, 1), 30*time.Millisecond, conn, func(ctx context.Context) { <-ctx.Done(); close(ctxDone) })
	select {
	case <-ctxDone:
	case <-time.After(time.Second):
		t.Fatal("setup context not cancelled")
	}
	select {
	case <-conn.closed:
	case <-time.After(time.Second):
		t.Fatal("timed out connection remained open")
	}
}

func TestDispatchTimeoutUnblocksFullScavenger(t *testing.T) {
	var mu sync.Mutex
	conn := &closeProbe{make(chan struct{})}
	queue := make(chan timedSession)
	pending, per := make(chan struct{}, 1), make(chan struct{}, 1)
	done := make(chan struct{})
	dispatchSlot(&mu, pending, per, 30*time.Millisecond, conn, func(ctx context.Context) {
		defer close(done)
		select {
		case queue <- timedSession{}:
		case <-ctx.Done():
		}
	})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scavenger send ignored deadline")
	}
	select {
	case <-conn.closed:
	case <-time.After(time.Second):
		t.Fatal("client not closed at deadline")
	}
	deadline := time.After(time.Second)
	for len(pending) != 0 || len(per) != 0 {
		select {
		case <-deadline:
			t.Fatal("pending capacity was not released")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestTimedOutCreationReleasesSlotAndClosesLateSession(t *testing.T) {
	var mu sync.Mutex
	gate := make(chan struct{})
	started := make(chan struct{})
	completed := make(chan struct{})
	late := testSession(t)
	pending, per := make(chan struct{}, 1), make(chan struct{}, 1)
	conn := &closeProbe{make(chan struct{})}
	limiter := make(chan struct{}, 1)
	dispatchSlot(&mu, pending, per, 30*time.Millisecond, conn, func(ctx context.Context) {
		defer close(completed)
		_, err := createSessionBounded(ctx, limiter, func(context.Context) (*smux.Session, error) { close(started); <-gate; return late, nil })
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("got %v, expected deadline", err)
		}
	})
	<-started
	select {
	case <-completed:
	case <-time.After(time.Second):
		close(gate)
		t.Fatal("slot setup remained blocked")
	}
	if !mu.TryLock() {
		close(gate)
		t.Fatal("slot lock remained held")
	}
	mu.Unlock()
	close(gate)
	deadline := time.After(time.Second)
	for !late.IsClosed() {
		select {
		case <-deadline:
			t.Fatal("late session leaked")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestTimedOutDialsRemainBounded(t *testing.T) {
	limiter := make(chan struct{}, 1)
	gate := make(chan struct{})
	started := make(chan struct{})
	var calls atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := createSessionBounded(ctx, limiter, func(context.Context) (*smux.Session, error) {
		calls.Add(1)
		close(started)
		<-gate
		return nil, errors.New("failed")
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		close(gate)
		t.Fatalf("first error %v", err)
	}
	<-started
	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel2()
	_, err = createSessionBounded(ctx2, limiter, func(context.Context) (*smux.Session, error) { calls.Add(1); return nil, nil })
	if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 {
		close(gate)
		t.Fatalf("unbounded attempts: %d, %v", calls.Load(), err)
	}
	close(gate)
}

// Eight non-cooperative dials exhaust the limiter. A timed-out request must
// not start an unbounded ninth dial; the limiter recovers once a dial returns.
func TestDialLimiterSaturationAndRecovery(t *testing.T) {
	const capacity = 8
	limiter := make(chan struct{}, capacity)
	blocked := make(chan struct{})
	var started atomic.Int32
	var callers sync.WaitGroup
	for i := 0; i < capacity; i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			_, err := createSessionBounded(ctx, limiter, func(context.Context) (*smux.Session, error) {
				started.Add(1)
				<-blocked
				return nil, errors.New("released")
			})
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("dial: %v", err)
			}
		}()
	}
	callers.Wait()
	if got := started.Load(); got != capacity {
		close(blocked)
		t.Fatalf("started %d, want %d", got, capacity)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := createSessionBounded(ctx, limiter, func(context.Context) (*smux.Session, error) { started.Add(1); return nil, nil })
	if !errors.Is(err, context.DeadlineExceeded) || started.Load() != capacity {
		close(blocked)
		t.Fatalf("ninth started or wrong error: %d, %v", started.Load(), err)
	}
	close(blocked)
	deadline := time.After(time.Second)
	for len(limiter) != 0 {
		select {
		case <-deadline:
			t.Fatal("released workers did not restore capacity")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// All cancellable workers must actually leave the limiter when the deadline
// expires, allowing a later attempt without restarting active connections.
func TestEightCancellableDialsReleaseCapacity(t *testing.T) {
	const count = 8
	limiter := make(chan struct{}, count)
	entered := make(chan struct{}, count)
	ctx, cancel := context.WithCancel(context.Background())
	var callers sync.WaitGroup
	for i := 0; i < count; i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			_, err := createSessionBounded(ctx, limiter, func(ctx context.Context) (*smux.Session, error) {
				entered <- struct{}{}
				<-ctx.Done()
				return nil, ctx.Err()
			})
			if !errors.Is(err, context.Canceled) {
				t.Errorf("got %v", err)
			}
		}()
	}
	for i := 0; i < count; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("workers did not start")
		}
	}
	cancel()
	callers.Wait()
	deadline := time.After(time.Second)
	for len(limiter) != 0 {
		select {
		case <-deadline:
			t.Fatal("canceled workers retained capacity")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	healthy := testSession(t)
	nextCtx, nextCancel := context.WithTimeout(context.Background(), time.Second)
	defer nextCancel()
	got, err := createSessionBounded(nextCtx, limiter, func(context.Context) (*smux.Session, error) { return healthy, nil })
	if err != nil || got != healthy {
		t.Fatalf("later dial: session=%v error=%v", got, err)
	}
}

func TestCanceledContextDoesNotStartDial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	limiter := make(chan struct{}, 1)
	var started atomic.Int32
	for i := 0; i < 100; i++ {
		_, err := createSessionBounded(ctx, limiter, func(context.Context) (*smux.Session, error) { started.Add(1); return nil, context.Canceled })
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
	}
	time.Sleep(30 * time.Millisecond)
	if n := started.Load(); n != 0 {
		t.Fatalf("started %d dials after cancellation", n)
	}
}

func TestDispatchTimeoutDuringTransferDoesNotLaunchClosedClient(t *testing.T) {
	for i := 0; i < 100; i++ {
		var mu sync.Mutex
		conn := &closeProbe{make(chan struct{})}
		pending, per := make(chan struct{}, 1), make(chan struct{}, 1)
		started, release, handled := make(chan struct{}), make(chan struct{}), make(chan struct{})
		dispatchSlot(&mu, pending, per, 10*time.Millisecond, conn, func(ctx context.Context) {
			close(started)
			<-release
			// setup must explicitly claim the connection before handing it off.
			if claimConnection(ctx) {
				close(handled)
			}
		})
		<-started
		<-conn.closed
		close(release)
		deadline := time.After(time.Second)
		for len(pending) > 0 {
			select {
			case <-deadline:
				t.Fatal("dispatch stuck")
			default:
				time.Sleep(time.Millisecond)
			}
		}
		select {
		case <-handled:
			t.Fatal("closed connection transferred")
		default:
		}
	}
}

func TestDispatchClaimPreservesConnectionAfterDeadline(t *testing.T) {
	var mu sync.Mutex
	conn := &closeProbe{make(chan struct{})}
	claimed := make(chan struct{})
	dispatchSlot(&mu, make(chan struct{}, 1), make(chan struct{}, 1), 30*time.Millisecond, conn, func(ctx context.Context) {
		if !claimConnection(ctx) {
			t.Error("claim failed")
			return
		}
		close(claimed)
	})
	<-claimed
	select {
	case <-conn.closed:
		t.Fatal("claimed connection closed by timeout")
	case <-time.After(80 * time.Millisecond):
	}
}

func TestDispatchClosesUnclaimedConnection(t *testing.T) {
	var mu sync.Mutex
	conn := &closeProbe{make(chan struct{})}
	dispatchSlot(&mu, make(chan struct{}, 1), make(chan struct{}, 1), time.Second, conn, func(context.Context) {})
	select {
	case <-conn.closed:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("unclaimed connection leaked")
	}
}
