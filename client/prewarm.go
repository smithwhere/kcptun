package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/xtaci/smux"
)

// prewarmSlot dials without holding the slot lock, allowing a request to
// establish its own session if it arrives before prewarming finishes.
func prewarmSlot(mu *sync.Mutex, slot *timedSession, autoExpire int, scavenger chan<- timedSession, create func() (*smux.Session, error)) {
	prewarmSlotContext(context.Background(), mu, slot, autoExpire, scavenger, create)
}

// prewarmSessions checks slots independently; a stalled dial occupies only
// its own worker and never prevents other completed dials from retrying.
func prewarmSessions(ctx context.Context, locks []sync.Mutex, slots []timedSession, autoExpire int, scavenger chan<- timedSession, retryInterval time.Duration, create func() (*smux.Session, error)) {
	prewarmSessionsWithTimeout(ctx, locks, slots, autoExpire, scavenger, retryInterval, 10*time.Second, func(context.Context) (*smux.Session, error) { return create() })
}

func prewarmSessionsWithTimeout(ctx context.Context, locks []sync.Mutex, slots []timedSession, autoExpire int, scavenger chan<- timedSession, retryInterval, dialTimeout time.Duration, create func(context.Context) (*smux.Session, error)) {
	const maxConcurrentDials = 8
	running := make([]bool, len(slots))
	nextAttempt := make([]time.Time, len(slots))
	completed := make(chan int, maxConcurrentDials)
	active, cursor := 0, 0
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()
	scan := func() {
		if ctx.Err() != nil {
			return
		}
		for checked := 0; checked < len(slots) && active < maxConcurrentDials; checked++ {
			if ctx.Err() != nil {
				return
			}
			i := cursor
			cursor = (cursor + 1) % len(slots)
			if running[i] || time.Now().Before(nextAttempt[i]) || !locks[i].TryLock() {
				continue
			}
			session := slots[i].session
			healthy := session != nil && !session.IsClosed() && (autoExpire <= 0 || time.Now().Before(slots[i].expiryDate))
			locks[i].Unlock()
			if healthy {
				continue
			}
			running[i] = true
			nextAttempt[i] = time.Now().Add(retryInterval)
			active++
			go func(i int) {
				dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
				defer cancel()
				prewarmSlotContext(dialCtx, &locks[i], &slots[i], autoExpire, scavenger, func() (*smux.Session, error) { return create(dialCtx) })
				completed <- i
			}(i)
		}
	}
	scan()
	for {
		select {
		case <-ctx.Done():
			return
		case i := <-completed:
			running[i] = false
			active--
			scan()
		case <-ticker.C:
			scan()
		}
	}
}

func prewarmSlotContext(ctx context.Context, mu *sync.Mutex, slot *timedSession, autoExpire int, scavenger chan<- timedSession, create func() (*smux.Session, error)) {
	session, err := create()
	if err != nil {
		log.Println("prewarm connect:", err)
		return
	}
	// Register before publishing, so no request can use an untracked session.
	installed := timedSession{session: session, expiryDate: time.Now().Add(time.Duration(autoExpire) * time.Second)}
	if autoExpire > 0 {
		select {
		case scavenger <- installed:
		case <-ctx.Done():
			session.Close()
			return
		}
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for !mu.TryLock() {
		select {
		case <-ctx.Done():
			session.Close()
			return
		case <-ticker.C:
		}
	}
	if ctx.Err() != nil || session.IsClosed() || (slot.session != nil && !slot.session.IsClosed() && (autoExpire <= 0 || time.Now().Before(slot.expiryDate))) {
		mu.Unlock()
		session.Close()
		return
	}
	*slot = installed
	mu.Unlock()
}
