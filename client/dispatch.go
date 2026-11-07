package main

import (
	"context"
	"io"
	"sync"
	"time"
)

type transferKey struct{}
type connectionTransfer struct {
	mu       sync.Mutex
	claimed  bool
	timedOut bool
}

// claimConnection atomically transfers ownership away from the timeout closer.
func claimConnection(ctx context.Context) bool {
	transfer, ok := ctx.Value(transferKey{}).(*connectionTransfer)
	if !ok {
		return false
	}
	transfer.mu.Lock()
	defer transfer.mu.Unlock()
	if transfer.timedOut || ctx.Err() != nil {
		return false
	}
	transfer.claimed = true
	return true
}

// The deadline covers waiting for the slot and the full setup operation.
func dispatchSlot(slot *sync.Mutex, pending, slotPending chan struct{}, timeout time.Duration, conn io.Closer, setup func(context.Context)) {
	select {
	case slotPending <- struct{}{}:
	default:
		conn.Close()
		return
	}
	select {
	case pending <- struct{}{}:
	default:
		<-slotPending
		conn.Close()
		return
	}
	go func() {
		defer func() { <-pending; <-slotPending }()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		transfer := &connectionTransfer{}
		ctx = context.WithValue(ctx, transferKey{}, transfer)
		stopClose := context.AfterFunc(ctx, func() {
			transfer.mu.Lock()
			if transfer.claimed {
				transfer.mu.Unlock()
				return
			}
			transfer.timedOut = true
			transfer.mu.Unlock()
			conn.Close()
		})
		defer func() {
			if stopClose() {
				transfer.mu.Lock()
				claimed := transfer.claimed
				transfer.mu.Unlock()
				if !claimed {
					conn.Close()
				}
			}
		}()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			if ctx.Err() != nil {
				return
			}
			if slot.TryLock() {
				defer slot.Unlock()
				if ctx.Err() == nil {
					setup(ctx)
				}
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}
