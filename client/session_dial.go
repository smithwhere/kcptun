package main

import (
	"context"
	"github.com/xtaci/smux"
)

type sessionResult struct {
	session *smux.Session
	err     error
}

// createSessionBounded isolates dialing from slot ownership. If a dial ignores
// cancellation, the caller can release its slot while the limiter bounds the
// number of such in-flight operations. Late sessions are closed by the worker.
func createSessionBounded(ctx context.Context, limiter chan struct{}, create func(context.Context) (*smux.Session, error)) (*smux.Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case limiter <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-limiter
		return nil, err
	}
	result := make(chan sessionResult)
	cleanup := make(chan bool, 1)
	go func() {
		defer func() { <-limiter }()
		session, err := create(ctx)
		select {
		case result <- sessionResult{session, err}:
			if <-cleanup && session != nil {
				session.Close()
			}
		case <-ctx.Done():
			if session != nil {
				session.Close()
			}
		}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-result:
		if err := ctx.Err(); err != nil {
			cleanup <- true
			return nil, err
		}
		cleanup <- false
		return r.session, r.err
	}
}
