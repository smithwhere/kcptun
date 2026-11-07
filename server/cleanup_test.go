package main

import (
	"errors"
	"io"
	"testing"
)

type auditCloser struct {
	closed bool
	err    error
}

func (c *auditCloser) Close() error { c.closed = true; return c.err }
func TestCloseListenersReportsFailuresAndClosesAll(t *testing.T) {
	problem := errors.New("cleanup failure")
	a, b := &auditCloser{err: problem}, &auditCloser{}
	err := closeListeners([]io.Closer{a, b})
	if !a.closed || !b.closed || !errors.Is(err, problem) {
		t.Fatalf("cleanup: a=%v b=%v err=%v", a.closed, b.closed, err)
	}
}
