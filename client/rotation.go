package main

import (
	"sync/atomic"
	"time"
)

// Only call for sessions removed from the accept loop's active slots.
func retiredSessionDone(s timedSession, config *Config, now time.Time) bool {
	if config.Graceful {
		// Count accepted clients, including goroutines that have not yet
		// opened a stream. Long-lived and idle streams have no forced TTL.
		return atomic.LoadInt64(s.clients) == 0 && s.session.NumStreams() == 0
	}
	return now.After(s.expiryDate.Add(time.Duration(config.ScavengeTTL) * time.Second))
}
