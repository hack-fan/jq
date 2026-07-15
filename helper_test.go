package jq

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestQueue returns a queue backed by a fresh in-memory redis.
func newTestQueue(t *testing.T, name string) (*Queue, *redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewQueue(name, rdb), rdb, mr
}

// waitFor polls cond until it is true or the deadline is reached.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	// one final check, the condition may have become true during the last sleep
	if cond() {
		return
	}
	t.Fatalf("condition not met within %s: %s", timeout, msg)
}

// awaitReturn asserts fn returns within timeout.
func awaitReturn(t *testing.T, timeout time.Duration, msg string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("%s: not finished within %s", msg, timeout)
	}
}
