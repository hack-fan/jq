package jq

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestQueue returns a queue backed by a fresh in-memory redis.
func newTestQueue(t *testing.T, name string, opts ...Option) (*Queue, *redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewQueue(name, rdb, opts...), rdb, mr
}

// mustPub publishes each payload or fails the test.
func mustPub(t *testing.T, q *Queue, payloads ...any) {
	t.Helper()
	for _, p := range payloads {
		if _, err := q.Pub(t.Context(), p); err != nil {
			t.Fatalf("pub: %s", err)
		}
	}
}

// mustStatus returns the queue status or fails the test.
func mustStatus(t *testing.T, q *Queue) *Status {
	t.Helper()
	status, err := q.Status(t.Context())
	if err != nil {
		t.Fatalf("status: %s", err)
	}
	return status
}

// fastOptions are worker options with short timings for tests.
func fastOptions() *WorkerOptions {
	return &WorkerOptions{
		MaxRetry: 1,
		Parallel: 2,
		Interval: 30 * time.Millisecond,
		Recover:  100 * time.Millisecond,
	}
}

// startWorker runs q.StartWorker in the background. cancel stops it and
// wait asserts it returns within a few seconds.
func startWorker(t *testing.T, q *Queue, fn HandlerFunc, opt *WorkerOptions) (cancel, wait func()) {
	t.Helper()
	ctx, cancelCtx := context.WithCancel(t.Context())
	// Join wg here as well as through opt.WG, so wait can not observe the
	// wait group before the worker goroutine has joined it.
	var wg sync.WaitGroup
	opt.WG = &wg
	wg.Go(func() { q.StartWorker(ctx, fn, opt) })
	wait = func() {
		t.Helper()
		awaitReturn(t, 3*time.Second, "worker stop", wg.Wait)
	}
	return cancelCtx, wait
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

// countingHandler is a slog.Handler that counts records per level.
type countingHandler struct {
	infos  atomic.Int32
	errors atomic.Int32
}

func (*countingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *countingHandler) Handle(_ context.Context, r slog.Record) error {
	switch r.Level {
	case slog.LevelInfo:
		h.infos.Add(1)
	case slog.LevelError:
		h.errors.Add(1)
	}
	return nil
}

func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *countingHandler) WithGroup(string) slog.Handler      { return h }
