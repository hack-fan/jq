package jq

import (
	"testing"
	"time"
)

// A fresh queue must report IsRunning=false: HGetAll returns an empty map,
// not redis.Nil, when the count key does not exist.
func TestStatusFreshQueue(t *testing.T) {
	q, _, _ := newTestQueue(t, "fresh")
	status, err := q.Status(t.Context())
	if err != nil {
		t.Fatalf("status: %s", err)
	}
	if status.IsRunning {
		t.Fatal("fresh queue should not be running")
	}
	if status.Total != 0 {
		t.Fatalf("fresh queue total should be 0, got %d", status.Total)
	}
}

func TestCountAndStatus(t *testing.T) {
	q, _, _ := newTestQueue(t, "count")
	ctx := t.Context()
	q.count(ctx, "process")
	q.count(ctx, "process")
	q.count(ctx, "success")
	q.count(ctx, "failed")
	q.count(ctx, "dropped")
	status, err := q.Status(ctx)
	if err != nil {
		t.Fatalf("status: %s", err)
	}
	if !status.IsRunning {
		t.Fatal("queue with counters should be running")
	}
	if status.Process != 2 || status.Success != 1 || status.Failed != 1 || status.Dropped != 1 {
		t.Fatalf("unexpected status: %+v", status)
	}
	if status.Total != 2 {
		t.Fatalf("total should be success+dropped=2, got %+v", status)
	}
}

func TestStatusInvalidCount(t *testing.T) {
	for _, field := range []string{"process", "success", "failed", "dropped"} {
		q, rdb, _ := newTestQueue(t, "invalid")
		ctx := t.Context()
		if err := rdb.HSet(ctx, "invalid:count", field, "abc").Err(); err != nil {
			t.Fatalf("hset: %s", err)
		}
		if _, err := q.Status(ctx); err == nil {
			t.Fatalf("status should fail on a non-numeric %s counter", field)
		}
	}
}

func TestStatusRedisDown(t *testing.T) {
	q, _, mr := newTestQueue(t, "down")
	mr.SetError("redis is down")
	if _, err := q.Status(t.Context()); err == nil {
		t.Fatal("status should propagate the redis error")
	}
}

func TestReset(t *testing.T) {
	q, rdb, _ := newTestQueue(t, "reset")
	ctx := t.Context()
	q.count(ctx, "success")
	q.reset(ctx)
	status, err := q.Status(ctx)
	if err != nil {
		t.Fatalf("status: %s", err)
	}
	if status.IsRunning || status.Success != 0 {
		t.Fatalf("reset queue should be fresh, got %+v", status)
	}
	n, _ := rdb.Exists(ctx, "reset:active").Result()
	if n != 0 {
		t.Fatal("reset should delete the active key")
	}
}

// The active timestamp written by count() must round-trip through redis.
func TestActiveAtRoundTrip(t *testing.T) {
	q, rdb, _ := newTestQueue(t, "active")
	ctx := t.Context()
	known := time.Now().Add(-time.Hour)
	if err := rdb.Set(ctx, "active:active", known, 0).Err(); err != nil {
		t.Fatalf("set: %s", err)
	}
	got := q.activeAt(ctx)
	if !got.Equal(known) {
		t.Fatalf("activeAt round-trip mismatch: want %s got %s", known, got)
	}
}

// Without an active key, activeAt falls back to now so the idle reporter
// does not fire on a queue that never ran.
func TestActiveAtFallback(t *testing.T) {
	q, _, _ := newTestQueue(t, "noactive")
	got := q.activeAt(t.Context())
	if since := time.Since(got); since < 0 || since > 2*time.Second {
		t.Fatalf("fallback activeAt should be about now, got %s", got)
	}
}
