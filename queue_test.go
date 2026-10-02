package jq

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

func TestPubAndGet(t *testing.T) {
	q, _, _ := newTestQueue(t, "pubget")
	if q.Name() != "pubget" {
		t.Fatalf("want name %q, got %q", "pubget", q.Name())
	}
	ctx := t.Context()
	id, err := q.Pub(ctx, testPayload{Name: "a", Count: 1})
	if err != nil {
		t.Fatalf("pub: %s", err)
	}
	if id == "" {
		t.Fatal("pub should return a job id")
	}
	job, err := q.Get(ctx)
	if err != nil {
		t.Fatalf("get: %s", err)
	}
	if job.ID != id {
		t.Fatalf("job id mismatch: want %s got %s", id, job.ID)
	}
	if job.Retried != 0 {
		t.Fatalf("new job should have Retried=0, got %d", job.Retried)
	}
	if since := time.Since(job.PubAt); since < 0 || since > time.Minute {
		t.Fatalf("PubAt looks wrong: %s", job.PubAt)
	}
	var got testPayload
	if err := job.Bind(&got); err != nil {
		t.Fatalf("bind: %s", err)
	}
	if got.Name != "a" || got.Count != 1 {
		t.Fatalf("unexpected payload: %+v", got)
	}
}

// An empty queue yields ErrEmpty, which also matches redis.Nil for old callers.
func TestGetEmpty(t *testing.T) {
	q, _, _ := newTestQueue(t, "empty")
	_, err := q.Get(t.Context())
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("want ErrEmpty, got %v", err)
	}
	if !errors.Is(err, redis.Nil) {
		t.Fatalf("want redis.Nil compatibility, got %v", err)
	}
}

func TestGetCorruptJob(t *testing.T) {
	q, rdb, _ := newTestQueue(t, "corrupt")
	ctx := t.Context()
	// 0xc1 is a reserved msgpack byte, never valid
	if err := rdb.LPush(ctx, "corrupt:queue", []byte{0xc1}).Err(); err != nil {
		t.Fatalf("lpush: %s", err)
	}
	_, err := q.Get(ctx)
	if err == nil || errors.Is(err, ErrEmpty) {
		t.Fatalf("corrupt job should fail with an unmarshal error, got %v", err)
	}
}

func TestPubTo(t *testing.T) {
	q, rdb, _ := newTestQueue(t, "src")
	ctx := t.Context()
	id, err := q.PubTo(ctx, "dst", "hello")
	if err != nil {
		t.Fatalf("pubto: %s", err)
	}
	job, err := NewQueue("dst", rdb).Get(ctx)
	if err != nil {
		t.Fatalf("get from dst: %s", err)
	}
	if job.ID != id {
		t.Fatalf("job id mismatch: want %s got %s", id, job.ID)
	}
	if _, err := q.Get(ctx); !errors.Is(err, ErrEmpty) {
		t.Fatalf("src queue should stay empty, got %v", err)
	}
}

func TestLen(t *testing.T) {
	q, _, _ := newTestQueue(t, "len")
	ctx := t.Context()
	if n, err := q.Len(ctx); err != nil || n != 0 {
		t.Fatalf("empty queue: want 0, got %d (err %v)", n, err)
	}
	mustPub(t, q, "x", "x", "x")
	if n, err := q.Len(ctx); err != nil || n != 3 {
		t.Fatalf("want 3, got %d (err %v)", n, err)
	}
}

// The queue is FIFO: jobs come out in publish order.
func TestFIFOOrder(t *testing.T) {
	q, _, _ := newTestQueue(t, "fifo")
	ctx := t.Context()
	mustPub(t, q, "a", "b", "c")
	for _, want := range []string{"a", "b", "c"} {
		job, err := q.Get(ctx)
		if err != nil {
			t.Fatalf("get: %s", err)
		}
		var got string
		if err := job.Bind(&got); err != nil {
			t.Fatalf("bind: %s", err)
		}
		if got != want {
			t.Fatalf("FIFO order broken: want %s got %s", want, got)
		}
	}
}

// Pub must be safe to call from many goroutines.
func TestConcurrentPub(t *testing.T) {
	q, rdb, _ := newTestQueue(t, "conc")
	ctx := t.Context()
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for j := range 10 {
				if _, err := q.Pub(ctx, j); err != nil {
					t.Errorf("pub: %s", err)
				}
			}
		})
	}
	wg.Wait()
	n, err := rdb.LLen(ctx, "conc:queue").Result()
	if err != nil || n != 100 {
		t.Fatalf("want 100 queued jobs, got %d (err %v)", n, err)
	}
}

func TestPubUnmarshalablePayload(t *testing.T) {
	q, _, _ := newTestQueue(t, "badpayload")
	if _, err := q.Pub(t.Context(), make(chan int)); err == nil {
		t.Fatal("pub should fail on a payload msgpack can not marshal")
	}
}

func TestPubRedisDown(t *testing.T) {
	q, _, mr := newTestQueue(t, "pubdown")
	mr.SetError("redis is down")
	if _, err := q.Pub(t.Context(), "x"); err == nil {
		t.Fatal("pub should propagate the redis error")
	}
}

func TestPubCancelledContext(t *testing.T) {
	q, _, _ := newTestQueue(t, "pubcancel")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := q.Pub(ctx, "x"); !errors.Is(err, context.Canceled) {
		t.Fatalf("pub should fail with the context error, got %v", err)
	}
}

func TestDrop(t *testing.T) {
	q, rdb, _ := newTestQueue(t, "drop")
	ctx := t.Context()
	job := &Job{ID: "j1", PubAt: time.Now(), Retried: 3, Payload: []byte("x")}
	if err := q.Drop(ctx, job); err != nil {
		t.Fatalf("drop: %s", err)
	}
	data, err := rdb.RPop(ctx, "drop:dropped").Bytes()
	if err != nil {
		t.Fatalf("dropped queue should have the job: %s", err)
	}
	var got Job
	if err := msgpack.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal dropped job: %s", err)
	}
	if got.ID != "j1" || got.Retried != 3 {
		t.Fatalf("unexpected dropped job: %+v", got)
	}
}

func TestDropRedisDown(t *testing.T) {
	q, _, mr := newTestQueue(t, "dropdown")
	mr.SetError("redis is down")
	if err := q.Drop(t.Context(), &Job{ID: "j1"}); err == nil {
		t.Fatal("drop should propagate the redis error")
	}
}

// Retry with a done context skips the backoff sleep and still republishes,
// so a worker restart does not lose the job.
func TestRetryRepublishes(t *testing.T) {
	q, _, _ := newTestQueue(t, "retry")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	job := &Job{ID: "r1", PubAt: time.Now(), Retried: 1, Payload: []byte("x")}
	awaitReturn(t, 2*time.Second, "retry with done context", func() {
		if err := q.Retry(ctx, job); err != nil {
			t.Errorf("retry: %s", err)
		}
	})
	got, err := q.Get(t.Context())
	if err != nil {
		t.Fatalf("get retried job: %s", err)
	}
	if got.ID != "r1" || got.Retried != 2 {
		t.Fatalf("retried job should have Retried=2, got %+v", got)
	}
}

// The exponential backoff must be clamped so a huge retried count can not
// produce an absurd delay and a negative one can not panic.
func TestBackoffCap(t *testing.T) {
	cases := []struct {
		retried int
		want    time.Duration
	}{
		{-5, time.Second},
		{0, time.Second},
		{3, 8 * time.Second},
		{10, 1024 * time.Second},
		{1000, 1024 * time.Second},
	}
	for _, c := range cases {
		if got := backoff(c.retried); got != c.want {
			t.Fatalf("backoff(%d): want %s got %s", c.retried, c.want, got)
		}
	}
}

// A transient redis outage must not lose the job: Retry keeps trying and
// republishes once redis is back.
func TestRetrySurvivesRedisOutage(t *testing.T) {
	old := retryInterval
	retryInterval = 50 * time.Millisecond
	defer func() { retryInterval = old }()

	q, _, mr := newTestQueue(t, "outage")
	mr.SetError("redis is down")
	// redis recovers while Retry is looping on publish failures
	// (the first attempt happens after the 1s backoff for Retried=0)
	timer := time.AfterFunc(1200*time.Millisecond, func() { mr.SetError("") })
	defer timer.Stop()

	job := &Job{ID: "o1", PubAt: time.Now(), Retried: 0, Payload: []byte("x")}
	awaitReturn(t, 5*time.Second, "retry through redis outage", func() {
		if err := q.Retry(t.Context(), job); err != nil {
			t.Errorf("retry: %s", err)
		}
	})
	got, err := q.Get(t.Context())
	if err != nil {
		t.Fatalf("job should be back in the queue after the outage: %s", err)
	}
	if got.ID != "o1" || got.Retried != 1 {
		t.Fatalf("unexpected republished job: %+v", got)
	}
}

// When redis is down and the context is done, Retry gives up with the push
// error instead of hanging.
func TestRetryGivesUpOnDoneContext(t *testing.T) {
	q, _, mr := newTestQueue(t, "giveup")
	mr.SetError("redis is down")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	job := &Job{ID: "g1", Payload: []byte("x")}
	awaitReturn(t, 2*time.Second, "retry give-up with redis down and done context", func() {
		if err := q.Retry(ctx, job); err == nil {
			t.Error("retry should report the lost job")
		}
	})
}

// WithLogger replaces the default silent logger; a nil logger falls back to it.
func TestWithLogger(t *testing.T) {
	h := new(countingHandler)
	q, _, mr := newTestQueue(t, "logger", WithLogger(nil), WithLogger(slog.New(h)))
	mr.SetError("redis is down")
	q.count(t.Context(), "process")
	if h.errors.Load() == 0 {
		t.Fatal("custom logger was never used")
	}
}
