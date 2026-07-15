package jq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

func TestPubAndGet(t *testing.T) {
	q, _, _ := newTestQueue(t, "pubget")
	id, err := q.Pub(testPayload{Name: "a", Count: 1})
	if err != nil {
		t.Fatalf("pub: %s", err)
	}
	if id == "" {
		t.Fatal("pub should return a job id")
	}
	job, err := q.Get()
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

func TestGetEmpty(t *testing.T) {
	q, _, _ := newTestQueue(t, "empty")
	_, err := q.Get()
	if !errors.Is(err, redis.Nil) {
		t.Fatalf("want redis.Nil, got %v", err)
	}
}

func TestGetCorruptJob(t *testing.T) {
	q, rdb, _ := newTestQueue(t, "corrupt")
	// 0xc1 is a reserved msgpack byte, never valid
	if err := rdb.LPush(context.Background(), "corrupt:queue", []byte{0xc1}).Err(); err != nil {
		t.Fatalf("lpush: %s", err)
	}
	_, err := q.Get()
	if err == nil || errors.Is(err, redis.Nil) {
		t.Fatalf("corrupt job should fail with an unmarshal error, got %v", err)
	}
}

func TestPubTo(t *testing.T) {
	q, rdb, _ := newTestQueue(t, "src")
	id, err := q.PubTo("dst", "hello")
	if err != nil {
		t.Fatalf("pubto: %s", err)
	}
	job, err := NewQueue("dst", rdb).Get()
	if err != nil {
		t.Fatalf("get from dst: %s", err)
	}
	if job.ID != id {
		t.Fatalf("job id mismatch: want %s got %s", id, job.ID)
	}
	if _, err := q.Get(); !errors.Is(err, redis.Nil) {
		t.Fatalf("src queue should stay empty, got %v", err)
	}
}

// The queue is FIFO: jobs come out in publish order.
func TestFIFOOrder(t *testing.T) {
	q, _, _ := newTestQueue(t, "fifo")
	for _, s := range []string{"a", "b", "c"} {
		if _, err := q.Pub(s); err != nil {
			t.Fatalf("pub: %s", err)
		}
	}
	for _, want := range []string{"a", "b", "c"} {
		job, err := q.Get()
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
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if _, err := q.Pub(j); err != nil {
					t.Errorf("pub: %s", err)
				}
			}
		}()
	}
	wg.Wait()
	n, err := rdb.LLen(context.Background(), "conc:queue").Result()
	if err != nil || n != 100 {
		t.Fatalf("want 100 queued jobs, got %d (err %v)", n, err)
	}
}

func TestPubUnmarshalablePayload(t *testing.T) {
	q, _, _ := newTestQueue(t, "badpayload")
	if _, err := q.Pub(make(chan int)); err == nil {
		t.Fatal("pub should fail on a payload msgpack can not marshal")
	}
}

func TestPubRedisDown(t *testing.T) {
	q, _, mr := newTestQueue(t, "pubdown")
	mr.SetError("redis is down")
	if _, err := q.Pub("x"); err == nil {
		t.Fatal("pub should propagate the redis error")
	}
}

func TestDrop(t *testing.T) {
	q, rdb, _ := newTestQueue(t, "drop")
	job := &Job{ID: "j1", PubAt: time.Now(), Retried: 3, Payload: []byte("x")}
	q.Drop(job)
	data, err := rdb.RPop(context.Background(), "drop:dropped").Bytes()
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

// Retry with a done context skips the backoff sleep and still republishes,
// so a worker restart does not lose the job.
func TestRetryRepublishes(t *testing.T) {
	q, _, _ := newTestQueue(t, "retry")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	job := &Job{ID: "r1", PubAt: time.Now(), Retried: 1, Payload: []byte("x")}
	awaitReturn(t, 2*time.Second, "retry with done context", func() {
		q.Retry(ctx, job)
	})
	got, err := q.Get()
	if err != nil {
		t.Fatalf("get retried job: %s", err)
	}
	if got.ID != "r1" || got.Retried != 2 {
		t.Fatalf("retried job should have Retried=2, got %+v", got)
	}
}

// The exponential backoff must be capped so a huge retried count can not
// overflow or produce an absurd delay.
func TestBackoffCap(t *testing.T) {
	cases := []struct {
		retried int
		want    time.Duration
	}{
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
		q.Retry(context.Background(), job)
	})
	got, err := q.Get()
	if err != nil {
		t.Fatalf("job should be back in the queue after the outage: %s", err)
	}
	if got.ID != "o1" || got.Retried != 1 {
		t.Fatalf("unexpected republished job: %+v", got)
	}
}

// When redis is down and the context is done, Retry gives up instead of hanging.
func TestRetryGivesUpOnDoneContext(t *testing.T) {
	q, _, mr := newTestQueue(t, "giveup")
	mr.SetError("redis is down")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	job := &Job{ID: "g1", Payload: []byte("x")}
	awaitReturn(t, 2*time.Second, "retry give-up with redis down and done context", func() {
		q.Retry(ctx, job)
	})
}
