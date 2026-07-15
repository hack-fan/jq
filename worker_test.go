package jq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fastOptions(wg *sync.WaitGroup) *WorkerOptions {
	return &WorkerOptions{
		MaxRetry: 1,
		Parallel: 2,
		Interval: 30 * time.Millisecond,
		Recover:  100 * time.Millisecond,
		WG:       wg,
	}
}

// waitGroupReleased asserts wg.Wait returns within timeout.
func waitGroupReleased(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	t.Helper()
	awaitReturn(t, timeout, "wait group release", wg.Wait)
}

func TestWorkerProcessesJobs(t *testing.T) {
	q, _, _ := newTestQueue(t, "work")
	for i := 0; i < 3; i++ {
		if _, err := q.Pub(testPayload{Name: "n", Count: i}); err != nil {
			t.Fatalf("pub: %s", err)
		}
	}
	var handled int32
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	go q.StartWorker(ctx, func(job *Job) error {
		var p testPayload
		if err := job.Bind(&p); err != nil {
			return err
		}
		atomic.AddInt32(&handled, 1)
		return nil
	}, fastOptions(&wg))

	waitFor(t, 3*time.Second, func() bool {
		return atomic.LoadInt32(&handled) == 3
	}, "3 jobs handled")
	cancel()
	waitGroupReleased(t, &wg, 3*time.Second)

	status, err := q.Status()
	if err != nil {
		t.Fatalf("status: %s", err)
	}
	if status.Process != 3 || status.Success != 3 || status.Failed != 0 {
		t.Fatalf("unexpected status: %+v", status)
	}
}

// A failing job is retried until MaxRetry, then counted as dropped; the job
// body is only saved to the dropped queue when SafeDrop is on.
func testWorkerRetryThenDrop(t *testing.T, safeDrop bool) {
	t.Helper()
	q, rdb, _ := newTestQueue(t, "faildrop")
	if _, err := q.Pub("bad"); err != nil {
		t.Fatalf("pub: %s", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	opt := fastOptions(&wg)
	opt.SafeDrop = safeDrop
	go q.StartWorker(ctx, func(*Job) error {
		return errors.New("boom")
	}, opt)

	// first failure, 1s retry backoff, second failure, then dropped
	waitFor(t, 5*time.Second, func() bool {
		status, err := q.Status()
		return err == nil && status.Dropped == 1
	}, "job dropped")
	cancel()
	waitGroupReleased(t, &wg, 3*time.Second)

	status, err := q.Status()
	if err != nil {
		t.Fatalf("status: %s", err)
	}
	if status.Failed != 2 {
		t.Fatalf("want 2 failures (initial + 1 retry), got %+v", status)
	}
	n, err := rdb.LLen(context.Background(), "faildrop:dropped").Result()
	if err != nil {
		t.Fatalf("llen: %s", err)
	}
	want := int64(0)
	if safeDrop {
		want = 1
	}
	if n != want {
		t.Fatalf("SafeDrop=%v: want %d jobs in dropped queue, got %d", safeDrop, want, n)
	}
}

func TestWorkerDropUnsafe(t *testing.T) { testWorkerRetryThenDrop(t, false) }
func TestWorkerDropSafe(t *testing.T)   { testWorkerRetryThenDrop(t, true) }

// A job that fails once and succeeds on retry must count as success,
// not dropped, and leave no queue behind.
func TestWorkerFailOnceThenSucceed(t *testing.T) {
	q, rdb, _ := newTestQueue(t, "flaky")
	if _, err := q.Pub("x"); err != nil {
		t.Fatalf("pub: %s", err)
	}
	var handled int32
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	go q.StartWorker(ctx, func(job *Job) error {
		if job.Retried == 0 {
			return errors.New("first attempt fails")
		}
		atomic.AddInt32(&handled, 1)
		return nil
	}, fastOptions(&wg))

	// first failure, 1s retry backoff, then success
	waitFor(t, 5*time.Second, func() bool {
		return atomic.LoadInt32(&handled) == 1
	}, "retried job handled")
	cancel()
	waitGroupReleased(t, &wg, 3*time.Second)

	status, err := q.Status()
	if err != nil {
		t.Fatalf("status: %s", err)
	}
	if status.Success != 1 || status.Failed != 1 || status.Dropped != 0 {
		t.Fatalf("unexpected status: %+v", status)
	}
	n, _ := rdb.LLen(context.Background(), "flaky:dropped").Result()
	if n != 0 {
		t.Fatalf("nothing should be dropped, got %d", n)
	}
}

// A redis error while getting jobs must not kill the worker: it sleeps
// opt.Recover and picks the job up once redis is back.
func TestWorkerRecoversFromRedisError(t *testing.T) {
	q, _, mr := newTestQueue(t, "recover")
	if _, err := q.Pub("x"); err != nil {
		t.Fatalf("pub: %s", err)
	}
	mr.SetError("redis is down")
	timer := time.AfterFunc(250*time.Millisecond, func() { mr.SetError("") })
	defer timer.Stop()

	var handled int32
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	go q.StartWorker(ctx, func(*Job) error {
		atomic.AddInt32(&handled, 1)
		return nil
	}, fastOptions(&wg))

	waitFor(t, 3*time.Second, func() bool {
		return atomic.LoadInt32(&handled) == 1
	}, "job handled after redis recovered")
	cancel()
	waitGroupReleased(t, &wg, 3*time.Second)
}

type recordLogger struct {
	infos int32
}

func (*recordLogger) Debugf(string, ...interface{})  {}
func (l *recordLogger) Infof(string, ...interface{}) { atomic.AddInt32(&l.infos, 1) }
func (*recordLogger) Errorf(string, ...interface{})  {}

// The Logger option must replace the default silent logger.
func TestWorkerCustomLogger(t *testing.T) {
	q, _, _ := newTestQueue(t, "logger")
	lg := new(recordLogger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	timer := time.AfterFunc(100*time.Millisecond, cancel)
	defer timer.Stop()
	opt := fastOptions(nil)
	opt.Logger = lg
	awaitReturn(t, 2*time.Second, "worker stop", func() {
		q.StartWorker(ctx, func(*Job) error { return nil }, opt)
	})
	if atomic.LoadInt32(&lg.infos) == 0 {
		t.Fatal("custom logger was never used")
	}
}

// StartWorker with nil options must not panic and must stop on cancel.
func TestWorkerNilOptions(t *testing.T) {
	q, _, _ := newTestQueue(t, "nilopt")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	timer := time.AfterFunc(100*time.Millisecond, cancel)
	defer timer.Stop()
	awaitReturn(t, 2*time.Second, "worker stop with nil options", func() {
		q.StartWorker(ctx, func(*Job) error { return nil }, nil)
	})
}

// Shutdown must wait for a running handler before releasing the wait group,
// so the caller can exit without abandoning a job mid-flight.
func TestWorkerDrainsInFlightJob(t *testing.T) {
	q, _, _ := newTestQueue(t, "drain")
	if _, err := q.Pub("slow"); err != nil {
		t.Fatalf("pub: %s", err)
	}
	var finished int32
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	go q.StartWorker(ctx, func(*Job) error {
		close(started)
		time.Sleep(500 * time.Millisecond)
		atomic.StoreInt32(&finished, 1)
		return nil
	}, fastOptions(&wg))

	select {
	case <-started: // handler is now mid-flight
	case <-time.After(3 * time.Second):
		t.Fatal("job never started")
	}
	cancel()
	waitGroupReleased(t, &wg, 3*time.Second)
	if atomic.LoadInt32(&finished) != 1 {
		t.Fatal("wait group released before the in-flight job finished")
	}
}

// The Parallel option must bound how many handlers run at once.
func TestWorkerParallelLimit(t *testing.T) {
	q, _, _ := newTestQueue(t, "parallel")
	for i := 0; i < 3; i++ {
		if _, err := q.Pub(i); err != nil {
			t.Fatalf("pub: %s", err)
		}
	}
	var inflight, over, handled int32
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	opt := fastOptions(&wg)
	opt.Parallel = 1
	go q.StartWorker(ctx, func(*Job) error {
		if atomic.AddInt32(&inflight, 1) > 1 {
			atomic.StoreInt32(&over, 1)
		}
		time.Sleep(80 * time.Millisecond)
		atomic.AddInt32(&inflight, -1)
		atomic.AddInt32(&handled, 1)
		return nil
	}, opt)

	waitFor(t, 3*time.Second, func() bool {
		return atomic.LoadInt32(&handled) == 3
	}, "3 jobs handled")
	cancel()
	waitGroupReleased(t, &wg, 3*time.Second)
	if atomic.LoadInt32(&over) != 0 {
		t.Fatal("Parallel=1: more than 1 handler ran concurrently")
	}
}

// Parallel=2 must actually run handlers concurrently, not serialize them.
func TestWorkerParallelConcurrency(t *testing.T) {
	q, _, _ := newTestQueue(t, "parallel2")
	for i := 0; i < 2; i++ {
		if _, err := q.Pub(i); err != nil {
			t.Fatalf("pub: %s", err)
		}
	}
	var inflight int32
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	go q.StartWorker(ctx, func(*Job) error {
		atomic.AddInt32(&inflight, 1)
		defer atomic.AddInt32(&inflight, -1)
		time.Sleep(500 * time.Millisecond)
		return nil
	}, fastOptions(&wg))

	waitFor(t, 3*time.Second, func() bool {
		return atomic.LoadInt32(&inflight) == 2
	}, "2 handlers running concurrently")
	cancel()
	waitGroupReleased(t, &wg, 3*time.Second)
}

// After the queue has been idle for opt.Idle, the reporter fires once with the
// collected counters, the counters reset, and it does not fire again with an
// all-zero status.
func TestWorkerIdleReporter(t *testing.T) {
	q, _, _ := newTestQueue(t, "report")
	if _, err := q.Pub("one"); err != nil {
		t.Fatalf("pub: %s", err)
	}
	var mu sync.Mutex
	var calls int
	var reported *Status
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	opt := fastOptions(&wg)
	opt.Idle = 300 * time.Millisecond
	opt.Reporter = func(status *Status) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		reported = status
	}
	go q.StartWorker(ctx, func(*Job) error { return nil }, opt)

	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 1
	}, "reporter called")

	// The counters were reset, so the reporter must not fire again while idle.
	time.Sleep(700 * time.Millisecond)
	mu.Lock()
	gotCalls, gotReported := calls, reported
	mu.Unlock()
	if gotCalls != 1 {
		t.Fatalf("reporter should fire once, fired %d times", gotCalls)
	}
	if gotReported.Success != 1 || !gotReported.IsRunning {
		t.Fatalf("unexpected reported status: %+v", gotReported)
	}
	cancel()
	waitGroupReleased(t, &wg, 3*time.Second)

	status, err := q.Status()
	if err != nil {
		t.Fatalf("status: %s", err)
	}
	if status.IsRunning {
		t.Fatalf("counters should be reset after report, got %+v", status)
	}
}
