package jq

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerProcessesJobs(t *testing.T) {
	q, _, _ := newTestQueue(t, "work")
	mustPub(t, q, testPayload{Count: 0}, testPayload{Count: 1}, testPayload{Count: 2})
	var handled atomic.Int32
	cancel, wait := startWorker(t, q, func(_ context.Context, job *Job) error {
		var p testPayload
		if err := job.Bind(&p); err != nil {
			return err
		}
		handled.Add(1)
		return nil
	}, fastOptions())

	waitFor(t, 3*time.Second, func() bool {
		return handled.Load() == 3
	}, "3 jobs handled")
	cancel()
	wait()

	status := mustStatus(t, q)
	if status.Process != 3 || status.Success != 3 || status.Failed != 0 {
		t.Fatalf("unexpected status: %+v", status)
	}
}

// A failing job is retried until MaxRetry, then counted as dropped; the job
// body is only saved to the dropped queue when SafeDrop is on.
func testWorkerRetryThenDrop(t *testing.T, safeDrop bool) {
	t.Helper()
	q, rdb, _ := newTestQueue(t, "faildrop")
	mustPub(t, q, "bad")
	opt := fastOptions()
	opt.SafeDrop = safeDrop
	cancel, wait := startWorker(t, q, func(context.Context, *Job) error {
		return errors.New("boom")
	}, opt)

	// first failure, 1s retry backoff, second failure, then dropped
	waitFor(t, 5*time.Second, func() bool {
		status, err := q.Status(t.Context())
		return err == nil && status.Dropped == 1
	}, "job dropped")
	cancel()
	wait()

	if status := mustStatus(t, q); status.Failed != 2 {
		t.Fatalf("want 2 failures (initial + 1 retry), got %+v", status)
	}
	n, err := rdb.LLen(t.Context(), "faildrop:dropped").Result()
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
	mustPub(t, q, "x")
	var handled atomic.Int32
	cancel, wait := startWorker(t, q, func(_ context.Context, job *Job) error {
		if job.Retried == 0 {
			return errors.New("first attempt fails")
		}
		handled.Add(1)
		return nil
	}, fastOptions())

	// first failure, 1s retry backoff, then success
	waitFor(t, 5*time.Second, func() bool {
		return handled.Load() == 1
	}, "retried job handled")
	cancel()
	wait()

	if status := mustStatus(t, q); status.Success != 1 || status.Failed != 1 || status.Dropped != 0 {
		t.Fatalf("unexpected status: %+v", status)
	}
	if n, _ := rdb.LLen(t.Context(), "flaky:dropped").Result(); n != 0 {
		t.Fatalf("nothing should be dropped, got %d", n)
	}
}

// A redis error while getting jobs must not kill the worker: it sleeps
// opt.Recover and picks the job up once redis is back.
func TestWorkerRecoversFromRedisError(t *testing.T) {
	q, _, mr := newTestQueue(t, "recover")
	mustPub(t, q, "x")
	mr.SetError("redis is down")
	timer := time.AfterFunc(250*time.Millisecond, func() { mr.SetError("") })
	defer timer.Stop()

	var handled atomic.Int32
	cancel, wait := startWorker(t, q, func(context.Context, *Job) error {
		handled.Add(1)
		return nil
	}, fastOptions())

	waitFor(t, 3*time.Second, func() bool {
		return handled.Load() == 1
	}, "job handled after redis recovered")
	cancel()
	wait()
}

// The worker logs through the queue logger, and a clean shutdown logs no error.
func TestWorkerLogs(t *testing.T) {
	h := new(countingHandler)
	q, _, _ := newTestQueue(t, "logger", WithLogger(slog.New(h)))
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	awaitReturn(t, 2*time.Second, "worker stop", func() {
		q.StartWorker(ctx, func(context.Context, *Job) error { return nil }, fastOptions())
	})
	if h.infos.Load() == 0 {
		t.Fatal("custom logger was never used")
	}
	if h.errors.Load() != 0 {
		t.Fatalf("a clean shutdown should not log errors, got %d", h.errors.Load())
	}
}

// StartWorker with nil options must not panic and must stop on cancel.
func TestWorkerNilOptions(t *testing.T) {
	q, _, _ := newTestQueue(t, "nilopt")
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	awaitReturn(t, 2*time.Second, "worker stop with nil options", func() {
		q.StartWorker(ctx, func(context.Context, *Job) error { return nil }, nil)
	})
}

// StartWorker must not modify the caller's options, so one struct can be
// shared between workers.
func TestWorkerDoesNotMutateOptions(t *testing.T) {
	q, _, _ := newTestQueue(t, "optcopy")
	opt := &WorkerOptions{}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	awaitReturn(t, 2*time.Second, "worker stop", func() {
		q.StartWorker(ctx, func(context.Context, *Job) error { return nil }, opt)
	})
	if opt.MaxRetry != 0 || opt.Parallel != 0 || opt.Interval != 0 || opt.Idle != 0 || opt.Recover != 0 {
		t.Fatalf("options were mutated: %+v", opt)
	}
}

// The reporter must not fire while a handler is still running, even if the
// queue is empty and the last counter update is older than Idle.
func TestWorkerNoReportWhileJobInFlight(t *testing.T) {
	q, _, _ := newTestQueue(t, "inflightreport")
	mustPub(t, q, "slow")
	var reports atomic.Int32
	release := make(chan struct{})
	opt := fastOptions()
	opt.Idle = 100 * time.Millisecond
	opt.Reporter = func(*Status) { reports.Add(1) }
	cancel, wait := startWorker(t, q, func(context.Context, *Job) error {
		<-release
		return nil
	}, opt)

	// Well past Idle with the queue empty, but the job is still running.
	time.Sleep(400 * time.Millisecond)
	if n := reports.Load(); n != 0 {
		t.Fatalf("reporter fired %d times while a job was in flight", n)
	}
	close(release)
	waitFor(t, 3*time.Second, func() bool { return reports.Load() == 1 }, "reporter fired after the job finished")
	cancel()
	wait()
	if status := mustStatus(t, q); status.IsRunning {
		t.Fatalf("counters should be reset after report, got %+v", status)
	}
}

// The handler receives the worker context, so it can observe shutdown, and
// the outcome of a job that finishes during shutdown is still counted.
func TestWorkerHandlerContext(t *testing.T) {
	q, _, _ := newTestQueue(t, "handlerctx")
	mustPub(t, q, "x")
	started := make(chan struct{})
	sawCancel := make(chan struct{})
	cancel, wait := startWorker(t, q, func(hctx context.Context, _ *Job) error {
		close(started)
		<-hctx.Done()
		close(sawCancel)
		return nil
	}, fastOptions())

	awaitReturn(t, 3*time.Second, "job start", func() { <-started })
	cancel()
	awaitReturn(t, 3*time.Second, "handler context cancelled with the worker", func() { <-sawCancel })
	wait()
	if status := mustStatus(t, q); status.Success != 1 {
		t.Fatalf("job finished during shutdown should be counted, got %+v", status)
	}
}

// Shutdown must wait for a running handler before releasing the wait group,
// so the caller can exit without abandoning a job mid-flight.
func TestWorkerDrainsInFlightJob(t *testing.T) {
	q, _, _ := newTestQueue(t, "drain")
	mustPub(t, q, "slow")
	var finished atomic.Bool
	started := make(chan struct{})
	cancel, wait := startWorker(t, q, func(context.Context, *Job) error {
		close(started)
		time.Sleep(500 * time.Millisecond)
		finished.Store(true)
		return nil
	}, fastOptions())

	awaitReturn(t, 3*time.Second, "job start", func() { <-started })
	cancel()
	wait()
	if !finished.Load() {
		t.Fatal("wait group released before the in-flight job finished")
	}
}

// The Parallel option must bound how many handlers run at once.
func TestWorkerParallelLimit(t *testing.T) {
	q, _, _ := newTestQueue(t, "parallel")
	mustPub(t, q, 0, 1, 2)
	var inflight, handled atomic.Int32
	var over atomic.Bool
	opt := fastOptions()
	opt.Parallel = 1
	cancel, wait := startWorker(t, q, func(context.Context, *Job) error {
		if inflight.Add(1) > 1 {
			over.Store(true)
		}
		time.Sleep(80 * time.Millisecond)
		inflight.Add(-1)
		handled.Add(1)
		return nil
	}, opt)

	waitFor(t, 3*time.Second, func() bool {
		return handled.Load() == 3
	}, "3 jobs handled")
	cancel()
	wait()
	if over.Load() {
		t.Fatal("Parallel=1: more than 1 handler ran concurrently")
	}
}

// Parallel=2 must actually run handlers concurrently, not serialize them.
func TestWorkerParallelConcurrency(t *testing.T) {
	q, _, _ := newTestQueue(t, "parallel2")
	mustPub(t, q, 0, 1)
	var inflight atomic.Int32
	cancel, wait := startWorker(t, q, func(context.Context, *Job) error {
		inflight.Add(1)
		defer inflight.Add(-1)
		time.Sleep(500 * time.Millisecond)
		return nil
	}, fastOptions())

	waitFor(t, 3*time.Second, func() bool {
		return inflight.Load() == 2
	}, "2 handlers running concurrently")
	cancel()
	wait()
}

// After the queue has been idle for opt.Idle, the reporter fires once with the
// collected counters, the counters reset, and it does not fire again with an
// all-zero status.
func TestWorkerIdleReporter(t *testing.T) {
	q, _, _ := newTestQueue(t, "report")
	mustPub(t, q, "one")
	var mu sync.Mutex
	var calls int
	var reported *Status
	opt := fastOptions()
	opt.Idle = 300 * time.Millisecond
	opt.Reporter = func(status *Status) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		reported = status
	}
	cancel, wait := startWorker(t, q, func(context.Context, *Job) error { return nil }, opt)

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
	wait()

	if status := mustStatus(t, q); status.IsRunning {
		t.Fatalf("counters should be reset after report, got %+v", status)
	}
}
