package jq

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

// HandlerFunc processes one job. It is called concurrently from up to
// WorkerOptions.Parallel goroutines, so it must be safe for concurrent use.
// ctx is the worker context: it is cancelled when the worker is stopping.
type HandlerFunc func(ctx context.Context, job *Job) error

// ReportFunc receives the counters collected since the last report.
// See WorkerOptions.Idle and WorkerOptions.Reporter.
type ReportFunc func(status *Status)

// WorkerOptions tunes a worker. The zero value uses the documented defaults.
type WorkerOptions struct {
	// MaxRetry is how many times a failed job is retried. Default: 3.
	MaxRetry int
	// Parallel is the number of jobs handled concurrently. Default: 2.
	Parallel int
	// Interval is how long to wait when the queue is empty. Default: 3s.
	Interval time.Duration
	// Idle is how long the queue must be inactive before Reporter is called
	// and the counters are reset. Default: 3min.
	Idle time.Duration
	// Reporter, when set, receives the counters after Idle of inactivity.
	Reporter ReportFunc
	// Recover is how long to wait after a Redis error. Default: 1min.
	Recover time.Duration
	// SafeDrop keeps jobs that exhausted their retries in the dropped list
	// instead of discarding them. Default: false, so the list can not grow forever.
	SafeDrop bool
	// WG, when set, is incremented when StartWorker starts and decremented
	// when it returns. Since the Add happens inside StartWorker, do not Wait
	// on it before StartWorker has begun; wg.Go(func() { q.StartWorker(...) })
	// is the simpler alternative.
	WG *sync.WaitGroup
}

func (opt *WorkerOptions) ensure() {
	if opt.MaxRetry == 0 {
		opt.MaxRetry = 3
	}
	if opt.Parallel <= 0 {
		opt.Parallel = 2
	}
	if opt.Interval == 0 {
		opt.Interval = 3 * time.Second
	}
	if opt.Idle == 0 {
		opt.Idle = 3 * time.Minute
	}
	if opt.Recover == 0 {
		opt.Recover = time.Minute
	}
}

// StartWorker consumes the queue with handle until ctx is done, then waits
// for in-flight jobs and pending retries before returning.
// A nil opt uses the defaults. opt is copied, so it can be shared between workers.
func (q *Queue) StartWorker(ctx context.Context, handle HandlerFunc, opt *WorkerOptions) {
	var o WorkerOptions
	if opt != nil {
		o = *opt
	}
	o.ensure()
	if o.WG != nil {
		o.WG.Add(1)
		defer o.WG.Done()
	}
	w := &worker{
		q:   q,
		opt: &o,
		fn:  handle,
		sem: semaphore.NewWeighted(int64(o.Parallel)),
		ctx: ctx,
		// The worker ctx means "stop taking work", never "abort Redis I/O":
		// a job's outcome must be recorded even when it finishes during shutdown.
		rctx: context.WithoutCancel(ctx),
	}
	w.run()
}

// worker is the state of one StartWorker call.
type worker struct {
	q   *Queue
	opt *WorkerOptions
	fn  HandlerFunc
	sem *semaphore.Weighted
	// ctx is cancelled to stop the worker; it gates waiting and the handler.
	ctx context.Context
	// rctx is ctx without cancellation, used for every Redis call.
	rctx context.Context
	// inflight tracks every goroutine the worker spawns (jobs and retries),
	// so run does not return while work is in flight.
	inflight sync.WaitGroup
}

func (w *worker) run() {
	q, opt, ctx := w.q, w.opt, w.ctx
	q.log.Info("worker started")
	for ctx.Err() == nil {
		// Acquire only fails when ctx is done.
		if err := w.sem.Acquire(ctx, 1); err != nil {
			break
		}
		// Get uses the cancellable ctx on purpose: a job must not be popped
		// once shutdown has started, or it would run against a dead ctx,
		// fail, and burn a retry.
		job, err := q.Get(ctx)
		if err != nil {
			w.sem.Release(1)
			switch {
			case ctx.Err() != nil:
				// Stopping, the error is just the cancelled context.
			case errors.Is(err, ErrEmpty):
				// Idleness can only be observed when the queue is empty.
				w.maybeReport()
				sleep(ctx, opt.Interval)
			default:
				q.log.Error("get job failed", "err", err)
				sleep(ctx, opt.Recover)
			}
			continue
		}
		w.inflight.Go(func() {
			defer w.sem.Release(1)
			w.handle(job)
		})
	}
	w.inflight.Wait()
	q.log.Info("worker stopped", "reason", ctx.Err())
}

// handle runs one job and records the outcome.
func (w *worker) handle(job *Job) {
	q := w.q
	q.count(w.rctx, "process")
	start := time.Now()
	err := w.fn(w.ctx, job)
	took := time.Since(start)
	if err == nil {
		q.log.Info("job done", "job", job.ID, "took", took)
		q.count(w.rctx, "success")
		return
	}
	q.log.Error("job failed", "job", job.ID, "took", took, "retried", job.Retried, "err", err)
	q.count(w.rctx, "failed")
	if job.Retried >= w.opt.MaxRetry {
		q.log.Error("job retry limit exceeded", "job", job.ID, "retried", job.Retried)
		q.count(w.rctx, "dropped")
		if w.opt.SafeDrop {
			if err := q.Drop(w.rctx, job); err != nil {
				q.log.Error("save dropped job failed", "job", job.ID, "err", err)
			}
		}
		return
	}
	w.inflight.Go(func() {
		if err := q.Retry(w.ctx, job); err != nil {
			q.log.Error("retry job lost", "job", job.ID, "err", err)
		}
	})
}

// maybeReport calls opt.Reporter and resets the counters when the queue has
// been idle for opt.Idle and no job is in flight.
func (w *worker) maybeReport() {
	q, opt := w.q, w.opt
	if opt.Reporter == nil {
		return
	}
	// Holding every slot proves no handler is running, so the counters are
	// not reset under a job that would then finish against zeroed counters.
	all := int64(opt.Parallel)
	if !w.sem.TryAcquire(all) {
		return
	}
	defer w.sem.Release(all)
	if time.Since(q.activeAt(w.rctx)) < opt.Idle {
		return
	}
	status, err := q.Status(w.rctx)
	if err != nil {
		q.log.Error("get status failed", "err", err)
		return
	}
	if !status.IsRunning {
		return
	}
	q.reset(w.rctx)
	opt.Reporter(status)
}
