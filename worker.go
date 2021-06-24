package jq

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"golang.org/x/sync/semaphore"
)

// HandlerFunc is your custom function to process job.
// Notice: It must be thread safe, since it will be called parallel.
type HandlerFunc func(job *Job) error

// ReportFunc work together with worker options "Idle",custom your counter report.
type ReportFunc func(status *Status)

// WorkerOptions is optional when starting a worker
type WorkerOptions struct {
	// If job handler fails,max retry these times. Default:10
	MaxRetry int
	// Parallel worker count. Default:2
	Parallel int64
	// If there is no job, worker will take a break Default: 3s
	Interval time.Duration
	// If the workers are inactive during these duration, watcher will clear count and make a report. Default: 3min
	Idle time.Duration
	// Working together with "Idle",custom your report.
	Reporter ReportFunc
	// If a redis server error occurred, wait and retry. Default: 1min
	Recover time.Duration
	// If a job exceeds the max retry time, save it to dropped queue, or really dropped.
	// Default is false, avoiding memory leaks.
	SafeDrop bool
	// You can use your own logger
	Logger Logger
	// If you pass a wait group in,worker will release it in the end of life.
	WG *sync.WaitGroup
}

func (opt *WorkerOptions) ensure() {
	if opt == nil {
		*opt = WorkerOptions{}
	}
	if opt.MaxRetry == 0 {
		opt.MaxRetry = 3
	}
	if opt.Parallel == 0 {
		opt.Parallel = 2
	}
	if opt.Interval == 0 {
		opt.Interval = time.Second * 3
	}
	if opt.Idle == 0 {
		opt.Idle = time.Minute * 3
	}
	if opt.Recover == 0 {
		opt.Recover = time.Minute * 1
	}
}

// StartWorker is blocked.
func (q *Queue) StartWorker(ctx context.Context, handle HandlerFunc, opt *WorkerOptions) {
	// Parse options
	opt.ensure()
	// Wait group
	if opt.WG != nil {
		opt.WG.Add(1)
		defer opt.WG.Done()
	}
	// overwrite logger
	if opt.Logger != nil {
		q.log = opt.Logger
	}
	// Start the ever loop
	var sem = semaphore.NewWeighted(opt.Parallel)
	q.log.Infof("job queue worker %s start", q.name)
	for {
		select {
		case <-ctx.Done():
			q.log.Infof("job queue %s stopped by context done signal", q.name)
			return
		default:
			// Make a report if the queue has been idle for a while
			if opt.Reporter != nil && q.activeAt().Add(opt.Idle).Before(time.Now()) {
				status, err := q.Status()
				if err != nil {
					q.log.Errorf("queue %s get status failed:%s", q.name, err)
				} else {
					if status.IsRunning {
						q.reset()
						opt.Reporter(status)
					}
				}
			}
			// Acquire sem, the only error here will be context error,
			// continue for done case if it happens.
			err := sem.Acquire(ctx, 1)
			if err != nil {
				continue
			}
			// Async run job for parallel.
			go func() {
				defer sem.Release(1)
				// Step 1: Get job
				job, err := q.Get()
				if errors.Is(err, redis.Nil) {
					// Empty queue, wait a while
					sleep(ctx, opt.Interval)
					return
				} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					q.log.Infof("context dead: %s", err)
					return
				} else if err != nil {
					q.log.Errorf("job queue %s get job error: %s", q.name, err)
					// Perhaps redis down,network error,sleep and retry
					sleep(ctx, opt.Recover)
					return
				}
				q.count("process")
				// Step 2: Handle job
				start := time.Now()
				err = handle(job)
				if err != nil {
					q.log.Errorf("[%s] job [%s] used %s failed: %s", q.name, job.ID, time.Since(start), err)
					// Count failed
					q.count("failed")
					// Retry or not
					if job.Retried >= opt.MaxRetry {
						q.log.Errorf("[%s] job [%s] retry limit exceeded: %s", q.name, job.ID, time.Since(start))
						q.count("dropped")
						q.Drop(job)
						return
					}
					go q.Retry(ctx, job)
					return
				}
				q.log.Infof("[%s] job [%s] used %s", q.name, job.ID, time.Since(start))
				// Count success
				q.count("success")
			}()
		}
	}
}
