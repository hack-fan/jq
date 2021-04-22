package jq

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
	"golang.org/x/sync/semaphore"
)

// HandlerFunc is your custom function to process job.
// Notice: It must be thread safe, since it will be called parallel.
type HandlerFunc func(job *Job) error

// WorkerOptions is optional when starting a worker
type WorkerOptions struct {
	// If job handler fails,max retry these times. Default:10
	MaxRetry int
	// Parallel worker count. Default:2
	Parallel int64
	// If there is no job, worker will take a break Default: 3s
	Interval time.Duration
	// If the workers are inactive during these duration, watcher will do a report. Default: 3min
	Idle time.Duration
	// If a redis server error occurred, wait and retry. Default: 1min
	Recover time.Duration
	// If a job exceeds the max retry time, save it to dropped queue, or really dropped.
	// Default is false, avoiding memory leaks.
	SafeDrop bool
	// You can use your own logger
	Logger Logger
}

func (opt *WorkerOptions) ensure() {
	if opt == nil {
		*opt = WorkerOptions{}
	}
	if opt.MaxRetry == 0 {
		opt.MaxRetry = 10
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
				job, err := q.Get(ctx)
				if errors.Is(err, redis.Nil) {
					// Empty queue, wait a while
					sleep(ctx, opt.Interval)
					return
				} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					q.log.Errorf("context dead: %s", err)
					return
				} else if err != nil {
					q.log.Errorf("job queue %s get job error: %s", q.name, err)
					// Perhaps redis down,network error,sleep and retry
					sleep(ctx, opt.Recover)
					return
				}
				q.count(ctx, "process", opt)
				// Step 2: Handle job
				start := time.Now()
				err = handle(job)
				if err != nil {
					q.log.Errorf("[%s] job [%s] used %s failed: %s", q.name, job.ID, time.Since(start), err)
					// Count failed
					q.count(ctx, "failed", opt)
					// Retry or not
					if job.Retried >= opt.MaxRetry {
						q.log.Errorf("[%s] job [%s] retry limit exceeded: %s", q.name, job.ID, time.Since(start))
						q.count(ctx, "dropped", opt)
						q.Drop(job)
						return
					}
					go q.Retry(ctx, job)
					return
				}
				q.log.Infof("[%s] job [%s] used %s", q.name, job.ID, time.Since(start))
				// Count success
				q.count(ctx, "success", opt)
			}()
		}
	}
}

// count process,success,failed,dropped jobs
// the value will be cleared when idle time reached opt.Idle
func (q *Queue) count(ctx context.Context, field string, opt *WorkerOptions) {
	key := q.name + ":count"
	pipe := q.rdb.TxPipeline()
	pipe.HIncrBy(ctx, key, field, 1)
	pipe.Expire(ctx, key, opt.Idle)
	_, err := pipe.Exec(ctx)
	if err != nil {
		q.log.Errorf("job queue %s count %s failed: %s", q.name, err)
	}
}
