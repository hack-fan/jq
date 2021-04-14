package jq

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
)

type HandlerFunc func(job *Job) error

// WorkerOptions is optional when starting a worker
type WorkerOptions struct {
	// If job handler fails,max retry these times. Default:10
	MaxRetry int
	// Parallel worker count. Default:1
	Parallel int
	// If there is no job, worker will take a break Default: 3s
	Interval time.Duration
	// If the workers are inactive during these duration, watcher will do a report. Default: 10min
	Idle time.Duration
	// You can use your own logger
	Logger Logger
}

// StartWorker is blocked.
func (q *Queue) StartWorker(ctx context.Context, handle HandlerFunc, opt *WorkerOptions) {
	var max = 10
	// var parallel = 1
	var interval = time.Second * 3
	var idle = time.Minute * 10
	var log = defaultLogger{}
	// var sem = semaphore.NewWeighted(int64(max))
	log.Infof("job queue worker %s start", q.name)
	for {
		select {
		case <-ctx.Done():
			log.Infof("job queue %s stopped by context done signal", q.name)
			return
		default:
			job, err := q.Get(ctx)
			if err == redis.Nil {
				// Empty queue, wait a while
				sleep(ctx, interval)
				continue
			} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				log.Errorf("context dead: %s", err)
				continue
			} else if err != nil {
				log.Errorf("job queue %s get job error: %s", q.name, err)
				// Perhaps redis down,network error,sleep and retry
				sleep(ctx, time.Minute)
				continue
			}
			start := time.Now()
			err = handle(job)
			if err != nil {
				log.Errorf("[%s] job [%s] used %s failed: %s", q.name, job.ID, time.Since(start), err)
				// Count fail
				err := q.rdb.HIncrBy(ctx, q.name+":count", "failed", 1).Err()
				if err != nil {
					log.Errorf("job queue %s count failed error: %s", q.name, err)
					continue
				}
				err = q.rdb.Expire(ctx, q.name+":count", idle).Err()
				if err != nil {
					log.Errorf("job queue %s count failed ttl error: %s", q.name, err)
					continue
				}
				// Retry or not
				if job.Retried >= max {
					log.Infof("[%s] job [%s] retry limit exceeded: %s", q.name, job.ID, time.Since(start))
					continue
				}
				// 等等再加到队列，防止最后一个任务失败光速重试
				job.Retried += 1
				go func() {
					sleep(ctx, time.Second*time.Duration(job.Retried))
					// FIXME:
					// err := q.Pub(job)
					// if err != nil {
					// 	xim.Errorf("队列%s的任务[%s]添加到重试队列时失败，放弃:%s", dq.key, job.ID, err)
					// }
				}()
				// Handler error end
				continue
			}
			log.Infof("[%s] job [%s] used %s", q.name, job.ID, time.Since(start))
			// Count success
			err = q.rdb.HIncrBy(ctx, q.name+":count", "success", 1).Err()
			if err != nil {
				log.Errorf("job queue %s count success error: %s", q.name, err)
				continue
			}
			err = q.rdb.Expire(ctx, q.name+":count", idle).Err()
			if err != nil {
				log.Errorf("job queue %s count success ttl error: %s", q.name, err)
				continue
			}
		}
	}
}
