package jq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/xid"
	"github.com/vmihailenco/msgpack/v5"
)

// ErrEmpty is returned by [Queue.Get] when the queue has no job.
// It wraps redis.Nil, so callers checking that still work.
var ErrEmpty = fmt.Errorf("jq: queue is empty: %w", redis.Nil)

// retryInterval is how long Retry waits between failed republish attempts.
// It is a variable so tests can shorten it.
var retryInterval = time.Minute

// keys holds the Redis keys of one queue.
type keys struct {
	queue, dropped, count, active string
}

func keysOf(name string) keys {
	return keys{
		queue:   name + ":queue",
		dropped: name + ":dropped",
		count:   name + ":count",
		active:  name + ":active",
	}
}

// Queue is a single named job queue.
type Queue struct {
	name string
	keys keys
	rdb  RedisClient
	log  *slog.Logger
}

// Option configures a Queue.
type Option func(*Queue)

// WithLogger sets the logger. By default nothing is logged.
func WithLogger(l *slog.Logger) Option {
	return func(q *Queue) { q.log = l }
}

// NewQueue creates a queue named name on rdb.
func NewQueue(name string, rdb RedisClient, opts ...Option) *Queue {
	q := &Queue{name: name, keys: keysOf(name), rdb: rdb}
	for _, opt := range opts {
		opt(q)
	}
	if q.log == nil {
		q.log = slog.New(slog.DiscardHandler)
	}
	q.log = q.log.With("queue", name)
	return q
}

// Name returns the queue name.
func (q *Queue) Name() string { return q.name }

// Pub publishes a job to this queue and returns its ID. The payload must be
// marshallable by msgpack (https://github.com/vmihailenco/msgpack).
func (q *Queue) Pub(ctx context.Context, payload any) (string, error) {
	return q.PubTo(ctx, q.name, payload)
}

// PubTo publishes a job to another queue on the same Redis.
func (q *Queue) PubTo(ctx context.Context, name string, payload any) (string, error) {
	data, err := msgpack.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("jq: marshal payload: %w", err)
	}
	job := &Job{
		ID:      xid.New().String(),
		PubAt:   time.Now(),
		Payload: data,
	}
	if err := q.push(ctx, keysOf(name).queue, job); err != nil {
		return "", err
	}
	return job.ID, nil
}

// push marshals job and LPUSHes it onto key.
func (q *Queue) push(ctx context.Context, key string, job *Job) error {
	data, err := msgpack.Marshal(job)
	if err != nil {
		return fmt.Errorf("jq: marshal job: %w", err)
	}
	if err := q.rdb.LPush(ctx, key, data).Err(); err != nil {
		return fmt.Errorf("jq: push job to redis: %w", err)
	}
	return nil
}

// Get pops one job from the queue. It returns [ErrEmpty] when there is none.
// Most callers should use [Queue.StartWorker] instead.
func (q *Queue) Get(ctx context.Context) (*Job, error) {
	data, err := q.rdb.RPop(ctx, q.keys.queue).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrEmpty
	}
	if err != nil {
		return nil, fmt.Errorf("jq: get job from redis: %w", err)
	}
	job := new(Job)
	if err := msgpack.Unmarshal(data, job); err != nil {
		return nil, fmt.Errorf("jq: unmarshal job: %w", err)
	}
	return job, nil
}

// Len returns the number of pending jobs.
func (q *Queue) Len(ctx context.Context) (int64, error) {
	n, err := q.rdb.LLen(ctx, q.keys.queue).Result()
	if err != nil {
		return 0, fmt.Errorf("jq: get queue length: %w", err)
	}
	return n, nil
}

// Retry republishes a failed job after an exponential backoff.
//
// ctx controls only the waiting, not the Redis calls: cancelling it skips the
// backoff so a stopping worker hands the job back quickly. If Redis is
// unreachable, Retry keeps trying every minute until ctx is done and then
// returns the last error, so a transient outage does not lose the job.
// Most callers should use [Queue.StartWorker] instead.
func (q *Queue) Retry(ctx context.Context, job *Job) error {
	sleep(ctx, backoff(job.Retried))
	job.Retried++
	pushCtx := context.WithoutCancel(ctx)
	for {
		err := q.push(pushCtx, q.keys.queue, job)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		q.log.Error("republish retry job failed, will try again", "job", job.ID, "err", err)
		sleep(ctx, retryInterval)
	}
}

// Drop moves a job to the dropped list. The worker calls it when a job has
// exhausted its retries and WorkerOptions.SafeDrop is set.
func (q *Queue) Drop(ctx context.Context, job *Job) error {
	return q.push(ctx, q.keys.dropped, job)
}

// backoff returns the exponential retry delay, clamped so a bogus retried
// count can not panic on a negative shift or sleep for days.
func backoff(retried int) time.Duration {
	return time.Second * time.Duration(1<<min(max(retried, 0), 10))
}

// sleep waits for duration or until ctx is done, whichever comes first.
func sleep(ctx context.Context, duration time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(duration):
	}
}
