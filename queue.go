package jq

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/xid"
	"github.com/vmihailenco/msgpack/v5"
)

// Queue is just one queue
type Queue struct {
	name string
	rdb  RedisClient
	log  Logger
}

// NewQueue create a queue
func NewQueue(name string, rdb RedisClient) *Queue {
	return &Queue{
		name: name,
		rdb:  rdb,
		log:  defaultLogger{},
	}
}

// Pub publish a job to queue，the payload must be able to be marshalled by
// [msgpack](https://github.com/vmihailenco/msgpack).
func (q *Queue) Pub(payload interface{}) (string, error) {
	return q.PubTo(q.name, payload)
}

// PubTo can pub a job to another queue which in same redis
func (q *Queue) PubTo(name string, payload interface{}) (string, error) {
	tmp, err := msgpack.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("your payload can not be marshalled by msgpack: %w", err)
	}
	var job = &Job{
		ID:      xid.New().String(),
		PubAt:   time.Now(),
		Retried: 0,
		Payload: tmp,
	}
	err = q.publish(name, job)
	if err != nil {
		return "", err
	}
	return job.ID, nil
}

// publish <job> to <queue>
func (q *Queue) publish(queue string, job *Job) error {
	data, err := msgpack.Marshal(job)
	if err != nil {
		return fmt.Errorf("your payload can not be marshalled by msgpack: %w", err)
	}
	err = q.rdb.LPush(context.Background(), queue+":queue", data).Err()
	if err != nil {
		return fmt.Errorf("push job to redis failed: %w", err)
	}
	return nil
}

// Get a single job from redis.
// The error returned would be redis.Nil, use errors.Is to check it.
// This function is not normally used, unless you want to write your own worker.
// You can use our out of box StartWorker()
func (q *Queue) Get(ctx context.Context) (*Job, error) {
	data, err := q.rdb.RPop(ctx, q.name+":queue").Bytes()
	if err != nil {
		return nil, fmt.Errorf("get job from redis failed: %w", err)
	}
	var job = new(Job)
	err = msgpack.Unmarshal(data, job)
	if err != nil {
		return nil, fmt.Errorf("unmarshal job failed: %w", err)
	}
	return job, nil
}

// Retry the job, before sending job to queue, it will sleep a while.
// Use context signal to control this sleep, if worker will restart.
// This function is not normally used, unless you want to write your own worker.
// You can use our out of box StartWorker()
func (q *Queue) Retry(ctx context.Context, job *Job) {
	sleep(ctx, time.Second*time.Duration(job.Retried))
	job.Retried += 1
	err := q.publish(q.name, job)
	if err != nil {
		q.log.Errorf("send retry job %s to queue %s failed: %s", job.ID, q.name, err)
	}
}

// Drop the job,put it to drop queue,if SafeDrop is true.
func (q *Queue) Drop(job *Job) {
	data, err := msgpack.Marshal(job)
	if err != nil {
		q.log.Errorf("your payload can not be marshalled by msgpack: %s", err)
	}
	err = q.rdb.LPush(context.Background(), q.name+":dropped", data).Err()
	if err != nil {
		q.log.Errorf("push job to redis failed: %s", err)
	}
}

// sleep to ctx done or duration, the lesser one.
func sleep(ctx context.Context, duration time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(duration):
	}
}
