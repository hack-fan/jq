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
}

// NewQueue create a queue
func NewQueue(name string, rdb RedisClient) *Queue {
	return &Queue{
		name: name,
		rdb:  rdb,
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
	var job = Job{
		ID:      xid.New().String(),
		PubAt:   time.Now(),
		Retried: 0,
		Payload: tmp,
	}
	data, err := msgpack.Marshal(job)
	if err != nil {
		return "", fmt.Errorf("your payload can not be marshalled by msgpack: %w", err)
	}
	err = q.rdb.LPush(context.Background(), name+":queue", data).Err()
	if err != nil {
		return "", fmt.Errorf("push job to redis failed: %w", err)
	}
	return job.ID, nil
}

// Get a single job from redis.
// The error returned would be redis.Nil, use errors.Is to check it.
// This is not normally used, unless you want to write your own worker.
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
