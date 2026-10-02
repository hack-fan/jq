package jq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Status holds the counters collected since the last reset.
type Status struct {
	// IsRunning is false when no counter exists, i.e. nothing happened since
	// the last reset.
	IsRunning bool
	Process   int
	Success   int
	Failed    int
	Dropped   int
	// Total is Success + Dropped: jobs that reached a final state.
	Total int
}

// Status returns the counters of the queue.
func (q *Queue) Status(ctx context.Context) (*Status, error) {
	res, err := q.rdb.HGetAll(ctx, q.keys.count).Result()
	if err != nil {
		return nil, fmt.Errorf("jq: get counters: %w", err)
	}
	status := new(Status)
	// HGetAll returns an empty map instead of redis.Nil when the key does not exist.
	if len(res) == 0 {
		return status, nil
	}
	fields := []struct {
		name string
		dst  *int
	}{
		{"process", &status.Process},
		{"success", &status.Success},
		{"failed", &status.Failed},
		{"dropped", &status.Dropped},
	}
	for _, f := range fields {
		raw := res[f.name]
		if raw == "" {
			continue
		}
		n, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("jq: invalid %s count %q", f.name, raw)
		}
		*f.dst = n
	}
	status.Total = status.Success + status.Dropped
	status.IsRunning = true
	return status, nil
}

// count increments one of the process/success/failed/dropped counters and
// records the time of the activity.
func (q *Queue) count(ctx context.Context, field string) {
	pipe := q.rdb.Pipeline()
	pipe.HIncrBy(ctx, q.keys.count, field, 1)
	pipe.Set(ctx, q.keys.active, time.Now(), 0)
	if _, err := pipe.Exec(ctx); err != nil {
		q.log.Error("count failed", "field", field, "err", err)
	}
}

// reset deletes the counters and the activity timestamp.
// Two single-key DELs in one pipeline, so it also works on Redis Cluster
// where the keys may live in different slots.
func (q *Queue) reset(ctx context.Context) {
	pipe := q.rdb.Pipeline()
	pipe.Del(ctx, q.keys.count)
	pipe.Del(ctx, q.keys.active)
	if _, err := pipe.Exec(ctx); err != nil {
		q.log.Error("reset counters failed", "err", err)
	}
}

// activeAt returns the time of the last counted activity. It falls back to
// now when unknown, so the idle reporter does not fire on a queue that never ran.
func (q *Queue) activeAt(ctx context.Context) time.Time {
	res := time.Now()
	err := q.rdb.Get(ctx, q.keys.active).Scan(&res)
	if err != nil && !errors.Is(err, redis.Nil) {
		q.log.Error("get active time failed", "err", err)
	}
	return res
}
