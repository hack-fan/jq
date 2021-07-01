package jq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

type Status struct {
	IsRunning bool
	Process   int
	Success   int
	Failed    int
	Dropped   int
	Total     int
}

// Status of the queue
func (q *Queue) Status() (*Status, error) {
	status := new(Status)
	res, err := q.rdb.HGetAll(context.Background(), q.name+":count").Result()
	if errors.Is(err, redis.Nil) {
		return status, nil
	} else if err != nil {
		return nil, err
	}
	if res["process"] != "" {
		status.Process, err = strconv.Atoi(res["process"])
		if err != nil {
			return nil, fmt.Errorf("invalid count %s", res["process"])
		}
	}
	if res["success"] != "" {
		status.Success, err = strconv.Atoi(res["success"])
		if err != nil {
			return nil, fmt.Errorf("invalid count %s", res["success"])
		}
	}
	if res["failed"] != "" {
		status.Failed, err = strconv.Atoi(res["failed"])
		if err != nil {
			return nil, fmt.Errorf("invalid count %s", res["failed"])
		}
	}
	if res["dropped"] != "" {
		status.Dropped, err = strconv.Atoi(res["dropped"])
		if err != nil {
			return nil, fmt.Errorf("invalid count %s", res["dropped"])
		}
	}
	status.Total = status.Success + status.Dropped
	status.IsRunning = true
	return status, nil
}

// count process,success,failed,dropped jobs
// the value will be cleared when idle time reached opt.Idle
func (q *Queue) count(field string) {
	ctx := context.Background()
	pipe := q.rdb.TxPipeline()
	pipe.HIncrBy(ctx, q.name+":count", field, 1)
	pipe.Set(ctx, q.name+":active", time.Now(), 0)
	_, err := pipe.Exec(ctx)
	if err != nil {
		q.log.Errorf("job queue %s count %s failed: %s", q.name, err)
	}
}

// reset the counter
func (q *Queue) reset() {
	err := q.rdb.Del(context.Background(), q.name+":count").Err()
	if err != nil {
		q.log.Errorf("queue %s reset counter failed:%s", q.name, err)
	}
	err = q.rdb.Del(context.Background(), q.name+":active").Err()
	if err != nil {
		q.log.Errorf("queue %s reset active failed:%s", q.name, err)
	}
}

func (q *Queue) activeAt() time.Time {
	var res = time.Now()
	err := q.rdb.Get(context.Background(), q.name+":active").Scan(&res)
	if err != nil && !errors.Is(err, redis.Nil) {
		q.log.Errorf("queue %s get active time failed:%s", q.name, err)
	}
	return res
}
