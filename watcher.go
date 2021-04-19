package jq

import (
	"context"
	"errors"
	"fmt"
	"strconv"

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
		status.Process, err = strconv.Atoi(res["dropped"])
		if err != nil {
			return nil, fmt.Errorf("invalid count %s", res["dropped"])
		}
	}
	status.Total = status.Success + status.Dropped
	status.IsRunning = true
	return status, nil
}
