# jq — Redis job queue

[![Go Reference](https://pkg.go.dev/badge/github.com/hack-fan/jq.svg)](https://pkg.go.dev/github.com/hack-fan/jq)
[![CI](https://github.com/hack-fan/jq/actions/workflows/ci.yml/badge.svg)](https://github.com/hack-fan/jq/actions/workflows/ci.yml)

A small job queue that uses a Redis list as its backend. Not Redis Streams.

Use it for:

* the simplest multi-publisher, multi-worker model;
* workloads where a few seconds of scheduling delay is fine;
* when you want to understand exactly what the queue does.

Not for:

* blocking consumers or push delivery;
* fan-out to several consumers;
* strict ordering or exactly-once delivery.

## Install

```sh
go get github.com/hack-fan/jq
```

Requires Go 1.26+ and [go-redis v9](https://github.com/redis/go-redis).

## Usage

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"

	"github.com/hack-fan/jq"
	"github.com/redis/go-redis/v9"
)

type Email struct {
	To, Subject, Body string
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	q := jq.NewQueue("email", rdb, jq.WithLogger(slog.Default()))

	// Publish. The payload can be anything msgpack can marshal.
	ctx := context.Background()
	id, err := q.Pub(ctx, Email{To: "a@example.com", Subject: "hi"})
	if err != nil {
		panic(err)
	}
	slog.Info("published", "id", id)

	// Consume until interrupted. StartWorker blocks and drains in-flight
	// jobs before returning.
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()
	q.StartWorker(ctx, func(ctx context.Context, job *jq.Job) error {
		var e Email
		if err := job.Bind(&e); err != nil {
			return err
		}
		return send(ctx, e) // returning an error schedules a retry
	}, &jq.WorkerOptions{
		Parallel: 4,
		MaxRetry: 5,
	})
}
```

### Worker options

| Option     | Default | Meaning                                                            |
|------------|---------|--------------------------------------------------------------------|
| `MaxRetry` | 3       | Retries before a job is dropped. Backoff is 1s, 2s, 4s, … (max 1024s). |
| `Parallel` | 2       | Jobs handled concurrently.                                          |
| `Interval` | 3s      | Wait when the queue is empty.                                       |
| `Recover`  | 1m      | Wait after a Redis error.                                           |
| `Idle`     | 3m      | Inactivity before `Reporter` is called and counters reset.          |
| `Reporter` | nil     | Receives a `*Status` with the counters collected since the last report. |
| `SafeDrop` | false   | Keep jobs that exhausted their retries in `<name>:dropped`.         |
| `WG`       | nil     | A `sync.WaitGroup` the worker joins for the duration of `StartWorker`. |

### Redis keys

For a queue named `email`:

| Key             | Type | Content                                          |
|-----------------|------|--------------------------------------------------|
| `email:queue`   | list | pending jobs                                     |
| `email:dropped` | list | jobs that exhausted their retries (`SafeDrop`)   |
| `email:count`   | hash | `process` / `success` / `failed` / `dropped`     |
| `email:active`  | str  | time of the last counter update (binary `time.Time`) |

### Lower level API

`Get`, `Retry`, `Drop`, `Len` and `Status` are exported so you can write your
own worker loop. `Get` returns `jq.ErrEmpty` (which also matches `redis.Nil`)
when there is nothing to do.

## Delivery semantics

A job is popped from the list before it is handled. If the process dies while
a handler is running, that job is lost. Cancelling the worker context is safe:
running handlers finish, pending retries are pushed back immediately, and the
final counters are still recorded.

## License

MIT
