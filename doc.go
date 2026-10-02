// Package jq is a small job queue backed by a Redis list.
//
// A [Queue] publishes jobs with [Queue.Pub] and consumes them with
// [Queue.StartWorker]. Payloads are encoded with msgpack, so anything msgpack
// can marshal can be a payload. Failed jobs are retried with exponential
// backoff and, after WorkerOptions.MaxRetry attempts, either discarded or
// moved to a "dropped" list when WorkerOptions.SafeDrop is set.
//
// For a queue named "email", the following Redis keys are used:
//
//	email:queue    list of pending jobs
//	email:dropped  list of jobs that exhausted their retries (SafeDrop only)
//	email:count    hash of process/success/failed/dropped counters
//	email:active   timestamp of the last counter update
//
// It intentionally uses a plain list rather than Redis Streams: there are no
// consumer groups, no acknowledgements and no fan-out. A job taken by a worker
// that dies mid-handler is lost.
package jq
