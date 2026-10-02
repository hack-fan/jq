package jq

import (
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// Job is the envelope stored in the Redis list.
type Job struct {
	ID      string
	PubAt   time.Time
	Retried int
	Payload []byte
}

// Bind decodes the job payload into v, which must be a pointer to the same
// type that was published.
func (j *Job) Bind(v any) error {
	return msgpack.Unmarshal(j.Payload, v)
}
