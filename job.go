package jq

import (
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// Job is what redis stored in list
type Job struct {
	ID      string
	PubAt   time.Time
	Retried int
	Payload []byte
}

// Bind job payload to target, make sure the target is a pointer, same as you published before.
func (j *Job) Bind(v interface{}) error {
	return msgpack.Unmarshal(j.Payload, v)
}
