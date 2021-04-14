package jq

import "time"

// Job is what redis stored in list
type Job struct {
	ID      string
	PubAt   time.Time
	Retried int
	Payload []byte
}
