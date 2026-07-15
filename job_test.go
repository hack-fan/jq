package jq

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

type testPayload struct {
	Name  string
	Count int
}

func TestJobBind(t *testing.T) {
	data, err := msgpack.Marshal(testPayload{Name: "a", Count: 2})
	if err != nil {
		t.Fatalf("marshal: %s", err)
	}
	job := &Job{Payload: data}
	var got testPayload
	if err := job.Bind(&got); err != nil {
		t.Fatalf("bind: %s", err)
	}
	if got.Name != "a" || got.Count != 2 {
		t.Fatalf("unexpected payload: %+v", got)
	}
}

func TestJobBindInvalidData(t *testing.T) {
	job := &Job{Payload: []byte{0xc1}} // reserved msgpack byte, never valid
	var got testPayload
	if err := job.Bind(&got); err == nil {
		t.Fatal("bind should fail on invalid data")
	}
}

func TestJobBindEmptyPayload(t *testing.T) {
	job := &Job{}
	var got testPayload
	if err := job.Bind(&got); err == nil {
		t.Fatal("bind should fail on an empty payload")
	}
}
