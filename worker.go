package jq

// WorkerOptions is optional when starting a worker
type WorkerOptions struct {
	MaxRetry int
	Parallel int
	Logger   Logger
}
