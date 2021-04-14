package jq

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisClient is because go-redis has many kind of clients.
type RedisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Incr(ctx context.Context, key string) *redis.IntCmd
	HIncrBy(ctx context.Context, key, field string, incr int64) *redis.IntCmd
	Exists(ctx context.Context, keys ...string) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	RPop(ctx context.Context, key string) *redis.StringCmd
	LLen(ctx context.Context, key string) *redis.IntCmd
}

// Logger can be logrus or zap sugared logger, or your own.
type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// defaultLogger do nothing
type defaultLogger struct{}

func (defaultLogger) Debugf(_ string, _ ...interface{}) {}
func (defaultLogger) Infof(_ string, _ ...interface{})  {}
func (defaultLogger) Errorf(_ string, _ ...interface{}) {}

func sleep(ctx context.Context, duration time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(duration):
	}
}
