package jq

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisClient is the subset of go-redis used by this package, so any of the
// go-redis client types (Client, ClusterClient, Ring, UniversalClient) fits.
type RedisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	HIncrBy(ctx context.Context, key, field string, incr int64) *redis.IntCmd
	HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd
	LPush(ctx context.Context, key string, values ...any) *redis.IntCmd
	RPop(ctx context.Context, key string) *redis.StringCmd
	LLen(ctx context.Context, key string) *redis.IntCmd
	Pipeline() redis.Pipeliner
}
