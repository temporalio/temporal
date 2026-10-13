package scheduleaudit

import (
	"context"
	"hash/fnv"

	"golang.org/x/time/rate"
)

const namespaceRateLimiterShards = 64

// NamespaceRateLimiter bounds the outbound RPC rate per namespace using a fixed set of hashed limiter shards. A
// namespace always maps to the same shard, so it cannot exceed rps; collisions only make unrelated namespaces share a
// budget. The fixed shard count keeps memory bounded for arbitrarily large target streams.
type NamespaceRateLimiter struct {
	shards []*rate.Limiter
}

// NewNamespaceRateLimiter returns a limiter allowing rps requests per second per namespace. A non-positive rps disables
// limiting (Wait returns immediately).
func NewNamespaceRateLimiter(rps int) *NamespaceRateLimiter {
	if rps <= 0 {
		return &NamespaceRateLimiter{}
	}
	shards := make([]*rate.Limiter, namespaceRateLimiterShards)
	for i := range shards {
		shards[i] = rate.NewLimiter(rate.Limit(rps), rps)
	}
	return &NamespaceRateLimiter{shards: shards}
}

// Wait blocks until a request to namespace may proceed under its rate limit, or until ctx is cancelled.
func (n *NamespaceRateLimiter) Wait(ctx context.Context, namespace string) error {
	if n == nil || len(n.shards) == 0 {
		return nil
	}
	return n.limiterFor(namespace).Wait(ctx)
}

func (n *NamespaceRateLimiter) limiterFor(namespace string) *rate.Limiter {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(namespace))
	return n.shards[hash.Sum64()%uint64(len(n.shards))]
}
