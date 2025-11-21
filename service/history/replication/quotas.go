package replication

import (
	"go.temporal.io/server/common/quotas"
	historyi "go.temporal.io/server/service/history/interfaces"
)

const (
	taskSchedulerToken = 1
)

type (
	ServerSchedulerRateLimiter quotas.RequestRateLimiter
	ClientSchedulerRateLimiter quotas.RequestRateLimiter
	PersistenceRateLimiter     quotas.RateLimiter
)

func ClientSchedulerRateLimiterProvider() ClientSchedulerRateLimiter {
	// Experiment with no op rate limiter
	return quotas.NoopRequestRateLimiter
}

func ServerSchedulerRateLimiterProvider() ServerSchedulerRateLimiter {
	// Experiment with no op rate limiter
	return quotas.NoopRequestRateLimiter
}

func PersistenceRateLimiterProvider(shardContext historyi.ShardContext) PersistenceRateLimiter {
	return quotas.NewDefaultOutgoingRateLimiter(
		func() float64 { return shardContext.GetConfig().ReplicationTaskProcessorApplyPersistenceQPS() },
	)
}
