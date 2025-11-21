package replication

import (
	"go.temporal.io/server/common/quotas"
)

const (
	taskSchedulerToken = 1
)

type (
	ServerSchedulerRateLimiter quotas.RequestRateLimiter
	ClientSchedulerRateLimiter quotas.RequestRateLimiter
	PersistenceRateLimiter     quotas.RequestRateLimiter
)

func ClientSchedulerRateLimiterProvider() ClientSchedulerRateLimiter {
	// Experiment with no op rate limiter
	return quotas.NoopRequestRateLimiter
}

func ServerSchedulerRateLimiterProvider() ServerSchedulerRateLimiter {
	// Experiment with no op rate limiter
	return quotas.NoopRequestRateLimiter
}

func PersistenceRateLimiterProvider() PersistenceRateLimiter {
	return quotas.NoopRequestRateLimiter
}
