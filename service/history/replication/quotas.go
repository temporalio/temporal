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
)

func ClientSchedulerRateLimiterProvider() (ClientSchedulerRateLimiter, error) {
	// Experiment with no op rate limiter
	return quotas.NoopRequestRateLimiter, nil
}

func ServerSchedulerRateLimiterProvider() (ServerSchedulerRateLimiter, error) {
	// Experiment with no op rate limiter
	return quotas.NoopRequestRateLimiter, nil
}
