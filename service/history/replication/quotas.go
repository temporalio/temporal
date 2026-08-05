package replication

import (
	"go.temporal.io/server/common/quotas"
)

const (
	taskSchedulerToken = 1
	// taskReaderToken is the cost of a single replication task read that is expected to
	// miss any read-through cache in front of persistence and hit the database.
	taskReaderToken = 1
)

type (
	ServerSchedulerRateLimiter quotas.RequestRateLimiter
	ClientSchedulerRateLimiter quotas.RequestRateLimiter
	PersistenceRateLimiter     quotas.RequestRateLimiter
	// TaskReaderRateLimiter paces replication task reads that are far enough behind the
	// tip of the queue that they are expected to be served from the database rather than
	// from cache. Requests are keyed by target cluster (Caller) and priority (CallerType)
	// so that one lagging cluster pair cannot exhaust the budget of the others.
	TaskReaderRateLimiter quotas.RequestRateLimiter
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

func TaskReaderRateLimiterProvider() TaskReaderRateLimiter {
	return quotas.NoopRequestRateLimiter
}
