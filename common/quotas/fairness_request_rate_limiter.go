package quotas

type (
	FairnessRequestRateLimiterFn func(req Request) FairnessRequestRateLimiter
	FairnessRequestRateLimiter   interface {
		RequestRateLimiter
		GetFairnessPriority(request Request) int64
	}
)
