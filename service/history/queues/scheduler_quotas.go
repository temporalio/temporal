package queues

import (
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/tasks"
)

type SchedulerRateLimiter quotas.RequestRateLimiter

func NewPrioritySchedulerRateLimiter(
	namespaceRateFn quotas.NamespaceRateFn,
	hostRateFn quotas.RateFn,
	persistenceNamespaceRateFn quotas.NamespaceRateFn,
	persistenceHostRateFn quotas.RateFn,
) (SchedulerRateLimiter, error) {

	namespaceRateFnWithFallback := func(namespace string) float64 {
		if rate := namespaceRateFn(namespace); rate > 0 {
			return rate
		}

		return persistenceNamespaceRateFn(namespace)
	}

	hostRateFnWithFallback := func() float64 {
		if rate := hostRateFn(); rate > 0 {
			return rate
		}

		return persistenceHostRateFn()
	}

	requestPriorityFn := func(req quotas.Request) int {
		// NOTE: task scheduler will use the string format for task priority as the caller type.
		// see channelQuotaRequestFn in scheduler.go
		// TODO: we don't need this hack when requestRateLimiter uses generics
		if priority, ok := tasks.CallerTypeToPriority[req.CallerType]; ok {
			return int(priority)
		}

		// default to low priority
		return int(tasks.PriorityPreemptable)
	}

	priorityToRateLimiters := make(map[int]quotas.RequestRateLimiter, len(tasks.PriorityName))
	for priority := range tasks.PriorityName {
		priorityToRateLimiters[int(priority)] = newTaskRequestRateLimiter(
			namespaceRateFnWithFallback,
			hostRateFnWithFallback,
		)
	}

	priorityLimiter := quotas.NewPriorityRateLimiter(requestPriorityFn, priorityToRateLimiters)

	return priorityLimiter, nil
}

func newTaskRequestRateLimiter(
	namespaceRateFn quotas.NamespaceRateFn,
	hostRateFn quotas.RateFn,
) quotas.RequestRateLimiter {
	hostRequestRateLimiter := quotas.NewRequestRateLimiterAdapter(
		quotas.NewDefaultIncomingRateLimiter(hostRateFn),
	)
	namespaceRequestRateLimiterFn := func(req quotas.Request) quotas.RequestRateLimiter {
		if len(req.Caller) == 0 {
			return quotas.NoopRequestRateLimiter
		}

		return quotas.NewRequestRateLimiterAdapter(
			quotas.NewDefaultIncomingRateLimiter(
				func() float64 {
					if rate := namespaceRateFn(req.Caller); rate > 0 {
						return rate
					}

					return hostRateFn()
				},
			),
		)
	}

	return quotas.NewMultiRequestRateLimiter(
		quotas.NewNamespaceRequestRateLimiter(namespaceRequestRateLimiterFn),
		hostRequestRateLimiter,
	)
}
