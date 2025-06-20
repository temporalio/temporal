package configs

import (
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/quotas"
)

const (
	// OperatorPriority is used to give precedence to calls coming from web UI or tctl
	OperatorPriority = 0
)

var (
	CallerTypeToPriority = map[string]int{
		headers.CallerTypeOperator:       OperatorPriority,
		headers.CallerTypeAPI:            1,
		headers.CallerTypeBackgroundHigh: 2,
		headers.CallerTypeBackgroundLow:  3,
		headers.CallerTypePreemptable:    4,
	}

	APIPrioritiesOrdered = []int{OperatorPriority, 1, 2, 3, 4}
)

func NewPriorityRateLimiter(
	rateFn quotas.RateFn,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RequestRateLimiter)
	for priority := range APIPrioritiesOrdered {
		if priority == OperatorPriority {
			rateLimiters[priority] = quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultIncomingRateLimiter(operatorRateFn(rateFn, operatorRPSRatio)))
		} else {
			rateLimiters[priority] = quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultIncomingRateLimiter(rateFn))
		}
	}
	return quotas.NewPriorityRateLimiter(func(req quotas.Request) int {
		if priority, ok := CallerTypeToPriority[req.CallerType]; ok {
			return priority
		}
		// unknown caller type, default to api to be consistent with existing behavior
		return CallerTypeToPriority[headers.CallerTypeAPI]
	}, rateLimiters)
}

func operatorRateFn(
	rateFn quotas.RateFn,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
) quotas.RateFn {
	return func() float64 {
		return operatorRPSRatio() * rateFn()
	}
}
