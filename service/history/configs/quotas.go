package configs

import (
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/quotas"
)

var (
	CallerTypeToPriority = map[string]int{
		headers.CallerTypeOperator:       quotas.OperatorPriority,
		headers.CallerTypeAPI:            1,
		headers.CallerTypeBackgroundHigh: 2,
		headers.CallerTypeBackgroundLow:  3,
		headers.CallerTypePreemptable:    4,
	}

	APIPrioritiesOrdered = []int{quotas.OperatorPriority, 1, 2, 3, 4}
)

func NewPriorityRateLimiter(
	rateFn quotas.RateFn,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
) quotas.RequestRateLimiter {
	return quotas.NewPriorityRateLimiterHelper(
		quotas.NewDefaultIncomingRateBurst(rateFn),
		operatorRPSRatio,
		RequestToPriority,
		APIPrioritiesOrdered,
	)
}

func NewNamespaceRateLimiter(
	namespaceRateFn quotas.NamespaceRateFn,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
) quotas.RequestRateLimiter {
	return quotas.NewNamespaceRequestRateLimiter(
		func(req quotas.Request) quotas.RequestRateLimiter {
			return quotas.NewPriorityRateLimiterHelper(
				quotas.NewNamespaceRateBurst(
					req.Caller,
					namespaceRateFn,
					// TODO: We can consider adding a separate burst ratio dynamic config
					// on namespace level rate limiter if needed.
					quotas.DefaultIncomingNamespaceBurstRatioFn,
				),
				operatorRPSRatio,
				RequestToPriority,
				APIPrioritiesOrdered,
			)
		},
	)
}

func RequestToPriority(req quotas.Request) int {
	if priority, ok := CallerTypeToPriority[req.CallerType]; ok {
		return priority
	}
	// unknown caller type, default to api to be consistent with existing behavior
	return CallerTypeToPriority[headers.CallerTypeAPI]
}
