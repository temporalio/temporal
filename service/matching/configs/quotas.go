// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	APIToPriority = map[string]int{
		"AddActivityTask":                        1,
		"AddWorkflowTask":                        1,
		"CancelOutstandingPoll":                  1,
		"DescribeTaskQueue":                      1,
		"ListTaskQueuePartitions":                1,
		"PollActivityTaskQueue":                  1,
		"PollWorkflowTaskQueue":                  1,
		"QueryWorkflow":                          1,
		"RespondQueryTaskCompleted":              1,
		"GetWorkerBuildIdCompatibility":          1,
		"UpdateWorkerBuildIdCompatibility":       1,
		"GetTaskQueueUserData":                   1,
		"ApplyTaskQueueUserDataReplicationEvent": 1,
		"GetBuildIdTaskQueueMapping":             1,
		"ForceUnloadTaskQueue":                   1,
		"UpdateTaskQueueUserData":                1,
		"ReplicateTaskQueueUserData":             1,
	}

	APIPrioritiesOrdered = []int{0, 1}
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
		if req.CallerType == headers.CallerTypeOperator {
			return OperatorPriority
		}
		if priority, ok := APIToPriority[req.API]; ok {
			return priority
		}
		return APIPrioritiesOrdered[len(APIPrioritiesOrdered)-1]
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
