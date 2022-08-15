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

package client

import (
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/quotas"
)

var (
	CallerTypeDefaultPriority = map[string]int{
		headers.CallerTypeAPI:        1,
		headers.CallerTypeBackground: 2,
	}

	APITypeCallOriginPriorityOverride = map[string]int{
		"StartWorkflowExecution":           0,
		"SignalWithStartWorkflowExecution": 0,
		"SignalWorkflowExecution":          0,
		"RequestCancelWorkflowExecution":   0,
		"TerminateWorkflowExecution":       0,
		"GetWorkflowExecutionHistory":      0,
		"UpdateWorkflow":                   0,
	}

	BackgroundTypeAPIPriorityOverride = map[string]int{
		"GetOrCreateShard": 0,
		"UpdateShard":      0,

		// this is a preprequisite for checkpoint queue process progress
		"RangeCompleteHistoryTasks": 0,
	}

	RequestPrioritiesOrdered = []int{0, 1, 2}
)

func NewPriorityRateLimiter(
	namespaceMaxQPS PersistenceNamespaceMaxQps,
	hostMaxQPS PersistenceMaxQps,
) quotas.RequestRateLimiter {
	hostRequestRateLimiter := newPriorityRateLimiter(
		func() float64 { return float64(hostMaxQPS()) },
	)

	return quotas.NewNamespaceRateLimiter(func(req quotas.Request) quotas.RequestRateLimiter {
		if req.Caller != "" && req.Caller != headers.CallerNameSystem {
			if namespaceMaxQPS != nil && namespaceMaxQPS(req.Caller) > 0 {
				return quotas.NewMultiRequestRateLimiter(
					newPriorityRateLimiter(
						func() float64 { return float64(namespaceMaxQPS(req.Caller)) },
					),
					hostRequestRateLimiter,
				)
			}
		}

		return hostRequestRateLimiter
	})
}

func newPriorityRateLimiter(
	rateFn quotas.RateFn,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RateLimiter)
	for priority := range RequestPrioritiesOrdered {
		rateLimiters[priority] = quotas.NewDefaultOutgoingRateLimiter(rateFn)
	}

	return quotas.NewPriorityRateLimiter(
		func(req quotas.Request) int {
			switch req.CallerType {
			case headers.CallerTypeAPI:
				if priority, ok := APITypeCallOriginPriorityOverride[req.Initiation]; ok {
					return priority
				}
				return CallerTypeDefaultPriority[req.CallerType]
			case headers.CallerTypeBackground:
				if priority, ok := BackgroundTypeAPIPriorityOverride[req.API]; ok {
					return priority
				}
				return CallerTypeDefaultPriority[req.CallerType]
			default:
				// default requests to high priority to be consistent with existing behavior
				return RequestPrioritiesOrdered[0]
			}
		},
		rateLimiters,
	)
}

func NewNoopPriorityRateLimiter(
	maxQPS PersistenceMaxQps,
) quotas.RequestRateLimiter {
	priority := RequestPrioritiesOrdered[0]

	return quotas.NewPriorityRateLimiter(
		func(_ quotas.Request) int { return priority },
		map[int]quotas.RateLimiter{
			priority: quotas.NewDefaultOutgoingRateLimiter(
				func() float64 { return float64(maxQPS()) },
			),
		},
	)
}
