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
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/tasks"
)

var (
	CallerTypeDefaultPriority = map[string]int{
		headers.CallerTypeAPI:        1,
		headers.CallerTypeBackground: 3,
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

		// This is a preprequisite for checkpointing queue process progress
		p.ConstructHistoryTaskAPI("RangeCompleteHistoryTasks", tasks.CategoryTransfer):   0,
		p.ConstructHistoryTaskAPI("RangeCompleteHistoryTasks", tasks.CategoryTimer):      0,
		p.ConstructHistoryTaskAPI("RangeCompleteHistoryTasks", tasks.CategoryVisibility): 0,

		// Task resource isolation assumes task can always be loaded.
		// When one namespace has high load, all task processing goroutines
		// may be busy and consumes all persistence request tokens, preventing
		// tasks for other namespaces to be loaded. So give task loading a higher
		// priority than other background requests.
		// NOTE: we also don't want task loading to consume all persistence request tokens,
		// and blocks all other operations. This is done by setting the queue host rps limit
		// dynamic config.
		p.ConstructHistoryTaskAPI("GetHistoryTasks", tasks.CategoryTransfer):   2,
		p.ConstructHistoryTaskAPI("GetHistoryTasks", tasks.CategoryTimer):      2,
		p.ConstructHistoryTaskAPI("GetHistoryTasks", tasks.CategoryVisibility): 2,
	}

	RequestPrioritiesOrdered = []int{0, 1, 2, 3}
)

func NewPriorityRateLimiter(
	namespaceMaxQPS PersistenceNamespaceMaxQps,
	hostMaxQPS PersistenceMaxQps,
	requestPriorityFn quotas.RequestPriorityFn,
) quotas.RequestRateLimiter {
	hostRequestRateLimiter := newPriorityRateLimiter(
		func() float64 { return float64(hostMaxQPS()) },
		requestPriorityFn,
	)

	return quotas.NewNamespaceRateLimiter(func(req quotas.Request) quotas.RequestRateLimiter {
		if req.Caller != "" && req.Caller != headers.CallerNameSystem {
			return quotas.NewMultiRequestRateLimiter(
				newPriorityRateLimiter(
					func() float64 {
						if namespaceMaxQPS == nil {
							return float64(hostMaxQPS())
						}

						namespaceQPS := float64(namespaceMaxQPS(req.Caller))
						if namespaceQPS <= 0 {
							return float64(hostMaxQPS())
						}

						return namespaceQPS
					},
					requestPriorityFn,
				),
				hostRequestRateLimiter,
			)
		}

		return hostRequestRateLimiter
	})
}

func newPriorityRateLimiter(
	rateFn quotas.RateFn,
	requestPriorityFn quotas.RequestPriorityFn,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RateLimiter)
	for priority := range RequestPrioritiesOrdered {
		rateLimiters[priority] = quotas.NewDefaultOutgoingRateLimiter(rateFn)
	}

	return quotas.NewPriorityRateLimiter(
		requestPriorityFn,
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

func RequestPriorityFn(req quotas.Request) int {
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
}
