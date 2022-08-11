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
	CallerTypePriority = map[string]int{
		headers.CallerTypeAPI:        0,
		headers.CallerTypeBackground: 2,
	}

	APIPriorityOverride = map[string]int{
		"GetOrCreateShard": 0,
		"UpdateShard":      0,

		// This is a preprequisite for checkpoint queue process progress
		"RangeCompleteHistoryTasks": 0,

		// Task resource isolation assumes task can always be loaded.
		// When one namespace has high load, all task processing goroutines
		// may be busy and consumes all persistence request tokens, preventing
		// tasks for other namespaces to be loaded. So give task loading a higher
		// priority than other background requests.
		// NOTE: we also don't want task loading to consume all persistence request tokens,
		// and blocks all other operations. This is done by limiting the total rps allow
		// for this priority.
		// TODO: exclude certain task type from this override, like replication.
		"GetHistoryTasks": 1,
	}

	RequestPrioritiesOrdered = []int{0, 1, 2}

	PriorityRatePercentage = map[int]float64{
		0: 1.0,
		1: 0.8,
		2: 1.0,
	}
)

func NewPriorityRateLimiter(
	maxQps PersistenceMaxQps,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RateLimiter)
	for priority := range RequestPrioritiesOrdered {
		rateLimiters[priority] = quotas.NewDefaultOutgoingRateLimiter(
			func() float64 { return float64(maxQps()) * PriorityRatePercentage[priority] },
		)
	}

	return quotas.NewPriorityRateLimiter(
		func(req quotas.Request) int {
			if priority, ok := APIPriorityOverride[req.API]; ok {
				return priority
			}

			if priority, ok := CallerTypePriority[req.CallerType]; ok {
				return priority
			}

			// default requests to high priority to be consistent with existing behavior
			return RequestPrioritiesOrdered[0]
		},
		rateLimiters,
	)
}

func NewNoopPriorityRateLimiter(
	maxQps PersistenceMaxQps,
) quotas.RequestRateLimiter {
	priority := RequestPrioritiesOrdered[0]

	return quotas.NewPriorityRateLimiter(
		func(_ quotas.Request) int { return priority },
		map[int]quotas.RateLimiter{
			priority: quotas.NewDefaultOutgoingRateLimiter(
				func() float64 { return float64(maxQps()) },
			),
		},
	)
}
