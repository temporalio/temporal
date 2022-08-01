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
		headers.CallerTypeBackground: 1,
	}

	APIPriorityOverride = map[string]int{
		"GetOrCreateShard": 0,
		"UpdateShard":      0,

		// this is a preprequisite for checkpoint queue process progress
		"RangeCompleteHistoryTasks": 0,
	}

	RequestPrioritiesOrdered = []int{0, 1}
)

func NewPriorityRateLimiter(
	rateFn quotas.RateFn,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RateLimiter)
	for priority := range RequestPrioritiesOrdered {
		rateLimiters[priority] = quotas.NewDefaultOutgoingRateLimiter(rateFn)
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
	rateFn quotas.RateFn,
) quotas.RequestRateLimiter {
	priority := RequestPrioritiesOrdered[0]

	return quotas.NewPriorityRateLimiter(
		func(_ quotas.Request) int { return priority },
		map[int]quotas.RateLimiter{
			priority: quotas.NewDefaultOutgoingRateLimiter(rateFn),
		},
	)
}
