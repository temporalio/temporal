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

package queues

import (
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/tasks"
)

func NewSchedulerRateLimiter(
	namespaceMaxQPS dynamicconfig.IntPropertyFnWithNamespaceFilter,
	hostMaxQPS dynamicconfig.IntPropertyFn,
) quotas.RequestRateLimiter {
	hostRateFn := func() float64 { return float64(hostMaxQPS()) }

	requestPriorityFn := func(req quotas.Request) int {
		if priority, ok := tasks.PriorityValue[req.CallerType]; ok {
			return int(priority)
		}

		// default to low priority
		return int(tasks.PriorityLow)
	}

	priorityToRateLimiters := make(map[int]quotas.RequestRateLimiter, len(tasks.PriorityName))
	for priority := range tasks.PriorityName {
		var requestRateLimiter quotas.RequestRateLimiter
		if priority == tasks.PriorityHigh {
			requestRateLimiter = newHighPriorityTaskRequestRateLimiter(namespaceMaxQPS, hostMaxQPS)
		} else {
			requestRateLimiter = quotas.NewRequestRateLimiterAdapter(
				quotas.NewDefaultOutgoingRateLimiter(hostRateFn),
			)
		}
		priorityToRateLimiters[int(priority)] = requestRateLimiter
	}

	return quotas.NewPriorityRateLimiter(
		requestPriorityFn,
		priorityToRateLimiters,
	)
}

func newHighPriorityTaskRequestRateLimiter(
	namespaceMaxQPS dynamicconfig.IntPropertyFnWithNamespaceFilter,
	hostMaxQPS dynamicconfig.IntPropertyFn,
) quotas.RequestRateLimiter {
	hostRequestRateLimiter := quotas.NewRequestRateLimiterAdapter(
		quotas.NewDefaultOutgoingRateLimiter(
			func() float64 { return float64(hostMaxQPS()) },
		),
	)
	namespaceRequestRateLimiterFn := func(req quotas.Request) quotas.RequestRateLimiter {
		return quotas.NewRequestRateLimiterAdapter(
			quotas.NewDefaultOutgoingRateLimiter(func() float64 {
				if namespaceMaxQPS == nil {
					return float64(hostMaxQPS())
				}

				namespaceQPS := float64(namespaceMaxQPS(req.Caller))
				if namespaceQPS <= 0 {
					return float64(hostMaxQPS())
				}
				return namespaceQPS
			}),
		)
	}

	return quotas.NewMultiRequestRateLimiter(
		quotas.NewNamespaceRateLimiter(namespaceRequestRateLimiterFn),
		hostRequestRateLimiter,
	)
}
