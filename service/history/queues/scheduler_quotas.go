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

type SchedulerRateLimiter quotas.RequestRateLimiter

func NewSchedulerRateLimiter(
	namespaceMaxQPS dynamicconfig.IntPropertyFnWithNamespaceFilter,
	hostMaxQPS dynamicconfig.IntPropertyFn,
	persistenceNamespaceMaxQPS dynamicconfig.IntPropertyFnWithNamespaceFilter,
	persistenceHostMaxQPS dynamicconfig.IntPropertyFn,
) SchedulerRateLimiter {
	hostRateFn := func() float64 {
		hostMaxQPS := float64(hostMaxQPS())
		if hostMaxQPS > 0 {
			return hostMaxQPS
		}
		return float64(persistenceHostMaxQPS())
	}

	requestPriorityFn := func(req quotas.Request) int {
		// NOTE: task scheduler will use the string format for task priority as the caller type.
		// see channelQuotaRequestFn in scheduler.go
		// TODO: we don't need this hack when requestRateLimiter uses generics
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
			requestRateLimiter = newHighPriorityTaskRequestRateLimiter(
				namespaceMaxQPS,
				persistenceNamespaceMaxQPS,
				hostRateFn,
			)
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
	persistenceNamespaceMaxQPS dynamicconfig.IntPropertyFnWithNamespaceFilter,
	hostRateFn quotas.RateFn,
) quotas.RequestRateLimiter {
	hostRequestRateLimiter := quotas.NewRequestRateLimiterAdapter(
		quotas.NewDefaultOutgoingRateLimiter(hostRateFn),
	)
	namespaceRequestRateLimiterFn := func(req quotas.Request) quotas.RequestRateLimiter {
		return quotas.NewRequestRateLimiterAdapter(
			quotas.NewDefaultOutgoingRateLimiter(func() float64 {
				if namespaceQPS := float64(namespaceMaxQPS(req.Caller)); namespaceQPS > 0 {
					return namespaceQPS
				}

				if persistenceNamespaceQPS := float64(persistenceNamespaceMaxQPS(req.Caller)); persistenceNamespaceQPS > 0 {
					return persistenceNamespaceQPS
				}

				return hostRateFn()
			}),
		)
	}

	return quotas.NewMultiRequestRateLimiter(
		quotas.NewNamespaceRequestRateLimiter(namespaceRequestRateLimiterFn),
		hostRequestRateLimiter,
	)
}
