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
	"go.temporal.io/server/common/quotas"
)

var (
	APIToPriority = map[string]int{
		"AddActivityTask":             0,
		"AddWorkflowTask":             0,
		"CancelOutstandingPoll":       0,
		"DescribeTaskQueue":           0,
		"ListTaskQueuePartitions":     0,
		"PollActivityTaskQueue":       0,
		"PollWorkflowTaskQueue":       0,
		"QueryWorkflow":               0,
		"RespondQueryTaskCompleted":   0,
		"GetWorkerBuildIdOrdering":    0,
		"UpdateWorkerBuildIdOrdering": 0,
	}

	APIPriorities = map[int]struct{}{
		0: {},
	}
)

func NewPriorityRateLimiter(
	rateFn quotas.RateFn,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RateLimiter)
	for priority := range APIPriorities {
		rateLimiters[priority] = quotas.NewDefaultIncomingRateLimiter(rateFn)
	}
	return quotas.NewPriorityRateLimiter(APIToPriority, rateLimiters)
}
