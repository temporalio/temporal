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
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/quotas"
)

var (
	ExecutionAPICountLimitOverride = map[string]int{
		"PollActivityTaskQueue":       1,
		"PollWorkflowTaskQueue":       1,
		"QueryWorkflow":               1,
		"UpdateWorkflowExecution":     1,
		"GetWorkflowExecutionHistory": 1,
	}

	ExecutionAPIToPriority = map[string]int{
		// priority 0
		"StartWorkflowExecution":           0,
		"SignalWithStartWorkflowExecution": 0,
		"SignalWorkflowExecution":          0,
		"RequestCancelWorkflowExecution":   0,
		"TerminateWorkflowExecution":       0,
		"GetWorkflowExecutionHistory":      0,
		"UpdateWorkflowExecution":          0,
		"PollWorkflowExecutionUpdate":      0,

		// priority 1
		"RecordActivityTaskHeartbeat":      1,
		"RecordActivityTaskHeartbeatById":  1,
		"RespondActivityTaskCanceled":      1,
		"RespondActivityTaskCanceledById":  1,
		"RespondActivityTaskFailed":        1,
		"RespondActivityTaskFailedById":    1,
		"RespondActivityTaskCompleted":     1,
		"RespondActivityTaskCompletedById": 1,
		"RespondWorkflowTaskCompleted":     1,

		// priority 2
		"ResetWorkflowExecution":             2,
		"DescribeWorkflowExecution":          2,
		"RespondWorkflowTaskFailed":          2,
		"QueryWorkflow":                      2,
		"RespondQueryTaskCompleted":          2,
		"PollWorkflowTaskQueue":              2,
		"PollActivityTaskQueue":              2,
		"GetWorkflowExecutionHistoryReverse": 2,
		"GetWorkerBuildIdCompatibility":      2,
		"GetWorkerTaskReachability":          2,
		"DeleteWorkflowExecution":            2,

		// priority 3
		"ResetStickyTaskQueue":    3,
		"DescribeTaskQueue":       3,
		"ListTaskQueuePartitions": 3,
	}

	ExecutionAPIPrioritiesOrdered = []int{0, 1, 2, 3}

	VisibilityAPIToPriority = map[string]int{
		"CountWorkflowExecutions":        0,
		"ScanWorkflowExecutions":         0,
		"ListOpenWorkflowExecutions":     0,
		"ListClosedWorkflowExecutions":   0,
		"ListWorkflowExecutions":         0,
		"ListArchivedWorkflowExecutions": 0,
	}

	VisibilityAPIPrioritiesOrdered = []int{0}

	// Special rate limiting for APIs that may insert replication tasks into a namespace replication queue.
	// The replication queue is used to propagate critical failover messages and this mapping prevents flooding the
	// queue and delaying failover.
	NamespaceReplicationInducingAPIToPriority = map[string]int{
		"UpdateNamespace":                  0,
		"UpdateWorkerBuildIdCompatibility": 1,
	}

	NamespaceReplicationInducingAPIPrioritiesOrdered = []int{0, 1}

	OtherAPIToPriority = map[string]int{
		"GetClusterInfo":      0,
		"GetSystemInfo":       0,
		"GetSearchAttributes": 0,

		"RegisterNamespace":  0,
		"DescribeNamespace":  0,
		"ListNamespaces":     0,
		"DeprecateNamespace": 0,

		"CreateSchedule":            0,
		"DescribeSchedule":          0,
		"UpdateSchedule":            0,
		"PatchSchedule":             0,
		"ListScheduleMatchingTimes": 0,
		"DeleteSchedule":            0,
		"ListSchedules":             0,

		// TODO(yx): added temporarily here; need to check if it's the right place and priority
		"DescribeBatchOperation": 0,
		"ListBatchOperations":    0,
		"StartBatchOperation":    0,
		"StopBatchOperation":     0,
	}

	OtherAPIPrioritiesOrdered = []int{0}
)

type (
	NamespaceRateBurstImpl struct {
		namespaceName string
		rateFn        dynamicconfig.FloatPropertyFnWithNamespaceFilter
		burstFn       dynamicconfig.IntPropertyFnWithNamespaceFilter
	}
)

var _ quotas.RateBurst = (*NamespaceRateBurstImpl)(nil)

func NewNamespaceRateBurst(
	namespaceName string,
	rateFn dynamicconfig.FloatPropertyFnWithNamespaceFilter,
	burstFn dynamicconfig.IntPropertyFnWithNamespaceFilter,
) *NamespaceRateBurstImpl {
	return &NamespaceRateBurstImpl{
		namespaceName: namespaceName,
		rateFn:        rateFn,
		burstFn:       burstFn,
	}
}

func (c *NamespaceRateBurstImpl) Rate() float64 {
	return c.rateFn(c.namespaceName)
}

func (c *NamespaceRateBurstImpl) Burst() int {
	return c.burstFn(c.namespaceName)
}

func NewRequestToRateLimiter(
	executionRateBurstFn quotas.RateBurst,
	visibilityRateBurstFn quotas.RateBurst,
	namespaceReplicationInducingRateBurstFn quotas.RateBurst,
	otherRateBurstFn quotas.RateBurst,
) quotas.RequestRateLimiter {
	mapping := make(map[string]quotas.RequestRateLimiter)

	executionRateLimiter := NewExecutionPriorityRateLimiter(executionRateBurstFn)
	visibilityRateLimiter := NewVisibilityPriorityRateLimiter(visibilityRateBurstFn)
	namespaceReplicationInducingRateLimiter := NewNamespaceReplicationInducingAPIPriorityRateLimiter(namespaceReplicationInducingRateBurstFn)
	otherRateLimiter := NewOtherAPIPriorityRateLimiter(otherRateBurstFn)

	for api := range ExecutionAPIToPriority {
		mapping[api] = executionRateLimiter
	}
	for api := range VisibilityAPIToPriority {
		mapping[api] = visibilityRateLimiter
	}
	for api := range NamespaceReplicationInducingAPIToPriority {
		mapping[api] = namespaceReplicationInducingRateLimiter
	}
	for api := range OtherAPIToPriority {
		mapping[api] = otherRateLimiter
	}

	return quotas.NewRoutingRateLimiter(mapping)
}

func NewExecutionPriorityRateLimiter(
	rateBurstFn quotas.RateBurst,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RequestRateLimiter)
	for priority := range ExecutionAPIPrioritiesOrdered {
		rateLimiters[priority] = quotas.NewRequestRateLimiterAdapter(quotas.NewDynamicRateLimiter(rateBurstFn, time.Minute))
	}
	return quotas.NewPriorityRateLimiter(func(req quotas.Request) int {
		if priority, ok := ExecutionAPIToPriority[req.API]; ok {
			return priority
		}
		return ExecutionAPIPrioritiesOrdered[len(ExecutionAPIPrioritiesOrdered)-1]
	}, rateLimiters)
}

func NewVisibilityPriorityRateLimiter(
	rateBurstFn quotas.RateBurst,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RequestRateLimiter)
	for priority := range VisibilityAPIPrioritiesOrdered {
		rateLimiters[priority] = quotas.NewRequestRateLimiterAdapter(quotas.NewDynamicRateLimiter(rateBurstFn, time.Minute))
	}
	return quotas.NewPriorityRateLimiter(func(req quotas.Request) int {
		if priority, ok := VisibilityAPIToPriority[req.API]; ok {
			return priority
		}
		return VisibilityAPIPrioritiesOrdered[len(VisibilityAPIPrioritiesOrdered)-1]
	}, rateLimiters)
}

func NewNamespaceReplicationInducingAPIPriorityRateLimiter(
	rateBurstFn quotas.RateBurst,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RequestRateLimiter)
	for priority := range NamespaceReplicationInducingAPIPrioritiesOrdered {
		rateLimiters[priority] = quotas.NewRequestRateLimiterAdapter(quotas.NewDynamicRateLimiter(rateBurstFn, time.Minute))
	}
	return quotas.NewPriorityRateLimiter(func(req quotas.Request) int {
		if priority, ok := NamespaceReplicationInducingAPIToPriority[req.API]; ok {
			return priority
		}
		return NamespaceReplicationInducingAPIPrioritiesOrdered[len(NamespaceReplicationInducingAPIPrioritiesOrdered)-1]
	}, rateLimiters)
}

func NewOtherAPIPriorityRateLimiter(
	rateBurstFn quotas.RateBurst,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RequestRateLimiter)
	for priority := range OtherAPIPrioritiesOrdered {
		rateLimiters[priority] = quotas.NewRequestRateLimiterAdapter(quotas.NewDynamicRateLimiter(rateBurstFn, time.Minute))
	}
	return quotas.NewPriorityRateLimiter(func(req quotas.Request) int {
		if priority, ok := OtherAPIToPriority[req.API]; ok {
			return priority
		}
		return OtherAPIPrioritiesOrdered[len(OtherAPIPrioritiesOrdered)-1]
	}, rateLimiters)
}
