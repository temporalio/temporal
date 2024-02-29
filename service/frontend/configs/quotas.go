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
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/quotas"
)

const (
	// OperatorPriority is used to give precedence to calls coming from web UI or tctl
	OperatorPriority = 0
)

var (
	// ExecutionAPICountLimitOverride determines how many tokens each of these API calls consumes from their
	// corresponding quota, which is determined by dynamicconfig.FrontendMaxConcurrentLongRunningRequestsPerInstance. If
	// the value is not set, then the method is not considered a long-running request and the number of concurrent
	// requests will not be throttled. The Poll* methods here are long-running because they block until there is a task
	// available. The GetWorkflowExecutionHistory method is blocking only if WaitNewEvent is true, otherwise it is not
	// long-running. The QueryWorkflow and UpdateWorkflowExecution methods are long-running because they both block
	// until a background WFT is complete.
	ExecutionAPICountLimitOverride = map[string]int{
		"PollActivityTaskQueue":       1,
		"PollWorkflowTaskQueue":       1,
		"PollWorkflowExecutionUpdate": 1,
		"QueryWorkflow":               1,
		"UpdateWorkflowExecution":     1,
		"GetWorkflowExecutionHistory": 1,
		"PollNexusTaskQueue":          1,
	}

	ExecutionAPIToPriority = map[string]int{
		// P0: System level APIs
		"GetClusterInfo":      0,
		"GetSystemInfo":       0,
		"GetSearchAttributes": 0,
		"DescribeNamespace":   0,
		"ListNamespaces":      0,
		"DeprecateNamespace":  0,

		// P1: External Event APIs
		"SignalWorkflowExecution":          1,
		"SignalWithStartWorkflowExecution": 1,
		"StartWorkflowExecution":           1,
		"UpdateWorkflowExecution":          1,
		"CreateSchedule":                   1,
		"StartBatchOperation":              1,

		// P2: Change State APIs
		"RequestCancelWorkflowExecution": 2,
		"TerminateWorkflowExecution":     2,
		"ResetWorkflowExecution":         2,
		"DeleteWorkflowExecution":        2,
		"GetWorkflowExecutionHistory":    2, // relatively high priority because it is required for replay
		"UpdateSchedule":                 2,
		"PatchSchedule":                  2,
		"DeleteSchedule":                 2,
		"StopBatchOperation":             2,

		// P3: Status Querying APIs
		"DescribeWorkflowExecution":     3,
		"DescribeTaskQueue":             3,
		"GetWorkerBuildIdCompatibility": 3,
		"GetWorkerTaskReachability":     3,
		"ListTaskQueuePartitions":       3,
		"QueryWorkflow":                 3,
		"DescribeSchedule":              3,
		"ListScheduleMatchingTimes":     3,
		"ListSchedules":                 3,
		"DescribeBatchOperation":        3,
		"ListBatchOperations":           3,

		// P4: Progress APIs
		"RecordActivityTaskHeartbeat":      4,
		"RecordActivityTaskHeartbeatById":  4,
		"RespondActivityTaskCanceled":      4,
		"RespondActivityTaskCanceledById":  4,
		"RespondActivityTaskFailed":        4,
		"RespondActivityTaskFailedById":    4,
		"RespondActivityTaskCompleted":     4,
		"RespondActivityTaskCompletedById": 4,
		"RespondWorkflowTaskCompleted":     4,
		"RespondWorkflowTaskFailed":        4,
		"RespondQueryTaskCompleted":        4,
		"RespondNexusTaskCompleted":        4,
		"RespondNexusTaskFailed":           4,

		// P5: Poll APIs and other low priority APIs
		"PollWorkflowTaskQueue":              5,
		"PollActivityTaskQueue":              5,
		"PollWorkflowExecutionUpdate":        5,
		"PollNexusTaskQueue":                 5,
		"ResetStickyTaskQueue":               5,
		"GetWorkflowExecutionHistoryReverse": 5,
	}

	ExecutionAPIPrioritiesOrdered = []int{0, 1, 2, 3, 4, 5}

	VisibilityAPIToPriority = map[string]int{
		"CountWorkflowExecutions":        1,
		"ScanWorkflowExecutions":         1,
		"ListOpenWorkflowExecutions":     1,
		"ListClosedWorkflowExecutions":   1,
		"ListWorkflowExecutions":         1,
		"ListArchivedWorkflowExecutions": 1,
	}

	VisibilityAPIPrioritiesOrdered = []int{0, 1}

	// Special rate limiting for APIs that may insert replication tasks into a namespace replication queue.
	// The replication queue is used to propagate critical failover messages and this mapping prevents flooding the
	// queue and delaying failover.
	NamespaceReplicationInducingAPIToPriority = map[string]int{
		"RegisterNamespace":                1,
		"UpdateNamespace":                  1,
		"UpdateWorkerBuildIdCompatibility": 2,
	}

	NamespaceReplicationInducingAPIPrioritiesOrdered = []int{0, 1, 2}
)

type (
	NamespaceRateBurstImpl struct {
		namespaceName string
		rateFn        dynamicconfig.FloatPropertyFnWithNamespaceFilter
		burstFn       dynamicconfig.IntPropertyFnWithNamespaceFilter
	}

	operatorRateBurstImpl struct {
		operatorRateRatio dynamicconfig.FloatPropertyFn
		baseRateBurstFn   quotas.RateBurst
	}
)

var _ quotas.RateBurst = (*NamespaceRateBurstImpl)(nil)
var _ quotas.RateBurst = (*operatorRateBurstImpl)(nil)

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

func newOperatorRateBurst(
	baseRateBurstFn quotas.RateBurst,
	operatorRateRatio dynamicconfig.FloatPropertyFn,
) *operatorRateBurstImpl {
	return &operatorRateBurstImpl{
		operatorRateRatio: operatorRateRatio,
		baseRateBurstFn:   baseRateBurstFn,
	}
}

func (c *operatorRateBurstImpl) Rate() float64 {
	return c.operatorRateRatio() * c.baseRateBurstFn.Rate()
}

func (c *operatorRateBurstImpl) Burst() int {
	return c.baseRateBurstFn.Burst()
}

func NewRequestToRateLimiter(
	executionRateBurstFn quotas.RateBurst,
	visibilityRateBurstFn quotas.RateBurst,
	namespaceReplicationInducingRateBurstFn quotas.RateBurst,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
) quotas.RequestRateLimiter {
	mapping := make(map[string]quotas.RequestRateLimiter)

	executionRateLimiter := NewExecutionPriorityRateLimiter(executionRateBurstFn, operatorRPSRatio)
	visibilityRateLimiter := NewVisibilityPriorityRateLimiter(visibilityRateBurstFn, operatorRPSRatio)
	namespaceReplicationInducingRateLimiter := NewNamespaceReplicationInducingAPIPriorityRateLimiter(namespaceReplicationInducingRateBurstFn, operatorRPSRatio)

	for api := range ExecutionAPIToPriority {
		mapping[api] = executionRateLimiter
	}
	for api := range VisibilityAPIToPriority {
		mapping[api] = visibilityRateLimiter
	}
	for api := range NamespaceReplicationInducingAPIToPriority {
		mapping[api] = namespaceReplicationInducingRateLimiter
	}

	return quotas.NewRoutingRateLimiter(mapping)
}

func NewExecutionPriorityRateLimiter(
	rateBurstFn quotas.RateBurst,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RequestRateLimiter)
	for priority := range ExecutionAPIPrioritiesOrdered {
		if priority == OperatorPriority {
			rateLimiters[priority] = quotas.NewRequestRateLimiterAdapter(quotas.NewDynamicRateLimiter(newOperatorRateBurst(rateBurstFn, operatorRPSRatio), time.Minute))
		} else {
			rateLimiters[priority] = quotas.NewRequestRateLimiterAdapter(quotas.NewDynamicRateLimiter(rateBurstFn, time.Minute))
		}
	}
	return quotas.NewPriorityRateLimiter(func(req quotas.Request) int {
		if req.CallerType == headers.CallerTypeOperator {
			return OperatorPriority
		}
		if priority, ok := ExecutionAPIToPriority[req.API]; ok {
			return priority
		}
		return ExecutionAPIPrioritiesOrdered[len(ExecutionAPIPrioritiesOrdered)-1]
	}, rateLimiters)
}

func NewVisibilityPriorityRateLimiter(
	rateBurstFn quotas.RateBurst,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RequestRateLimiter)
	for priority := range VisibilityAPIPrioritiesOrdered {
		if priority == OperatorPriority {
			rateLimiters[priority] = quotas.NewRequestRateLimiterAdapter(quotas.NewDynamicRateLimiter(newOperatorRateBurst(rateBurstFn, operatorRPSRatio), time.Minute))
		} else {
			rateLimiters[priority] = quotas.NewRequestRateLimiterAdapter(quotas.NewDynamicRateLimiter(rateBurstFn, time.Minute))
		}
	}
	return quotas.NewPriorityRateLimiter(func(req quotas.Request) int {
		if req.CallerType == headers.CallerTypeOperator {
			return OperatorPriority
		}
		if priority, ok := VisibilityAPIToPriority[req.API]; ok {
			return priority
		}
		return VisibilityAPIPrioritiesOrdered[len(VisibilityAPIPrioritiesOrdered)-1]
	}, rateLimiters)
}

func NewNamespaceReplicationInducingAPIPriorityRateLimiter(
	rateBurstFn quotas.RateBurst,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RequestRateLimiter)
	for priority := range NamespaceReplicationInducingAPIPrioritiesOrdered {
		if priority == OperatorPriority {
			rateLimiters[priority] = quotas.NewRequestRateLimiterAdapter(quotas.NewDynamicRateLimiter(newOperatorRateBurst(rateBurstFn, operatorRPSRatio), time.Minute))
		} else {
			rateLimiters[priority] = quotas.NewRequestRateLimiterAdapter(quotas.NewDynamicRateLimiter(rateBurstFn, time.Minute))
		}
	}
	return quotas.NewPriorityRateLimiter(func(req quotas.Request) int {
		if req.CallerType == headers.CallerTypeOperator {
			return OperatorPriority
		}
		if priority, ok := NamespaceReplicationInducingAPIToPriority[req.API]; ok {
			return priority
		}
		return NamespaceReplicationInducingAPIPrioritiesOrdered[len(NamespaceReplicationInducingAPIPrioritiesOrdered)-1]
	}, rateLimiters)
}
