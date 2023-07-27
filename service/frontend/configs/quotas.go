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
	// corresponding quota, which is determined by dynamicconfig.FrontendMaxNamespaceCountPerInstance. If the value is
	// not set, then the method is not considered a long-running request and the number of concurrent requests will not
	// be throttled. The Poll* methods here are long-running because they block until there is a task available. The
	// GetWorkflowExecutionHistory method is blocking only if WaitNewEvent is true, otherwise it is not long-running.
	// The QueryWorkflow and UpdateWorkflowExecution methods are long-running because they both block until a background
	// WFT is complete.
	ExecutionAPICountLimitOverride = map[string]int{
		"PollActivityTaskQueue":       1,
		"PollWorkflowTaskQueue":       1,
		"QueryWorkflow":               1,
		"UpdateWorkflowExecution":     1,
		"GetWorkflowExecutionHistory": 1,
	}

	ExecutionAPIToPriority = map[string]int{
		// priority 1
		"StartWorkflowExecution":           1,
		"SignalWithStartWorkflowExecution": 1,
		"SignalWorkflowExecution":          1,
		"RequestCancelWorkflowExecution":   1,
		"TerminateWorkflowExecution":       1,
		"GetWorkflowExecutionHistory":      1,
		"UpdateWorkflowExecution":          1,
		"PollWorkflowExecutionUpdate":      1,

		// priority 2
		"RecordActivityTaskHeartbeat":      2,
		"RecordActivityTaskHeartbeatById":  2,
		"RespondActivityTaskCanceled":      2,
		"RespondActivityTaskCanceledById":  2,
		"RespondActivityTaskFailed":        2,
		"RespondActivityTaskFailedById":    2,
		"RespondActivityTaskCompleted":     2,
		"RespondActivityTaskCompletedById": 2,
		"RespondWorkflowTaskCompleted":     2,

		// priority 3
		"ResetWorkflowExecution":             3,
		"DescribeWorkflowExecution":          3,
		"RespondWorkflowTaskFailed":          3,
		"QueryWorkflow":                      3,
		"RespondQueryTaskCompleted":          3,
		"PollWorkflowTaskQueue":              3,
		"PollActivityTaskQueue":              3,
		"GetWorkflowExecutionHistoryReverse": 3,
		"GetWorkerBuildIdCompatibility":      3,
		"GetWorkerTaskReachability":          3,
		"DeleteWorkflowExecution":            3,

		// priority 4
		"ResetStickyTaskQueue":    4,
		"DescribeTaskQueue":       4,
		"ListTaskQueuePartitions": 4,
	}

	ExecutionAPIPrioritiesOrdered = []int{0, 1, 2, 3, 4}

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

	OtherAPIToPriority = map[string]int{
		"GetClusterInfo":      1,
		"GetSystemInfo":       1,
		"GetSearchAttributes": 1,

		"DescribeNamespace":  1,
		"ListNamespaces":     1,
		"DeprecateNamespace": 1,

		"CreateSchedule":            1,
		"DescribeSchedule":          1,
		"UpdateSchedule":            1,
		"PatchSchedule":             1,
		"ListScheduleMatchingTimes": 1,
		"DeleteSchedule":            1,
		"ListSchedules":             1,

		// TODO(yx): added temporarily here; need to check if it's the right place and priority
		"DescribeBatchOperation": 1,
		"ListBatchOperations":    1,
		"StartBatchOperation":    1,
		"StopBatchOperation":     1,
	}

	OtherAPIPrioritiesOrdered = []int{0, 1}
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
	otherRateBurstFn quotas.RateBurst,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
) quotas.RequestRateLimiter {
	mapping := make(map[string]quotas.RequestRateLimiter)

	executionRateLimiter := NewExecutionPriorityRateLimiter(executionRateBurstFn, operatorRPSRatio)
	visibilityRateLimiter := NewVisibilityPriorityRateLimiter(visibilityRateBurstFn, operatorRPSRatio)
	namespaceReplicationInducingRateLimiter := NewNamespaceReplicationInducingAPIPriorityRateLimiter(namespaceReplicationInducingRateBurstFn, operatorRPSRatio)
	otherRateLimiter := NewOtherAPIPriorityRateLimiter(otherRateBurstFn, operatorRPSRatio)

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

func NewOtherAPIPriorityRateLimiter(
	rateBurstFn quotas.RateBurst,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RequestRateLimiter)
	for priority := range OtherAPIPrioritiesOrdered {
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
		if priority, ok := OtherAPIToPriority[req.API]; ok {
			return priority
		}
		return OtherAPIPrioritiesOrdered[len(OtherAPIPrioritiesOrdered)-1]
	}, rateLimiters)
}
