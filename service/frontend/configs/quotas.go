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
	"math"
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/quotas"
)

const (
	// OperatorPriority is used to give precedence to calls coming from web UI or tctl
	OperatorPriority = 0
)

const (
	// These names do not map to an underlying gRPC service. This format is used for consistency with the
	// gRPC API names on which the authorizer - the consumer of this string - may depend.
	OpenAPIV3APIName                                = "/temporal.api.openapi.v1.OpenAPIService/GetOpenAPIV3Docs"
	OpenAPIV2APIName                                = "/temporal.api.openapi.v1.OpenAPIService/GetOpenAPIV2Docs"
	DispatchNexusTaskByNamespaceAndTaskQueueAPIName = "/temporal.api.nexusservice.v1.NexusService/DispatchByNamespaceAndTaskQueue"
	DispatchNexusTaskByEndpointAPIName              = "/temporal.api.nexusservice.v1.NexusService/DispatchByEndpoint"
	CompleteNexusOperation                          = "/temporal.api.nexusservice.v1.NexusService/CompleteNexusOperation"
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
		"/temporal.api.workflowservice.v1.WorkflowService/PollActivityTaskQueue":       1,
		"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue":       1,
		"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowExecutionUpdate": 1,
		"/temporal.api.workflowservice.v1.WorkflowService/QueryWorkflow":               1,
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkflowExecution":     1,
		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory": 1,
		"/temporal.api.workflowservice.v1.WorkflowService/PollNexusTaskQueue":          1,

		// potentially long-running, depending on the operations
		"/temporal.api.workflowservice.v1.WorkflowService/ExecuteMultiOperation": 1,

		// Dispatching a Nexus task is a potentially long running RPC, it's classified in the same bucket as QueryWorkflow.
		DispatchNexusTaskByNamespaceAndTaskQueueAPIName: 1,
		DispatchNexusTaskByEndpointAPIName:              1,
	}

	// APIToPriority determines common API priorities.
	// If APIs rely on visibility, they should be added to VisibilityAPIToPriority.
	// If APIs result in replication in namespace replication queue, they belong to NamespaceReplicationInducingAPIToPriority
	APIToPriority = map[string]int{
		// P0: System level APIs
		"/temporal.api.workflowservice.v1.WorkflowService/GetClusterInfo":      0,
		"/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo":       0,
		"/temporal.api.workflowservice.v1.WorkflowService/GetSearchAttributes": 0,
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace":   0,
		"/temporal.api.workflowservice.v1.WorkflowService/ListNamespaces":      0,
		"/temporal.api.workflowservice.v1.WorkflowService/DeprecateNamespace":  0,

		// P1: External Event APIs
		"/temporal.api.workflowservice.v1.WorkflowService/SignalWorkflowExecution":          1,
		"/temporal.api.workflowservice.v1.WorkflowService/SignalWithStartWorkflowExecution": 1,
		"/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution":           1,
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkflowExecution":          1,
		"/temporal.api.workflowservice.v1.WorkflowService/ExecuteMultiOperation":            1,
		"/temporal.api.workflowservice.v1.WorkflowService/CreateSchedule":                   1,
		"/temporal.api.workflowservice.v1.WorkflowService/StartBatchOperation":              1,
		DispatchNexusTaskByNamespaceAndTaskQueueAPIName:                                     1,
		DispatchNexusTaskByEndpointAPIName:                                                  1,

		// P2: Change State APIs
		"/temporal.api.workflowservice.v1.WorkflowService/RequestCancelWorkflowExecution": 2,
		"/temporal.api.workflowservice.v1.WorkflowService/TerminateWorkflowExecution":     2,
		"/temporal.api.workflowservice.v1.WorkflowService/ResetWorkflowExecution":         2,
		"/temporal.api.workflowservice.v1.WorkflowService/DeleteWorkflowExecution":        2,
		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory":    2, // relatively high priority because it is required for replay
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateSchedule":                 2,
		"/temporal.api.workflowservice.v1.WorkflowService/PatchSchedule":                  2,
		"/temporal.api.workflowservice.v1.WorkflowService/DeleteSchedule":                 2,
		"/temporal.api.workflowservice.v1.WorkflowService/StopBatchOperation":             2,
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateActivityOptionsById":      2,

		// P3: Status Querying APIs
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeWorkflowExecution":     3,
		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkerBuildIdCompatibility": 3,
		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkerVersioningRules":      3,
		"/temporal.api.workflowservice.v1.WorkflowService/ListTaskQueuePartitions":       3,
		"/temporal.api.workflowservice.v1.WorkflowService/QueryWorkflow":                 3,
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeSchedule":              3,
		"/temporal.api.workflowservice.v1.WorkflowService/ListScheduleMatchingTimes":     3,
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeBatchOperation":        3,

		// P4: Progress APIs
		"/temporal.api.workflowservice.v1.WorkflowService/RecordActivityTaskHeartbeat":      4,
		"/temporal.api.workflowservice.v1.WorkflowService/RecordActivityTaskHeartbeatById":  4,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCanceled":      4,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCanceledById":  4,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskFailed":        4,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskFailedById":    4,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted":     4,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompletedById": 4,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted":     4,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskFailed":        4,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondQueryTaskCompleted":        4,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondNexusTaskCompleted":        4,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondNexusTaskFailed":           4,
		CompleteNexusOperation: 4,

		// P5: Poll APIs and other low priority APIs
		"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue":              5,
		"/temporal.api.workflowservice.v1.WorkflowService/PollActivityTaskQueue":              5,
		"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowExecutionUpdate":        5,
		"/temporal.api.workflowservice.v1.WorkflowService/PollNexusTaskQueue":                 5,
		"/temporal.api.workflowservice.v1.WorkflowService/ResetStickyTaskQueue":               5,
		"/temporal.api.workflowservice.v1.WorkflowService/ShutdownWorker":                     5,
		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistoryReverse": 5,

		// P6: Informational API that aren't required for the temporal service to function
		OpenAPIV3APIName: 6,
		OpenAPIV2APIName: 6,
	}

	ExecutionAPIPrioritiesOrdered = []int{0, 1, 2, 3, 4, 5, 6}

	VisibilityAPIToPriority = map[string]int{
		"/temporal.api.workflowservice.v1.WorkflowService/CountWorkflowExecutions":        1,
		"/temporal.api.workflowservice.v1.WorkflowService/ScanWorkflowExecutions":         1,
		"/temporal.api.workflowservice.v1.WorkflowService/ListOpenWorkflowExecutions":     1,
		"/temporal.api.workflowservice.v1.WorkflowService/ListClosedWorkflowExecutions":   1,
		"/temporal.api.workflowservice.v1.WorkflowService/ListWorkflowExecutions":         1,
		"/temporal.api.workflowservice.v1.WorkflowService/ListArchivedWorkflowExecutions": 1,

		// APIs that rely on visibility
		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkerTaskReachability": 1,
		"/temporal.api.workflowservice.v1.WorkflowService/ListSchedules":             1,
		"/temporal.api.workflowservice.v1.WorkflowService/ListBatchOperations":       1,
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeTaskQueue":         1,
	}

	VisibilityAPIPrioritiesOrdered = []int{0, 1}

	// Special rate limiting for APIs that may insert replication tasks into a namespace replication queue.
	// The replication queue is used to propagate critical failover messages and this mapping prevents flooding the
	// queue and delaying failover.
	NamespaceReplicationInducingAPIToPriority = map[string]int{
		"/temporal.api.workflowservice.v1.WorkflowService/RegisterNamespace":                1,
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateNamespace":                  1,
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkerBuildIdCompatibility": 2,
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkerVersioningRules":      2,
	}

	NamespaceReplicationInducingAPIPrioritiesOrdered = []int{0, 1, 2}
)

type (
	NamespaceRateBurstImpl struct {
		namespaceName string
		rateFn        quotas.NamespaceRateFn
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
	rateFn quotas.NamespaceRateFn,
	burstRatioFn dynamicconfig.FloatPropertyFnWithNamespaceFilter,
) *NamespaceRateBurstImpl {
	return &NamespaceRateBurstImpl{
		namespaceName: namespaceName,
		rateFn:        rateFn,
		burstFn: func(namespace string) int {
			return int(rateFn(namespace) * math.Max(1, burstRatioFn(namespace)))
		},
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

	for api := range APIToPriority {
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
		if priority, ok := APIToPriority[req.API]; ok {
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
