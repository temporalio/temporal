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
	// PollWorkflowHistoryAPIName is used instead of GetWorkflowExecutionHistory if WaitNewEvent is true in request.
	PollWorkflowHistoryAPIName = "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowExecutionHistory"
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
		// TODO: Map to PollActivityResult if request is long-polling
		"/temporal.api.workflowservice.v1.WorkflowService/GetActivityResult": 1,

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
		"/temporal.api.workflowservice.v1.WorkflowService/StartActivityExecution":           1,
		DispatchNexusTaskByNamespaceAndTaskQueueAPIName:                                     1,
		DispatchNexusTaskByEndpointAPIName:                                                  1,

		// P1: Progress APIs for reporting heartbeats and task completions.
		// Rejecting them could result in more load to retry the workflow/activity/nexus tasks.
		"/temporal.api.workflowservice.v1.WorkflowService/RecordActivityTaskHeartbeat":      1,
		"/temporal.api.workflowservice.v1.WorkflowService/RecordActivityTaskHeartbeatById":  1,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted":     1,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompletedById": 1,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted":     1,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondQueryTaskCompleted":        1,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondNexusTaskCompleted":        1,
		CompleteNexusOperation: 1,

		// P2: Change State APIs
		"/temporal.api.workflowservice.v1.WorkflowService/RequestCancelWorkflowExecution":        2,
		"/temporal.api.workflowservice.v1.WorkflowService/TerminateWorkflowExecution":            2,
		"/temporal.api.workflowservice.v1.WorkflowService/ResetWorkflowExecution":                2,
		"/temporal.api.workflowservice.v1.WorkflowService/DeleteWorkflowExecution":               2,
		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory":           2, // relatively high priority because it is required for replay
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateSchedule":                        2,
		"/temporal.api.workflowservice.v1.WorkflowService/PatchSchedule":                         2,
		"/temporal.api.workflowservice.v1.WorkflowService/DeleteSchedule":                        2,
		"/temporal.api.workflowservice.v1.WorkflowService/StopBatchOperation":                    2,
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateActivityOptions":                 2,
		"/temporal.api.workflowservice.v1.WorkflowService/PauseActivity":                         2,
		"/temporal.api.workflowservice.v1.WorkflowService/UnpauseActivity":                       2,
		"/temporal.api.workflowservice.v1.WorkflowService/ResetActivity":                         2,
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkflowExecutionOptions":        2,
		"/temporal.api.workflowservice.v1.WorkflowService/SetCurrentDeployment":                  2, // [cleanup-wv-pre-release]
		"/temporal.api.workflowservice.v1.WorkflowService/SetCurrentDeploymentVersion":           2, // [cleanup-wv-pre-release]
		"/temporal.api.workflowservice.v1.WorkflowService/SetWorkerDeploymentCurrentVersion":     2,
		"/temporal.api.workflowservice.v1.WorkflowService/SetWorkerDeploymentRampingVersion":     2,
		"/temporal.api.workflowservice.v1.WorkflowService/SetWorkerDeploymentManager":            2,
		"/temporal.api.workflowservice.v1.WorkflowService/DeleteWorkerDeployment":                2,
		"/temporal.api.workflowservice.v1.WorkflowService/DeleteWorkerDeploymentVersion":         2,
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkerDeploymentVersionMetadata": 2,
		"/temporal.api.workflowservice.v1.WorkflowService/CreateWorkflowRule":                    2,
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeWorkflowRule":                  2,
		"/temporal.api.workflowservice.v1.WorkflowService/DeleteWorkflowRule":                    2,
		"/temporal.api.workflowservice.v1.WorkflowService/ListWorkflowRules":                     2,
		"/temporal.api.workflowservice.v1.WorkflowService/TriggerWorkflowRule":                   2,
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateTaskQueueConfig":                 2,
		"/temporal.api.workflowservice.v1.WorkflowService/RequestCancelActivityExecution":        2,
		"/temporal.api.workflowservice.v1.WorkflowService/TerminateActivityExecution":            2,
		"/temporal.api.workflowservice.v1.WorkflowService/DeleteActivityExecution":               2,
		"/temporal.api.workflowservice.v1.WorkflowService/GetActivityResult":                     2,

		// P3: Status Querying APIs
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeWorkflowExecution":       3,
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeTaskQueue":               3,
		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkerBuildIdCompatibility":   3,
		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkerVersioningRules":        3,
		"/temporal.api.workflowservice.v1.WorkflowService/ListTaskQueuePartitions":         3,
		"/temporal.api.workflowservice.v1.WorkflowService/QueryWorkflow":                   3,
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeSchedule":                3,
		"/temporal.api.workflowservice.v1.WorkflowService/ListScheduleMatchingTimes":       3,
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeBatchOperation":          3,
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeDeployment":              3, // [cleanup-wv-pre-release]
		"/temporal.api.workflowservice.v1.WorkflowService/GetCurrentDeployment":            3, // [cleanup-wv-pre-release]
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeWorkerDeploymentVersion": 3,
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeWorkerDeployment":        3,
		"/temporal.api.workflowservice.v1.WorkflowService/ListWorkerDeployments":           3,
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeActivityExecution":       3,

		// P3: Progress APIs for reporting cancellations and failures.
		// They are relatively low priority as the tasks need to be retried anyway.
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCanceled":     3,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCanceledById": 3,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskFailed":       3,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskFailedById":   3,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskFailed":       3,
		"/temporal.api.workflowservice.v1.WorkflowService/RespondNexusTaskFailed":          3,

		// P4: Poll APIs and other low priority APIs
		"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue":              4,
		"/temporal.api.workflowservice.v1.WorkflowService/PollActivityTaskQueue":              4,
		"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowExecutionUpdate":        4,
		"/temporal.api.workflowservice.v1.WorkflowService/PollNexusTaskQueue":                 4,
		"/temporal.api.workflowservice.v1.WorkflowService/ResetStickyTaskQueue":               4,
		"/temporal.api.workflowservice.v1.WorkflowService/ShutdownWorker":                     4,
		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistoryReverse": 4,
		"/temporal.api.workflowservice.v1.WorkflowService/RecordWorkerHeartbeat":              4,
		"/temporal.api.workflowservice.v1.WorkflowService/FetchWorkerConfig":                  4,
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkerConfig":                 4,

		// P5: GetWorkflowExecutionHistory with WaitNewEvent set to true is a long poll API.
		// Treat as long-poll but lower priority (5) so spikes don’t block Poll* APIs.
		PollWorkflowHistoryAPIName: 5,
		// Informational API that aren't required for the temporal service to function
		OpenAPIV3APIName: 5,
		OpenAPIV2APIName: 5,
	}

	ExecutionAPIPrioritiesOrdered = []int{0, 1, 2, 3, 4, 5}

	VisibilityAPIToPriority = map[string]int{
		"/temporal.api.workflowservice.v1.WorkflowService/CountWorkflowExecutions":        1,
		"/temporal.api.workflowservice.v1.WorkflowService/ScanWorkflowExecutions":         1,
		"/temporal.api.workflowservice.v1.WorkflowService/ListOpenWorkflowExecutions":     1,
		"/temporal.api.workflowservice.v1.WorkflowService/ListClosedWorkflowExecutions":   1,
		"/temporal.api.workflowservice.v1.WorkflowService/ListWorkflowExecutions":         1,
		"/temporal.api.workflowservice.v1.WorkflowService/ListArchivedWorkflowExecutions": 1,
		"/temporal.api.workflowservice.v1.WorkflowService/ListWorkers":                    1,
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeWorker":                 1,
		"/temporal.api.workflowservice.v1.WorkflowService/CountActivityExecutions":        1,
		"/temporal.api.workflowservice.v1.WorkflowService/ListActivityExecutions":         1,

		// APIs that rely on visibility
		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkerTaskReachability":         1,
		"/temporal.api.workflowservice.v1.WorkflowService/ListSchedules":                     1,
		"/temporal.api.workflowservice.v1.WorkflowService/ListBatchOperations":               1,
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeTaskQueueWithReachability": 1, // note this isn't a real method name
		"/temporal.api.workflowservice.v1.WorkflowService/ListDeployments":                   1,
		"/temporal.api.workflowservice.v1.WorkflowService/GetDeploymentReachability":         1,
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

	// APIs that are not considered as a namespace operation. Namespace operations are used to track the usage of a namespace.
	// This includes some APIs, history tasks, etc.
	operationExcludedAPIs = map[string]struct{}{
		// Poll requests are not considered as namespace operations. We will count these operations when we try to return a task
		// from matching service to this request.
		"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue": {},
		"/temporal.api.workflowservice.v1.WorkflowService/PollActivityTaskQueue": {},

		// Replication-related APIs are not counted as operations.
		"/temporal.server.api.adminservice.v1.AdminService/GetWorkflowExecutionRawHistory":   {},
		"/temporal.server.api.adminservice.v1.AdminService/GetWorkflowExecutionRawHistoryV2": {},
		"/temporal.server.api.adminservice.v1.AdminService/ReapplyEvents":                    {},
		"/temporal.server.api.adminservice.v1.AdminService/SyncWorkflowState":                {},
	}
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
			return max(1, int(math.Ceil(rateFn(namespace)*burstRatioFn(namespace))))
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

func IsAPIOperation(apiFullName string) bool {
	if _, ok := operationExcludedAPIs[apiFullName]; ok {
		return false
	}

	_, inAPI := APIToPriority[apiFullName]
	_, inNamespaceReplicationInducingAPI := NamespaceReplicationInducingAPIToPriority[apiFullName]

	return inAPI || inNamespaceReplicationInducingAPI
}
