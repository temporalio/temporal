package configs

import (
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/quotas"
)

var (
	APIToPriority = map[string]int{
		"/temporal.server.api.matchingservice.v1.MatchingService/AddActivityTask":                        1,
		"/temporal.server.api.matchingservice.v1.MatchingService/AddWorkflowTask":                        1,
		"/temporal.server.api.matchingservice.v1.MatchingService/CancelOutstandingPoll":                  1,
		"/temporal.server.api.matchingservice.v1.MatchingService/CancelOutstandingWorkerPolls":           2,
		"/temporal.server.api.matchingservice.v1.MatchingService/DescribeTaskQueue":                      1,
		"/temporal.server.api.matchingservice.v1.MatchingService/ListTaskQueuePartitions":                1,
		"/temporal.server.api.matchingservice.v1.MatchingService/PollActivityTaskQueue":                  1,
		"/temporal.server.api.matchingservice.v1.MatchingService/PollWorkflowTaskQueue":                  1,
		"/temporal.server.api.matchingservice.v1.MatchingService/QueryWorkflow":                          1,
		"/temporal.server.api.matchingservice.v1.MatchingService/RespondQueryTaskCompleted":              1,
		"/temporal.server.api.matchingservice.v1.MatchingService/GetWorkerBuildIdCompatibility":          1,
		"/temporal.server.api.matchingservice.v1.MatchingService/UpdateWorkerBuildIdCompatibility":       1,
		"/temporal.server.api.matchingservice.v1.MatchingService/GetTaskQueueUserData":                   1,
		"/temporal.server.api.matchingservice.v1.MatchingService/ApplyTaskQueueUserDataReplicationEvent": 1,
		"/temporal.server.api.matchingservice.v1.MatchingService/GetBuildIdTaskQueueMapping":             1,
		"/temporal.server.api.matchingservice.v1.MatchingService/ForceUnloadTaskQueuePartition":          1,
		"/temporal.server.api.matchingservice.v1.MatchingService/ForceUnloadTaskQueue":                   1,
		"/temporal.server.api.matchingservice.v1.MatchingService/ForceLoadTaskQueuePartition":            1,
		"/temporal.server.api.matchingservice.v1.MatchingService/UpdateTaskQueueUserData":                1,
		"/temporal.server.api.matchingservice.v1.MatchingService/ReplicateTaskQueueUserData":             1,
		"/temporal.server.api.matchingservice.v1.MatchingService/CheckTaskQueueUserDataPropagation":      1,
		"/temporal.server.api.matchingservice.v1.MatchingService/PollNexusTaskQueue":                     1,
		"/temporal.server.api.matchingservice.v1.MatchingService/RespondNexusTaskCompleted":              1,
		"/temporal.server.api.matchingservice.v1.MatchingService/RespondNexusTaskFailed":                 1,
		"/temporal.server.api.matchingservice.v1.MatchingService/DispatchNexusTask":                      1,
		"/temporal.server.api.matchingservice.v1.MatchingService/CreateNexusEndpoint":                    1,
		"/temporal.server.api.matchingservice.v1.MatchingService/UpdateNexusEndpoint":                    1,
		"/temporal.server.api.matchingservice.v1.MatchingService/ListNexusEndpoints":                     1,
		"/temporal.server.api.matchingservice.v1.MatchingService/DeleteNexusEndpoint":                    1,
		"/temporal.server.api.matchingservice.v1.MatchingService/UpdateWorkerVersioningRules":            1,
		"/temporal.server.api.matchingservice.v1.MatchingService/GetWorkerVersioningRules":               1,
		"/temporal.server.api.matchingservice.v1.MatchingService/DescribeTaskQueuePartition":             1,
		"/temporal.server.api.matchingservice.v1.MatchingService/DescribeVersionedTaskQueues":            1,
		"/temporal.server.api.matchingservice.v1.MatchingService/SyncDeploymentUserData":                 1,
		"/temporal.server.api.matchingservice.v1.MatchingService/RecordWorkerHeartbeat":                  1,
		"/temporal.server.api.matchingservice.v1.MatchingService/ListWorkers":                            1,
		"/temporal.server.api.matchingservice.v1.MatchingService/UpdateTaskQueueConfig":                  1,
		"/temporal.server.api.matchingservice.v1.MatchingService/DescribeWorker":                         1,
		"/temporal.server.api.matchingservice.v1.MatchingService/UpdateFairnessState":                    1,
		"/temporal.server.api.matchingservice.v1.MatchingService/CheckTaskQueueVersionMembership":        1,
	}

	APIPrioritiesOrdered = []int{0, 1, 2}
)

func NewPriorityRateLimiter(
	rateFn quotas.RateFn,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
) quotas.RequestRateLimiter {
	return quotas.NewPriorityRateLimiterHelper(
		quotas.NewDefaultIncomingRateBurst(rateFn),
		operatorRPSRatio,
		RequestToPriority,
		APIPrioritiesOrdered,
	)
}

func NewNamespaceRateLimiter(
	namespaceRateFn quotas.NamespaceRateFn,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
) quotas.RequestRateLimiter {
	return quotas.NewNamespaceRequestRateLimiter(
		func(req quotas.Request) quotas.RequestRateLimiter {
			return quotas.NewPriorityRateLimiterHelper(
				quotas.NewNamespaceRateBurst(
					req.Caller,
					namespaceRateFn,
					// TODO: We can consider adding a separate burst ratio dynamic config
					// on namespace level rate limiter if needed.
					quotas.DefaultIncomingNamespaceBurstRatioFn,
				),
				operatorRPSRatio,
				RequestToPriority,
				APIPrioritiesOrdered,
			)
		},
	)
}

func RequestToPriority(req quotas.Request) int {
	if req.CallerType == headers.CallerTypeOperator {
		return quotas.OperatorPriority
	}
	if priority, ok := APIToPriority[req.API]; ok {
		return priority
	}
	return APIPrioritiesOrdered[len(APIPrioritiesOrdered)-1]
}
