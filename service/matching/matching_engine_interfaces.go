package matching

import (
	"context"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/metrics"
)

type (
	// Engine exposes interfaces for clients to interact with the matching engine
	Engine interface {
		Start()
		Stop()
		AddWorkflowTask(ctx context.Context, addRequest *matchingservice.AddWorkflowTaskRequest) (buildId string, syncMatch bool, err error)
		AddActivityTask(ctx context.Context, addRequest *matchingservice.AddActivityTaskRequest) (buildId string, syncMatch bool, err error)
		PollWorkflowTaskQueue(ctx context.Context, request *matchingservice.PollWorkflowTaskQueueRequest, opMetrics metrics.Handler) (*matchingservice.PollWorkflowTaskQueueResponse, error)
		PollActivityTaskQueue(ctx context.Context, request *matchingservice.PollActivityTaskQueueRequest, opMetrics metrics.Handler) (*matchingservice.PollActivityTaskQueueResponse, error)
		QueryWorkflow(ctx context.Context, request *matchingservice.QueryWorkflowRequest) (*matchingservice.QueryWorkflowResponse, error)
		RespondQueryTaskCompleted(ctx context.Context, request *matchingservice.RespondQueryTaskCompletedRequest, opMetrics metrics.Handler) error
		CancelOutstandingPoll(ctx context.Context, request *matchingservice.CancelOutstandingPollRequest) error
		DescribeTaskQueue(ctx context.Context, request *matchingservice.DescribeTaskQueueRequest) (*matchingservice.DescribeTaskQueueResponse, error)
		DescribeTaskQueuePartition(ctx context.Context, request *matchingservice.DescribeTaskQueuePartitionRequest) (*matchingservice.DescribeTaskQueuePartitionResponse, error)
		ListTaskQueuePartitions(ctx context.Context, request *matchingservice.ListTaskQueuePartitionsRequest) (*matchingservice.ListTaskQueuePartitionsResponse, error)
		UpdateWorkerBuildIdCompatibility(ctx context.Context, request *matchingservice.UpdateWorkerBuildIdCompatibilityRequest) (*matchingservice.UpdateWorkerBuildIdCompatibilityResponse, error)
		GetWorkerBuildIdCompatibility(ctx context.Context, request *matchingservice.GetWorkerBuildIdCompatibilityRequest) (*matchingservice.GetWorkerBuildIdCompatibilityResponse, error)
		GetTaskQueueUserData(ctx context.Context, request *matchingservice.GetTaskQueueUserDataRequest) (*matchingservice.GetTaskQueueUserDataResponse, error)
		SyncDeploymentUserData(ctx context.Context, request *matchingservice.SyncDeploymentUserDataRequest) (*matchingservice.SyncDeploymentUserDataResponse, error)
		ApplyTaskQueueUserDataReplicationEvent(ctx context.Context, request *matchingservice.ApplyTaskQueueUserDataReplicationEventRequest) (*matchingservice.ApplyTaskQueueUserDataReplicationEventResponse, error)
		GetBuildIdTaskQueueMapping(ctx context.Context, request *matchingservice.GetBuildIdTaskQueueMappingRequest) (*matchingservice.GetBuildIdTaskQueueMappingResponse, error)
		ForceUnloadTaskQueuePartition(ctx context.Context, request *matchingservice.ForceUnloadTaskQueuePartitionRequest) (*matchingservice.ForceUnloadTaskQueuePartitionResponse, error)
		ForceUnloadTaskQueue(ctx context.Context, request *matchingservice.ForceUnloadTaskQueueRequest) (*matchingservice.ForceUnloadTaskQueueResponse, error)
		ForceLoadTaskQueuePartition(ctx context.Context, request *matchingservice.ForceLoadTaskQueuePartitionRequest) (*matchingservice.ForceLoadTaskQueuePartitionResponse, error)
		UpdateTaskQueueUserData(ctx context.Context, request *matchingservice.UpdateTaskQueueUserDataRequest) (*matchingservice.UpdateTaskQueueUserDataResponse, error)
		ReplicateTaskQueueUserData(ctx context.Context, request *matchingservice.ReplicateTaskQueueUserDataRequest) (*matchingservice.ReplicateTaskQueueUserDataResponse, error)
		CheckTaskQueueUserDataPropagation(ctx context.Context, request *matchingservice.CheckTaskQueueUserDataPropagationRequest) (*matchingservice.CheckTaskQueueUserDataPropagationResponse, error)
		CheckTaskQueueVersionMembership(ctx context.Context, request *matchingservice.CheckTaskQueueVersionMembershipRequest) (*matchingservice.CheckTaskQueueVersionMembershipResponse, error)
		DispatchNexusTask(ctx context.Context, request *matchingservice.DispatchNexusTaskRequest) (*matchingservice.DispatchNexusTaskResponse, error)
		PollNexusTaskQueue(ctx context.Context, request *matchingservice.PollNexusTaskQueueRequest, opMetrics metrics.Handler) (*matchingservice.PollNexusTaskQueueResponse, error)
		RespondNexusTaskCompleted(ctx context.Context, request *matchingservice.RespondNexusTaskCompletedRequest, opMetrics metrics.Handler) (*matchingservice.RespondNexusTaskCompletedResponse, error)
		RespondNexusTaskFailed(ctx context.Context, request *matchingservice.RespondNexusTaskFailedRequest, opMetrics metrics.Handler) (*matchingservice.RespondNexusTaskFailedResponse, error)
		CreateNexusEndpoint(ctx context.Context, request *matchingservice.CreateNexusEndpointRequest) (*matchingservice.CreateNexusEndpointResponse, error)
		UpdateNexusEndpoint(ctx context.Context, request *matchingservice.UpdateNexusEndpointRequest) (*matchingservice.UpdateNexusEndpointResponse, error)
		DeleteNexusEndpoint(ctx context.Context, request *matchingservice.DeleteNexusEndpointRequest) (*matchingservice.DeleteNexusEndpointResponse, error)
		ListNexusEndpoints(ctx context.Context, request *matchingservice.ListNexusEndpointsRequest) (*matchingservice.ListNexusEndpointsResponse, error)
		UpdateWorkerVersioningRules(ctx context.Context, request *matchingservice.UpdateWorkerVersioningRulesRequest) (*matchingservice.UpdateWorkerVersioningRulesResponse, error)
		GetWorkerVersioningRules(ctx context.Context, request *matchingservice.GetWorkerVersioningRulesRequest) (*matchingservice.GetWorkerVersioningRulesResponse, error)
		DescribeVersionedTaskQueues(ctx context.Context, request *matchingservice.DescribeVersionedTaskQueuesRequest) (*matchingservice.DescribeVersionedTaskQueuesResponse, error)
		UpdateTaskQueueConfig(ctx context.Context, request *matchingservice.UpdateTaskQueueConfigRequest) (*matchingservice.UpdateTaskQueueConfigResponse, error)
		UpdateFairnessState(ctx context.Context, request *matchingservice.UpdateFairnessStateRequest) (*matchingservice.UpdateFairnessStateResponse, error)
	}
)
