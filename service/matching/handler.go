package matching

import (
	"context"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/service/matching/workers"
	"go.temporal.io/server/service/worker/workerdeployment"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type (
	// Handler - gRPC handler interface for matchingservice
	Handler struct {
		matchingservice.UnimplementedMatchingServiceServer

		engine            Engine
		config            *Config
		metricsHandler    metrics.Handler
		logger            log.Logger
		startWG           sync.WaitGroup
		throttledLogger   log.Logger
		namespaceRegistry namespace.Registry
		workersRegistry   workers.Registry
	}

	HandlerParams struct {
		fx.In

		Config                        *Config
		Logger                        log.Logger
		ThrottledLogger               log.Logger
		TaskManager                   persistence.TaskManager
		FairTaskManager               persistence.FairTaskManager
		HistoryClient                 resource.HistoryClient
		MatchingRawClient             resource.MatchingRawClient
		WorkerDeploymentClient        workerdeployment.Client
		HostInfoProvider              membership.HostInfoProvider
		MatchingServiceResolver       membership.ServiceResolver
		MetricsHandler                metrics.Handler
		NamespaceRegistry             namespace.Registry
		ClusterMetadata               cluster.Metadata
		NamespaceReplicationQueue     persistence.NamespaceReplicationQueue
		VisibilityManager             manager.VisibilityManager
		NexusEndpointManager          persistence.NexusEndpointManager
		TestHooks                     testhooks.TestHooks
		SearchAttributeProvider       searchattribute.Provider
		SearchAttributeMapperProvider searchattribute.MapperProvider
		RateLimiter                   TaskDispatchRateLimiter `optional:"true"`
		WorkersRegistry               workers.Registry
	}
)

const (
	serviceName = "temporal.api.workflowservice.v1.MatchingService"
)

var (
	_ matchingservice.MatchingServiceServer = (*Handler)(nil)
)

// NewHandler creates a gRPC handler for the matchingservice
func NewHandler(
	params HandlerParams,
) *Handler {
	handler := &Handler{
		config:          params.Config,
		metricsHandler:  params.MetricsHandler,
		logger:          params.Logger,
		throttledLogger: params.ThrottledLogger,
		engine: NewEngine(
			params.TaskManager,
			params.FairTaskManager,
			params.HistoryClient,
			params.MatchingRawClient, // Use non retry client inside matching
			params.WorkerDeploymentClient,
			params.Config,
			params.Logger,
			params.ThrottledLogger,
			params.MetricsHandler,
			params.NamespaceRegistry,
			params.HostInfoProvider,
			params.MatchingServiceResolver,
			params.ClusterMetadata,
			params.NamespaceReplicationQueue,
			params.VisibilityManager,
			params.NexusEndpointManager,
			params.TestHooks,
			params.SearchAttributeProvider,
			params.SearchAttributeMapperProvider,
			params.RateLimiter,
		),
		namespaceRegistry: params.NamespaceRegistry,
		workersRegistry:   params.WorkersRegistry,
	}

	// prevent from serving requests before matching engine is started and ready
	handler.startWG.Add(1)

	return handler
}

// Start starts the handler
func (h *Handler) Start() {
	h.engine.Start()
	h.startWG.Done()
}

// Stop stops the handler
func (h *Handler) Stop() {
	h.engine.Stop()
}

func (h *Handler) opMetricsHandler(
	namespaceID string,
	taskQueue *taskqueuepb.TaskQueue,
	taskQueueType enumspb.TaskQueueType,
	operation string,
) metrics.Handler {
	nsName := h.namespaceName(namespace.ID(namespaceID))
	partition := tqid.UnsafePartitionFromProto(taskQueue, namespaceID, taskQueueType)
	return metrics.GetPerTaskQueuePartitionIDScope(
		h.metricsHandler.WithTags(metrics.OperationTag(operation)),
		nsName.String(),
		partition,
		h.config.BreakdownMetricsByTaskQueue(nsName.String(), partition.TaskQueue().Name(), partition.TaskType()),
		h.config.BreakdownMetricsByPartition(nsName.String(), partition.TaskQueue().Name(), partition.TaskType()),
	)
}

// AddActivityTask - adds an activity task.
func (h *Handler) AddActivityTask(
	ctx context.Context,
	request *matchingservice.AddActivityTaskRequest,
) (_ *matchingservice.AddActivityTaskResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	startT := time.Now().UTC()
	opMetrics := h.opMetricsHandler(
		request.GetNamespaceId(),
		request.GetTaskQueue(),
		enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		metrics.MatchingAddActivityTaskScope,
	)

	if request.GetForwardInfo() != nil {
		h.reportForwardedPerTaskQueueCounter(opMetrics, namespace.ID(request.GetNamespaceId()))
	}

	assignedBuildId, syncMatch, err := h.engine.AddActivityTask(ctx, request)
	if syncMatch {
		metrics.SyncMatchLatencyPerTaskQueue.With(opMetrics).Record(time.Since(startT))
	}
	return &matchingservice.AddActivityTaskResponse{AssignedBuildId: assignedBuildId}, err
}

// AddWorkflowTask - adds a workflow task.
func (h *Handler) AddWorkflowTask(
	ctx context.Context,
	request *matchingservice.AddWorkflowTaskRequest,
) (_ *matchingservice.AddWorkflowTaskResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	startT := time.Now().UTC()
	opMetrics := h.opMetricsHandler(
		request.GetNamespaceId(),
		request.GetTaskQueue(),
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		metrics.MatchingAddWorkflowTaskScope,
	)

	if request.GetForwardInfo() != nil {
		h.reportForwardedPerTaskQueueCounter(opMetrics, namespace.ID(request.GetNamespaceId()))
	}

	assignedBuildId, syncMatch, err := h.engine.AddWorkflowTask(ctx, request)
	if syncMatch {
		metrics.SyncMatchLatencyPerTaskQueue.With(opMetrics).Record(time.Since(startT))
	}
	return &matchingservice.AddWorkflowTaskResponse{AssignedBuildId: assignedBuildId}, err
}

// PollActivityTaskQueue - long poll for an activity task.
func (h *Handler) PollActivityTaskQueue(
	ctx context.Context,
	request *matchingservice.PollActivityTaskQueueRequest,
) (_ *matchingservice.PollActivityTaskQueueResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	opMetrics := h.opMetricsHandler(
		request.GetNamespaceId(),
		request.GetPollRequest().GetTaskQueue(),
		enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		metrics.MatchingPollActivityTaskQueueScope,
	)

	if request.GetForwardedSource() != "" {
		h.reportForwardedPerTaskQueueCounter(opMetrics, namespace.ID(request.GetNamespaceId()))
	}

	if _, err := common.ValidateLongPollContextTimeoutIsSet(
		ctx,
		"PollActivityTaskQueue",
		h.throttledLogger,
	); err != nil {
		return nil, err
	}

	return h.engine.PollActivityTaskQueue(ctx, request, opMetrics)
}

// PollWorkflowTaskQueue - long poll for a workflow task.
func (h *Handler) PollWorkflowTaskQueue(
	ctx context.Context,
	request *matchingservice.PollWorkflowTaskQueueRequest,
) (_ *matchingservice.PollWorkflowTaskQueueResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	opMetrics := h.opMetricsHandler(
		request.GetNamespaceId(),
		request.GetPollRequest().GetTaskQueue(),
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		metrics.MatchingPollWorkflowTaskQueueScope,
	)

	if request.GetForwardedSource() != "" {
		h.reportForwardedPerTaskQueueCounter(opMetrics, namespace.ID(request.GetNamespaceId()))
	}

	if _, err := common.ValidateLongPollContextTimeoutIsSet(
		ctx,
		"PollWorkflowTaskQueue",
		h.throttledLogger,
	); err != nil {
		return nil, err
	}

	return h.engine.PollWorkflowTaskQueue(ctx, request, opMetrics)
}

// QueryWorkflow queries a given workflow synchronously and return the query result.
func (h *Handler) QueryWorkflow(
	ctx context.Context,
	request *matchingservice.QueryWorkflowRequest,
) (_ *matchingservice.QueryWorkflowResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	opMetrics := h.opMetricsHandler(
		request.GetNamespaceId(),
		request.GetTaskQueue(),
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		metrics.MatchingQueryWorkflowScope,
	)

	if request.GetForwardInfo() != nil {
		h.reportForwardedPerTaskQueueCounter(opMetrics, namespace.ID(request.GetNamespaceId()))
	}

	return h.engine.QueryWorkflow(ctx, request)
}

// RespondQueryTaskCompleted responds a query task completed
func (h *Handler) RespondQueryTaskCompleted(
	ctx context.Context,
	request *matchingservice.RespondQueryTaskCompletedRequest,
) (_ *matchingservice.RespondQueryTaskCompletedResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	opMetrics := h.opMetricsHandler(
		request.GetNamespaceId(),
		request.GetTaskQueue(),
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		metrics.MatchingRespondQueryTaskCompletedScope,
	)

	err := h.engine.RespondQueryTaskCompleted(ctx, request, opMetrics)
	return &matchingservice.RespondQueryTaskCompletedResponse{}, err
}

// CancelOutstandingPoll is used to cancel outstanding pollers
func (h *Handler) CancelOutstandingPoll(ctx context.Context,
	request *matchingservice.CancelOutstandingPollRequest) (_ *matchingservice.CancelOutstandingPollResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	err := h.engine.CancelOutstandingPoll(ctx, request)
	return &matchingservice.CancelOutstandingPollResponse{}, err
}

// DescribeTaskQueue returns information about the target task queue, right now this API returns the
// pollers which polled this task queue in last few minutes. If includeTaskQueueStatus field is true,
// it will also return status of task queue's ackManager (readLevel, ackLevel, backlogCountHint and taskIDBlock).
func (h *Handler) DescribeTaskQueue(
	ctx context.Context,
	request *matchingservice.DescribeTaskQueueRequest,
) (_ *matchingservice.DescribeTaskQueueResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	resp, err := h.engine.DescribeTaskQueue(ctx, request)
	if err != nil {
		return nil, err
	}

	// TODO: remove after 1.24.0-m3
	if len(resp.DescResponse.Pollers) > 0 || resp.DescResponse.TaskQueueStatus != nil {
		// Expand pollerinfo and task queue status into tags 1 and 2 for old frontend to handle
		// proto incompatibility. This only works without ugly protowire code because
		// workflowservice.DescribeTaskQueueResponse and the previous version of
		// matchingservice.DescribeTaskQueueResponse have the same first two fields.
		oldResp := &workflowservice.DescribeTaskQueueResponse{
			Pollers:         resp.DescResponse.Pollers,
			TaskQueueStatus: resp.DescResponse.TaskQueueStatus,
		}
		if b, err := proto.Marshal(oldResp); err == nil {
			resp.ProtoReflect().SetUnknown(protoreflect.RawFields(b))
		}
	}

	return resp, nil
}

func (h *Handler) DescribeVersionedTaskQueues(
	ctx context.Context,
	request *matchingservice.DescribeVersionedTaskQueuesRequest,
) (_ *matchingservice.DescribeVersionedTaskQueuesResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.DescribeVersionedTaskQueues(ctx, request)
}

// DescribeTaskQueuePartition returns information about the target task queue partition.
func (h *Handler) DescribeTaskQueuePartition(
	ctx context.Context,
	request *matchingservice.DescribeTaskQueuePartitionRequest,
) (_ *matchingservice.DescribeTaskQueuePartitionResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.DescribeTaskQueuePartition(ctx, request)
}

// ListTaskQueuePartitions returns information about partitions for a taskQueue
func (h *Handler) ListTaskQueuePartitions(
	ctx context.Context,
	request *matchingservice.ListTaskQueuePartitionsRequest,
) (_ *matchingservice.ListTaskQueuePartitionsResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.ListTaskQueuePartitions(ctx, request)
}

// UpdateWorkerVersioningRules allows updating the Build ID assignment and redirect rules for a given Task Queue.
func (h *Handler) UpdateWorkerVersioningRules(
	ctx context.Context,
	request *matchingservice.UpdateWorkerVersioningRulesRequest,
) (_ *matchingservice.UpdateWorkerVersioningRulesResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.UpdateWorkerVersioningRules(ctx, request)
}

// GetWorkerVersioningRules fetches the Build ID assignment and redirect rules for a Task Queue
func (h *Handler) GetWorkerVersioningRules(
	ctx context.Context,
	request *matchingservice.GetWorkerVersioningRulesRequest,
) (_ *matchingservice.GetWorkerVersioningRulesResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.GetWorkerVersioningRules(ctx, request)
}

// UpdateWorkerBuildIdCompatibility allows changing the worker versioning graph for a task queue
func (h *Handler) UpdateWorkerBuildIdCompatibility(
	ctx context.Context,
	request *matchingservice.UpdateWorkerBuildIdCompatibilityRequest,
) (_ *matchingservice.UpdateWorkerBuildIdCompatibilityResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.UpdateWorkerBuildIdCompatibility(ctx, request)
}

// GetWorkerBuildIdCompatibility fetches the worker versioning data for a task queue
func (h *Handler) GetWorkerBuildIdCompatibility(
	ctx context.Context,
	request *matchingservice.GetWorkerBuildIdCompatibilityRequest,
) (_ *matchingservice.GetWorkerBuildIdCompatibilityResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.GetWorkerBuildIdCompatibility(ctx, request)
}

func (h *Handler) GetTaskQueueUserData(
	ctx context.Context,
	request *matchingservice.GetTaskQueueUserDataRequest,
) (_ *matchingservice.GetTaskQueueUserDataResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.GetTaskQueueUserData(ctx, request)
}

func (h *Handler) SyncDeploymentUserData(
	ctx context.Context,
	request *matchingservice.SyncDeploymentUserDataRequest,
) (_ *matchingservice.SyncDeploymentUserDataResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.SyncDeploymentUserData(ctx, request)
}

func (h *Handler) ApplyTaskQueueUserDataReplicationEvent(
	ctx context.Context,
	request *matchingservice.ApplyTaskQueueUserDataReplicationEventRequest,
) (_ *matchingservice.ApplyTaskQueueUserDataReplicationEventResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.ApplyTaskQueueUserDataReplicationEvent(ctx, request)
}

func (h *Handler) GetBuildIdTaskQueueMapping(
	ctx context.Context,
	request *matchingservice.GetBuildIdTaskQueueMappingRequest,
) (_ *matchingservice.GetBuildIdTaskQueueMappingResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.GetBuildIdTaskQueueMapping(ctx, request)
}

func (h *Handler) ForceUnloadTaskQueue(
	ctx context.Context,
	request *matchingservice.ForceUnloadTaskQueueRequest,
) (_ *matchingservice.ForceUnloadTaskQueueResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.ForceUnloadTaskQueue(ctx, request)
}

func (h *Handler) ForceUnloadTaskQueuePartition(
	ctx context.Context,
	request *matchingservice.ForceUnloadTaskQueuePartitionRequest,
) (_ *matchingservice.ForceUnloadTaskQueuePartitionResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.ForceUnloadTaskQueuePartition(ctx, request)
}

func (h *Handler) ForceLoadTaskQueuePartition(
	ctx context.Context,
	request *matchingservice.ForceLoadTaskQueuePartitionRequest,
) (_ *matchingservice.ForceLoadTaskQueuePartitionResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.ForceLoadTaskQueuePartition(ctx, request)
}

func (h *Handler) UpdateTaskQueueUserData(
	ctx context.Context,
	request *matchingservice.UpdateTaskQueueUserDataRequest,
) (_ *matchingservice.UpdateTaskQueueUserDataResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.UpdateTaskQueueUserData(ctx, request)
}

func (h *Handler) ReplicateTaskQueueUserData(
	ctx context.Context,
	request *matchingservice.ReplicateTaskQueueUserDataRequest,
) (_ *matchingservice.ReplicateTaskQueueUserDataResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.ReplicateTaskQueueUserData(ctx, request)
}

func (h *Handler) CheckTaskQueueUserDataPropagation(
	ctx context.Context,
	request *matchingservice.CheckTaskQueueUserDataPropagationRequest,
) (_ *matchingservice.CheckTaskQueueUserDataPropagationResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.CheckTaskQueueUserDataPropagation(ctx, request)
}

func (h *Handler) CheckTaskQueueVersionMembership(
	ctx context.Context,
	request *matchingservice.CheckTaskQueueVersionMembershipRequest,
) (_ *matchingservice.CheckTaskQueueVersionMembershipResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.CheckTaskQueueVersionMembership(ctx, request)
}

func (h *Handler) DispatchNexusTask(ctx context.Context, request *matchingservice.DispatchNexusTaskRequest) (_ *matchingservice.DispatchNexusTaskResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.DispatchNexusTask(ctx, request)
}

func (h *Handler) PollNexusTaskQueue(ctx context.Context, request *matchingservice.PollNexusTaskQueueRequest) (_ *matchingservice.PollNexusTaskQueueResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	opMetrics := h.opMetricsHandler(
		request.GetNamespaceId(),
		request.GetRequest().GetTaskQueue(),
		enumspb.TASK_QUEUE_TYPE_NEXUS,
		metrics.MatchingPollWorkflowTaskQueueScope,
	)

	if request.GetForwardedSource() != "" {
		h.reportForwardedPerTaskQueueCounter(opMetrics, namespace.ID(request.GetNamespaceId()))
	}

	if _, err := common.ValidateLongPollContextTimeoutIsSet(
		ctx,
		"PollNexusTaskQueue",
		h.throttledLogger,
	); err != nil {
		return nil, err
	}
	return h.engine.PollNexusTaskQueue(ctx, request, opMetrics)
}

func (h *Handler) RespondNexusTaskCompleted(ctx context.Context, request *matchingservice.RespondNexusTaskCompletedRequest) (_ *matchingservice.RespondNexusTaskCompletedResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	opMetrics := h.opMetricsHandler(
		request.GetNamespaceId(),
		request.GetTaskQueue(),
		enumspb.TASK_QUEUE_TYPE_NEXUS,
		metrics.MatchingRespondNexusTaskCompletedScope,
	)

	return h.engine.RespondNexusTaskCompleted(ctx, request, opMetrics)
}

func (h *Handler) RespondNexusTaskFailed(ctx context.Context, request *matchingservice.RespondNexusTaskFailedRequest) (_ *matchingservice.RespondNexusTaskFailedResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	opMetrics := h.opMetricsHandler(
		request.GetNamespaceId(),
		request.GetTaskQueue(),
		enumspb.TASK_QUEUE_TYPE_NEXUS,
		metrics.MatchingRespondNexusTaskFailedScope,
	)

	return h.engine.RespondNexusTaskFailed(ctx, request, opMetrics)
}

func (h *Handler) CreateNexusEndpoint(ctx context.Context, request *matchingservice.CreateNexusEndpointRequest) (_ *matchingservice.CreateNexusEndpointResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.CreateNexusEndpoint(ctx, request)
}

func (h *Handler) UpdateNexusEndpoint(ctx context.Context, request *matchingservice.UpdateNexusEndpointRequest) (_ *matchingservice.UpdateNexusEndpointResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.UpdateNexusEndpoint(ctx, request)
}

func (h *Handler) DeleteNexusEndpoint(ctx context.Context, request *matchingservice.DeleteNexusEndpointRequest) (_ *matchingservice.DeleteNexusEndpointResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.DeleteNexusEndpoint(ctx, request)
}

func (h *Handler) ListNexusEndpoints(ctx context.Context, request *matchingservice.ListNexusEndpointsRequest) (_ *matchingservice.ListNexusEndpointsResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	return h.engine.ListNexusEndpoints(ctx, request)
}

// RecordWorkerHeartbeat receive heartbeat request from the worker.
func (h *Handler) RecordWorkerHeartbeat(
	_ context.Context, request *matchingservice.RecordWorkerHeartbeatRequest,
) (*matchingservice.RecordWorkerHeartbeatResponse, error) {
	nsID := namespace.ID(request.GetNamespaceId())
	nsName := h.namespaceName(nsID)

	h.workersRegistry.RecordWorkerHeartbeats(nsID, nsName, request.GetHeartbeartRequest().GetWorkerHeartbeat())
	return &matchingservice.RecordWorkerHeartbeatResponse{}, nil
}

// ListWorkers retrieves a list of workers in the specified namespace that match the provided filters.
func (h *Handler) ListWorkers(
	_ context.Context, request *matchingservice.ListWorkersRequest,
) (*matchingservice.ListWorkersResponse, error) {
	nsID := namespace.ID(request.GetNamespaceId())
	listRequest := request.GetListRequest()
	resp, err := h.workersRegistry.ListWorkers(nsID, workers.ListWorkersParams{
		Query:         listRequest.GetQuery(),
		PageSize:      int(listRequest.GetPageSize()),
		NextPageToken: listRequest.GetNextPageToken(),
	})
	if err != nil {
		return nil, err
	}
	var workersInfo []*workerpb.WorkerInfo
	for _, heartbeat := range resp.Workers {
		workersInfo = append(workersInfo, &workerpb.WorkerInfo{
			WorkerHeartbeat: heartbeat,
		})
	}
	return &matchingservice.ListWorkersResponse{
		WorkersInfo:   workersInfo,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func (h *Handler) UpdateFairnessState(
	ctx context.Context, request *matchingservice.UpdateFairnessStateRequest,
) (*matchingservice.UpdateFairnessStateResponse, error) {
	return h.engine.UpdateFairnessState(ctx, request)
}

func (h *Handler) namespaceName(id namespace.ID) namespace.Name {
	entry, err := h.namespaceRegistry.GetNamespaceByID(id)
	if err != nil {
		return ""
	}
	return entry.Name()
}

func (h *Handler) reportForwardedPerTaskQueueCounter(opMetrics metrics.Handler, namespaceId namespace.ID) {
	metrics.ForwardedPerTaskQueueCounter.With(opMetrics).Record(1)
	metrics.MatchingClientForwardedCounter.With(h.metricsHandler).
		Record(
			1,
			metrics.OperationTag(metrics.MatchingAddWorkflowTaskScope),
			metrics.NamespaceTag(h.namespaceName(namespaceId).String()),
			metrics.ServiceRoleTag(metrics.MatchingRoleTagValue))
}

func (h *Handler) UpdateTaskQueueConfig(
	ctx context.Context, request *matchingservice.UpdateTaskQueueConfigRequest,
) (*matchingservice.UpdateTaskQueueConfigResponse, error) {
	return h.engine.UpdateTaskQueueConfig(ctx, request)
}

func (h *Handler) DescribeWorker(
	_ context.Context, request *matchingservice.DescribeWorkerRequest,
) (*matchingservice.DescribeWorkerResponse, error) {
	nsID := namespace.ID(request.GetNamespaceId())
	hb, err := h.workersRegistry.DescribeWorker(
		nsID, request.Request.GetWorkerInstanceKey())
	if err != nil {
		return nil, err
	}
	return &matchingservice.DescribeWorkerResponse{
		WorkerInfo: &workerpb.WorkerInfo{
			WorkerHeartbeat: hb,
		},
	}, nil
}
