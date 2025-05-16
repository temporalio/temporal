package history

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/fx"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type (

	// Handler - gRPC handler interface for historyservice
	Handler struct {
		historyservice.UnimplementedHistoryServiceServer

		status int32

		tokenSerializer              *tasktoken.Serializer
		startWG                      sync.WaitGroup
		config                       *configs.Config
		eventNotifier                events.Notifier
		logger                       log.Logger
		throttledLogger              log.Logger
		persistenceExecutionManager  persistence.ExecutionManager
		persistenceShardManager      persistence.ShardManager
		persistenceVisibilityManager manager.VisibilityManager
		persistenceHealthSignal      persistence.HealthSignalAggregator
		healthServer                 *health.Server
		historyServiceResolver       membership.ServiceResolver
		metricsHandler               metrics.Handler
		payloadSerializer            serialization.Serializer
		timeSource                   clock.TimeSource
		namespaceRegistry            namespace.Registry
		saProvider                   searchattribute.Provider
		clusterMetadata              cluster.Metadata
		archivalMetadata             archiver.ArchivalMetadata
		hostInfoProvider             membership.HostInfoProvider
		controller                   shard.Controller
		tracer                       trace.Tracer
		taskQueueManager             persistence.HistoryTaskQueueManager
		taskCategoryRegistry         tasks.TaskCategoryRegistry
		dlqMetricsEmitter            *persistence.DLQMetricsEmitter
		healthSignalAggregator       HealthSignalAggregator

		replicationTaskFetcherFactory    replication.TaskFetcherFactory
		replicationTaskConverterProvider replication.SourceTaskConverterProvider
		streamReceiverMonitor            replication.StreamReceiverMonitor
	}

	NewHandlerArgs struct {
		fx.In

		Config                       *configs.Config
		Logger                       log.SnTaggedLogger
		ThrottledLogger              log.ThrottledLogger
		PersistenceExecutionManager  persistence.ExecutionManager
		PersistenceShardManager      persistence.ShardManager
		PersistenceHealthSignal      persistence.HealthSignalAggregator
		HealthServer                 *health.Server
		PersistenceVisibilityManager manager.VisibilityManager
		HistoryServiceResolver       membership.ServiceResolver
		MetricsHandler               metrics.Handler
		PayloadSerializer            serialization.Serializer
		TimeSource                   clock.TimeSource
		NamespaceRegistry            namespace.Registry
		SaProvider                   searchattribute.Provider
		ClusterMetadata              cluster.Metadata
		ArchivalMetadata             archiver.ArchivalMetadata
		HostInfoProvider             membership.HostInfoProvider
		ShardController              shard.Controller
		EventNotifier                events.Notifier
		TracerProvider               trace.TracerProvider
		TaskQueueManager             persistence.HistoryTaskQueueManager
		TaskCategoryRegistry         tasks.TaskCategoryRegistry
		DLQMetricsEmitter            *persistence.DLQMetricsEmitter
		HealthSignalAggregator       HealthSignalAggregator

		ReplicationTaskFetcherFactory   replication.TaskFetcherFactory
		ReplicationTaskConverterFactory replication.SourceTaskConverterProvider
		StreamReceiverMonitor           replication.StreamReceiverMonitor
	}
)

const (
	serviceName = "temporal.api.workflowservice.v1.HistoryService"
)

var (
	_ historyservice.HistoryServiceServer = (*Handler)(nil)

	errNamespaceNotSet         = serviceerror.NewInvalidArgument("Namespace not set on request.")
	errWorkflowExecutionNotSet = serviceerror.NewInvalidArgument("WorkflowExecution not set on request.")
	errTaskQueueNotSet         = serviceerror.NewInvalidArgument("Task queue not set.")
	errWorkflowIDNotSet        = serviceerror.NewInvalidArgument("WorkflowId is not set on request.")
	errRunIDNotValid           = serviceerror.NewInvalidArgument("RunId is not valid UUID.")
	errSourceClusterNotSet     = serviceerror.NewInvalidArgument("Source Cluster not set on request.")
	errShardIDNotSet           = serviceerror.NewInvalidArgument("ShardId not set on request.")
	errTimestampNotSet         = serviceerror.NewInvalidArgument("Timestamp not set on request.")

	errShuttingDown = serviceerror.NewUnavailable("Shutting down")
)

// Start starts the handler
func (h *Handler) Start() {
	if !atomic.CompareAndSwapInt32(
		&h.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	h.replicationTaskFetcherFactory.Start()
	h.streamReceiverMonitor.Start()
	// events notifier must starts before controller
	h.eventNotifier.Start()
	h.controller.Start()
	h.dlqMetricsEmitter.Start()
	h.healthSignalAggregator.Start()

	h.startWG.Done()
}

// Stop stops the handler
func (h *Handler) Stop() {
	if !atomic.CompareAndSwapInt32(
		&h.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	h.streamReceiverMonitor.Stop()
	h.replicationTaskFetcherFactory.Stop()
	h.controller.Stop()
	h.eventNotifier.Stop()
	h.dlqMetricsEmitter.Stop()
	h.healthSignalAggregator.Stop()
}

func (h *Handler) isStopped() bool {
	return atomic.LoadInt32(&h.status) == common.DaemonStatusStopped
}

func (h *Handler) DeepHealthCheck(
	ctx context.Context,
	_ *historyservice.DeepHealthCheckRequest,
) (_ *historyservice.DeepHealthCheckResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	h.startWG.Wait()

	// Ensure that the hosts are marked internally as healthy.
	status, err := h.healthServer.Check(ctx, &healthpb.HealthCheckRequest{Service: serviceName})
	if err != nil {
		return nil, err
	}
	if status.Status != healthpb.HealthCheckResponse_SERVING {
		return &historyservice.DeepHealthCheckResponse{State: enumsspb.HEALTH_STATE_DECLINED_SERVING}, nil
	}
	// Check that the RPC latency doesn't exceed the threshold.
	if _, ok := h.healthSignalAggregator.(*noopSignalAggregator); ok {
		h.logger.Warn("health signal aggregator is using noop implementation")
	}
	if h.healthSignalAggregator.AverageLatency() > h.config.HealthRPCLatencyFailure() {
		metrics.HistoryHostHealthGauge.With(h.metricsHandler).Record(float64(enumsspb.HEALTH_STATE_NOT_SERVING))
		return &historyservice.DeepHealthCheckResponse{State: enumsspb.HEALTH_STATE_NOT_SERVING}, nil
	}

	// Check if the persistence layer is healthy.
	latency := h.persistenceHealthSignal.AverageLatency()
	errRatio := h.persistenceHealthSignal.ErrorRatio()

	if latency > h.config.HealthPersistenceLatencyFailure() || errRatio > h.config.HealthPersistenceErrorRatio() {
		metrics.HistoryHostHealthGauge.With(h.metricsHandler).Record(float64(enumsspb.HEALTH_STATE_DECLINED_SERVING))
		return &historyservice.DeepHealthCheckResponse{State: enumsspb.HEALTH_STATE_NOT_SERVING}, nil
	}
	metrics.HistoryHostHealthGauge.With(h.metricsHandler).Record(float64(enumsspb.HEALTH_STATE_SERVING))
	return &historyservice.DeepHealthCheckResponse{State: enumsspb.HEALTH_STATE_SERVING}, nil
}

// IsWorkflowTaskValid - whether workflow task is still valid
func (h *Handler) IsWorkflowTaskValid(ctx context.Context, request *historyservice.IsWorkflowTaskValidRequest) (_ *historyservice.IsWorkflowTaskValidResponse, retError error) {
	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("IsWorkflowTaskValid", time.Since(startTime), retError)
		metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	}()
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}
	workflowID := request.Execution.WorkflowId

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	response, err := engine.IsWorkflowTaskValid(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return response, nil
}

// IsActivityTaskValid - whether activity task is still valid
func (h *Handler) IsActivityTaskValid(ctx context.Context, request *historyservice.IsActivityTaskValidRequest) (_ *historyservice.IsActivityTaskValidResponse, retError error) {
	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("IsActivityTaskValid", time.Since(startTime), retError)
		metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	}()
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}
	workflowID := request.Execution.WorkflowId

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	response, err := engine.IsActivityTaskValid(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return response, nil
}

func (h *Handler) RecordActivityTaskHeartbeat(ctx context.Context, request *historyservice.RecordActivityTaskHeartbeatRequest) (_ *historyservice.RecordActivityTaskHeartbeatResponse, retError error) {
	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("RecordActivityTaskHeartbeat", time.Since(startTime), retError)
		metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	}()
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	heartbeatRequest := request.HeartbeatRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(heartbeatRequest.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}

	err0 = validateTaskToken(taskToken)
	if err0 != nil {
		return nil, h.convertError(err0)
	}
	workflowID := taskToken.GetWorkflowId()

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	response, err2 := engine.RecordActivityTaskHeartbeat(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return response, nil
}

// RecordActivityTaskStarted - Record Activity Task started.
func (h *Handler) RecordActivityTaskStarted(ctx context.Context, request *historyservice.RecordActivityTaskStartedRequest) (_ *historyservice.RecordActivityTaskStartedResponse, retError error) {
	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("RecordActivityTaskStarted", time.Since(startTime), retError)
		metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	}()
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	if request.GetNamespaceId() == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	response, err := engine.RecordActivityTaskStarted(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	response.Clock, err = shardContext.NewVectorClock()
	if err != nil {
		return nil, h.convertError(err)
	}
	return response, nil
}

// RecordWorkflowTaskStarted - Record Workflow Task started.
func (h *Handler) RecordWorkflowTaskStarted(ctx context.Context, request *historyservice.RecordWorkflowTaskStartedRequest) (_ *historyservice.RecordWorkflowTaskStartedResponseWithRawHistory, retError error) {
	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("RecordWorkflowTaskStarted", time.Since(startTime), retError)
		metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	}()
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	if request.PollRequest == nil || request.PollRequest.TaskQueue.GetName() == "" {
		return nil, h.convertError(errTaskQueueNotSet)
	}

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		h.logger.Error("RecordWorkflowTaskStarted failed.",
			tag.Error(err),
			tag.WorkflowID(request.WorkflowExecution.GetWorkflowId()),
			tag.WorkflowRunID(request.WorkflowExecution.GetRunId()),
			tag.WorkflowScheduledEventID(request.GetScheduledEventId()),
		)
		return nil, h.convertError(err)
	}

	response, err := engine.RecordWorkflowTaskStarted(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	response.Clock, err = shardContext.NewVectorClock()
	if err != nil {
		return nil, h.convertError(err)
	}
	return response, nil
}

// RespondActivityTaskCompleted - records completion of an activity task
func (h *Handler) RespondActivityTaskCompleted(ctx context.Context, request *historyservice.RespondActivityTaskCompletedRequest) (_ *historyservice.RespondActivityTaskCompletedResponse, retError error) {
	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("RespondActivityTaskCompleted", time.Since(startTime), retError)
		metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	}()
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	completeRequest := request.CompleteRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}

	err0 = validateTaskToken(taskToken)
	if err0 != nil {
		return nil, h.convertError(err0)
	}
	workflowID := taskToken.GetWorkflowId()

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err2 := engine.RespondActivityTaskCompleted(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

// RespondActivityTaskFailed - records failure of an activity task
func (h *Handler) RespondActivityTaskFailed(ctx context.Context, request *historyservice.RespondActivityTaskFailedRequest) (_ *historyservice.RespondActivityTaskFailedResponse, retError error) {
	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("RespondActivityTaskFailed", time.Since(startTime), retError)
		metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	}()
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	failRequest := request.FailedRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(failRequest.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}

	err0 = validateTaskToken(taskToken)
	if err0 != nil {
		return nil, h.convertError(err0)
	}
	workflowID := taskToken.GetWorkflowId()

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err2 := engine.RespondActivityTaskFailed(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

// RespondActivityTaskCanceled - records failure of an activity task
func (h *Handler) RespondActivityTaskCanceled(ctx context.Context, request *historyservice.RespondActivityTaskCanceledRequest) (_ *historyservice.RespondActivityTaskCanceledResponse, retError error) {
	startTime := h.timeSource.Now()
	defer func() {
		h.healthSignalAggregator.Record("RespondActivityTaskCanceled", time.Since(startTime), retError)
		metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	}()
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	cancelRequest := request.CancelRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(cancelRequest.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}

	err0 = validateTaskToken(taskToken)
	if err0 != nil {
		return nil, h.convertError(err0)
	}
	workflowID := taskToken.GetWorkflowId()

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err2 := engine.RespondActivityTaskCanceled(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

// convertError is a helper method to convert ShardOwnershipLostError from persistence layer
// returned by various HistoryEngine API calls to ShardOwnershipLost error return by
// HistoryService for client to be redirected to the correct shard.
func (h *Handler) convertError(err error) error {
	// Add your error conversion logic here, or restore the previous implementation.
	return err
}

func validateTaskToken(taskToken *tasktoken.Task) error {
	if taskToken.GetWorkflowId() == "" {
		return errWorkflowIDNotSet
	}
	return nil
}
