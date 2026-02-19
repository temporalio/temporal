package history

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	healthspb "go.temporal.io/server/api/health/v1"
	"go.temporal.io/server/api/historyservice/v1"
	namespacespb "go.temporal.io/server/api/namespace/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	healthcheck "go.temporal.io/server/common/health"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/searchattribute"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/deletedlqtasks"
	"go.temporal.io/server/service/history/api/forcedeleteworkflowexecution"
	"go.temporal.io/server/service/history/api/getdlqtasks"
	"go.temporal.io/server/service/history/api/listqueues"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/fx"
	"google.golang.org/grpc/health"
	grpchealthspb "google.golang.org/grpc/health/grpc_health_v1"
)

type (

	// Handler - gRPC handler interface for historyservice
	Handler struct {
		historyservice.UnimplementedHistoryServiceServer

		status int32

		tokenSerializer              *tasktoken.Serializer
		config                       *configs.Config
		eventNotifier                events.Notifier
		logger                       log.Logger
		throttledLogger              log.Logger
		persistenceExecutionManager  persistence.ExecutionManager
		persistenceShardManager      persistence.ShardManager
		persistenceVisibilityManager manager.VisibilityManager
		persistenceHealthSignal      persistence.HealthSignalAggregator
		healthServer                 *health.Server
		historyHealthSignal          interceptor.HealthSignalAggregator
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
		chasmEngine                  chasm.Engine
		chasmRegistry                *chasm.Registry
		nexusHandler                 nexus.Handler

		replicationTaskFetcherFactory    replication.TaskFetcherFactory
		replicationTaskConverterProvider replication.SourceTaskConverterProvider
		streamReceiverMonitor            replication.StreamReceiverMonitor
		replicationServerRateLimiter     replication.ServerSchedulerRateLimiter
	}

	NewHandlerArgs struct {
		fx.In

		Config                       *configs.Config
		Logger                       log.SnTaggedLogger
		ThrottledLogger              log.ThrottledLogger
		PersistenceExecutionManager  persistence.ExecutionManager
		PersistenceShardManager      persistence.ShardManager
		PersistenceHealthSignal      persistence.HealthSignalAggregator
		HistoryHealthSignal          interceptor.HealthSignalAggregator
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
		ChasmEngine                  chasm.Engine
		ChasmRegistry                *chasm.Registry

		ReplicationTaskFetcherFactory   replication.TaskFetcherFactory
		ReplicationTaskConverterFactory replication.SourceTaskConverterProvider
		StreamReceiverMonitor           replication.StreamReceiverMonitor
		ReplicationServerRateLimiter    replication.ServerSchedulerRateLimiter
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
	errBusinessIDNotSet        = serviceerror.NewInvalidArgument("Business ID is not set on request.")
	errRunIDNotValid           = serviceerror.NewInvalidArgument("RunId is not valid UUID.")
	errSourceClusterNotSet     = serviceerror.NewInvalidArgument("Source Cluster not set on request.")
	errShardIDNotSet           = serviceerror.NewInvalidArgument("ShardId not set on request.")
	errTimestampNotSet         = serviceerror.NewInvalidArgument("Timestamp not set on request.")
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
}

func (h *Handler) DeepHealthCheck(
	ctx context.Context,
	_ *historyservice.DeepHealthCheckRequest,
) (*historyservice.DeepHealthCheckResponse, error) {

	var checks []*healthspb.HealthCheck
	overallState := enumsspb.HEALTH_STATE_SERVING

	// Check 1: gRPC health (graceful shutdown / hysteresis).
	// If this fails, return early with only this check — no point running
	// metric checks if we can't even reach the gRPC health server.
	status, err := h.healthServer.Check(ctx, &grpchealthspb.HealthCheckRequest{Service: serviceName})
	if err != nil {
		checks = append(checks, &healthspb.HealthCheck{
			CheckType: healthcheck.CheckTypeGRPCHealth,
			State:     enumsspb.HEALTH_STATE_NOT_SERVING,
			Message:   fmt.Sprintf("gRPC health check failed: %v", err),
		})
		metrics.HistoryHostHealthGauge.With(h.metricsHandler).Record(float64(enumsspb.HEALTH_STATE_NOT_SERVING))
		return &historyservice.DeepHealthCheckResponse{
			State:  enumsspb.HEALTH_STATE_NOT_SERVING,
			Checks: checks,
		}, nil
	}
	grpcState := enumsspb.HEALTH_STATE_SERVING
	grpcMsg := ""
	if status.Status != grpchealthspb.HealthCheckResponse_SERVING {
		grpcState = enumsspb.HEALTH_STATE_DECLINED_SERVING
		overallState = enumsspb.HEALTH_STATE_DECLINED_SERVING
		grpcMsg = "gRPC health server not serving"
	}
	checks = append(checks, &healthspb.HealthCheck{
		CheckType: healthcheck.CheckTypeGRPCHealth,
		State:     grpcState,
		Message:   grpcMsg,
	})

	// Check 2: RPC latency
	rpcLatency := h.historyHealthSignal.AverageLatency()
	rpcLatencyThreshold := h.config.HealthRPCLatencyFailure()
	rpcLatencyState := enumsspb.HEALTH_STATE_SERVING
	rpcLatencyMsg := ""
	if rpcLatency > rpcLatencyThreshold {
		rpcLatencyState = enumsspb.HEALTH_STATE_NOT_SERVING
		rpcLatencyMsg = fmt.Sprintf("RPC latency %.2fms exceeded %.2fms threshold", rpcLatency, rpcLatencyThreshold)
		if overallState != enumsspb.HEALTH_STATE_DECLINED_SERVING {
			overallState = enumsspb.HEALTH_STATE_NOT_SERVING
		}
	}
	checks = append(checks, &healthspb.HealthCheck{
		CheckType: healthcheck.CheckTypeRPCLatency,
		State:     rpcLatencyState,
		Value:     rpcLatency,
		Threshold: rpcLatencyThreshold,
		Message:   rpcLatencyMsg,
	})

	// Check 3: RPC error ratio
	rpcErrRatio := h.historyHealthSignal.ErrorRatio()
	rpcErrThreshold := h.config.HealthRPCErrorRatio()
	rpcErrState := enumsspb.HEALTH_STATE_SERVING
	rpcErrMsg := ""
	if rpcErrRatio > rpcErrThreshold {
		rpcErrState = enumsspb.HEALTH_STATE_NOT_SERVING
		rpcErrMsg = fmt.Sprintf("RPC error ratio %.4f exceeded %.4f threshold", rpcErrRatio, rpcErrThreshold)
		if overallState != enumsspb.HEALTH_STATE_DECLINED_SERVING {
			overallState = enumsspb.HEALTH_STATE_NOT_SERVING
		}
	}
	checks = append(checks, &healthspb.HealthCheck{
		CheckType: healthcheck.CheckTypeRPCErrorRatio,
		State:     rpcErrState,
		Value:     rpcErrRatio,
		Threshold: rpcErrThreshold,
		Message:   rpcErrMsg,
	})

	// Check 4: Persistence latency
	persLatency := h.persistenceHealthSignal.AverageLatency()
	persLatencyThreshold := h.config.HealthPersistenceLatencyFailure()
	persLatencyState := enumsspb.HEALTH_STATE_SERVING
	persLatencyMsg := ""
	if persLatency > persLatencyThreshold {
		persLatencyState = enumsspb.HEALTH_STATE_NOT_SERVING
		persLatencyMsg = fmt.Sprintf("Persistence latency %.2fms exceeded %.2fms threshold", persLatency, persLatencyThreshold)
		if overallState != enumsspb.HEALTH_STATE_DECLINED_SERVING {
			overallState = enumsspb.HEALTH_STATE_NOT_SERVING
		}
	}
	checks = append(checks, &healthspb.HealthCheck{
		CheckType: healthcheck.CheckTypePersistenceLatency,
		State:     persLatencyState,
		Value:     persLatency,
		Threshold: persLatencyThreshold,
		Message:   persLatencyMsg,
	})

	// Check 5: Persistence error ratio
	persErrRatio := h.persistenceHealthSignal.ErrorRatio()
	persErrThreshold := h.config.HealthPersistenceErrorRatio()
	persErrState := enumsspb.HEALTH_STATE_SERVING
	persErrMsg := ""
	if persErrRatio > persErrThreshold {
		persErrState = enumsspb.HEALTH_STATE_NOT_SERVING
		persErrMsg = fmt.Sprintf("Persistence error ratio %.4f exceeded %.4f threshold", persErrRatio, persErrThreshold)
		if overallState != enumsspb.HEALTH_STATE_DECLINED_SERVING {
			overallState = enumsspb.HEALTH_STATE_NOT_SERVING
		}
	}
	checks = append(checks, &healthspb.HealthCheck{
		CheckType: healthcheck.CheckTypePersistenceErrRatio,
		State:     persErrState,
		Value:     persErrRatio,
		Threshold: persErrThreshold,
		Message:   persErrMsg,
	})

	metrics.HistoryHostHealthGauge.With(h.metricsHandler).Record(float64(overallState))
	return &historyservice.DeepHealthCheckResponse{
		State:  overallState,
		Checks: checks,
	}, nil
}

// IsWorkflowTaskValid - whether workflow task is still valid
func (h *Handler) IsWorkflowTaskValid(ctx context.Context, request *historyservice.IsWorkflowTaskValidRequest) (*historyservice.IsWorkflowTaskValidResponse, error) {

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
func (h *Handler) IsActivityTaskValid(ctx context.Context, request *historyservice.IsActivityTaskValidRequest) (*historyservice.IsActivityTaskValidResponse, error) {

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

func (h *Handler) RecordActivityTaskHeartbeat(ctx context.Context, request *historyservice.RecordActivityTaskHeartbeatRequest) (*historyservice.RecordActivityTaskHeartbeatResponse, error) {
	taskToken, err := h.tokenSerializer.Deserialize(request.GetHeartbeatRequest().GetTaskToken())
	if err != nil {
		return nil, consts.ErrDeserializingToken
	}

	if err := validateTaskToken(taskToken); err != nil {
		return nil, h.convertError(err)
	}

	// Handle as standalone activity if token has component ref.
	if componentRef := taskToken.GetComponentRef(); len(componentRef) > 0 {
		response, _, err := chasm.UpdateComponent(
			ctx,
			componentRef,
			(*activity.Activity).RecordHeartbeat,
			activity.WithToken[*historyservice.RecordActivityTaskHeartbeatRequest]{
				Token:   taskToken,
				Request: request,
			},
		)
		return response, h.convertError(err)
	}

	// Handle worklow activity (mutable state backed implementation).
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, taskToken.GetWorkflowId())
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	response, err := engine.RecordActivityTaskHeartbeat(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return response, nil
}

// RecordActivityTaskStarted - Record Activity Task started.
func (h *Handler) RecordActivityTaskStarted(ctx context.Context, request *historyservice.RecordActivityTaskStartedRequest) (*historyservice.RecordActivityTaskStartedResponse, error) {
	// Handle as standalone activity if request has component ref.
	if activityRefProto := request.GetComponentRef(); len(activityRefProto) > 0 {
		response, _, err := chasm.UpdateComponent(
			ctx,
			activityRefProto,
			(*activity.Activity).HandleStarted,
			request,
		)
		if err != nil {
			return nil, err
		}
		return response, nil
	}

	// Handle worklow activity (mutable state backed implementation).
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, request.GetWorkflowExecution().GetWorkflowId())
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
func (h *Handler) RecordWorkflowTaskStarted(ctx context.Context, request *historyservice.RecordWorkflowTaskStartedRequest) (*historyservice.RecordWorkflowTaskStartedResponseWithRawHistory, error) {
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
func (h *Handler) RespondActivityTaskCompleted(ctx context.Context, request *historyservice.RespondActivityTaskCompletedRequest) (*historyservice.RespondActivityTaskCompletedResponse, error) {
	taskToken, err := h.tokenSerializer.Deserialize(request.CompleteRequest.GetTaskToken())
	if err != nil {
		return nil, consts.ErrDeserializingToken
	}

	if err := validateTaskToken(taskToken); err != nil {
		return nil, h.convertError(err)
	}

	// Handle standalone activity if component ref is present in the token.
	if componentRef := taskToken.GetComponentRef(); len(componentRef) > 0 {
		namespaceName, err := h.namespaceRegistry.GetNamespaceName(namespace.ID(request.GetNamespaceId()))
		if err != nil {
			return nil, err
		}

		response, _, err := chasm.UpdateComponent(
			ctx,
			componentRef,
			(*activity.Activity).HandleCompleted,
			activity.RespondCompletedEvent{
				Request: request,
				Token:   taskToken,
				MetricsHandlerBuilderParams: activity.MetricsHandlerBuilderParams{
					Handler:                     h.metricsHandler,
					NamespaceName:               namespaceName.String(),
					BreakdownMetricsByTaskQueue: h.config.BreakdownMetricsByTaskQueue,
				},
			},
		)
		if err != nil {
			return nil, err
		}
		return response, nil
	}

	// Handle worklow activity (mutable state backed implementation).
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, taskToken.GetWorkflowId())
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.RespondActivityTaskCompleted(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return resp, nil
}

// RespondActivityTaskFailed - records failure of an activity task
func (h *Handler) RespondActivityTaskFailed(ctx context.Context, request *historyservice.RespondActivityTaskFailedRequest) (*historyservice.RespondActivityTaskFailedResponse, error) {
	taskToken, err := h.tokenSerializer.Deserialize(request.FailedRequest.GetTaskToken())
	if err != nil {
		return nil, consts.ErrDeserializingToken
	}

	if err := validateTaskToken(taskToken); err != nil {
		return nil, h.convertError(err)
	}

	// Handle standalone activity if component ref is present in the token.
	if componentRef := taskToken.GetComponentRef(); len(componentRef) > 0 {
		namespaceName, err := h.namespaceRegistry.GetNamespaceName(namespace.ID(request.GetNamespaceId()))
		if err != nil {
			return nil, err
		}

		response, _, err := chasm.UpdateComponent(
			ctx,
			componentRef,
			(*activity.Activity).HandleFailed,
			activity.RespondFailedEvent{
				Request: request,
				Token:   taskToken,
				MetricsHandlerBuilderParams: activity.MetricsHandlerBuilderParams{
					Handler:                     h.metricsHandler,
					NamespaceName:               namespaceName.String(),
					BreakdownMetricsByTaskQueue: h.config.BreakdownMetricsByTaskQueue,
				},
			},
		)
		if err != nil {
			return nil, err
		}
		return response, nil
	}

	// Handle worklow activity (mutable state backed implementation).
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, taskToken.GetWorkflowId())
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.RespondActivityTaskFailed(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return resp, nil
}

// RespondActivityTaskCanceled - records failure of an activity task
func (h *Handler) RespondActivityTaskCanceled(ctx context.Context, request *historyservice.RespondActivityTaskCanceledRequest) (*historyservice.RespondActivityTaskCanceledResponse, error) {
	taskToken, err := h.tokenSerializer.Deserialize(request.CancelRequest.GetTaskToken())
	if err != nil {
		return nil, consts.ErrDeserializingToken
	}

	if err := validateTaskToken(taskToken); err != nil {
		return nil, h.convertError(err)
	}

	// Handle standalone activity if component ref is present in the token.
	if componentRef := taskToken.GetComponentRef(); len(componentRef) > 0 {
		namespaceName, err := h.namespaceRegistry.GetNamespaceName(namespace.ID(request.GetNamespaceId()))
		if err != nil {
			return nil, err
		}

		response, _, err := chasm.UpdateComponent(
			ctx,
			componentRef,
			(*activity.Activity).HandleCanceled,
			activity.RespondCancelledEvent{
				Request: request,
				Token:   taskToken,
				MetricsHandlerBuilderParams: activity.MetricsHandlerBuilderParams{
					Handler:                     h.metricsHandler,
					NamespaceName:               namespaceName.String(),
					BreakdownMetricsByTaskQueue: h.config.BreakdownMetricsByTaskQueue,
				},
			},
		)
		if err != nil {
			return nil, err
		}
		return response, nil
	}

	// Handle worklow activity (mutable state backed implementation).
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, taskToken.GetWorkflowId())
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.RespondActivityTaskCanceled(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return resp, nil
}

// RespondWorkflowTaskCompleted - records completion of a workflow task
func (h *Handler) RespondWorkflowTaskCompleted(ctx context.Context, request *historyservice.RespondWorkflowTaskCompletedRequest) (*historyservice.RespondWorkflowTaskCompletedResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	completeRequest := request.CompleteRequest
	token, err := h.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err != nil {
		return nil, consts.ErrDeserializingToken
	}

	h.logger.Debug("RespondWorkflowTaskCompleted",
		tag.WorkflowNamespaceID(token.GetNamespaceId()),
		tag.WorkflowID(token.GetWorkflowId()),
		tag.WorkflowRunID(token.GetRunId()),
		tag.WorkflowScheduledEventID(token.GetScheduledEventId()))

	err = validateTaskToken(token)
	if err != nil {
		return nil, h.convertError(err)
	}
	workflowID := token.GetWorkflowId()

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	response, err := engine.RespondWorkflowTaskCompleted(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return response, nil
}

// RespondWorkflowTaskFailed - failed response to workflow task
func (h *Handler) RespondWorkflowTaskFailed(ctx context.Context, request *historyservice.RespondWorkflowTaskFailedRequest) (*historyservice.RespondWorkflowTaskFailedResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	failedRequest := request.FailedRequest
	token, err := h.tokenSerializer.Deserialize(failedRequest.TaskToken)
	if err != nil {
		return nil, consts.ErrDeserializingToken
	}

	h.logger.Debug("RespondWorkflowTaskFailed",
		tag.WorkflowNamespaceID(token.GetNamespaceId()),
		tag.WorkflowID(token.GetWorkflowId()),
		tag.WorkflowRunID(token.GetRunId()),
		tag.WorkflowScheduledEventID(token.GetScheduledEventId()))

	err = validateTaskToken(token)
	if err != nil {
		return nil, h.convertError(err)
	}
	workflowID := token.GetWorkflowId()

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	err = engine.RespondWorkflowTaskFailed(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return &historyservice.RespondWorkflowTaskFailedResponse{}, nil
}

// StartWorkflowExecution - creates a new workflow execution
func (h *Handler) StartWorkflowExecution(ctx context.Context, request *historyservice.StartWorkflowExecutionRequest) (*historyservice.StartWorkflowExecutionResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	startRequest := request.StartRequest
	workflowID := startRequest.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	response, err := engine.StartWorkflowExecution(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	if response.Clock == nil {
		response.Clock, err = shardContext.NewVectorClock()
		if err != nil {
			return nil, h.convertError(err)
		}
	}
	return response, nil
}

func (h *Handler) ExecuteMultiOperation(
	ctx context.Context,
	request *historyservice.ExecuteMultiOperationRequest,
) (*historyservice.ExecuteMultiOperationResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, request.WorkflowId)
	if err != nil {
		return nil, h.convertError(err)
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	response, err := engine.ExecuteMultiOperation(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	for _, opResp := range response.Responses {
		if startResp := opResp.GetStartWorkflow(); startResp != nil {
			if startResp.Clock == nil {
				startResp.Clock, err = shardContext.NewVectorClock()
				if err != nil {
					return nil, h.convertError(err)
				}
			}
		}
	}

	return response, nil
}

// DescribeHistoryHost returns information about the internal states of a history host
func (h *Handler) DescribeHistoryHost(_ context.Context, req *historyservice.DescribeHistoryHostRequest) (*historyservice.DescribeHistoryHostResponse, error) {
	// This API supports describe history host by 1. address 2. shard id 3. namespace id + workflow id
	// if option 2/3 is provided, we want to check on the shard ownership to return the correct host address.
	shardID := req.GetShardId()
	if len(req.GetNamespaceId()) != 0 && req.GetWorkflowExecution() != nil {
		shardID = common.WorkflowIDToHistoryShard(req.GetNamespaceId(), req.GetWorkflowExecution().GetWorkflowId(), h.config.NumberOfShards)
	}
	if shardID > 0 {
		_, err := h.controller.GetShardByID(shardID)
		if err != nil {
			return nil, err
		}
	}

	itemsInRegistryByIDCount, itemsInRegistryByNameCount := h.namespaceRegistry.GetRegistrySize()
	ownedShardIDs := h.controller.ShardIDs()
	resp := &historyservice.DescribeHistoryHostResponse{
		ShardsNumber: int32(len(ownedShardIDs)),
		ShardIds:     ownedShardIDs,
		NamespaceCache: &namespacespb.NamespaceCacheInfo{
			ItemsInCacheByIdCount:   itemsInRegistryByIDCount,
			ItemsInCacheByNameCount: itemsInRegistryByNameCount,
		},
		Address: h.hostInfoProvider.HostInfo().GetAddress(),
	}
	return resp, nil
}

// RemoveTask returns information about the internal states of a history host
func (h *Handler) RemoveTask(ctx context.Context, request *historyservice.RemoveTaskRequest) (_ *historyservice.RemoveTaskResponse, retError error) {
	var err error
	category, ok := h.taskCategoryRegistry.GetCategoryByID(int(request.Category))
	if !ok {
		return nil, serviceerror.NewInvalidArgumentf("Invalid task category ID: %v", request.Category)
	}

	key := tasks.NewKey(
		timestamp.TimeValue(request.GetVisibilityTime()),
		request.GetTaskId(),
	)
	if err := tasks.ValidateKey(key); err != nil {
		return nil, serviceerror.NewInvalidArgumentf("Invalid task key: %v", err.Error())
	}

	err = h.persistenceExecutionManager.CompleteHistoryTask(ctx, &persistence.CompleteHistoryTaskRequest{
		ShardID:      request.GetShardId(),
		TaskCategory: category,
		TaskKey:      key,
	})

	return &historyservice.RemoveTaskResponse{}, err
}

// CloseShard closes a shard hosted by this instance
func (h *Handler) CloseShard(_ context.Context, request *historyservice.CloseShardRequest) (*historyservice.CloseShardResponse, error) {
	h.controller.CloseShardByID(request.GetShardId())
	return &historyservice.CloseShardResponse{}, nil
}

// GetShard gets a shard hosted by this instance
func (h *Handler) GetShard(ctx context.Context, request *historyservice.GetShardRequest) (*historyservice.GetShardResponse, error) {
	resp, err := h.persistenceShardManager.GetOrCreateShard(ctx, &persistence.GetOrCreateShardRequest{
		ShardID: request.ShardId,
	})
	if err != nil {
		return nil, err
	}
	return &historyservice.GetShardResponse{ShardInfo: resp.ShardInfo}, nil
}

// RebuildMutableState attempts to rebuild mutable state according to persisted history events
func (h *Handler) RebuildMutableState(ctx context.Context, request *historyservice.RebuildMutableStateRequest) (*historyservice.RebuildMutableStateResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	if err := engine.RebuildMutableState(ctx, namespaceID, &commonpb.WorkflowExecution{
		WorkflowId: workflowExecution.WorkflowId,
		RunId:      workflowExecution.RunId,
	}); err != nil {
		return nil, h.convertError(err)
	}
	return &historyservice.RebuildMutableStateResponse{}, nil
}

// ImportWorkflowExecution attempts to workflow execution according to persisted history events
func (h *Handler) ImportWorkflowExecution(ctx context.Context, request *historyservice.ImportWorkflowExecutionRequest) (*historyservice.ImportWorkflowExecutionResponse, error) {

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	if workflowID == "" {
		return nil, h.convertError(errWorkflowIDNotSet)
	}
	runID := workflowExecution.GetRunId()
	if runID == "" {
		return nil, h.convertError(errRunIDNotValid)
	}
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	if !shardContext.GetClusterMetadata().IsGlobalNamespaceEnabled() {
		return nil, serviceerror.NewUnimplemented("ImportWorkflowExecution must be used in global namespace mode")
	}
	resp, err := engine.ImportWorkflowExecution(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return resp, nil
}

// DescribeMutableState - returns the internal analysis of workflow execution state
func (h *Handler) DescribeMutableState(ctx context.Context, request *historyservice.DescribeMutableStateRequest) (*historyservice.DescribeMutableStateResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.DescribeMutableState(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return resp, nil
}

// GetMutableState - returns the id of the next event in the execution's history
func (h *Handler) GetMutableState(ctx context.Context, request *historyservice.GetMutableStateRequest) (*historyservice.GetMutableStateResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.GetMutableState(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return resp, nil
}

// PollMutableState - returns the id of the next event in the execution's history
func (h *Handler) PollMutableState(ctx context.Context, request *historyservice.PollMutableStateRequest) (*historyservice.PollMutableStateResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.PollMutableState(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return resp, nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (h *Handler) DescribeWorkflowExecution(ctx context.Context, request *historyservice.DescribeWorkflowExecutionRequest) (*historyservice.DescribeWorkflowExecutionResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.Request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.DescribeWorkflowExecution(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return resp, nil
}

// RequestCancelWorkflowExecution - requests cancellation of a workflow
func (h *Handler) RequestCancelWorkflowExecution(ctx context.Context, request *historyservice.RequestCancelWorkflowExecutionRequest) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" || request.CancelRequest.GetNamespace() == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	cancelRequest := request.CancelRequest
	h.logger.Debug("RequestCancelWorkflowExecution",
		tag.WorkflowNamespace(cancelRequest.GetNamespace()),
		tag.WorkflowNamespaceID(request.GetNamespaceId()),
		tag.WorkflowID(cancelRequest.WorkflowExecution.GetWorkflowId()),
		tag.WorkflowRunID(cancelRequest.WorkflowExecution.GetRunId()))

	workflowID := cancelRequest.WorkflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.RequestCancelWorkflowExecution(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return resp, nil
}

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a workflow task being created for the execution.
func (h *Handler) SignalWorkflowExecution(ctx context.Context, request *historyservice.SignalWorkflowExecutionRequest) (*historyservice.SignalWorkflowExecutionResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.SignalRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.SignalWorkflowExecution(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return resp, nil
}

// SignalWithStartWorkflowExecution is used to ensure sending a signal event to a workflow execution.
// If workflow is running, this results in WorkflowExecutionSignaled event recorded in the history
// and a workflow task being created for the execution.
// If workflow is not running or not found, this results in WorkflowExecutionStarted and WorkflowExecutionSignaled
// event recorded in history, and a workflow task being created for the execution
func (h *Handler) SignalWithStartWorkflowExecution(ctx context.Context, request *historyservice.SignalWithStartWorkflowExecutionRequest) (*historyservice.SignalWithStartWorkflowExecutionResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	signalWithStartRequest := request.SignalWithStartRequest
	workflowID := signalWithStartRequest.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	for {
		resp, err := engine.SignalWithStartWorkflowExecution(ctx, request)
		if err == nil {
			return resp, nil
		}

		// Two simultaneous SignalWithStart requests might try to start a workflow at the same time.
		// This can result in one of the requests failing with one of two possible errors:
		//    CurrentWorkflowConditionFailedError || WorkflowConditionFailedError
		// If either error occurs, just go ahead and retry. It should succeed on the subsequent attempt.
		// For simplicity, we keep trying unless the context finishes or we get an error that is not one of the
		// two mentioned above.
		_, isCurrentWorkflowConditionFailedErr := err.(*persistence.CurrentWorkflowConditionFailedError)
		_, isWorkflowConditionFailedErr := err.(*persistence.WorkflowConditionFailedError)

		isContextDone := false
		select {
		case <-ctx.Done():
			isContextDone = true
			if ctxErr := ctx.Err(); ctxErr != nil {
				err = ctxErr
			}
		default:
		}

		if (!isCurrentWorkflowConditionFailedErr && !isWorkflowConditionFailedErr) || isContextDone {
			return nil, h.convertError(err)
		}
	}
}

// RemoveSignalMutableState is used to remove a signal request ID that was previously recorded.  This is currently
// used to clean execution info when signal workflow task finished.
func (h *Handler) RemoveSignalMutableState(ctx context.Context, request *historyservice.RemoveSignalMutableStateRequest) (*historyservice.RemoveSignalMutableStateResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.RemoveSignalMutableState(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return resp, nil
}

// TerminateWorkflowExecution terminates an existing workflow execution by recording WorkflowExecutionTerminated event
// in the history and immediately terminating the execution instance.
func (h *Handler) TerminateWorkflowExecution(ctx context.Context, request *historyservice.TerminateWorkflowExecutionRequest) (*historyservice.TerminateWorkflowExecutionResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.TerminateRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.TerminateWorkflowExecution(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return resp, nil
}

func (h *Handler) DeleteWorkflowExecution(ctx context.Context, request *historyservice.DeleteWorkflowExecutionRequest) (*historyservice.DeleteWorkflowExecutionResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	h.logger.Info("DeleteWorkflowExecution requested",
		tag.WorkflowNamespaceID(request.GetNamespaceId()),
		tag.WorkflowID(workflowExecution.GetWorkflowId()),
		tag.WorkflowRunID(workflowExecution.GetRunId()))

	resp, err := engine.DeleteWorkflowExecution(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return resp, nil
}

// ResetWorkflowExecution reset an existing workflow execution
// in the history and immediately terminating the execution instance.
func (h *Handler) ResetWorkflowExecution(ctx context.Context, request *historyservice.ResetWorkflowExecutionRequest) (*historyservice.ResetWorkflowExecutionResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.ResetRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.ResetWorkflowExecution(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return resp, nil
}

// UpdateWorkflowExecutionOptions updates the options of a workflow execution.
// Can be used to set and unset versioning behavior override.
func (h *Handler) UpdateWorkflowExecutionOptions(ctx context.Context, request *historyservice.UpdateWorkflowExecutionOptionsRequest) (*historyservice.UpdateWorkflowExecutionOptionsResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.UpdateRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.UpdateWorkflowExecutionOptions(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return resp, nil
}

// QueryWorkflow queries a workflow.
func (h *Handler) QueryWorkflow(ctx context.Context, request *historyservice.QueryWorkflowRequest) (*historyservice.QueryWorkflowResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowID := request.GetRequest().GetExecution().GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.QueryWorkflow(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return resp, nil
}

// ScheduleWorkflowTask is used for creating a workflow task for already started workflow execution.  This is mainly
// used by transfer queue processor during the processing of StartChildWorkflowExecution task, where it first starts
// child execution without creating the workflow task and then calls this API after updating the mutable state of
// parent execution.
func (h *Handler) ScheduleWorkflowTask(ctx context.Context, request *historyservice.ScheduleWorkflowTaskRequest) (*historyservice.ScheduleWorkflowTaskResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	if request.WorkflowExecution == nil {
		return nil, h.convertError(errWorkflowExecutionNotSet)
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	err = engine.ScheduleWorkflowTask(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return &historyservice.ScheduleWorkflowTaskResponse{}, nil
}

func (h *Handler) VerifyFirstWorkflowTaskScheduled(
	ctx context.Context,
	request *historyservice.VerifyFirstWorkflowTaskScheduledRequest,
) (*historyservice.VerifyFirstWorkflowTaskScheduledResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	if request.WorkflowExecution == nil {
		return nil, h.convertError(errWorkflowExecutionNotSet)
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	err = engine.VerifyFirstWorkflowTaskScheduled(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return &historyservice.VerifyFirstWorkflowTaskScheduledResponse{}, nil
}

// RecordChildExecutionCompleted is used for reporting the completion of child workflow execution to parent.
// This is mainly called by transfer queue processor during the processing of DeleteExecution task.
func (h *Handler) RecordChildExecutionCompleted(ctx context.Context, request *historyservice.RecordChildExecutionCompletedRequest) (*historyservice.RecordChildExecutionCompletedResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	if request.GetParentExecution() == nil {
		return nil, h.convertError(errWorkflowExecutionNotSet)
	}

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, request.GetParentExecution().WorkflowId)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.RecordChildExecutionCompleted(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return resp, nil
}

func (h *Handler) VerifyChildExecutionCompletionRecorded(
	ctx context.Context,
	request *historyservice.VerifyChildExecutionCompletionRecordedRequest,
) (*historyservice.VerifyChildExecutionCompletionRecordedResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	if request.ParentExecution == nil {
		return nil, h.convertError(errWorkflowExecutionNotSet)
	}

	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, request.ParentExecution.GetWorkflowId())
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.VerifyChildExecutionCompletionRecorded(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return resp, nil
}

// ResetStickyTaskQueue reset the volatile information in mutable state of a given workflow.
// Volatile information are the information related to client, such as:
// 1. StickyTaskQueue
// 2. StickyScheduleToStartTimeout
func (h *Handler) ResetStickyTaskQueue(ctx context.Context, request *historyservice.ResetStickyTaskQueueRequest) (*historyservice.ResetStickyTaskQueueResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowID := request.Execution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.ResetStickyTaskQueue(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return resp, nil
}

// ReplicateEventsV2 is called by processor to replicate history events for passive namespaces
func (h *Handler) ReplicateEventsV2(ctx context.Context, request *historyservice.ReplicateEventsV2Request) (*historyservice.ReplicateEventsV2Response, error) {
	if err := api.ValidateReplicationConfig(h.clusterMetadata); err != nil {
		return nil, err
	}

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	err = engine.ReplicateEventsV2(ctx, request)
	if err == nil || errors.Is(err, consts.ErrDuplicate) {
		return &historyservice.ReplicateEventsV2Response{}, nil
	}
	return nil, h.convertError(err)
}

// ReplicateWorkflowState is called by processor to replicate workflow state for passive namespaces
func (h *Handler) ReplicateWorkflowState(
	ctx context.Context,
	request *historyservice.ReplicateWorkflowStateRequest,
) (*historyservice.ReplicateWorkflowStateResponse, error) {
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(
		namespace.ID(request.GetWorkflowState().GetExecutionInfo().GetNamespaceId()),
		request.GetWorkflowState().GetExecutionInfo().GetWorkflowId(),
	)
	if err != nil {
		return nil, h.convertError(err)
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	err = engine.ReplicateWorkflowState(ctx, request)
	if err == nil || errors.Is(err, consts.ErrDuplicate) {
		return &historyservice.ReplicateWorkflowStateResponse{}, nil
	}
	return nil, err
}

// SyncShardStatus is called by processor to sync history shard information from another cluster
func (h *Handler) SyncShardStatus(ctx context.Context, request *historyservice.SyncShardStatusRequest) (*historyservice.SyncShardStatusResponse, error) {
	if request.GetSourceCluster() == "" {
		return nil, h.convertError(errSourceClusterNotSet)
	}

	if request.GetShardId() == 0 {
		return nil, h.convertError(errShardIDNotSet)
	}

	if timestamp.TimeValue(request.GetStatusTime()).IsZero() {
		return nil, h.convertError(errTimestampNotSet)
	}

	shardContext, err := h.controller.GetShardByID(request.GetShardId())
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	err = engine.SyncShardStatus(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return &historyservice.SyncShardStatusResponse{}, nil
}

// SyncActivity is called by processor to sync activity
func (h *Handler) SyncActivity(ctx context.Context, request *historyservice.SyncActivityRequest) (*historyservice.SyncActivityResponse, error) {
	if err := api.ValidateReplicationConfig(h.clusterMetadata); err != nil {
		return nil, err
	}

	namespaceID := namespace.ID(request.GetNamespaceId())
	if request.GetNamespaceId() == "" || uuid.Validate(request.GetNamespaceId()) != nil {
		return nil, h.convertError(errNamespaceNotSet)
	}

	if request.GetWorkflowId() == "" {
		return nil, h.convertError(errWorkflowIDNotSet)
	}

	if request.GetRunId() == "" || uuid.Validate(request.GetRunId()) != nil {
		return nil, h.convertError(errRunIDNotValid)
	}

	workflowID := request.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	err = engine.SyncActivity(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return &historyservice.SyncActivityResponse{}, nil
}

// GetReplicationMessages is called by remote peers to get replicated messages for cross DC replication
func (h *Handler) GetReplicationMessages(ctx context.Context, request *historyservice.GetReplicationMessagesRequest) (*historyservice.GetReplicationMessagesResponse, error) {
	if err := api.ValidateReplicationConfig(h.clusterMetadata); err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	wg.Add(len(request.Tokens))
	result := new(sync.Map)

	for _, token := range request.Tokens {
		go func(token *replicationspb.ReplicationToken) {
			defer wg.Done()

			shardContext, err := h.controller.GetShardByID(token.GetShardId())
			if err != nil {
				h.logger.Warn("History engine not found for shard", tag.Error(err))
				return
			}
			engine, err := shardContext.GetEngine(ctx)
			if err != nil {
				h.logger.Warn("History engine not found for shard", tag.Error(err))
				return
			}

			replicationTasks, err := engine.GetReplicationMessages(
				ctx,
				request.GetClusterName(),
				token.GetLastProcessedMessageId(),
				timestamp.TimeValue(token.LastProcessedVisibilityTime),
				token.GetLastRetrievedMessageId(),
			)
			if err != nil {
				h.logger.Warn("Failed to get replication tasks for shard", tag.Error(err))
				return
			}

			result.Store(token.GetShardId(), replicationTasks)
		}(token)
	}

	wg.Wait()

	messagesByShard := make(map[int32]*replicationspb.ReplicationMessages)
	result.Range(func(key, value any) bool {
		shardID := key.(int32)
		messagesByShard[shardID] = value.(*replicationspb.ReplicationMessages)
		return true
	})

	h.logger.Debug("GetReplicationMessages succeeded.")

	return &historyservice.GetReplicationMessagesResponse{ShardMessages: messagesByShard}, nil
}

// GetDLQReplicationMessages is called by remote peers to get replicated messages for DLQ merging
func (h *Handler) GetDLQReplicationMessages(ctx context.Context, request *historyservice.GetDLQReplicationMessagesRequest) (*historyservice.GetDLQReplicationMessagesResponse, error) {
	if err := api.ValidateReplicationConfig(h.clusterMetadata); err != nil {
		return nil, err
	}

	taskInfoPerShard := map[int32][]*replicationspb.ReplicationTaskInfo{}
	// do batch based on workflow ID and run ID
	for _, taskInfo := range request.GetTaskInfos() {
		shardID := h.config.GetShardID(
			namespace.ID(taskInfo.GetNamespaceId()),
			taskInfo.GetWorkflowId(),
		)
		if _, ok := taskInfoPerShard[shardID]; !ok {
			taskInfoPerShard[shardID] = []*replicationspb.ReplicationTaskInfo{}
		}
		taskInfoPerShard[shardID] = append(taskInfoPerShard[shardID], taskInfo)
	}

	var wg sync.WaitGroup
	wg.Add(len(taskInfoPerShard))
	tasksChan := make(chan *replicationspb.ReplicationTask, len(request.GetTaskInfos()))
	handleTaskInfoPerShard := func(taskInfos []*replicationspb.ReplicationTaskInfo) {
		defer wg.Done()
		if len(taskInfos) == 0 {
			return
		}

		shardContext, err := h.controller.GetShardByNamespaceWorkflow(
			namespace.ID(taskInfos[0].GetNamespaceId()),
			taskInfos[0].GetWorkflowId(),
		)
		if err != nil {
			h.logger.Warn("History engine not found for workflow ID.", tag.Error(err))
			return
		}
		engine, err := shardContext.GetEngine(ctx)
		if err != nil {
			h.logger.Warn("History engine not found for workflow ID.", tag.Error(err))
			return
		}

		dlqTasks, err := engine.GetDLQReplicationMessages(
			ctx,
			taskInfos,
		)
		if err != nil {
			h.logger.Error("Failed to get dlq replication tasks.", tag.Error(err))
			return
		}

		for _, t := range dlqTasks {
			tasksChan <- t
		}
	}

	for _, replicationTaskInfos := range taskInfoPerShard {
		go handleTaskInfoPerShard(replicationTaskInfos)
	}
	wg.Wait()
	close(tasksChan)

	replicationTasks := make([]*replicationspb.ReplicationTask, 0, len(tasksChan))
	for t := range tasksChan {
		replicationTasks = append(replicationTasks, t)
	}
	return &historyservice.GetDLQReplicationMessagesResponse{
		ReplicationTasks: replicationTasks,
	}, nil
}

// ReapplyEvents applies stale events to the current workflow and the current run
func (h *Handler) ReapplyEvents(ctx context.Context, request *historyservice.ReapplyEventsRequest) (*historyservice.ReapplyEventsResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	workflowID := request.GetRequest().GetWorkflowExecution().GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	// deserialize history event object
	eventsBlob := request.GetRequest().GetEvents()
	historyEvents, err := h.payloadSerializer.DeserializeEvents(&commonpb.DataBlob{
		EncodingType: cmp.Or(eventsBlob.GetEncodingType(), enumspb.ENCODING_TYPE_PROTO3),
		Data:         eventsBlob.GetData(),
	})
	if err != nil {
		return nil, h.convertError(err)
	}

	execution := request.GetRequest().GetWorkflowExecution()
	if err := engine.ReapplyEvents(
		ctx,
		namespace.ID(request.GetNamespaceId()),
		execution.GetWorkflowId(),
		execution.GetRunId(),
		historyEvents,
	); err != nil {
		return nil, h.convertError(err)
	}
	return &historyservice.ReapplyEventsResponse{}, nil
}

func (h *Handler) GetDLQMessages(ctx context.Context, request *historyservice.GetDLQMessagesRequest) (*historyservice.GetDLQMessagesResponse, error) {
	shardContext, err := h.controller.GetShardByID(request.GetShardId())
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.GetDLQMessages(ctx, request)
	if err != nil {
		err = h.convertError(err)
		return nil, err
	}

	return resp, nil
}

func (h *Handler) PurgeDLQMessages(ctx context.Context, request *historyservice.PurgeDLQMessagesRequest) (*historyservice.PurgeDLQMessagesResponse, error) {
	shardContext, err := h.controller.GetShardByID(request.GetShardId())
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.PurgeDLQMessages(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return resp, nil
}

func (h *Handler) MergeDLQMessages(ctx context.Context, request *historyservice.MergeDLQMessagesRequest) (*historyservice.MergeDLQMessagesResponse, error) {
	shardContext, err := h.controller.GetShardByID(request.GetShardId())
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.MergeDLQMessages(ctx, request)
	if err != nil {
		err = h.convertError(err)
		return nil, err
	}

	return resp, nil
}

func (h *Handler) RefreshWorkflowTasks(ctx context.Context, request *historyservice.RefreshWorkflowTasksRequest) (*historyservice.RefreshWorkflowTasksResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	execution := request.GetRequest().GetExecution()
	workflowID := execution.GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	err = engine.RefreshWorkflowTasks(
		ctx,
		namespaceID,
		execution,
		request.GetArchetypeId(),
	)

	if err != nil {
		err = h.convertError(err)
		return nil, err
	}

	return &historyservice.RefreshWorkflowTasksResponse{}, nil
}

func (h *Handler) GenerateLastHistoryReplicationTasks(
	ctx context.Context,
	request *historyservice.GenerateLastHistoryReplicationTasksRequest,
) (*historyservice.GenerateLastHistoryReplicationTasksResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	workflowID := request.GetExecution().GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.GenerateLastHistoryReplicationTasks(ctx, request)

	if err != nil {
		err = h.convertError(err)
		return nil, err
	}

	return resp, nil
}

func (h *Handler) GetReplicationStatus(
	ctx context.Context,
	request *historyservice.GetReplicationStatusRequest,
) (*historyservice.GetReplicationStatusResponse, error) {
	if err := api.ValidateReplicationConfig(h.clusterMetadata); err != nil {
		return nil, err
	}

	resp := &historyservice.GetReplicationStatusResponse{}
	for _, shardID := range h.controller.ShardIDs() {
		shardContext, err := h.controller.GetShardByID(shardID)
		if err != nil {
			return nil, h.convertError(err)
		}
		engine, err := shardContext.GetEngine(ctx)
		if err != nil {
			return nil, h.convertError(err)
		}

		status, err := engine.GetReplicationStatus(ctx, request)
		if err != nil {
			return nil, err
		}
		resp.Shards = append(resp.Shards, status)
	}
	return resp, nil
}

func (h *Handler) DeleteWorkflowVisibilityRecord(
	ctx context.Context,
	request *historyservice.DeleteWorkflowVisibilityRecordRequest,
) (*historyservice.DeleteWorkflowVisibilityRecordResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, errNamespaceNotSet
	}

	if request.Execution == nil {
		return nil, errWorkflowExecutionNotSet
	}

	// NOTE: the deletion is best effort, for sql visibility implementation,
	// we can't guarantee there's no update or record close request for this workflow since
	// visibility queue processing is async. Operator can call this api (through admin workflow
	// delete) again to delete again if this happens.
	// For ES implementation, we used max int64 as the TaskID (version) to make sure deletion is
	// the last operation applied for this workflow
	err := h.persistenceVisibilityManager.DeleteWorkflowExecution(ctx, &manager.VisibilityDeleteWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  request.Execution.GetWorkflowId(),
		RunID:       request.Execution.GetRunId(),
		TaskID:      math.MaxInt64,
	})
	if err != nil {
		return nil, h.convertError(err)
	}

	return &historyservice.DeleteWorkflowVisibilityRecordResponse{}, nil
}

func (h *Handler) UpdateWorkflowExecution(
	ctx context.Context,
	request *historyservice.UpdateWorkflowExecutionRequest,
) (*historyservice.UpdateWorkflowExecutionResponse, error) {
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(
		namespace.ID(request.GetNamespaceId()),
		request.GetRequest().GetWorkflowExecution().GetWorkflowId(),
	)
	if err != nil {
		return nil, h.convertError(err)
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	return engine.UpdateWorkflowExecution(ctx, request)
}

func (h *Handler) PollWorkflowExecutionUpdate(
	ctx context.Context,
	request *historyservice.PollWorkflowExecutionUpdateRequest,
) (*historyservice.PollWorkflowExecutionUpdateResponse, error) {
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(
		namespace.ID(request.GetNamespaceId()),
		request.GetRequest().GetUpdateRef().GetWorkflowExecution().GetWorkflowId(),
	)
	if err != nil {
		return nil, h.convertError(err)
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	return engine.PollWorkflowExecutionUpdate(ctx, request)
}

func (h *Handler) StreamWorkflowReplicationMessages(
	server historyservice.HistoryService_StreamWorkflowReplicationMessagesServer,
) (retErr error) {
	// Note that since this is not a unary RPC, we cannot use the interceptor to capture panics.
	metrics.CapturePanic(h.logger, h.metricsHandler, &retErr)
	getter := headers.NewGRPCHeaderGetter(server.Context())
	clientClusterShardID, serverClusterShardID, err := history.DecodeClusterShardMD(getter)
	if err != nil {
		return err
	}
	if serverClusterShardID.ClusterID != int32(h.clusterMetadata.GetClusterID()) {
		return serviceerror.NewInvalidArgumentf(
			"wrong cluster: target: %v, current: %v",
			serverClusterShardID.ClusterID,
			h.clusterMetadata.GetClusterID(),
		)
	}
	shardContext, err := h.controller.GetShardByID(serverClusterShardID.ShardID)
	if err != nil {
		return h.convertError(err)
	}
	engine, err := shardContext.GetEngine(server.Context())
	if err != nil {
		return h.convertError(err)
	}
	allClusterInfo := shardContext.GetClusterMetadata().GetAllClusterInfo()
	clientClusterName, clientShardCount, err := replication.ClusterIDToClusterNameShardCount(allClusterInfo, clientClusterShardID.ClusterID)
	if err != nil {
		return h.convertError(err)
	}
	_, serverShardCount, err := replication.ClusterIDToClusterNameShardCount(allClusterInfo, int32(shardContext.GetClusterMetadata().GetClusterID()))
	if err != nil {
		return h.convertError(err)
	}
	err = common.VerifyShardIDMapping(clientShardCount, serverShardCount, clientClusterShardID.ShardID, serverClusterShardID.ShardID)
	if err != nil {
		return h.convertError(err)
	}
	streamSender := replication.NewStreamSender(
		server,
		shardContext,
		engine,
		h.replicationServerRateLimiter,
		h.replicationTaskConverterProvider(
			engine,
			shardContext,
			clientClusterName,
			h.payloadSerializer,
		),
		clientClusterName,
		clientShardCount,
		replication.NewClusterShardKey(clientClusterShardID.ClusterID, clientClusterShardID.ShardID),
		replication.NewClusterShardKey(serverClusterShardID.ClusterID, serverClusterShardID.ShardID),
		h.config,
	)
	streamSender.Start()
	h.streamReceiverMonitor.RegisterInboundStream(streamSender)
	defer streamSender.Stop()
	streamSender.Wait()
	return nil
}

func (h *Handler) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *historyservice.GetWorkflowExecutionHistoryRequest,
) (*historyservice.GetWorkflowExecutionHistoryResponseWithRaw, error) {
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(
		namespace.ID(request.GetNamespaceId()),
		request.Request.GetExecution().GetWorkflowId(),
	)
	if err != nil {
		return nil, h.convertError(err)
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	return engine.GetWorkflowExecutionHistory(ctx, request)
}

func (h *Handler) GetWorkflowExecutionHistoryReverse(
	ctx context.Context,
	request *historyservice.GetWorkflowExecutionHistoryReverseRequest,
) (*historyservice.GetWorkflowExecutionHistoryReverseResponse, error) {
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(
		namespace.ID(request.GetNamespaceId()),
		request.GetRequest().GetExecution().GetWorkflowId(),
	)
	if err != nil {
		return nil, h.convertError(err)
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	return engine.GetWorkflowExecutionHistoryReverse(ctx, request)
}

func (h *Handler) GetWorkflowExecutionRawHistory(
	ctx context.Context,
	request *historyservice.GetWorkflowExecutionRawHistoryRequest,
) (*historyservice.GetWorkflowExecutionRawHistoryResponse, error) {
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(
		namespace.ID(request.GetNamespaceId()),
		request.GetRequest().GetExecution().GetWorkflowId(),
	)
	if err != nil {
		return nil, h.convertError(err)
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	return engine.GetWorkflowExecutionRawHistory(ctx, request)
}

func (h *Handler) GetWorkflowExecutionRawHistoryV2(
	ctx context.Context,
	request *historyservice.GetWorkflowExecutionRawHistoryV2Request,
) (*historyservice.GetWorkflowExecutionRawHistoryV2Response, error) {
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(
		namespace.ID(request.GetNamespaceId()),
		request.GetRequest().GetExecution().GetWorkflowId(),
	)
	if err != nil {
		return nil, h.convertError(err)
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	return engine.GetWorkflowExecutionRawHistoryV2(ctx, request)
}

func (h *Handler) ForceDeleteWorkflowExecution(
	ctx context.Context,
	request *historyservice.ForceDeleteWorkflowExecutionRequest,
) (*historyservice.ForceDeleteWorkflowExecutionResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	err := api.ValidateNamespaceUUID(namespaceID)
	if err != nil {
		return nil, err
	}
	shardID := common.WorkflowIDToHistoryShard(
		namespaceID.String(),
		request.Request.Execution.WorkflowId,
		h.config.NumberOfShards,
	)

	workflowExecution := request.GetRequest().GetExecution()
	h.logger.Info("ForceDeleteWorkflowExecution requested",
		tag.WorkflowNamespaceID(request.GetNamespaceId()),
		tag.WorkflowID(workflowExecution.GetWorkflowId()),
		tag.WorkflowRunID(workflowExecution.GetRunId()))

	return forcedeleteworkflowexecution.Invoke(
		ctx,
		request,
		shardID,
		h.chasmRegistry,
		h.persistenceExecutionManager,
		h.persistenceVisibilityManager,
		h.logger,
	)
}

func (h *Handler) GetDLQTasks(
	ctx context.Context,
	request *historyservice.GetDLQTasksRequest,
) (*historyservice.GetDLQTasksResponse, error) {
	return getdlqtasks.Invoke(ctx, h.taskQueueManager, h.taskCategoryRegistry, request)
}

func (h *Handler) DeleteDLQTasks(
	ctx context.Context,
	request *historyservice.DeleteDLQTasksRequest,
) (*historyservice.DeleteDLQTasksResponse, error) {
	return deletedlqtasks.Invoke(ctx, h.taskQueueManager, request, h.taskCategoryRegistry)
}

// AddTasks calls the [addtasks.Invoke] API with a [shard.Context] for the given shardID.
func (h *Handler) AddTasks(
	ctx context.Context,
	request *historyservice.AddTasksRequest,
) (*historyservice.AddTasksResponse, error) {
	shardContext, err := h.controller.GetShardByID(request.ShardId)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}
	return engine.AddTasks(ctx, request)
}

func (h *Handler) ListQueues(
	ctx context.Context,
	request *historyservice.ListQueuesRequest,
) (*historyservice.ListQueuesResponse, error) {
	return listqueues.Invoke(ctx, h.taskQueueManager, request)
}

func (h *Handler) ListTasks(
	ctx context.Context,
	request *historyservice.ListTasksRequest,
) (*historyservice.ListTasksResponse, error) {
	shardContext, err := h.controller.GetShardByID(request.Request.GetShardId())
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.ListTasks(ctx, request)
	if err != nil {
		err = h.convertError(err)
		return nil, err
	}

	return resp, nil
}

func (h *Handler) CompleteNexusOperation(ctx context.Context, request *historyservice.CompleteNexusOperationRequest) (*historyservice.CompleteNexusOperationResponse, error) {
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespace.ID(request.Completion.NamespaceId), request.Completion.WorkflowId)
	if err != nil {
		return nil, h.convertError(err)
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	ref := hsm.Ref{
		WorkflowKey:     definition.NewWorkflowKey(request.Completion.NamespaceId, request.Completion.WorkflowId, request.Completion.RunId),
		StateMachineRef: request.Completion.Ref,
	}
	var opErr *nexus.OperationError
	if request.State != string(nexus.OperationStateSucceeded) {
		failure := commonnexus.ProtoFailureToNexusFailure(request.GetFailure())
		recvdErr, err := nexusrpc.DefaultFailureConverter().FailureToError(failure)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument("unable to convert failure to error")
		}
		// Backward compatibility: if the received error is not of type OperationError, wrap the error in OperationError.
		var ok bool
		if opErr, ok = recvdErr.(*nexus.OperationError); !ok {
			opErr = &nexus.OperationError{
				State:   nexus.OperationState(request.GetState()),
				Message: "nexus operation completed unsuccessfully",
				Cause:   recvdErr,
			}
			if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
				return nil, serviceerror.NewInvalidArgument("unable to convert operation error to failure")
			}
		}
	}
	err = nexusoperations.CompletionHandler(
		ctx,
		engine.StateMachineEnvironment(metrics.OperationTag(metrics.HistoryCompleteNexusOperationScope)),
		ref,
		request.Completion.RequestId,
		request.OperationToken,
		request.StartTime,
		request.Links,
		request.GetSuccess(),
		opErr,
	)
	if err != nil {
		return nil, h.convertError(err)
	}
	return &historyservice.CompleteNexusOperationResponse{}, nil
}

func (h *Handler) CompleteNexusOperationChasm(
	ctx context.Context,
	request *historyservice.CompleteNexusOperationChasmRequest,
) (*historyservice.CompleteNexusOperationChasmResponse, error) {
	completion := &persistencespb.ChasmNexusCompletion{
		CloseTime: request.CloseTime,
		RequestId: request.Completion.RequestId,
	}
	switch variant := request.Outcome.(type) {
	case *historyservice.CompleteNexusOperationChasmRequest_Failure:
		completion.Outcome = &persistencespb.ChasmNexusCompletion_Failure{
			Failure: variant.Failure,
		}
	case *historyservice.CompleteNexusOperationChasmRequest_Success:
		completion.Outcome = &persistencespb.ChasmNexusCompletion_Success{
			Success: variant.Success,
		}
	default:
		return nil, serviceerror.NewUnimplemented("unhandled Nexus operation outcome")
	}

	// Attempt to access the component and call its invocation method. We execute
	// this similarly as we would a pure task (holding an exclusive lock), as the
	// assumption is that the accessed component will be recording (or generating a
	// task) based on this result.
	_, _, err := chasm.UpdateComponent(
		ctx,
		request.GetCompletion().GetComponentRef(),
		func(c chasm.NexusCompletionHandler, ctx chasm.MutableContext, completion *persistencespb.ChasmNexusCompletion) (chasm.NoValue, error) {
			return nil, c.HandleNexusCompletion(ctx, completion)
		},
		completion)
	if err != nil {
		return nil, h.convertError(err)
	}

	return &historyservice.CompleteNexusOperationChasmResponse{}, nil
}

// convertError is a helper method to convert ShardOwnershipLostError from persistence layer returned by various
// HistoryEngine API calls to ShardOwnershipLost error return by HistoryService for client to be redirected to the
// correct shard.
func (h *Handler) convertError(err error) error {
	switch err := err.(type) {
	case *persistence.ShardOwnershipLostError:
		hostInfo := h.hostInfoProvider.HostInfo()
		if ownerInfo, err := h.historyServiceResolver.Lookup(convert.Int32ToString(err.ShardID)); err == nil {
			return serviceerrors.NewShardOwnershipLost(ownerInfo.GetAddress(), hostInfo.GetAddress())
		}
		return serviceerrors.NewShardOwnershipLost("", hostInfo.GetAddress())
	case *persistence.AppendHistoryTimeoutError:
		return serviceerror.NewUnavailable(err.Msg)
	case *persistence.WorkflowConditionFailedError:
		return serviceerror.NewUnavailable(err.Msg)
	case *persistence.CurrentWorkflowConditionFailedError:
		return serviceerror.NewUnavailable(err.Msg)
	case *persistence.TransactionSizeLimitError:
		return serviceerror.NewInvalidArgument(err.Msg)
	case *persistence.TimeoutError:
		return serviceerror.NewDeadlineExceeded(err.Msg)
	}

	return err
}

func validateTaskToken(taskToken *tokenspb.Task) error {
	if len(taskToken.GetComponentRef()) == 0 && taskToken.GetWorkflowId() == "" {
		return errBusinessIDNotSet
	}

	return nil
}

func (h *Handler) InvokeStateMachineMethod(ctx context.Context, request *historyservice.InvokeStateMachineMethodRequest) (*historyservice.InvokeStateMachineMethodResponse, error) {
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespace.ID(request.NamespaceId), request.WorkflowId)
	if err != nil {
		return nil, h.convertError(err)
	}

	registry := shardContext.StateMachineRegistry()

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	ref := hsm.Ref{
		WorkflowKey:     definition.NewWorkflowKey(request.NamespaceId, request.WorkflowId, request.RunId),
		StateMachineRef: request.Ref,
	}

	bytes, err := registry.ExecuteRemoteMethod(
		ctx,
		engine.StateMachineEnvironment(metrics.OperationTag(request.MethodName)),
		ref,
		request.MethodName,
		request.Input,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.InvokeStateMachineMethodResponse{
		Output: bytes,
	}, nil
}

func (h *Handler) SyncWorkflowState(ctx context.Context, request *historyservice.SyncWorkflowStateRequest) (*historyservice.SyncWorkflowStateResponse, error) {
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

	response, err := engine.SyncWorkflowState(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return response, nil
}

func (h *Handler) UpdateActivityOptions(
	ctx context.Context, request *historyservice.UpdateActivityOptionsRequest,
) (*historyservice.UpdateActivityOptionsResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	workflowID := request.GetUpdateRequest().GetExecution().GetWorkflowId()
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

	response, err := engine.UpdateActivityOptions(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return response, nil
}

func (h *Handler) PauseActivity(
	ctx context.Context, request *historyservice.PauseActivityRequest,
) (*historyservice.PauseActivityResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	workflowID := request.GetFrontendRequest().GetExecution().GetWorkflowId()
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

	response, err := engine.PauseActivity(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return response, nil
}

func (h *Handler) UnpauseActivity(
	ctx context.Context, request *historyservice.UnpauseActivityRequest,
) (*historyservice.UnpauseActivityResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	workflowID := request.GetFrontendRequest().GetExecution().GetWorkflowId()
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

	response, err := engine.UnpauseActivity(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return response, nil
}

func (h *Handler) ResetActivity(
	ctx context.Context, request *historyservice.ResetActivityRequest,
) (*historyservice.ResetActivityResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	workflowID := request.GetFrontendRequest().GetExecution().GetWorkflowId()
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

	response, err := engine.ResetActivity(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return response, nil
}

// PauseWorkflowExecution is used to pause a running workflow execution. This results in
// WorkflowExecutionPaused event recorded in the history.
func (h *Handler) PauseWorkflowExecution(ctx context.Context, request *historyservice.PauseWorkflowExecutionRequest) (*historyservice.PauseWorkflowExecutionResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowID := request.GetPauseRequest().GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	resp, err := engine.PauseWorkflowExecution(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}

	return resp, nil
}

func (h *Handler) UnpauseWorkflowExecution(ctx context.Context, request *historyservice.UnpauseWorkflowExecutionRequest) (*historyservice.UnpauseWorkflowExecutionResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	workflowID := request.GetUnpauseRequest().GetWorkflowId()
	shardContext, err := h.controller.GetShardByNamespaceWorkflow(namespaceID, workflowID)
	if err != nil {
		return nil, h.convertError(err)
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, h.convertError(err)
	}

	unpauseResp, unpauseErr := engine.UnpauseWorkflowExecution(ctx, request)
	if unpauseErr != nil {
		return nil, h.convertError(unpauseErr)
	}

	return unpauseResp, nil
}

func (h *Handler) StartNexusOperation(
	ctx context.Context,
	req *historyservice.StartNexusOperationRequest,
) (*historyservice.StartNexusOperationResponse, error) {
	requestID := req.GetRequest().GetRequestId()
	// Build nexus.StartOperationOptions from the request
	options := nexus.StartOperationOptions{
		// Header not supported for system endpoint operations.
		Header:         make(nexus.Header),
		RequestID:      requestID,
		CallbackURL:    req.GetRequest().GetCallback(),
		CallbackHeader: nexus.Header(req.GetRequest().GetCallbackHeader()),
		Links:          commonnexus.ConvertLinksFromProto(req.GetRequest().GetLinks()),
	}

	// Wrap the payload in a LazyValue
	input := createLazyValueFromPayload(req.GetRequest().GetPayload())

	// Set up handler context before invoking the handler
	ctx = nexus.WithHandlerContext(ctx, nexus.HandlerInfo{
		Service:   req.GetRequest().GetService(),
		Operation: req.GetRequest().GetOperation(),
		Header:    options.Header,
	})

	// Invoke the operation via the handler
	if h.nexusHandler == nil {
		return nil, serviceerror.NewUnimplemented("no nexus services registered")
	}
	result, err := h.nexusHandler.StartOperation(ctx, req.GetRequest().GetService(), req.GetRequest().GetOperation(), input, options)
	if err != nil {
		var opErr *nexus.OperationError
		if errors.As(err, &opErr) {
			nexusFailure, convErr := nexusrpc.DefaultFailureConverter().ErrorToFailure(opErr)
			if convErr != nil {
				return nil, convErr
			}
			temporalFailure, convErr := commonnexus.NexusFailureToTemporalFailure(nexusFailure)
			if convErr != nil {
				return nil, convErr
			}
			return &historyservice.StartNexusOperationResponse{
				Response: &nexuspb.StartOperationResponse{
					Variant: &nexuspb.StartOperationResponse_Failure{
						Failure: temporalFailure,
					},
				},
			}, nil
		}
		// TODO: redact certain errors
		return nil, err
	}
	links := nexus.HandlerLinks(ctx)

	// Convert the result to the response
	response := &nexuspb.StartOperationResponse{}
	switch r := result.(type) {
	case interface{ ValueAsAny() any }:
		ps, err := payloads.Encode(r.ValueAsAny())
		if err != nil {
			h.logger.Error("failed to encode payload", tag.Error(err), tag.RequestID(requestID))
			return nil, serviceerror.NewInternal("internal error (request ID: " + requestID + ")")
		}
		var payload *commonpb.Payload
		if len(ps.GetPayloads()) == 1 {
			payload = ps.GetPayloads()[0]
		}
		response.Variant = &nexuspb.StartOperationResponse_SyncSuccess{
			SyncSuccess: &nexuspb.StartOperationResponse_Sync{
				Payload: payload,
				Links:   commonnexus.ConvertLinksToProto(links),
			},
		}
	case *nexus.HandlerStartOperationResultAsync:
		response.Variant = &nexuspb.StartOperationResponse_AsyncSuccess{
			AsyncSuccess: &nexuspb.StartOperationResponse_Async{
				OperationToken: r.OperationToken,
				Links:          commonnexus.ConvertLinksToProto(links),
			},
		}
	default:
		h.logger.Error(fmt.Sprintf("invalid result type: %T", result), tag.RequestID(req.Request.RequestId))
		return nil, serviceerror.NewInternal("internal error (request ID: " + requestID + ")")
	}

	return &historyservice.StartNexusOperationResponse{
		Response: response,
	}, nil
}

func (h *Handler) CancelNexusOperation(
	ctx context.Context,
	req *historyservice.CancelNexusOperationRequest,
) (*historyservice.CancelNexusOperationResponse, error) {
	// Build nexus.CancelOperationOptions from the request
	options := nexus.CancelOperationOptions{
		// Header not supported for system endpoint operations.
		Header: make(nexus.Header),
	}

	// Set up handler context before invoking the handler
	ctx = nexus.WithHandlerContext(ctx, nexus.HandlerInfo{
		Service:   req.GetRequest().GetService(),
		Operation: req.GetRequest().GetOperation(),
		// Header not supported for system endpoint operations.
		Header: make(nexus.Header),
	})

	// Invoke the cancel operation via the handler
	if h.nexusHandler == nil {
		return nil, serviceerror.NewUnimplemented("no nexus services registered")
	}
	err := h.nexusHandler.CancelOperation(ctx, req.GetRequest().GetService(), req.GetRequest().GetOperation(), req.GetRequest().GetOperationToken(), options)
	if err != nil {
		// TODO: redact certain errors
		return nil, err
	}

	return &historyservice.CancelNexusOperationResponse{
		Response: &nexuspb.CancelOperationResponse{},
	}, nil
}

func createLazyValueFromPayload(payload *commonpb.Payload) *nexus.LazyValue {
	// Create a serializer that wraps the payload.
	// When Deserialize is called, it will directly unmarshal the payload.
	// This avoids unnecessary serialization/deserialization since the payload is already in the correct format.
	serializer := &payloadSerializer{payload: payload}
	return nexus.NewLazyValue(
		serializer,
		&nexus.Reader{
			ReadCloser: io.NopCloser(bytes.NewReader(nil)),
		},
	)
}

// payloadSerializer is a nexus.Serializer that wraps a commonpb.Payload.
// It only implements Deserialize since CHASM operations work directly with payloads.
type payloadSerializer struct {
	payload *commonpb.Payload
}

// Deserialize unmarshals the wrapped payload into the provided value using the SDK's default data converter.
func (p *payloadSerializer) Deserialize(_ *nexus.Content, v any) error {
	return payloads.Decode(&commonpb.Payloads{Payloads: []*commonpb.Payload{p.payload}}, v)
}

// Serialize should never be called since we only use this serializer for deserialization.
func (p *payloadSerializer) Serialize(v any) (*nexus.Content, error) {
	// nolint:forbidigo // Panic is expected as this method should never be called.
	panic("Serialize not supported on payloadSerializer")
}
