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

package history

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/pborman/uuid"
	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.uber.org/fx"
	"google.golang.org/grpc/metadata"

	"go.temporal.io/server/api/historyservice/v1"
	namespacespb "go.temporal.io/server/api/namespace/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/deletedlqtasks"
	"go.temporal.io/server/service/history/api/forcedeleteworkflowexecution"
	"go.temporal.io/server/service/history/api/getdlqtasks"
	"go.temporal.io/server/service/history/api/listqueues"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

type (

	// Handler - gRPC handler interface for historyservice
	Handler struct {
		historyservice.UnsafeHistoryServiceServer

		status int32

		tokenSerializer              common.TaskTokenSerializer
		startWG                      sync.WaitGroup
		config                       *configs.Config
		eventNotifier                events.Notifier
		logger                       log.Logger
		throttledLogger              log.Logger
		persistenceExecutionManager  persistence.ExecutionManager
		persistenceShardManager      persistence.ShardManager
		persistenceVisibilityManager manager.VisibilityManager
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

	errDeserializeTaskTokenMessage = "Error to deserialize task token. Error: %v."

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
}

func (h *Handler) isStopped() bool {
	return atomic.LoadInt32(&h.status) == common.DaemonStatusStopped
}

// IsWorkflowTaskValid - whether workflow task is still valid
func (h *Handler) IsWorkflowTaskValid(ctx context.Context, request *historyservice.IsWorkflowTaskValidRequest) (_ *historyservice.IsWorkflowTaskValidResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
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
	defer log.CapturePanic(h.logger, &retError)
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
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	heartbeatRequest := request.HeartbeatRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(heartbeatRequest.TaskToken)
	if err0 != nil {
		return nil, h.convertError(serviceerror.NewInvalidArgument(fmt.Sprintf(errDeserializeTaskTokenMessage, err0)))
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
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
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
func (h *Handler) RecordWorkflowTaskStarted(ctx context.Context, request *historyservice.RecordWorkflowTaskStartedRequest) (_ *historyservice.RecordWorkflowTaskStartedResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
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
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	completeRequest := request.CompleteRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err0 != nil {
		return nil, h.convertError(serviceerror.NewInvalidArgument(fmt.Sprintf(errDeserializeTaskTokenMessage, err0)))
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
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	failRequest := request.FailedRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(failRequest.TaskToken)
	if err0 != nil {
		return nil, h.convertError(serviceerror.NewInvalidArgument(fmt.Sprintf(errDeserializeTaskTokenMessage, err0)))
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
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	cancelRequest := request.CancelRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(cancelRequest.TaskToken)
	if err0 != nil {
		return nil, h.convertError(serviceerror.NewInvalidArgument(fmt.Sprintf(errDeserializeTaskTokenMessage, err0)))
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

// RespondWorkflowTaskCompleted - records completion of a workflow task
func (h *Handler) RespondWorkflowTaskCompleted(ctx context.Context, request *historyservice.RespondWorkflowTaskCompletedRequest) (_ *historyservice.RespondWorkflowTaskCompletedResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	completeRequest := request.CompleteRequest
	token, err0 := h.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err0 != nil {
		return nil, h.convertError(serviceerror.NewInvalidArgument(fmt.Sprintf(errDeserializeTaskTokenMessage, err0)))
	}

	h.logger.Debug("RespondWorkflowTaskCompleted",
		tag.WorkflowNamespaceID(token.GetNamespaceId()),
		tag.WorkflowID(token.GetWorkflowId()),
		tag.WorkflowRunID(token.GetRunId()),
		tag.WorkflowScheduledEventID(token.GetScheduledEventId()))

	err0 = validateTaskToken(token)
	if err0 != nil {
		return nil, h.convertError(err0)
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

	response, err2 := engine.RespondWorkflowTaskCompleted(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return response, nil
}

// RespondWorkflowTaskFailed - failed response to workflow task
func (h *Handler) RespondWorkflowTaskFailed(ctx context.Context, request *historyservice.RespondWorkflowTaskFailedRequest) (_ *historyservice.RespondWorkflowTaskFailedResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	namespaceID := namespace.ID(request.GetNamespaceId())
	if namespaceID == "" {
		return nil, h.convertError(errNamespaceNotSet)
	}

	failedRequest := request.FailedRequest
	token, err0 := h.tokenSerializer.Deserialize(failedRequest.TaskToken)
	if err0 != nil {
		return nil, h.convertError(serviceerror.NewInvalidArgument(fmt.Sprintf(errDeserializeTaskTokenMessage, err0)))
	}

	h.logger.Debug("RespondWorkflowTaskFailed",
		tag.WorkflowNamespaceID(token.GetNamespaceId()),
		tag.WorkflowID(token.GetWorkflowId()),
		tag.WorkflowRunID(token.GetRunId()),
		tag.WorkflowScheduledEventID(token.GetScheduledEventId()))

	err0 = validateTaskToken(token)
	if err0 != nil {
		return nil, h.convertError(err0)
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

	err2 := engine.RespondWorkflowTaskFailed(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return &historyservice.RespondWorkflowTaskFailedResponse{}, nil
}

// StartWorkflowExecution - creates a new workflow execution
func (h *Handler) StartWorkflowExecution(ctx context.Context, request *historyservice.StartWorkflowExecutionRequest) (_ *historyservice.StartWorkflowExecutionResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

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
) (_ *historyservice.ExecuteMultiOperationResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

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
func (h *Handler) DescribeHistoryHost(_ context.Context, _ *historyservice.DescribeHistoryHostRequest) (_ *historyservice.DescribeHistoryHostResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	itemsInCacheByIDCount, itemsInCacheByNameCount := h.namespaceRegistry.GetCacheSize()

	ownedShardIDs := h.controller.ShardIDs()
	resp := &historyservice.DescribeHistoryHostResponse{
		ShardsNumber: int32(len(ownedShardIDs)),
		ShardIds:     ownedShardIDs,
		NamespaceCache: &namespacespb.NamespaceCacheInfo{
			ItemsInCacheByIdCount:   itemsInCacheByIDCount,
			ItemsInCacheByNameCount: itemsInCacheByNameCount,
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
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid task category ID: %v", request.Category))
	}

	key := tasks.NewKey(
		timestamp.TimeValue(request.GetVisibilityTime()),
		request.GetTaskId(),
	)
	if err := tasks.ValidateKey(key); err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid task key: %v", err.Error()))
	}

	err = h.persistenceExecutionManager.CompleteHistoryTask(ctx, &persistence.CompleteHistoryTaskRequest{
		ShardID:      request.GetShardId(),
		TaskCategory: category,
		TaskKey:      key,
	})

	return &historyservice.RemoveTaskResponse{}, err
}

// CloseShard closes a shard hosted by this instance
func (h *Handler) CloseShard(_ context.Context, request *historyservice.CloseShardRequest) (_ *historyservice.CloseShardResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.controller.CloseShardByID(request.GetShardId())
	return &historyservice.CloseShardResponse{}, nil
}

// GetShard gets a shard hosted by this instance
func (h *Handler) GetShard(ctx context.Context, request *historyservice.GetShardRequest) (_ *historyservice.GetShardResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	resp, err := h.persistenceShardManager.GetOrCreateShard(ctx, &persistence.GetOrCreateShardRequest{
		ShardID: request.ShardId,
	})
	if err != nil {
		return nil, err
	}
	return &historyservice.GetShardResponse{ShardInfo: resp.ShardInfo}, nil
}

// RebuildMutableState attempts to rebuild mutable state according to persisted history events
func (h *Handler) RebuildMutableState(ctx context.Context, request *historyservice.RebuildMutableStateRequest) (_ *historyservice.RebuildMutableStateResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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
func (h *Handler) ImportWorkflowExecution(ctx context.Context, request *historyservice.ImportWorkflowExecutionRequest) (_ *historyservice.ImportWorkflowExecutionResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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
func (h *Handler) DescribeMutableState(ctx context.Context, request *historyservice.DescribeMutableStateRequest) (_ *historyservice.DescribeMutableStateResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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

	resp, err2 := engine.DescribeMutableState(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}
	return resp, nil
}

// GetMutableState - returns the id of the next event in the execution's history
func (h *Handler) GetMutableState(ctx context.Context, request *historyservice.GetMutableStateRequest) (_ *historyservice.GetMutableStateResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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

	resp, err2 := engine.GetMutableState(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}
	return resp, nil
}

// PollMutableState - returns the id of the next event in the execution's history
func (h *Handler) PollMutableState(ctx context.Context, request *historyservice.PollMutableStateRequest) (_ *historyservice.PollMutableStateResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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

	resp, err2 := engine.PollMutableState(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}
	return resp, nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (h *Handler) DescribeWorkflowExecution(ctx context.Context, request *historyservice.DescribeWorkflowExecutionRequest) (_ *historyservice.DescribeWorkflowExecutionResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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

	resp, err2 := engine.DescribeWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}
	return resp, nil
}

// RequestCancelWorkflowExecution - requests cancellation of a workflow
func (h *Handler) RequestCancelWorkflowExecution(ctx context.Context, request *historyservice.RequestCancelWorkflowExecutionRequest) (_ *historyservice.RequestCancelWorkflowExecutionResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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

	resp, err2 := engine.RequestCancelWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a workflow task being created for the execution.
func (h *Handler) SignalWorkflowExecution(ctx context.Context, request *historyservice.SignalWorkflowExecutionRequest) (_ *historyservice.SignalWorkflowExecutionResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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

	resp, err2 := engine.SignalWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

// SignalWithStartWorkflowExecution is used to ensure sending a signal event to a workflow execution.
// If workflow is running, this results in WorkflowExecutionSignaled event recorded in the history
// and a workflow task being created for the execution.
// If workflow is not running or not found, this results in WorkflowExecutionStarted and WorkflowExecutionSignaled
// event recorded in history, and a workflow task being created for the execution
func (h *Handler) SignalWithStartWorkflowExecution(ctx context.Context, request *historyservice.SignalWithStartWorkflowExecutionRequest) (_ *historyservice.SignalWithStartWorkflowExecutionResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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
		resp, err2 := engine.SignalWithStartWorkflowExecution(ctx, request)
		if err2 == nil {
			return resp, nil
		}

		// Two simultaneous SignalWithStart requests might try to start a workflow at the same time.
		// This can result in one of the requests failing with one of two possible errors:
		//    CurrentWorkflowConditionFailedError || WorkflowConditionFailedError
		// If either error occurs, just go ahead and retry. It should succeed on the subsequent attempt.
		// For simplicity, we keep trying unless the context finishes or we get an error that is not one of the
		// two mentioned above.
		_, isCurrentWorkflowConditionFailedErr := err2.(*persistence.CurrentWorkflowConditionFailedError)
		_, isWorkflowConditionFailedErr := err2.(*persistence.WorkflowConditionFailedError)

		isContextDone := false
		select {
		case <-ctx.Done():
			isContextDone = true
			if ctxErr := ctx.Err(); ctxErr != nil {
				err2 = ctxErr
			}
		default:
		}

		if (!isCurrentWorkflowConditionFailedErr && !isWorkflowConditionFailedErr) || isContextDone {
			return nil, h.convertError(err2)
		}
	}
}

// RemoveSignalMutableState is used to remove a signal request ID that was previously recorded.  This is currently
// used to clean execution info when signal workflow task finished.
func (h *Handler) RemoveSignalMutableState(ctx context.Context, request *historyservice.RemoveSignalMutableStateRequest) (_ *historyservice.RemoveSignalMutableStateResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
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

	resp, err2 := engine.RemoveSignalMutableState(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

// TerminateWorkflowExecution terminates an existing workflow execution by recording WorkflowExecutionTerminated event
// in the history and immediately terminating the execution instance.
func (h *Handler) TerminateWorkflowExecution(ctx context.Context, request *historyservice.TerminateWorkflowExecutionRequest) (_ *historyservice.TerminateWorkflowExecutionResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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

	resp, err2 := engine.TerminateWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

func (h *Handler) DeleteWorkflowExecution(ctx context.Context, request *historyservice.DeleteWorkflowExecutionRequest) (_ *historyservice.DeleteWorkflowExecutionResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
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

	resp, err := engine.DeleteWorkflowExecution(ctx, request)
	if err != nil {
		return nil, h.convertError(err)
	}
	return resp, nil
}

// ResetWorkflowExecution reset an existing workflow execution
// in the history and immediately terminating the execution instance.
func (h *Handler) ResetWorkflowExecution(ctx context.Context, request *historyservice.ResetWorkflowExecutionRequest) (_ *historyservice.ResetWorkflowExecutionResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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

	resp, err2 := engine.ResetWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

// QueryWorkflow queries a workflow.
func (h *Handler) QueryWorkflow(ctx context.Context, request *historyservice.QueryWorkflowRequest) (_ *historyservice.QueryWorkflowResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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

	resp, err2 := engine.QueryWorkflow(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

// ScheduleWorkflowTask is used for creating a workflow task for already started workflow execution.  This is mainly
// used by transfer queue processor during the processing of StartChildWorkflowExecution task, where it first starts
// child execution without creating the workflow task and then calls this API after updating the mutable state of
// parent execution.
func (h *Handler) ScheduleWorkflowTask(ctx context.Context, request *historyservice.ScheduleWorkflowTaskRequest) (_ *historyservice.ScheduleWorkflowTaskResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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

	err2 := engine.ScheduleWorkflowTask(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return &historyservice.ScheduleWorkflowTaskResponse{}, nil
}

func (h *Handler) VerifyFirstWorkflowTaskScheduled(
	ctx context.Context,
	request *historyservice.VerifyFirstWorkflowTaskScheduledRequest,
) (_ *historyservice.VerifyFirstWorkflowTaskScheduledResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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

	err2 := engine.VerifyFirstWorkflowTaskScheduled(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return &historyservice.VerifyFirstWorkflowTaskScheduledResponse{}, nil
}

// RecordChildExecutionCompleted is used for reporting the completion of child workflow execution to parent.
// This is mainly called by transfer queue processor during the processing of DeleteExecution task.
func (h *Handler) RecordChildExecutionCompleted(ctx context.Context, request *historyservice.RecordChildExecutionCompletedRequest) (_ *historyservice.RecordChildExecutionCompletedResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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

	resp, err2 := engine.RecordChildExecutionCompleted(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

func (h *Handler) VerifyChildExecutionCompletionRecorded(
	ctx context.Context,
	request *historyservice.VerifyChildExecutionCompletionRecordedRequest,
) (_ *historyservice.VerifyChildExecutionCompletionRecordedResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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

	resp, err2 := engine.VerifyChildExecutionCompletionRecorded(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return resp, nil
}

// ResetStickyTaskQueue reset the volatile information in mutable state of a given workflow.
// Volatile information are the information related to client, such as:
// 1. StickyTaskQueue
// 2. StickyScheduleToStartTimeout
func (h *Handler) ResetStickyTaskQueue(ctx context.Context, request *historyservice.ResetStickyTaskQueueRequest) (_ *historyservice.ResetStickyTaskQueueResponse, retError error) {

	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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
func (h *Handler) ReplicateEventsV2(ctx context.Context, request *historyservice.ReplicateEventsV2Request) (_ *historyservice.ReplicateEventsV2Response, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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

	err2 := engine.ReplicateEventsV2(ctx, request)
	if err2 != nil {
		return nil, h.convertError(err2)
	}

	return &historyservice.ReplicateEventsV2Response{}, nil
}

// ReplicateWorkflowState is called by processor to replicate workflow state for passive namespaces
func (h *Handler) ReplicateWorkflowState(
	ctx context.Context,
	request *historyservice.ReplicateWorkflowStateRequest,
) (_ *historyservice.ReplicateWorkflowStateResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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
	if err != nil {
		return nil, err
	}
	return &historyservice.ReplicateWorkflowStateResponse{}, nil
}

// SyncShardStatus is called by processor to sync history shard information from another cluster
func (h *Handler) SyncShardStatus(ctx context.Context, request *historyservice.SyncShardStatusRequest) (_ *historyservice.SyncShardStatusResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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
func (h *Handler) SyncActivity(ctx context.Context, request *historyservice.SyncActivityRequest) (_ *historyservice.SyncActivityResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

	if err := api.ValidateReplicationConfig(h.clusterMetadata); err != nil {
		return nil, err
	}

	namespaceID := namespace.ID(request.GetNamespaceId())
	if request.GetNamespaceId() == "" || uuid.Parse(request.GetNamespaceId()) == nil {
		return nil, h.convertError(errNamespaceNotSet)
	}

	if request.GetWorkflowId() == "" {
		return nil, h.convertError(errWorkflowIDNotSet)
	}

	if request.GetRunId() == "" || uuid.Parse(request.GetRunId()) == nil {
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
func (h *Handler) GetReplicationMessages(ctx context.Context, request *historyservice.GetReplicationMessagesRequest) (_ *historyservice.GetReplicationMessagesResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}
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
	result.Range(func(key, value interface{}) bool {
		shardID := key.(int32)
		messagesByShard[shardID] = value.(*replicationspb.ReplicationMessages)
		return true
	})

	h.logger.Debug("GetReplicationMessages succeeded.")

	return &historyservice.GetReplicationMessagesResponse{ShardMessages: messagesByShard}, nil
}

// GetDLQReplicationMessages is called by remote peers to get replicated messages for DLQ merging
func (h *Handler) GetDLQReplicationMessages(ctx context.Context, request *historyservice.GetDLQReplicationMessagesRequest) (_ *historyservice.GetDLQReplicationMessagesResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}
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
func (h *Handler) ReapplyEvents(ctx context.Context, request *historyservice.ReapplyEventsRequest) (_ *historyservice.ReapplyEventsResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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
	historyEvents, err := h.payloadSerializer.DeserializeEvents(&commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         request.GetRequest().GetEvents().GetData(),
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

func (h *Handler) GetDLQMessages(ctx context.Context, request *historyservice.GetDLQMessagesRequest) (_ *historyservice.GetDLQMessagesResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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

func (h *Handler) PurgeDLQMessages(ctx context.Context, request *historyservice.PurgeDLQMessagesRequest) (_ *historyservice.PurgeDLQMessagesResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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

func (h *Handler) MergeDLQMessages(ctx context.Context, request *historyservice.MergeDLQMessagesRequest) (_ *historyservice.MergeDLQMessagesResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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

func (h *Handler) RefreshWorkflowTasks(ctx context.Context, request *historyservice.RefreshWorkflowTasksRequest) (_ *historyservice.RefreshWorkflowTasksResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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
		&commonpb.WorkflowExecution{
			WorkflowId: execution.WorkflowId,
			RunId:      execution.RunId,
		},
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
) (_ *historyservice.GenerateLastHistoryReplicationTasksResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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
) (_ *historyservice.GetReplicationStatusResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}
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
) (_ *historyservice.DeleteWorkflowVisibilityRecordResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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
) (_ *historyservice.UpdateWorkflowExecutionResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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
) (_ *historyservice.PollWorkflowExecutionUpdateResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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
) (retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return errShuttingDown
	}

	ctxMetadata, ok := metadata.FromIncomingContext(server.Context())
	if !ok {
		return serviceerror.NewInvalidArgument("missing cluster & shard ID metadata")
	}
	clientClusterShardID, serverClusterShardID, err := history.DecodeClusterShardMD(ctxMetadata)
	if err != nil {
		return err
	}
	if serverClusterShardID.ClusterID != int32(h.clusterMetadata.GetClusterID()) {
		return serviceerror.NewInvalidArgument(fmt.Sprintf(
			"wrong cluster: target: %v, current: %v",
			serverClusterShardID.ClusterID,
			h.clusterMetadata.GetClusterID(),
		))
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
		h.replicationTaskConverterProvider(
			engine,
			shardContext,
			clientClusterName,
		),
		clientClusterName,
		clientShardCount,
		replication.NewClusterShardKey(clientClusterShardID.ClusterID, clientClusterShardID.ShardID),
		replication.NewClusterShardKey(serverClusterShardID.ClusterID, serverClusterShardID.ShardID),
		h.config,
	)
	h.streamReceiverMonitor.RegisterInboundStream(streamSender)
	streamSender.Start()
	defer streamSender.Stop()
	streamSender.Wait()
	return nil
}

func (h *Handler) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *historyservice.GetWorkflowExecutionHistoryRequest,
) (_ *historyservice.GetWorkflowExecutionHistoryResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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
) (_ *historyservice.GetWorkflowExecutionHistoryReverseResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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
) (_ *historyservice.GetWorkflowExecutionRawHistoryResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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
) (_ *historyservice.GetWorkflowExecutionRawHistoryV2Response, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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
) (_ *historyservice.ForceDeleteWorkflowExecutionResponse, retErr error) {
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
	return forcedeleteworkflowexecution.Invoke(
		ctx,
		request,
		shardID,
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
) (_ *historyservice.ListTasksResponse, retError error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retError)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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

func (h *Handler) CompleteNexusOperation(ctx context.Context, request *historyservice.CompleteNexusOperationRequest) (_ *historyservice.CompleteNexusOperationResponse, retErr error) {
	defer metrics.CapturePanic(h.logger, h.metricsHandler, &retErr)
	h.startWG.Wait()

	if h.isStopped() {
		return nil, errShuttingDown
	}

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
	var opErr *nexus.UnsuccessfulOperationError
	if request.State != string(nexus.OperationStateSucceeded) {
		opErr = &nexus.UnsuccessfulOperationError{
			State:   nexus.OperationState(request.GetState()),
			Failure: *commonnexus.ProtoFailureToNexusFailure(request.GetFailure()),
		}
	}
	err = nexusoperations.CompletionHandler(ctx, engine.StateMachineEnvironment(), ref, request.GetSuccess(), opErr)
	if err != nil {
		return nil, h.convertError(err)
	}
	return &historyservice.CompleteNexusOperationResponse{}, nil
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
	}

	return err
}

func validateTaskToken(taskToken *tokenspb.Task) error {
	if taskToken.GetWorkflowId() == "" {
		return errWorkflowIDNotSet
	}
	return nil
}
