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
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	workflowpb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/addtasks"
	"go.temporal.io/server/service/history/api/deleteworkflow"
	"go.temporal.io/server/service/history/api/describemutablestate"
	"go.temporal.io/server/service/history/api/describeworkflow"
	"go.temporal.io/server/service/history/api/getworkflowexecutionhistory"
	"go.temporal.io/server/service/history/api/getworkflowexecutionhistoryreverse"
	"go.temporal.io/server/service/history/api/getworkflowexecutionrawhistory"
	"go.temporal.io/server/service/history/api/getworkflowexecutionrawhistoryv2"
	"go.temporal.io/server/service/history/api/isactivitytaskvalid"
	"go.temporal.io/server/service/history/api/isworkflowtaskvalid"
	"go.temporal.io/server/service/history/api/listtasks"
	"go.temporal.io/server/service/history/api/multioperation"
	"go.temporal.io/server/service/history/api/pollupdate"
	"go.temporal.io/server/service/history/api/queryworkflow"
	"go.temporal.io/server/service/history/api/reapplyevents"
	"go.temporal.io/server/service/history/api/recordactivitytaskheartbeat"
	"go.temporal.io/server/service/history/api/recordactivitytaskstarted"
	"go.temporal.io/server/service/history/api/recordchildworkflowcompleted"
	"go.temporal.io/server/service/history/api/recordworkflowtaskstarted"
	"go.temporal.io/server/service/history/api/refreshworkflow"
	"go.temporal.io/server/service/history/api/removesignalmutablestate"
	replicationapi "go.temporal.io/server/service/history/api/replication"
	"go.temporal.io/server/service/history/api/replicationadmin"
	"go.temporal.io/server/service/history/api/requestcancelworkflow"
	"go.temporal.io/server/service/history/api/resetstickytaskqueue"
	"go.temporal.io/server/service/history/api/resetworkflow"
	"go.temporal.io/server/service/history/api/respondactivitytaskcanceled"
	"go.temporal.io/server/service/history/api/respondactivitytaskcompleted"
	"go.temporal.io/server/service/history/api/respondactivitytaskfailed"
	"go.temporal.io/server/service/history/api/respondworkflowtaskcompleted"
	"go.temporal.io/server/service/history/api/respondworkflowtaskfailed"
	"go.temporal.io/server/service/history/api/scheduleworkflowtask"
	"go.temporal.io/server/service/history/api/signalwithstartworkflow"
	"go.temporal.io/server/service/history/api/signalworkflow"
	"go.temporal.io/server/service/history/api/startworkflow"
	"go.temporal.io/server/service/history/api/terminateworkflow"
	"go.temporal.io/server/service/history/api/updateworkflow"
	"go.temporal.io/server/service/history/api/verifychildworkflowcompletionrecorded"
	"go.temporal.io/server/service/history/api/verifyfirstworkflowtaskscheduled"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	historyEngineImpl struct {
		status                     int32
		currentClusterName         string
		shardContext               shard.Context
		timeSource                 clock.TimeSource
		clusterMetadata            cluster.Metadata
		executionManager           persistence.ExecutionManager
		queueProcessors            map[tasks.Category]queues.Queue
		replicationAckMgr          replication.AckManager
		nDCHistoryReplicator       ndc.HistoryReplicator
		nDCHistoryImporter         ndc.HistoryImporter
		nDCActivityStateReplicator ndc.ActivityStateReplicator
		nDCWorkflowStateReplicator ndc.WorkflowStateReplicator
		nDCHSMStateReplicator      ndc.HSMStateReplicator
		replicationProcessorMgr    replication.TaskProcessor
		eventNotifier              events.Notifier
		tokenSerializer            common.TaskTokenSerializer
		metricsHandler             metrics.Handler
		logger                     log.Logger
		throttledLogger            log.Logger
		config                     *configs.Config
		workflowRebuilder          workflowRebuilder
		workflowResetter           ndc.WorkflowResetter
		sdkClientFactory           sdk.ClientFactory
		eventsReapplier            ndc.EventsReapplier
		matchingClient             matchingservice.MatchingServiceClient
		rawMatchingClient          matchingservice.MatchingServiceClient
		replicationDLQHandler      replication.DLQHandler
		persistenceVisibilityMgr   manager.VisibilityManager
		searchAttributesValidator  *searchattribute.Validator
		workflowDeleteManager      deletemanager.DeleteManager
		eventSerializer            serialization.Serializer
		workflowConsistencyChecker api.WorkflowConsistencyChecker
		versionChecker             headers.VersionChecker
		tracer                     trace.Tracer
		taskCategoryRegistry       tasks.TaskCategoryRegistry
		commandHandlerRegistry     *workflow.CommandHandlerRegistry
		stateMachineEnvironment    *stateMachineEnvironment
	}
)

// NewEngineWithShardContext creates an instance of history engine
func NewEngineWithShardContext(
	shard shard.Context,
	clientBean client.Bean,
	matchingClient matchingservice.MatchingServiceClient,
	sdkClientFactory sdk.ClientFactory,
	eventNotifier events.Notifier,
	config *configs.Config,
	rawMatchingClient matchingservice.MatchingServiceClient,
	workflowCache wcache.Cache,
	eventSerializer serialization.Serializer,
	queueProcessorFactories []QueueFactory,
	replicationTaskFetcherFactory replication.TaskFetcherFactory,
	replicationTaskExecutorProvider replication.TaskExecutorProvider,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	tracerProvider trace.TracerProvider,
	persistenceVisibilityMgr manager.VisibilityManager,
	eventBlobCache persistence.XDCCache,
	taskCategoryRegistry tasks.TaskCategoryRegistry,
	dlqWriter replication.DLQWriter,
	commandHandlerRegistry *workflow.CommandHandlerRegistry,
) shard.Engine {
	currentClusterName := shard.GetClusterMetadata().GetCurrentClusterName()

	logger := shard.GetLogger()
	executionManager := shard.GetExecutionManager()

	workflowDeleteManager := deletemanager.NewDeleteManager(
		shard,
		workflowCache,
		config,
		shard.GetTimeSource(),
		persistenceVisibilityMgr,
	)

	historyEngImpl := &historyEngineImpl{
		status:                     common.DaemonStatusInitialized,
		currentClusterName:         currentClusterName,
		shardContext:               shard,
		clusterMetadata:            shard.GetClusterMetadata(),
		timeSource:                 shard.GetTimeSource(),
		executionManager:           executionManager,
		tokenSerializer:            common.NewProtoTaskTokenSerializer(),
		logger:                     log.With(logger, tag.ComponentHistoryEngine),
		throttledLogger:            log.With(shard.GetThrottledLogger(), tag.ComponentHistoryEngine),
		metricsHandler:             shard.GetMetricsHandler(),
		eventNotifier:              eventNotifier,
		config:                     config,
		sdkClientFactory:           sdkClientFactory,
		matchingClient:             matchingClient,
		rawMatchingClient:          rawMatchingClient,
		persistenceVisibilityMgr:   persistenceVisibilityMgr,
		workflowDeleteManager:      workflowDeleteManager,
		eventSerializer:            eventSerializer,
		workflowConsistencyChecker: workflowConsistencyChecker,
		versionChecker:             headers.NewDefaultVersionChecker(),
		tracer:                     tracerProvider.Tracer(consts.LibraryName),
		taskCategoryRegistry:       taskCategoryRegistry,
		commandHandlerRegistry:     commandHandlerRegistry,
		stateMachineEnvironment: &stateMachineEnvironment{
			shardContext:   shard,
			cache:          workflowCache,
			metricsHandler: shard.GetMetricsHandler(),
			logger:         logger,
		},
	}

	historyEngImpl.queueProcessors = make(map[tasks.Category]queues.Queue)
	for _, factory := range queueProcessorFactories {
		processor := factory.CreateQueue(shard, workflowCache)
		historyEngImpl.queueProcessors[processor.Category()] = processor
	}

	historyEngImpl.eventsReapplier = ndc.NewEventsReapplier(shard.GetMetricsHandler(), logger)

	if shard.GetClusterMetadata().IsGlobalNamespaceEnabled() {
		historyEngImpl.replicationAckMgr = replication.NewAckManager(
			shard,
			workflowCache,
			eventBlobCache,
			executionManager,
			logger,
		)
		historyEngImpl.nDCHistoryReplicator = ndc.NewHistoryReplicator(
			shard,
			workflowCache,
			historyEngImpl.eventsReapplier,
			eventSerializer,
			logger,
		)
		historyEngImpl.nDCHistoryImporter = ndc.NewHistoryImporter(
			shard,
			workflowCache,
			logger,
		)
		historyEngImpl.nDCActivityStateReplicator = ndc.NewActivityStateReplicator(
			shard,
			workflowCache,
			logger,
		)
		historyEngImpl.nDCWorkflowStateReplicator = ndc.NewWorkflowStateReplicator(
			shard,
			workflowCache,
			historyEngImpl.eventsReapplier,
			eventSerializer,
			logger,
		)
		historyEngImpl.nDCHSMStateReplicator = ndc.NewHSMStateReplicator(
			shard,
			workflowCache,
			logger,
		)
	}
	historyEngImpl.workflowRebuilder = NewWorkflowRebuilder(
		shard,
		workflowCache,
		logger,
	)
	historyEngImpl.workflowResetter = ndc.NewWorkflowResetter(
		shard,
		workflowCache,
		logger,
	)

	historyEngImpl.searchAttributesValidator = searchattribute.NewValidator(
		shard.GetSearchAttributesProvider(),
		shard.GetSearchAttributesMapperProvider(),
		config.SearchAttributesNumberOfKeysLimit,
		config.SearchAttributesSizeOfValueLimit,
		config.SearchAttributesTotalSizeLimit,
		persistenceVisibilityMgr,
		visibility.AllowListForValidation(
			persistenceVisibilityMgr.GetStoreNames(),
			config.VisibilityAllowList,
		),
		config.SuppressErrorSetSystemSearchAttribute,
	)

	historyEngImpl.replicationDLQHandler = replication.NewLazyDLQHandler(
		shard,
		workflowDeleteManager,
		workflowCache,
		clientBean,
		replicationTaskExecutorProvider,
	)
	historyEngImpl.replicationProcessorMgr = replication.NewTaskProcessorManager(
		config,
		shard,
		historyEngImpl,
		workflowCache,
		workflowDeleteManager,
		clientBean,
		eventSerializer,
		replicationTaskFetcherFactory,
		replicationTaskExecutorProvider,
		dlqWriter,
	)
	return historyEngImpl
}

// Start will spin up all the components needed to start serving this shard.
// Make sure all the components are loaded lazily so start can return immediately.  This is important because
// ShardController calls start sequentially for all the shards for a given host during startup.
func (e *historyEngineImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&e.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	e.logger.Info("", tag.LifeCycleStarting)
	defer e.logger.Info("", tag.LifeCycleStarted)

	e.registerNamespaceStateChangeCallback()

	for _, queueProcessor := range e.queueProcessors {
		queueProcessor.Start()
	}
	e.replicationProcessorMgr.Start()
}

// Stop the service.
func (e *historyEngineImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&e.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	e.logger.Info("", tag.LifeCycleStopping)
	defer e.logger.Info("", tag.LifeCycleStopped)

	for _, queueProcessor := range e.queueProcessors {
		queueProcessor.Stop()
	}
	e.replicationProcessorMgr.Stop()
	// unset the failover callback
	e.shardContext.GetNamespaceRegistry().UnregisterStateChangeCallback(e)
}

func (e *historyEngineImpl) registerNamespaceStateChangeCallback() {

	e.shardContext.GetNamespaceRegistry().RegisterStateChangeCallback(e, func(ns *namespace.Namespace, deletedFromDb bool) {
		if e.shardContext.GetClusterMetadata().IsGlobalNamespaceEnabled() {
			e.shardContext.UpdateHandoverNamespace(ns, deletedFromDb)
		}

		if deletedFromDb {
			return
		}

		if ns.IsGlobalNamespace() &&
			ns.ReplicationPolicy() == namespace.ReplicationPolicyMultiCluster &&
			ns.ActiveClusterName() == e.currentClusterName {

			for _, queueProcessor := range e.queueProcessors {
				queueProcessor.FailoverNamespace(ns.ID().String())
			}
		}
	})
}

// StartWorkflowExecution starts a workflow execution
// Consistency guarantee: always write
func (e *historyEngineImpl) StartWorkflowExecution(
	ctx context.Context,
	startRequest *historyservice.StartWorkflowExecutionRequest,
) (resp *historyservice.StartWorkflowExecutionResponse, retError error) {
	starter, err := startworkflow.NewStarter(
		e.shardContext,
		e.workflowConsistencyChecker,
		e.tokenSerializer,
		e.persistenceVisibilityMgr,
		startRequest,
	)
	if err != nil {
		return nil, err
	}
	return starter.Invoke(ctx, startworkflow.BeforeCreateHookNoop)
}

func (e *historyEngineImpl) ExecuteMultiOperation(
	ctx context.Context,
	request *historyservice.ExecuteMultiOperationRequest,
) (*historyservice.ExecuteMultiOperationResponse, error) {
	return multioperation.Invoke(
		ctx,
		request,
		e.shardContext,
		e.workflowConsistencyChecker,
		e.tokenSerializer,
		e.persistenceVisibilityMgr,
		e.matchingClient,
	)
}

// GetMutableState retrieves the mutable state of the workflow execution
func (e *historyEngineImpl) GetMutableState(
	ctx context.Context,
	request *historyservice.GetMutableStateRequest,
) (*historyservice.GetMutableStateResponse, error) {
	return api.GetOrPollMutableState(ctx, e.shardContext, request, e.workflowConsistencyChecker, e.eventNotifier)
}

// PollMutableState retrieves the mutable state of the workflow execution with long polling
func (e *historyEngineImpl) PollMutableState(
	ctx context.Context,
	request *historyservice.PollMutableStateRequest,
) (*historyservice.PollMutableStateResponse, error) {

	response, err := api.GetOrPollMutableState(
		ctx,
		e.shardContext,
		&historyservice.GetMutableStateRequest{
			NamespaceId:         request.GetNamespaceId(),
			Execution:           request.Execution,
			ExpectedNextEventId: request.ExpectedNextEventId,
			CurrentBranchToken:  request.CurrentBranchToken,
			VersionHistoryItem:  request.GetVersionHistoryItem(),
		},
		e.workflowConsistencyChecker,
		e.eventNotifier,
	)
	if err != nil {
		return nil, err
	}

	return &historyservice.PollMutableStateResponse{
		Execution:                             response.Execution,
		WorkflowType:                          response.WorkflowType,
		NextEventId:                           response.NextEventId,
		PreviousStartedEventId:                response.PreviousStartedEventId,
		LastFirstEventId:                      response.LastFirstEventId,
		LastFirstEventTxnId:                   response.LastFirstEventTxnId,
		TaskQueue:                             response.TaskQueue,
		StickyTaskQueue:                       response.StickyTaskQueue,
		StickyTaskQueueScheduleToStartTimeout: response.StickyTaskQueueScheduleToStartTimeout,
		CurrentBranchToken:                    response.CurrentBranchToken,
		VersionHistories:                      response.VersionHistories,
		WorkflowState:                         response.WorkflowState,
		WorkflowStatus:                        response.WorkflowStatus,
		FirstExecutionRunId:                   response.FirstExecutionRunId,
	}, nil
}

func (e *historyEngineImpl) QueryWorkflow(
	ctx context.Context,
	request *historyservice.QueryWorkflowRequest,
) (_ *historyservice.QueryWorkflowResponse, retErr error) {
	return queryworkflow.Invoke(ctx, request, e.shardContext, e.workflowConsistencyChecker, e.rawMatchingClient, e.matchingClient)
}

func (e *historyEngineImpl) DescribeMutableState(
	ctx context.Context,
	request *historyservice.DescribeMutableStateRequest,
) (response *historyservice.DescribeMutableStateResponse, retError error) {
	return describemutablestate.Invoke(ctx, request, e.shardContext, e.workflowConsistencyChecker)
}

// ResetStickyTaskQueue reset the volatile information in mutable state of a given workflow.
// Volatile information are the information related to client, such as:
// 1. StickyTaskQueue
// 2. StickyScheduleToStartTimeout
func (e *historyEngineImpl) ResetStickyTaskQueue(
	ctx context.Context,
	resetRequest *historyservice.ResetStickyTaskQueueRequest,
) (*historyservice.ResetStickyTaskQueueResponse, error) {
	return resetstickytaskqueue.Invoke(ctx, resetRequest, e.shardContext, e.workflowConsistencyChecker)
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (e *historyEngineImpl) DescribeWorkflowExecution(
	ctx context.Context,
	request *historyservice.DescribeWorkflowExecutionRequest,
) (_ *historyservice.DescribeWorkflowExecutionResponse, retError error) {
	return describeworkflow.Invoke(
		ctx,
		request,
		e.shardContext,
		e.workflowConsistencyChecker,
		e.persistenceVisibilityMgr,
	)
}

func (e *historyEngineImpl) RecordActivityTaskStarted(
	ctx context.Context,
	request *historyservice.RecordActivityTaskStartedRequest,
) (*historyservice.RecordActivityTaskStartedResponse, error) {
	return recordactivitytaskstarted.Invoke(ctx, request, e.shardContext, e.workflowConsistencyChecker)
}

// ScheduleWorkflowTask schedules a workflow task if no outstanding workflow task found
func (e *historyEngineImpl) ScheduleWorkflowTask(
	ctx context.Context,
	req *historyservice.ScheduleWorkflowTaskRequest,
) error {
	return scheduleworkflowtask.Invoke(ctx, req, e.shardContext, e.workflowConsistencyChecker)
}

func (e *historyEngineImpl) VerifyFirstWorkflowTaskScheduled(
	ctx context.Context,
	request *historyservice.VerifyFirstWorkflowTaskScheduledRequest,
) (retError error) {
	return verifyfirstworkflowtaskscheduled.Invoke(ctx, request, e.workflowConsistencyChecker)
}

// RecordWorkflowTaskStarted starts a workflow task
func (e *historyEngineImpl) RecordWorkflowTaskStarted(
	ctx context.Context,
	request *historyservice.RecordWorkflowTaskStartedRequest,
) (*historyservice.RecordWorkflowTaskStartedResponse, error) {
	return recordworkflowtaskstarted.Invoke(
		ctx,
		request,
		e.shardContext,
		e.config,
		e.eventNotifier,
		e.persistenceVisibilityMgr,
		e.workflowConsistencyChecker,
	)
}

// RespondWorkflowTaskCompleted completes a workflow task
func (e *historyEngineImpl) RespondWorkflowTaskCompleted(
	ctx context.Context,
	req *historyservice.RespondWorkflowTaskCompletedRequest,
) (*historyservice.RespondWorkflowTaskCompletedResponse, error) {
	h := respondworkflowtaskcompleted.NewWorkflowTaskCompletedHandler(
		e.shardContext,
		e.tokenSerializer,
		e.eventNotifier,
		e.commandHandlerRegistry,
		e.searchAttributesValidator,
		e.persistenceVisibilityMgr,
		e.workflowConsistencyChecker,
	)
	return h.Invoke(ctx, req)
}

// RespondWorkflowTaskFailed fails a workflow task
func (e *historyEngineImpl) RespondWorkflowTaskFailed(
	ctx context.Context,
	req *historyservice.RespondWorkflowTaskFailedRequest,
) error {
	return respondworkflowtaskfailed.Invoke(ctx, req, e.shardContext, e.tokenSerializer, e.workflowConsistencyChecker)
}

// RespondActivityTaskCompleted completes an activity task.
func (e *historyEngineImpl) RespondActivityTaskCompleted(
	ctx context.Context,
	req *historyservice.RespondActivityTaskCompletedRequest,
) (*historyservice.RespondActivityTaskCompletedResponse, error) {
	return respondactivitytaskcompleted.Invoke(ctx, req, e.shardContext, e.workflowConsistencyChecker)
}

// RespondActivityTaskFailed completes an activity task failure.
func (e *historyEngineImpl) RespondActivityTaskFailed(
	ctx context.Context,
	req *historyservice.RespondActivityTaskFailedRequest,
) (*historyservice.RespondActivityTaskFailedResponse, error) {
	return respondactivitytaskfailed.Invoke(ctx, req, e.shardContext, e.workflowConsistencyChecker)
}

// RespondActivityTaskCanceled completes an activity task failure.
func (e *historyEngineImpl) RespondActivityTaskCanceled(
	ctx context.Context,
	req *historyservice.RespondActivityTaskCanceledRequest,
) (*historyservice.RespondActivityTaskCanceledResponse, error) {
	return respondactivitytaskcanceled.Invoke(ctx, req, e.shardContext, e.workflowConsistencyChecker)
}

// RecordActivityTaskHeartbeat records an hearbeat for a task.
// This method can be used for two purposes.
// - For reporting liveness of the activity.
// - For reporting progress of the activity, this can be done even if the liveness is not configured.
func (e *historyEngineImpl) RecordActivityTaskHeartbeat(
	ctx context.Context,
	req *historyservice.RecordActivityTaskHeartbeatRequest,
) (*historyservice.RecordActivityTaskHeartbeatResponse, error) {
	return recordactivitytaskheartbeat.Invoke(ctx, req, e.shardContext, e.workflowConsistencyChecker)
}

// RequestCancelWorkflowExecution records request cancellation event for workflow execution
func (e *historyEngineImpl) RequestCancelWorkflowExecution(
	ctx context.Context,
	req *historyservice.RequestCancelWorkflowExecutionRequest,
) (resp *historyservice.RequestCancelWorkflowExecutionResponse, retError error) {
	return requestcancelworkflow.Invoke(ctx, req, e.shardContext, e.workflowConsistencyChecker)
}

func (e *historyEngineImpl) SignalWorkflowExecution(
	ctx context.Context,
	req *historyservice.SignalWorkflowExecutionRequest,
) (resp *historyservice.SignalWorkflowExecutionResponse, retError error) {
	return signalworkflow.Invoke(ctx, req, e.shardContext, e.workflowConsistencyChecker)
}

// SignalWithStartWorkflowExecution signals current workflow (if running) or creates & signals a new workflow
// Consistency guarantee: always write
func (e *historyEngineImpl) SignalWithStartWorkflowExecution(
	ctx context.Context,
	req *historyservice.SignalWithStartWorkflowExecutionRequest,
) (_ *historyservice.SignalWithStartWorkflowExecutionResponse, retError error) {
	return signalwithstartworkflow.Invoke(ctx, req, e.shardContext, e.workflowConsistencyChecker)
}

func (e *historyEngineImpl) UpdateWorkflowExecution(
	ctx context.Context,
	req *historyservice.UpdateWorkflowExecutionRequest,
) (*historyservice.UpdateWorkflowExecutionResponse, error) {
	updater := updateworkflow.NewUpdater(
		e.shardContext,
		e.workflowConsistencyChecker,
		e.matchingClient,
		req,
	)
	return updater.Invoke(ctx)
}

func (e *historyEngineImpl) PollWorkflowExecutionUpdate(
	ctx context.Context,
	req *historyservice.PollWorkflowExecutionUpdateRequest,
) (*historyservice.PollWorkflowExecutionUpdateResponse, error) {
	return pollupdate.Invoke(ctx, req, e.shardContext, e.workflowConsistencyChecker)
}

// RemoveSignalMutableState remove the signal request id in signal_requested for deduplicate
func (e *historyEngineImpl) RemoveSignalMutableState(
	ctx context.Context,
	req *historyservice.RemoveSignalMutableStateRequest,
) (*historyservice.RemoveSignalMutableStateResponse, error) {
	return removesignalmutablestate.Invoke(ctx, req, e.shardContext, e.workflowConsistencyChecker)
}

func (e *historyEngineImpl) TerminateWorkflowExecution(
	ctx context.Context,
	req *historyservice.TerminateWorkflowExecutionRequest,
) (*historyservice.TerminateWorkflowExecutionResponse, error) {
	return terminateworkflow.Invoke(ctx, req, e.shardContext, e.workflowConsistencyChecker)
}

func (e *historyEngineImpl) DeleteWorkflowExecution(
	ctx context.Context,
	request *historyservice.DeleteWorkflowExecutionRequest,
) (*historyservice.DeleteWorkflowExecutionResponse, error) {
	return deleteworkflow.Invoke(ctx, request, e.shardContext, e.workflowConsistencyChecker, e.workflowDeleteManager)
}

// RecordChildExecutionCompleted records the completion of child execution into parent execution history
func (e *historyEngineImpl) RecordChildExecutionCompleted(
	ctx context.Context,
	req *historyservice.RecordChildExecutionCompletedRequest,
) (*historyservice.RecordChildExecutionCompletedResponse, error) {
	return recordchildworkflowcompleted.Invoke(ctx, req, e.shardContext, e.workflowConsistencyChecker)
}

// IsActivityTaskValid - whether activity task is still valid
func (e *historyEngineImpl) IsActivityTaskValid(
	ctx context.Context,
	req *historyservice.IsActivityTaskValidRequest,
) (*historyservice.IsActivityTaskValidResponse, error) {
	return isactivitytaskvalid.Invoke(ctx, req, e.shardContext, e.workflowConsistencyChecker)
}

// IsWorkflowTaskValid - whether workflow task is still valid
func (e *historyEngineImpl) IsWorkflowTaskValid(
	ctx context.Context,
	req *historyservice.IsWorkflowTaskValidRequest,
) (*historyservice.IsWorkflowTaskValidResponse, error) {
	return isworkflowtaskvalid.Invoke(ctx, req, e.shardContext, e.workflowConsistencyChecker)
}

func (e *historyEngineImpl) VerifyChildExecutionCompletionRecorded(
	ctx context.Context,
	req *historyservice.VerifyChildExecutionCompletionRecordedRequest,
) (*historyservice.VerifyChildExecutionCompletionRecordedResponse, error) {
	return verifychildworkflowcompletionrecorded.Invoke(ctx, req, e.workflowConsistencyChecker)
}

func (e *historyEngineImpl) ReplicateEventsV2(
	ctx context.Context,
	replicateRequest *historyservice.ReplicateEventsV2Request,
) error {
	return e.nDCHistoryReplicator.ApplyEvents(ctx, replicateRequest)
}

func (e *historyEngineImpl) ReplicateHistoryEvents(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	baseExecutionInfo *workflowpb.BaseExecutionInfo,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	historyEvents [][]*historypb.HistoryEvent,
	newEvents []*historypb.HistoryEvent,
	newRunID string,
) error {
	return e.nDCHistoryReplicator.ReplicateHistoryEvents(
		ctx,
		workflowKey,
		baseExecutionInfo,
		versionHistoryItems,
		historyEvents,
		newEvents,
		newRunID,
	)
}

func (e *historyEngineImpl) SyncActivity(
	ctx context.Context,
	request *historyservice.SyncActivityRequest,
) error {
	return e.nDCActivityStateReplicator.SyncActivityState(ctx, request)
}

func (e *historyEngineImpl) SyncActivities(
	ctx context.Context,
	request *historyservice.SyncActivitiesRequest,
) error {
	return e.nDCActivityStateReplicator.SyncActivitiesState(ctx, request)
}

func (e *historyEngineImpl) SyncHSM(
	ctx context.Context,
	request *shard.SyncHSMRequest,
) error {
	return e.nDCHSMStateReplicator.SyncHSMState(ctx, request)
}

// ReplicateWorkflowState is an experimental method to replicate workflow state. This should not expose outside of history service role.
func (e *historyEngineImpl) ReplicateWorkflowState(
	ctx context.Context,
	request *historyservice.ReplicateWorkflowStateRequest,
) error {
	return e.nDCWorkflowStateReplicator.SyncWorkflowState(ctx, request)
}

func (e *historyEngineImpl) ImportWorkflowExecution(
	ctx context.Context,
	request *historyservice.ImportWorkflowExecutionRequest,
) (*historyservice.ImportWorkflowExecutionResponse, error) {
	historyEvents, err := ndc.DeserializeBlobs(e.eventSerializer, request.HistoryBatches)
	if err != nil {
		return nil, err
	}
	token, eventsApplied, err := e.nDCHistoryImporter.ImportWorkflow(
		ctx,
		definition.NewWorkflowKey(
			request.NamespaceId,
			request.Execution.GetWorkflowId(),
			request.Execution.GetRunId(),
		),
		request.VersionHistory.Items,
		historyEvents,
		request.Token,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.ImportWorkflowExecutionResponse{
		Token:         token,
		EventsApplied: eventsApplied,
	}, nil
}

func (e *historyEngineImpl) SyncShardStatus(
	ctx context.Context,
	request *historyservice.SyncShardStatusRequest,
) error {

	clusterName := request.GetSourceCluster()
	now := timestamp.TimeValue(request.GetStatusTime())

	// here there are 3 main things
	// 1. update the view of remote cluster's shard time
	// 2. notify the timer gate in the timer queue standby processor
	// 3, notify the transfer (essentially a no op, just put it here so it looks symmetric)
	e.shardContext.SetCurrentTime(clusterName, now)
	for _, processor := range e.queueProcessors {
		processor.NotifyNewTasks([]tasks.Task{})
	}
	return nil
}

// ResetWorkflowExecution terminates current workflow (if running) and replay & create new workflow
// Consistency guarantee: always write
func (e *historyEngineImpl) ResetWorkflowExecution(
	ctx context.Context,
	req *historyservice.ResetWorkflowExecutionRequest,
) (*historyservice.ResetWorkflowExecutionResponse, error) {
	return resetworkflow.Invoke(ctx, req, e.shardContext, e.workflowConsistencyChecker)
}

func (e *historyEngineImpl) NotifyNewHistoryEvent(
	notification *events.Notification,
) {

	e.eventNotifier.NotifyNewHistoryEvent(notification)
}

func (e *historyEngineImpl) NotifyNewTasks(
	newTasks map[tasks.Category][]tasks.Task,
) {
	for category, tasksByCategory := range newTasks {
		// TODO: make replicatorProcessor part of queueProcessors list
		// and get rid of the special case here.
		if category == tasks.CategoryReplication {
			if e.replicationAckMgr != nil {
				e.replicationAckMgr.NotifyNewTasks(tasksByCategory)
			}
			continue
		}

		if len(tasksByCategory) > 0 {
			e.queueProcessors[category].NotifyNewTasks(tasksByCategory)
		}
	}
}

func (e *historyEngineImpl) AddSpeculativeWorkflowTaskTimeoutTask(task *tasks.WorkflowTaskTimeoutTask) {
	e.queueProcessors[tasks.CategoryMemoryTimer].NotifyNewTasks([]tasks.Task{task})
}

func (e *historyEngineImpl) GetReplicationMessages(
	ctx context.Context,
	pollingCluster string,
	ackMessageID int64,
	ackTimestamp time.Time,
	queryMessageID int64,
) (*replicationspb.ReplicationMessages, error) {
	return replicationapi.GetTasks(ctx, e.shardContext, e.replicationAckMgr, pollingCluster, ackMessageID, ackTimestamp, queryMessageID)
}

func (e *historyEngineImpl) SubscribeReplicationNotification() (<-chan struct{}, string) {
	return e.replicationAckMgr.SubscribeNotification()
}

func (e *historyEngineImpl) UnsubscribeReplicationNotification(subscriberID string) {
	e.replicationAckMgr.UnsubscribeNotification(subscriberID)
}

func (e *historyEngineImpl) ConvertReplicationTask(
	ctx context.Context,
	task tasks.Task,
) (*replicationspb.ReplicationTask, error) {
	return e.replicationAckMgr.ConvertTask(ctx, task)
}

func (e *historyEngineImpl) GetReplicationTasksIter(
	ctx context.Context,
	pollingCluster string,
	minInclusiveTaskID int64,
	maxExclusiveTaskID int64,
) (collection.Iterator[tasks.Task], error) {
	return e.replicationAckMgr.GetReplicationTasksIter(ctx, pollingCluster, minInclusiveTaskID, maxExclusiveTaskID)
}

func (e *historyEngineImpl) GetDLQReplicationMessages(
	ctx context.Context,
	taskInfos []*replicationspb.ReplicationTaskInfo,
) ([]*replicationspb.ReplicationTask, error) {
	return replicationapi.GetDLQTasks(ctx, e.shardContext, e.replicationAckMgr, taskInfos)
}

func (e *historyEngineImpl) ReapplyEvents(
	ctx context.Context,
	namespaceUUID namespace.ID,
	workflowID string,
	runID string,
	reapplyEvents []*historypb.HistoryEvent,
) error {
	return reapplyevents.Invoke(ctx, namespaceUUID, workflowID, runID, reapplyEvents, e.shardContext, e.workflowConsistencyChecker, e.workflowResetter, e.eventsReapplier)
}

func (e *historyEngineImpl) GetDLQMessages(
	ctx context.Context,
	request *historyservice.GetDLQMessagesRequest,
) (*historyservice.GetDLQMessagesResponse, error) {
	return replicationadmin.GetDLQ(ctx, request, e.shardContext, e.replicationDLQHandler)
}

func (e *historyEngineImpl) PurgeDLQMessages(
	ctx context.Context,
	request *historyservice.PurgeDLQMessagesRequest,
) (*historyservice.PurgeDLQMessagesResponse, error) {
	return replicationadmin.PurgeDLQ(ctx, request, e.shardContext, e.replicationDLQHandler)
}

func (e *historyEngineImpl) MergeDLQMessages(
	ctx context.Context,
	request *historyservice.MergeDLQMessagesRequest,
) (*historyservice.MergeDLQMessagesResponse, error) {
	return replicationadmin.MergeDLQ(ctx, request, e.shardContext, e.replicationDLQHandler)
}

func (e *historyEngineImpl) RebuildMutableState(
	ctx context.Context,
	namespaceUUID namespace.ID,
	execution *commonpb.WorkflowExecution,
) error {
	return e.workflowRebuilder.rebuild(
		ctx,
		definition.NewWorkflowKey(
			namespaceUUID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
	)
}

func (e *historyEngineImpl) RefreshWorkflowTasks(
	ctx context.Context,
	namespaceUUID namespace.ID,
	execution *commonpb.WorkflowExecution,
) (retError error) {
	return refreshworkflow.Invoke(
		ctx,
		definition.NewWorkflowKey(namespaceUUID.String(), execution.WorkflowId, execution.RunId),
		e.shardContext,
		e.workflowConsistencyChecker,
	)
}

func (e *historyEngineImpl) GenerateLastHistoryReplicationTasks(
	ctx context.Context,
	request *historyservice.GenerateLastHistoryReplicationTasksRequest,
) (_ *historyservice.GenerateLastHistoryReplicationTasksResponse, retError error) {
	return replicationapi.GenerateTask(ctx, request, e.shardContext, e.workflowConsistencyChecker)
}

func (e *historyEngineImpl) GetReplicationStatus(
	ctx context.Context,
	request *historyservice.GetReplicationStatusRequest,
) (_ *historyservice.ShardReplicationStatus, retError error) {
	return replicationapi.GetStatus(ctx, request, e.shardContext, e.replicationAckMgr)
}

func (e *historyEngineImpl) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *historyservice.GetWorkflowExecutionHistoryRequest,
) (_ *historyservice.GetWorkflowExecutionHistoryResponse, retError error) {
	return getworkflowexecutionhistory.Invoke(ctx, e.shardContext, e.workflowConsistencyChecker, e.versionChecker, e.eventNotifier, request, e.persistenceVisibilityMgr)
}

func (e *historyEngineImpl) GetWorkflowExecutionHistoryReverse(
	ctx context.Context,
	request *historyservice.GetWorkflowExecutionHistoryReverseRequest,
) (_ *historyservice.GetWorkflowExecutionHistoryReverseResponse, retError error) {
	return getworkflowexecutionhistoryreverse.Invoke(ctx, e.shardContext, e.workflowConsistencyChecker, e.eventNotifier, request, e.persistenceVisibilityMgr)
}

func (e *historyEngineImpl) GetWorkflowExecutionRawHistory(ctx context.Context, request *historyservice.GetWorkflowExecutionRawHistoryRequest) (*historyservice.GetWorkflowExecutionRawHistoryResponse, error) {
	return getworkflowexecutionrawhistory.Invoke(ctx, e.shardContext, e.workflowConsistencyChecker, e.eventNotifier, request)
}

func (e *historyEngineImpl) GetWorkflowExecutionRawHistoryV2(
	ctx context.Context,
	request *historyservice.GetWorkflowExecutionRawHistoryV2Request,
) (_ *historyservice.GetWorkflowExecutionRawHistoryV2Response, retError error) {
	return getworkflowexecutionrawhistoryv2.Invoke(ctx, e.shardContext, e.workflowConsistencyChecker, e.eventNotifier, request)
}

func (e *historyEngineImpl) AddTasks(
	ctx context.Context,
	request *historyservice.AddTasksRequest,
) (_ *historyservice.AddTasksResponse, retError error) {
	return addtasks.Invoke(
		ctx,
		e.shardContext,
		e.eventSerializer,
		int(e.config.NumberOfShards),
		request,
		e.taskCategoryRegistry,
	)
}

func (e *historyEngineImpl) ListTasks(
	ctx context.Context,
	request *historyservice.ListTasksRequest,
) (_ *historyservice.ListTasksResponse, retError error) {
	return listtasks.Invoke(
		ctx,
		e.taskCategoryRegistry,
		e.executionManager,
		request,
	)
}

// StateMachineEnvironment implements shard.Engine.
func (e *historyEngineImpl) StateMachineEnvironment() hsm.Environment {
	return e.stateMachineEnvironment
}
