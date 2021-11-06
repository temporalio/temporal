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

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	"go.temporal.io/server/common/searchattribute"

	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type (
	// workflow task business logic handler
	workflowTaskHandlerCallbacks interface {
		handleWorkflowTaskScheduled(context.Context, *historyservice.ScheduleWorkflowTaskRequest) error
		handleWorkflowTaskStarted(context.Context,
			*historyservice.RecordWorkflowTaskStartedRequest) (*historyservice.RecordWorkflowTaskStartedResponse, error)
		handleWorkflowTaskFailed(context.Context,
			*historyservice.RespondWorkflowTaskFailedRequest) error
		handleWorkflowTaskCompleted(context.Context,
			*historyservice.RespondWorkflowTaskCompletedRequest) (*historyservice.RespondWorkflowTaskCompletedResponse, error)
		// TODO also include the handle of workflow task timeout here
	}

	workflowTaskHandlerCallbacksImpl struct {
		currentClusterName     string
		config                 *configs.Config
		shard                  shard.Context
		timeSource             clock.TimeSource
		historyEngine          *historyEngineImpl
		namespaceRegistry      namespace.Registry
		historyCache           workflow.Cache
		txProcessor            transferQueueProcessor
		timerProcessor         timerQueueProcessor
		tokenSerializer        common.TaskTokenSerializer
		metricsClient          metrics.Client
		logger                 log.Logger
		throttledLogger        log.Logger
		commandAttrValidator   *commandAttrValidator
		searchAttributesMapper searchattribute.Mapper
	}
)

func newWorkflowTaskHandlerCallback(historyEngine *historyEngineImpl) *workflowTaskHandlerCallbacksImpl {
	return &workflowTaskHandlerCallbacksImpl{
		currentClusterName: historyEngine.currentClusterName,
		config:             historyEngine.config,
		shard:              historyEngine.shard,
		timeSource:         historyEngine.shard.GetTimeSource(),
		historyEngine:      historyEngine,
		namespaceRegistry:  historyEngine.shard.GetNamespaceRegistry(),
		historyCache:       historyEngine.historyCache,
		txProcessor:        historyEngine.txProcessor,
		timerProcessor:     historyEngine.timerProcessor,
		tokenSerializer:    historyEngine.tokenSerializer,
		metricsClient:      historyEngine.metricsClient,
		logger:             historyEngine.logger,
		throttledLogger:    historyEngine.throttledLogger,
		commandAttrValidator: newCommandAttrValidator(
			historyEngine.shard.GetNamespaceRegistry(),
			historyEngine.config,
			historyEngine.searchAttributesValidator,
		),
		searchAttributesMapper: historyEngine.searchAttributesMapper,
	}
}

func (handler *workflowTaskHandlerCallbacksImpl) handleWorkflowTaskScheduled(
	ctx context.Context,
	req *historyservice.ScheduleWorkflowTaskRequest,
) error {

	namespaceEntry, err := handler.historyEngine.getActiveNamespaceEntry(namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return err
	}
	namespaceID := namespaceEntry.ID()

	execution := commonpb.WorkflowExecution{
		WorkflowId: req.WorkflowExecution.WorkflowId,
		RunId:      req.WorkflowExecution.RunId,
	}

	return handler.historyEngine.updateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		func(context workflow.Context, mutableState workflow.MutableState) (*updateWorkflowAction, error) {
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			if mutableState.HasProcessedOrPendingWorkflowTask() {
				return &updateWorkflowAction{
					noop: true,
				}, nil
			}

			startEvent, err := mutableState.GetStartEvent()
			if err != nil {
				return nil, err
			}
			if err := mutableState.AddFirstWorkflowTaskScheduled(
				startEvent,
			); err != nil {
				return nil, err
			}

			return &updateWorkflowAction{}, nil
		})
}

func (handler *workflowTaskHandlerCallbacksImpl) handleWorkflowTaskStarted(
	ctx context.Context,
	req *historyservice.RecordWorkflowTaskStartedRequest,
) (*historyservice.RecordWorkflowTaskStartedResponse, error) {

	namespaceEntry, err := handler.historyEngine.getActiveNamespaceEntry(namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	execution := commonpb.WorkflowExecution{
		WorkflowId: req.WorkflowExecution.WorkflowId,
		RunId:      req.WorkflowExecution.RunId,
	}

	scheduleID := req.GetScheduleId()
	requestID := req.GetRequestId()

	var resp *historyservice.RecordWorkflowTaskStartedResponse
	err = handler.historyEngine.updateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		func(context workflow.Context, mutableState workflow.MutableState) (*updateWorkflowAction, error) {
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			workflowTask, isRunning := mutableState.GetWorkflowTaskInfo(scheduleID)
			metricsScope := handler.metricsClient.Scope(metrics.HistoryRecordWorkflowTaskStartedScope)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduleID >= mutableState.GetNextEventID() {
				metricsScope.IncCounter(metrics.StaleMutableStateCounter)
				// Reload workflow execution history
				// ErrStaleState will trigger updateWorkflowExecution function to reload the mutable state
				return nil, consts.ErrStaleState
			}

			// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
			// task is not outstanding than it is most probably a duplicate and complete the task.
			if !isRunning {
				// Looks like WorkflowTask already completed as a result of another call.
				// It is OK to drop the task at this point.
				return nil, serviceerror.NewNotFound("Workflow task not found.")
			}

			updateAction := &updateWorkflowAction{}

			if workflowTask.StartedID != common.EmptyEventID {
				// If workflow task is started as part of the current request scope then return a positive response
				if workflowTask.RequestID == requestID {
					resp, err = handler.createRecordWorkflowTaskStartedResponse(mutableState, workflowTask, req.PollRequest.GetIdentity())
					if err != nil {
						return nil, err
					}
					updateAction.noop = true
					return updateAction, nil
				}

				// Looks like WorkflowTask already started as a result of another call.
				// It is OK to drop the task at this point.
				return nil, serviceerrors.NewTaskAlreadyStarted("Workflow")
			}

			_, workflowTask, err = mutableState.AddWorkflowTaskStartedEvent(
				scheduleID,
				requestID,
				req.PollRequest.TaskQueue,
				req.PollRequest.Identity,
			)
			if err != nil {
				// Unable to add WorkflowTaskStarted event to history
				return nil, err
			}

			workflowScheduleToStartLatency := workflowTask.StartedTime.Sub(*workflowTask.ScheduledTime)
			namespaceName := namespaceEntry.Name()
			taskQueue := workflowTask.TaskQueue
			metrics.GetPerTaskQueueScope(metricsScope, namespaceName.String(), taskQueue.GetName(), taskQueue.GetKind()).
				Tagged(metrics.TaskTypeTag("workflow")).
				RecordTimer(metrics.TaskScheduleToStartLatency, workflowScheduleToStartLatency)

			resp, err = handler.createRecordWorkflowTaskStartedResponse(mutableState, workflowTask, req.PollRequest.GetIdentity())
			if err != nil {
				return nil, err
			}
			return updateAction, nil
		})

	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (handler *workflowTaskHandlerCallbacksImpl) handleWorkflowTaskFailed(
	ctx context.Context,
	req *historyservice.RespondWorkflowTaskFailedRequest,
) (retError error) {

	namespaceEntry, err := handler.historyEngine.getActiveNamespaceEntry(namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return err
	}
	namespaceID := namespaceEntry.ID()

	request := req.FailedRequest
	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return consts.ErrDeserializingToken
	}

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: token.GetWorkflowId(),
		RunId:      token.GetRunId(),
	}

	return handler.historyEngine.updateWorkflowExecution(
		ctx,
		namespaceID,
		workflowExecution,
		func(context workflow.Context, mutableState workflow.MutableState) (*updateWorkflowAction, error) {
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			scheduleID := token.GetScheduleId()
			workflowTask, isRunning := mutableState.GetWorkflowTaskInfo(scheduleID)
			if !isRunning || workflowTask.Attempt != token.ScheduleAttempt || workflowTask.StartedID == common.EmptyEventID {
				return nil, serviceerror.NewNotFound("Workflow task not found.")
			}

			_, err := mutableState.AddWorkflowTaskFailedEvent(workflowTask.ScheduleID, workflowTask.StartedID, request.GetCause(), request.GetFailure(),
				request.GetIdentity(), request.GetBinaryChecksum(), "", "", 0)
			if err != nil {
				return nil, err
			}
			return &updateWorkflowAction{
				noop:               false,
				createWorkflowTask: true,
			}, nil
		})
}

func (handler *workflowTaskHandlerCallbacksImpl) handleWorkflowTaskCompleted(
	ctx context.Context,
	req *historyservice.RespondWorkflowTaskCompletedRequest,
) (resp *historyservice.RespondWorkflowTaskCompletedResponse, retError error) {

	namespaceEntry, err := handler.historyEngine.getActiveNamespaceEntry(namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	request := req.CompleteRequest
	token, err0 := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: token.GetWorkflowId(),
		RunId:      token.GetRunId(),
	}

	weContext, release, err := handler.historyCache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		workflowExecution,
		workflow.CallerTypeAPI,
	)
	if err != nil {
		return nil, err
	}
	defer func() { release(retError) }()

Update_History_Loop:
	for attempt := 1; attempt <= conditionalRetryCount; attempt++ {
		msBuilder, err := weContext.LoadWorkflowExecution()
		if err != nil {
			return nil, err
		}
		if !msBuilder.IsWorkflowExecutionRunning() {
			return nil, consts.ErrWorkflowCompleted
		}
		executionStats, err := weContext.LoadExecutionStats()
		if err != nil {
			return nil, err
		}

		executionInfo := msBuilder.GetExecutionInfo()

		scheduleID := token.GetScheduleId()
		currentWorkflowTask, isRunning := msBuilder.GetWorkflowTaskInfo(scheduleID)

		// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
		// some extreme cassandra failure cases.
		if !isRunning && scheduleID >= msBuilder.GetNextEventID() {
			handler.metricsClient.IncCounter(metrics.HistoryRespondWorkflowTaskCompletedScope, metrics.StaleMutableStateCounter)
			// Reload workflow execution history
			weContext.Clear()
			continue Update_History_Loop
		}

		if !msBuilder.IsWorkflowExecutionRunning() || !isRunning || currentWorkflowTask.Attempt != token.ScheduleAttempt ||
			currentWorkflowTask.StartedID == common.EmptyEventID {
			return nil, serviceerror.NewNotFound("Workflow task not found.")
		}

		startedID := currentWorkflowTask.StartedID
		maxResetPoints := handler.config.MaxAutoResetPoints(namespaceEntry.Name().String())
		if msBuilder.GetExecutionInfo().AutoResetPoints != nil && maxResetPoints == len(msBuilder.GetExecutionInfo().AutoResetPoints.Points) {
			handler.metricsClient.IncCounter(metrics.HistoryRespondWorkflowTaskCompletedScope, metrics.AutoResetPointsLimitExceededCounter)
		}

		workflowTaskHeartbeating := request.GetForceCreateNewWorkflowTask() && len(request.Commands) == 0
		var workflowTaskHeartbeatTimeout bool
		var completedEvent *historypb.HistoryEvent
		if workflowTaskHeartbeating {
			namespace := namespaceEntry.Name()
			timeout := handler.config.WorkflowTaskHeartbeatTimeout(namespace.String())
			origSchedTime := timestamp.TimeValue(currentWorkflowTask.OriginalScheduledTime)
			if origSchedTime.UnixNano() > 0 && handler.timeSource.Now().After(origSchedTime.Add(timeout)) {
				workflowTaskHeartbeatTimeout = true
				scope := handler.metricsClient.Scope(metrics.HistoryRespondWorkflowTaskCompletedScope, metrics.NamespaceTag(namespace.String()))
				scope.IncCounter(metrics.WorkflowTaskHeartbeatTimeoutCounter)
				completedEvent, err = msBuilder.AddWorkflowTaskTimedOutEvent(currentWorkflowTask.ScheduleID, currentWorkflowTask.StartedID)
				if err != nil {
					return nil, err
				}
				msBuilder.ClearStickyness()
			} else {
				completedEvent, err = msBuilder.AddWorkflowTaskCompletedEvent(scheduleID, startedID, request, maxResetPoints)
				if err != nil {
					return nil, err
				}
			}
		} else {
			completedEvent, err = msBuilder.AddWorkflowTaskCompletedEvent(scheduleID, startedID, request, maxResetPoints)
			if err != nil {
				return nil, err
			}
		}

		var (
			wtFailedCause               *workflowTaskFailedCause
			activityNotStartedCancelled bool
			newStateBuilder             workflow.MutableState

			hasUnhandledEvents bool
		)
		hasUnhandledEvents = msBuilder.HasBufferedEvents()

		if request.StickyAttributes == nil || request.StickyAttributes.WorkerTaskQueue == nil {
			handler.metricsClient.IncCounter(metrics.HistoryRespondWorkflowTaskCompletedScope, metrics.CompleteWorkflowTaskWithStickyDisabledCounter)
			executionInfo.StickyTaskQueue = ""
			executionInfo.StickyScheduleToStartTimeout = timestamp.DurationFromSeconds(0)
		} else {
			handler.metricsClient.IncCounter(metrics.HistoryRespondWorkflowTaskCompletedScope, metrics.CompleteWorkflowTaskWithStickyEnabledCounter)
			executionInfo.StickyTaskQueue = request.StickyAttributes.WorkerTaskQueue.GetName()
			executionInfo.StickyScheduleToStartTimeout = request.StickyAttributes.GetScheduleToStartTimeout()
		}

		binChecksum := request.GetBinaryChecksum()
		if err := namespaceEntry.VerifyBinaryChecksum(binChecksum); err != nil {
			wtFailedCause = NewWorkflowTaskFailedCause(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_BINARY, serviceerror.NewInvalidArgument(fmt.Sprintf("binary %v is already marked as bad deployment", binChecksum)))
		} else {
			namespace := namespaceEntry.Name()
			workflowSizeChecker := newWorkflowSizeChecker(
				handler.config.BlobSizeLimitWarn(namespace.String()),
				handler.config.BlobSizeLimitError(namespace.String()),
				handler.config.MemoSizeLimitWarn(namespace.String()),
				handler.config.MemoSizeLimitError(namespace.String()),
				handler.config.HistorySizeLimitWarn(namespace.String()),
				handler.config.HistorySizeLimitError(namespace.String()),
				handler.config.HistoryCountLimitWarn(namespace.String()),
				handler.config.HistoryCountLimitError(namespace.String()),
				completedEvent.GetEventId(),
				msBuilder,
				handler.historyEngine.searchAttributesValidator,
				executionStats,
				handler.metricsClient.Scope(metrics.HistoryRespondWorkflowTaskCompletedScope, metrics.NamespaceTag(namespace.String())),
				handler.throttledLogger,
			)

			workflowTaskHandler := newWorkflowTaskHandler(
				request.GetIdentity(),
				completedEvent.GetEventId(),
				msBuilder,
				handler.commandAttrValidator,
				workflowSizeChecker,
				handler.logger,
				handler.namespaceRegistry,
				handler.metricsClient,
				handler.config,
				handler.shard,
				handler.searchAttributesMapper,
			)

			if err := workflowTaskHandler.handleCommands(
				request.Commands,
			); err != nil {
				return nil, err
			}

			// set the vars used by following logic
			// further refactor should also clean up the vars used below
			wtFailedCause = workflowTaskHandler.workflowTaskFailedCause

			// failMessage is not used by workflowTaskHandlerCallbacks
			activityNotStartedCancelled = workflowTaskHandler.activityNotStartedCancelled
			// continueAsNewTimerTasks is not used by workflowTaskHandlerCallbacks

			newStateBuilder = workflowTaskHandler.newStateBuilder

			hasUnhandledEvents = workflowTaskHandler.hasBufferedEvents
		}

		if wtFailedCause != nil {
			handler.metricsClient.IncCounter(metrics.HistoryRespondWorkflowTaskCompletedScope, metrics.FailedWorkflowTasksCounter)
			handler.logger.Info("Failing the workflow task.",
				tag.Value(wtFailedCause.Message()),
				tag.WorkflowID(token.GetWorkflowId()),
				tag.WorkflowRunID(token.GetRunId()),
				tag.WorkflowNamespaceID(namespaceID.String()))
			msBuilder, err = handler.historyEngine.failWorkflowTask(weContext, scheduleID, startedID, wtFailedCause, request)
			if err != nil {
				return nil, err
			}
			hasUnhandledEvents = true
			newStateBuilder = nil
		}

		createNewWorkflowTask := msBuilder.IsWorkflowExecutionRunning() && (hasUnhandledEvents || request.GetForceCreateNewWorkflowTask() || activityNotStartedCancelled)
		var newWorkflowTaskScheduledID int64
		if createNewWorkflowTask {
			var newWorkflowTask *workflow.WorkflowTaskInfo
			var err error
			if workflowTaskHeartbeating && !workflowTaskHeartbeatTimeout {
				newWorkflowTask, err = msBuilder.AddWorkflowTaskScheduledEventAsHeartbeat(
					request.GetReturnNewWorkflowTask(),
					currentWorkflowTask.OriginalScheduledTime,
				)
			} else {
				newWorkflowTask, err = msBuilder.AddWorkflowTaskScheduledEvent(
					request.GetReturnNewWorkflowTask(),
				)
			}
			if err != nil {
				return nil, err
			}

			newWorkflowTaskScheduledID = newWorkflowTask.ScheduleID
			// skip transfer task for workflow task if request asking to return new workflow task
			if request.GetReturnNewWorkflowTask() {
				// start the new workflow task if request asked to do so
				// TODO: replace the poll request
				_, _, err := msBuilder.AddWorkflowTaskStartedEvent(
					newWorkflowTask.ScheduleID,
					"request-from-RespondWorkflowTaskCompleted",
					newWorkflowTask.TaskQueue,
					request.Identity,
				)
				if err != nil {
					return nil, err
				}
			}
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict then reload
		// the history and try the operation again.
		var updateErr error
		if newStateBuilder != nil {
			newWorkflowExecutionInfo := newStateBuilder.GetExecutionInfo()
			newWorkflowExecutionState := newStateBuilder.GetExecutionState()
			updateErr = weContext.UpdateWorkflowExecutionWithNewAsActive(
				handler.shard.GetTimeSource().Now(),
				workflow.NewContext(
					namespace.ID(newWorkflowExecutionInfo.NamespaceId),
					commonpb.WorkflowExecution{
						WorkflowId: newWorkflowExecutionInfo.WorkflowId,
						RunId:      newWorkflowExecutionState.RunId,
					},
					handler.shard,
					handler.logger,
				),
				newStateBuilder,
			)
		} else {
			updateErr = weContext.UpdateWorkflowExecutionAsActive(handler.shard.GetTimeSource().Now())
		}

		if updateErr != nil {
			if updateErr == consts.ErrConflict {
				handler.metricsClient.IncCounter(metrics.HistoryRespondWorkflowTaskCompletedScope, metrics.ConcurrencyUpdateFailureCounter)
				continue Update_History_Loop
			}

			// if updateErr resulted in TransactionSizeLimitError then fail workflow
			switch updateErr.(type) {
			case *persistence.TransactionSizeLimitError:
				// must reload mutable state because the first call to updateWorkflowExecutionWithContext or continueAsNewWorkflowExecution
				// clears mutable state if error is returned
				msBuilder, err = weContext.LoadWorkflowExecution()
				if err != nil {
					return nil, err
				}

				eventBatchFirstEventID := msBuilder.GetNextEventID()
				if err := workflow.TerminateWorkflow(
					msBuilder,
					eventBatchFirstEventID,
					common.FailureReasonTransactionSizeExceedsLimit,
					payloads.EncodeString(updateErr.Error()),
					consts.IdentityHistoryService,
				); err != nil {
					return nil, err
				}
				if err := weContext.UpdateWorkflowExecutionAsActive(
					handler.shard.GetTimeSource().Now(),
				); err != nil {
					return nil, err
				}
			}

			return nil, updateErr
		}

		handler.handleBufferedQueries(msBuilder, req.GetCompleteRequest().GetQueryResults(), createNewWorkflowTask, namespaceEntry, workflowTaskHeartbeating)

		if workflowTaskHeartbeatTimeout {
			// at this point, update is successful, but we still return an error to client so that the worker will give up this workflow
			return nil, serviceerror.NewNotFound("workflow task heartbeat timeout")
		}

		if wtFailedCause != nil {
			return nil, serviceerror.NewInvalidArgument(wtFailedCause.Message())
		}

		resp = &historyservice.RespondWorkflowTaskCompletedResponse{}
		if request.GetReturnNewWorkflowTask() && createNewWorkflowTask {
			workflowTask, _ := msBuilder.GetWorkflowTaskInfo(newWorkflowTaskScheduledID)
			resp.StartedResponse, err = handler.createRecordWorkflowTaskStartedResponse(msBuilder, workflowTask, request.GetIdentity())
			if err != nil {
				return nil, err
			}
			// sticky is always enabled when worker request for new workflow task from RespondWorkflowTaskCompleted
			resp.StartedResponse.StickyExecutionEnabled = true
		}

		return resp, nil
	}

	return nil, consts.ErrMaxAttemptsExceeded
}

func (handler *workflowTaskHandlerCallbacksImpl) createRecordWorkflowTaskStartedResponse(
	msBuilder workflow.MutableState,
	workflowTask *workflow.WorkflowTaskInfo,
	identity string,
) (*historyservice.RecordWorkflowTaskStartedResponse, error) {

	response := &historyservice.RecordWorkflowTaskStartedResponse{}
	response.WorkflowType = msBuilder.GetWorkflowType()
	executionInfo := msBuilder.GetExecutionInfo()
	if executionInfo.LastWorkflowTaskStartId != common.EmptyEventID {
		response.PreviousStartedEventId = executionInfo.LastWorkflowTaskStartId
	}

	// Starting workflowTask could result in different scheduleID if workflowTask was transient and new new events came in
	// before it was started.
	response.ScheduledEventId = workflowTask.ScheduleID
	response.StartedEventId = workflowTask.StartedID
	response.StickyExecutionEnabled = msBuilder.IsStickyTaskQueueEnabled()
	response.NextEventId = msBuilder.GetNextEventID()
	response.Attempt = workflowTask.Attempt
	response.WorkflowExecutionTaskQueue = &taskqueuepb.TaskQueue{
		Name: executionInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	response.ScheduledTime = workflowTask.ScheduledTime
	response.StartedTime = workflowTask.StartedTime

	if workflowTask.Attempt > 1 {
		// This workflowTask is retried from mutable state
		// Also return schedule and started which are not written to history yet
		scheduledEvent, startedEvent := msBuilder.CreateTransientWorkflowTaskEvents(workflowTask, identity)
		response.WorkflowTaskInfo = &historyspb.TransientWorkflowTaskInfo{}
		response.WorkflowTaskInfo.ScheduledEvent = scheduledEvent
		response.WorkflowTaskInfo.StartedEvent = startedEvent
	}
	currentBranchToken, err := msBuilder.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}
	response.BranchToken = currentBranchToken

	qr := msBuilder.GetQueryRegistry()
	buffered := qr.GetBufferedIDs()
	queries := make(map[string]*querypb.WorkflowQuery)
	for _, id := range buffered {
		input, err := qr.GetQueryInput(id)
		if err != nil {
			continue
		}
		queries[id] = input
	}
	response.Queries = queries
	return response, nil
}

func (handler *workflowTaskHandlerCallbacksImpl) handleBufferedQueries(msBuilder workflow.MutableState, queryResults map[string]*querypb.WorkflowQueryResult, createNewWorkflowTask bool, namespaceEntry *namespace.Namespace, workflowTaskHeartbeating bool) {
	queryRegistry := msBuilder.GetQueryRegistry()
	if !queryRegistry.HasBufferedQuery() {
		return
	}

	namespaceName := namespaceEntry.Name()
	workflowID := msBuilder.GetExecutionInfo().WorkflowId
	runID := msBuilder.GetExecutionState().GetRunId()

	scope := handler.metricsClient.Scope(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.NamespaceTag(namespaceEntry.Name().String()),
		metrics.CommandTypeTag("ConsistentQuery"))

	// if its a heartbeat workflow task it means local activities may still be running on the worker
	// which were started by an external event which happened before the query
	if workflowTaskHeartbeating {
		return
	}

	sizeLimitError := handler.config.BlobSizeLimitError(namespaceName.String())
	sizeLimitWarn := handler.config.BlobSizeLimitWarn(namespaceName.String())

	// Complete or fail all queries we have results for
	for id, result := range queryResults {
		if err := common.CheckEventBlobSizeLimit(
			result.GetAnswer().Size(),
			sizeLimitWarn,
			sizeLimitError,
			namespaceName.String(),
			workflowID,
			runID,
			scope,
			handler.throttledLogger,
			tag.BlobSizeViolationOperation("ConsistentQuery"),
		); err != nil {
			handler.logger.Info("failing query because query result size is too large",
				tag.WorkflowNamespace(namespaceName.String()),
				tag.WorkflowID(workflowID),
				tag.WorkflowRunID(runID),
				tag.QueryID(id),
				tag.Error(err))
			failedTerminationState := &workflow.QueryTerminationState{
				QueryTerminationType: workflow.QueryTerminationTypeFailed,
				Failure:              err,
			}
			if err := queryRegistry.SetTerminationState(id, failedTerminationState); err != nil {
				handler.logger.Error(
					"failed to set query termination state to failed",
					tag.WorkflowNamespace(namespaceName.String()),
					tag.WorkflowID(workflowID),
					tag.WorkflowRunID(runID),
					tag.QueryID(id),
					tag.Error(err))
				scope.IncCounter(metrics.QueryRegistryInvalidStateCount)
			}
		} else {
			completedTerminationState := &workflow.QueryTerminationState{
				QueryTerminationType: workflow.QueryTerminationTypeCompleted,
				QueryResult:          result,
			}
			if err := queryRegistry.SetTerminationState(id, completedTerminationState); err != nil {
				handler.logger.Error(
					"failed to set query termination state to completed",
					tag.WorkflowNamespace(namespaceName.String()),
					tag.WorkflowID(workflowID),
					tag.WorkflowRunID(runID),
					tag.QueryID(id),
					tag.Error(err))
				scope.IncCounter(metrics.QueryRegistryInvalidStateCount)
			}
		}
	}

	// If no workflow task was created then it means no buffered events came in during this workflow task's handling.
	// This means all unanswered buffered queries can be dispatched directly through matching at this point.
	if !createNewWorkflowTask {
		buffered := queryRegistry.GetBufferedIDs()
		for _, id := range buffered {
			unblockTerminationState := &workflow.QueryTerminationState{
				QueryTerminationType: workflow.QueryTerminationTypeUnblocked,
			}
			if err := queryRegistry.SetTerminationState(id, unblockTerminationState); err != nil {
				handler.logger.Error(
					"failed to set query termination state to unblocked",
					tag.WorkflowNamespace(namespaceName.String()),
					tag.WorkflowID(workflowID),
					tag.WorkflowRunID(runID),
					tag.QueryID(id),
					tag.Error(err))
				scope.IncCounter(metrics.QueryRegistryInvalidStateCount)
			}
		}
	}
}
