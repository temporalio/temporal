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
	"time"

	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	querypb "go.temporal.io/temporal-proto/query"
	"go.temporal.io/temporal-proto/serviceerror"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
	"go.temporal.io/temporal-proto/workflowservice"

	eventgenpb "github.com/temporalio/temporal/.gen/proto/event"
	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/headers"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
)

type (
	// decision business logic handler
	decisionHandler interface {
		handleDecisionTaskScheduled(context.Context, *historyservice.ScheduleDecisionTaskRequest) error
		handleDecisionTaskStarted(context.Context,
			*historyservice.RecordDecisionTaskStartedRequest) (*historyservice.RecordDecisionTaskStartedResponse, error)
		handleDecisionTaskFailed(context.Context,
			*historyservice.RespondDecisionTaskFailedRequest) error
		handleDecisionTaskCompleted(context.Context,
			*historyservice.RespondDecisionTaskCompletedRequest) (*historyservice.RespondDecisionTaskCompletedResponse, error)
		// TODO also include the handle of decision timeout here
	}

	decisionHandlerImpl struct {
		currentClusterName    string
		config                *Config
		shard                 ShardContext
		timeSource            clock.TimeSource
		historyEngine         *historyEngineImpl
		namespaceCache        cache.NamespaceCache
		historyCache          *historyCache
		txProcessor           transferQueueProcessor
		timerProcessor        timerQueueProcessor
		tokenSerializer       common.TaskTokenSerializer
		metricsClient         metrics.Client
		logger                log.Logger
		throttledLogger       log.Logger
		decisionAttrValidator *decisionAttrValidator
		versionChecker        headers.VersionChecker
	}
)

func newDecisionHandler(historyEngine *historyEngineImpl) *decisionHandlerImpl {
	return &decisionHandlerImpl{
		currentClusterName: historyEngine.currentClusterName,
		config:             historyEngine.config,
		shard:              historyEngine.shard,
		timeSource:         historyEngine.shard.GetTimeSource(),
		historyEngine:      historyEngine,
		namespaceCache:     historyEngine.shard.GetNamespaceCache(),
		historyCache:       historyEngine.historyCache,
		txProcessor:        historyEngine.txProcessor,
		timerProcessor:     historyEngine.timerProcessor,
		tokenSerializer:    historyEngine.tokenSerializer,
		metricsClient:      historyEngine.metricsClient,
		logger:             historyEngine.logger,
		throttledLogger:    historyEngine.throttledLogger,
		decisionAttrValidator: newDecisionAttrValidator(
			historyEngine.shard.GetNamespaceCache(),
			historyEngine.config,
			historyEngine.logger,
		),
		versionChecker: headers.NewVersionChecker(),
	}
}

func (handler *decisionHandlerImpl) handleDecisionTaskScheduled(
	ctx context.Context,
	req *historyservice.ScheduleDecisionTaskRequest,
) error {

	namespaceEntry, err := handler.historyEngine.getActiveNamespaceEntry(req.GetNamespaceId())
	if err != nil {
		return err
	}
	namespaceID := namespaceEntry.GetInfo().Id

	execution := executionpb.WorkflowExecution{
		WorkflowId: req.WorkflowExecution.WorkflowId,
		RunId:      req.WorkflowExecution.RunId,
	}

	return handler.historyEngine.updateWorkflowExecutionWithAction(ctx, primitives.UUIDString(namespaceID), execution,
		func(context workflowExecutionContext, mutableState mutableState) (*updateWorkflowAction, error) {
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			if mutableState.HasProcessedOrPendingDecision() {
				return &updateWorkflowAction{
					noop: true,
				}, nil
			}

			startEvent, err := mutableState.GetStartEvent()
			if err != nil {
				return nil, err
			}
			if err := mutableState.AddFirstDecisionTaskScheduled(
				startEvent,
			); err != nil {
				return nil, err
			}

			return &updateWorkflowAction{}, nil
		})
}

func (handler *decisionHandlerImpl) handleDecisionTaskStarted(
	ctx context.Context,
	req *historyservice.RecordDecisionTaskStartedRequest,
) (*historyservice.RecordDecisionTaskStartedResponse, error) {

	namespaceEntry, err := handler.historyEngine.getActiveNamespaceEntry(req.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.GetInfo().Id

	execution := executionpb.WorkflowExecution{
		WorkflowId: req.WorkflowExecution.WorkflowId,
		RunId:      req.WorkflowExecution.RunId,
	}

	scheduleID := req.GetScheduleId()
	requestID := req.GetRequestId()

	var resp *historyservice.RecordDecisionTaskStartedResponse
	err = handler.historyEngine.updateWorkflowExecutionWithAction(ctx, primitives.UUIDString(namespaceID), execution,
		func(context workflowExecutionContext, mutableState mutableState) (*updateWorkflowAction, error) {
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			decision, isRunning := mutableState.GetDecisionInfo(scheduleID)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduleID >= mutableState.GetNextEventID() {
				handler.metricsClient.IncCounter(metrics.HistoryRecordDecisionTaskStartedScope, metrics.StaleMutableStateCounter)
				// Reload workflow execution history
				// ErrStaleState will trigger updateWorkflowExecutionWithAction function to reload the mutable state
				return nil, ErrStaleState
			}

			// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
			// task is not outstanding than it is most probably a duplicate and complete the task.
			if !isRunning {
				// Looks like DecisionTask already completed as a result of another call.
				// It is OK to drop the task at this point.
				return nil, serviceerror.NewNotFound("Decision task not found.")
			}

			updateAction := &updateWorkflowAction{}

			if decision.StartedID != common.EmptyEventID {
				// If decision is started as part of the current request scope then return a positive response
				if decision.RequestID == requestID {
					resp, err = handler.createRecordDecisionTaskStartedResponse(primitives.UUIDString(namespaceID), mutableState, decision, req.PollRequest.GetIdentity())
					if err != nil {
						return nil, err
					}
					updateAction.noop = true
					return updateAction, nil
				}

				// Looks like DecisionTask already started as a result of another call.
				// It is OK to drop the task at this point.
				return nil, serviceerror.NewEventAlreadyStarted("Decision task already started.")
			}

			_, decision, err = mutableState.AddDecisionTaskStartedEvent(scheduleID, requestID, req.PollRequest)
			if err != nil {
				// Unable to add DecisionTaskStarted event to history
				return nil, serviceerror.NewInternal("Unable to add DecisionTaskStarted event to history.")
			}

			resp, err = handler.createRecordDecisionTaskStartedResponse(primitives.UUIDString(namespaceID), mutableState, decision, req.PollRequest.GetIdentity())
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

func (handler *decisionHandlerImpl) handleDecisionTaskFailed(
	ctx context.Context,
	req *historyservice.RespondDecisionTaskFailedRequest,
) (retError error) {

	namespaceEntry, err := handler.historyEngine.getActiveNamespaceEntry(req.GetNamespaceId())
	if err != nil {
		return err
	}
	namespaceID := namespaceEntry.GetInfo().Id

	request := req.FailedRequest
	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return ErrDeserializingToken
	}

	workflowExecution := executionpb.WorkflowExecution{
		WorkflowId: token.GetWorkflowId(),
		RunId:      primitives.UUIDString(token.GetRunId()),
	}

	return handler.historyEngine.updateWorkflowExecution(ctx, primitives.UUIDString(namespaceID), workflowExecution, true,
		func(context workflowExecutionContext, mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return ErrWorkflowCompleted
			}

			scheduleID := token.GetScheduleId()
			decision, isRunning := mutableState.GetDecisionInfo(scheduleID)
			if !isRunning || decision.Attempt != token.ScheduleAttempt || decision.StartedID == common.EmptyEventID {
				return serviceerror.NewNotFound("Decision task not found.")
			}

			_, err := mutableState.AddDecisionTaskFailedEvent(decision.ScheduleID, decision.StartedID, request.GetCause(), request.Details,
				request.GetIdentity(), "", request.GetBinaryChecksum(), "", "", 0)
			return err
		})
}

func (handler *decisionHandlerImpl) handleDecisionTaskCompleted(
	ctx context.Context,
	req *historyservice.RespondDecisionTaskCompletedRequest,
) (resp *historyservice.RespondDecisionTaskCompletedResponse, retError error) {

	namespaceEntry, err := handler.historyEngine.getActiveNamespaceEntry(req.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.GetInfo().Id

	request := req.CompleteRequest
	token, err0 := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return nil, ErrDeserializingToken
	}

	workflowExecution := executionpb.WorkflowExecution{
		WorkflowId: token.GetWorkflowId(),
		RunId:      primitives.UUIDString(token.GetRunId()),
	}

	clientHeaders := headers.GetValues(ctx, headers.ClientVersionHeaderName, headers.ClientFeatureVersionHeaderName, headers.ClientImplHeaderName)
	clientLibVersion := clientHeaders[0]
	clientFeatureVersion := clientHeaders[1]
	clientImpl := clientHeaders[2]

	weContext, release, err := handler.historyCache.getOrCreateWorkflowExecution(ctx, primitives.UUIDString(namespaceID), workflowExecution)
	if err != nil {
		return nil, err
	}
	defer func() { release(retError) }()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err := weContext.loadWorkflowExecution()
		if err != nil {
			return nil, err
		}
		if !msBuilder.IsWorkflowExecutionRunning() {
			return nil, ErrWorkflowCompleted
		}
		executionStats, err := weContext.loadExecutionStats()
		if err != nil {
			return nil, err
		}

		executionInfo := msBuilder.GetExecutionInfo()

		scheduleID := token.GetScheduleId()
		currentDecision, isRunning := msBuilder.GetDecisionInfo(scheduleID)

		// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
		// some extreme cassandra failure cases.
		if !isRunning && scheduleID >= msBuilder.GetNextEventID() {
			handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.StaleMutableStateCounter)
			// Reload workflow execution history
			weContext.clear()
			continue Update_History_Loop
		}

		if !msBuilder.IsWorkflowExecutionRunning() || !isRunning || currentDecision.Attempt != token.ScheduleAttempt ||
			currentDecision.StartedID == common.EmptyEventID {
			return nil, serviceerror.NewNotFound("Decision task not found.")
		}

		startedID := currentDecision.StartedID
		maxResetPoints := handler.config.MaxAutoResetPoints(namespaceEntry.GetInfo().Name)
		if msBuilder.GetExecutionInfo().AutoResetPoints != nil && maxResetPoints == len(msBuilder.GetExecutionInfo().AutoResetPoints.Points) {
			handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.AutoResetPointsLimitExceededCounter)
		}

		decisionHeartbeating := request.GetForceCreateNewDecisionTask() && len(request.Decisions) == 0
		var decisionHeartbeatTimeout bool
		var completedEvent *eventpb.HistoryEvent
		if decisionHeartbeating {
			namespace := namespaceEntry.GetInfo().Name
			timeout := handler.config.DecisionHeartbeatTimeout(namespace)
			if currentDecision.OriginalScheduledTimestamp > 0 && handler.timeSource.Now().After(time.Unix(0, currentDecision.OriginalScheduledTimestamp).Add(timeout)) {
				decisionHeartbeatTimeout = true
				scope := handler.metricsClient.Scope(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.NamespaceTag(namespace))
				scope.IncCounter(metrics.DecisionHeartbeatTimeoutCounter)
				completedEvent, err = msBuilder.AddDecisionTaskTimedOutEvent(currentDecision.ScheduleID, currentDecision.StartedID)
				if err != nil {
					return nil, serviceerror.NewInternal("Failed to add decision timeout event.")
				}
				msBuilder.ClearStickyness()
			} else {
				completedEvent, err = msBuilder.AddDecisionTaskCompletedEvent(scheduleID, startedID, request, maxResetPoints)
				if err != nil {
					return nil, serviceerror.NewInternal("Unable to add DecisionTaskCompleted event to history.")
				}
			}
		} else {
			completedEvent, err = msBuilder.AddDecisionTaskCompletedEvent(scheduleID, startedID, request, maxResetPoints)
			if err != nil {
				return nil, serviceerror.NewInternal("Unable to add DecisionTaskCompleted event to history.")
			}
		}

		var (
			failDecision                *failDecisionInfo
			activityNotStartedCancelled bool
			continueAsNewBuilder        mutableState

			hasUnhandledEvents bool
		)
		hasUnhandledEvents = msBuilder.HasBufferedEvents()

		if request.StickyAttributes == nil || request.StickyAttributes.WorkerTaskList == nil {
			handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.CompleteDecisionWithStickyDisabledCounter)
			executionInfo.StickyTaskList = ""
			executionInfo.StickyScheduleToStartTimeout = 0
		} else {
			handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.CompleteDecisionWithStickyEnabledCounter)
			executionInfo.StickyTaskList = request.StickyAttributes.WorkerTaskList.GetName()
			executionInfo.StickyScheduleToStartTimeout = request.StickyAttributes.GetScheduleToStartTimeoutSeconds()
		}
		executionInfo.ClientLibraryVersion = clientLibVersion
		executionInfo.ClientFeatureVersion = clientFeatureVersion
		executionInfo.ClientImpl = clientImpl

		binChecksum := request.GetBinaryChecksum()
		if _, ok := namespaceEntry.GetConfig().GetBadBinaries().GetBinaries()[binChecksum]; ok {
			failDecision = &failDecisionInfo{
				cause:   eventpb.DecisionTaskFailedCause_BadBinary,
				message: fmt.Sprintf("binary %v is already marked as bad deployment", binChecksum),
			}
		} else {

			namespace := namespaceEntry.GetInfo().Name
			workflowSizeChecker := newWorkflowSizeChecker(
				handler.config.BlobSizeLimitWarn(namespace),
				handler.config.BlobSizeLimitError(namespace),
				handler.config.HistorySizeLimitWarn(namespace),
				handler.config.HistorySizeLimitError(namespace),
				handler.config.HistoryCountLimitWarn(namespace),
				handler.config.HistoryCountLimitError(namespace),
				completedEvent.GetEventId(),
				msBuilder,
				executionStats,
				handler.metricsClient.Scope(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.NamespaceTag(namespace)),
				handler.throttledLogger,
			)

			decisionTaskHandler := newDecisionTaskHandler(
				request.GetIdentity(),
				completedEvent.GetEventId(),
				namespaceEntry,
				msBuilder,
				handler.decisionAttrValidator,
				workflowSizeChecker,
				handler.logger,
				handler.namespaceCache,
				handler.metricsClient,
				handler.config,
			)

			if err := decisionTaskHandler.handleDecisions(
				request.ExecutionContext,
				request.Decisions,
			); err != nil {
				return nil, err
			}

			// set the vars used by following logic
			// further refactor should also clean up the vars used below
			failDecision = decisionTaskHandler.failDecisionInfo

			// failMessage is not used by decisionTaskHandler
			activityNotStartedCancelled = decisionTaskHandler.activityNotStartedCancelled
			// continueAsNewTimerTasks is not used by decisionTaskHandler

			continueAsNewBuilder = decisionTaskHandler.continueAsNewBuilder

			hasUnhandledEvents = decisionTaskHandler.hasUnhandledEventsBeforeDecisions
		}

		if failDecision != nil {
			handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.FailedDecisionsCounter)
			handler.logger.Info("Failing the decision.", tag.WorkflowDecisionFailCause(int64(failDecision.cause)),
				tag.WorkflowID(token.GetWorkflowId()),
				tag.WorkflowRunIDBytes(token.GetRunId()),
				tag.WorkflowNamespaceIDBytes(namespaceID))
			msBuilder, err = handler.historyEngine.failDecision(weContext, scheduleID, startedID, failDecision.cause, []byte(failDecision.message), request)
			if err != nil {
				return nil, err
			}
			hasUnhandledEvents = true
			continueAsNewBuilder = nil
		}

		createNewDecisionTask := msBuilder.IsWorkflowExecutionRunning() && (hasUnhandledEvents || request.GetForceCreateNewDecisionTask() || activityNotStartedCancelled)
		var newDecisionTaskScheduledID int64
		if createNewDecisionTask {
			var newDecision *decisionInfo
			var err error
			if decisionHeartbeating && !decisionHeartbeatTimeout {
				newDecision, err = msBuilder.AddDecisionTaskScheduledEventAsHeartbeat(
					request.GetReturnNewDecisionTask(),
					currentDecision.OriginalScheduledTimestamp,
				)
			} else {
				newDecision, err = msBuilder.AddDecisionTaskScheduledEvent(
					request.GetReturnNewDecisionTask(),
				)
			}
			if err != nil {
				return nil, serviceerror.NewInternal("Failed to add decision scheduled event.")
			}

			newDecisionTaskScheduledID = newDecision.ScheduleID
			// skip transfer task for decision if request asking to return new decision task
			if request.GetReturnNewDecisionTask() {
				// start the new decision task if request asked to do so
				// TODO: replace the poll request
				_, _, err := msBuilder.AddDecisionTaskStartedEvent(newDecision.ScheduleID, "request-from-RespondDecisionTaskCompleted", &workflowservice.PollForDecisionTaskRequest{
					TaskList: &tasklistpb.TaskList{Name: newDecision.TaskList},
					Identity: request.Identity,
				})
				if err != nil {
					return nil, err
				}
			}
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict then reload
		// the history and try the operation again.
		var updateErr error
		if continueAsNewBuilder != nil {
			continueAsNewExecutionInfo := continueAsNewBuilder.GetExecutionInfo()
			updateErr = weContext.updateWorkflowExecutionWithNewAsActive(
				handler.shard.GetTimeSource().Now(),
				newWorkflowExecutionContext(
					continueAsNewExecutionInfo.NamespaceID,
					executionpb.WorkflowExecution{
						WorkflowId: continueAsNewExecutionInfo.WorkflowID,
						RunId:      continueAsNewExecutionInfo.RunID,
					},
					handler.shard,
					handler.shard.GetExecutionManager(),
					handler.logger,
				),
				continueAsNewBuilder,
			)
		} else {
			updateErr = weContext.updateWorkflowExecutionAsActive(handler.shard.GetTimeSource().Now())
		}

		if updateErr != nil {
			if updateErr == ErrConflict {
				handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.ConcurrencyUpdateFailureCounter)
				continue Update_History_Loop
			}

			// if updateErr resulted in TransactionSizeLimitError then fail workflow
			switch updateErr.(type) {
			case *persistence.TransactionSizeLimitError:
				// must reload mutable state because the first call to updateWorkflowExecutionWithContext or continueAsNewWorkflowExecution
				// clears mutable state if error is returned
				msBuilder, err = weContext.loadWorkflowExecution()
				if err != nil {
					return nil, err
				}

				eventBatchFirstEventID := msBuilder.GetNextEventID()
				if err := terminateWorkflow(
					msBuilder,
					eventBatchFirstEventID,
					common.FailureReasonTransactionSizeExceedsLimit,
					[]byte(updateErr.Error()),
					identityHistoryService,
				); err != nil {
					return nil, err
				}
				if err := weContext.updateWorkflowExecutionAsActive(
					handler.shard.GetTimeSource().Now(),
				); err != nil {
					return nil, err
				}
			}

			return nil, updateErr
		}

		handler.handleBufferedQueries(msBuilder, req.GetCompleteRequest().GetQueryResults(), createNewDecisionTask, namespaceEntry, decisionHeartbeating)

		if decisionHeartbeatTimeout {
			// at this point, update is successful, but we still return an error to client so that the worker will give up this workflow
			return nil, serviceerror.NewNotFound(fmt.Sprintf("decision heartbeat timeout"))
		}

		resp = &historyservice.RespondDecisionTaskCompletedResponse{}
		if request.GetReturnNewDecisionTask() && createNewDecisionTask {
			decision, _ := msBuilder.GetDecisionInfo(newDecisionTaskScheduledID)
			resp.StartedResponse, err = handler.createRecordDecisionTaskStartedResponse(primitives.UUIDString(namespaceID), msBuilder, decision, request.GetIdentity())
			if err != nil {
				return nil, err
			}
			// sticky is always enabled when worker request for new decision task from RespondDecisionTaskCompleted
			resp.StartedResponse.StickyExecutionEnabled = true
		}

		return resp, nil
	}

	return nil, ErrMaxAttemptsExceeded
}

func (handler *decisionHandlerImpl) createRecordDecisionTaskStartedResponse(
	namespaceID string,
	msBuilder mutableState,
	decision *decisionInfo,
	identity string,
) (*historyservice.RecordDecisionTaskStartedResponse, error) {

	response := &historyservice.RecordDecisionTaskStartedResponse{}
	response.WorkflowType = msBuilder.GetWorkflowType()
	executionInfo := msBuilder.GetExecutionInfo()
	if executionInfo.LastProcessedEvent != common.EmptyEventID {
		response.PreviousStartedEventId = executionInfo.LastProcessedEvent
	}

	// Starting decision could result in different scheduleID if decision was transient and new new events came in
	// before it was started.
	response.ScheduledEventId = decision.ScheduleID
	response.StartedEventId = decision.StartedID
	response.StickyExecutionEnabled = msBuilder.IsStickyTaskListEnabled()
	response.NextEventId = msBuilder.GetNextEventID()
	response.Attempt = decision.Attempt
	response.WorkflowExecutionTaskList = &tasklistpb.TaskList{
		Name: executionInfo.TaskList,
		Kind: tasklistpb.TaskListKind_Normal,
	}
	response.ScheduledTimestamp = decision.ScheduledTimestamp
	response.StartedTimestamp = decision.StartedTimestamp

	if decision.Attempt > 0 {
		// This decision is retried from mutable state
		// Also return schedule and started which are not written to history yet
		scheduledEvent, startedEvent := msBuilder.CreateTransientDecisionEvents(decision, identity)
		response.DecisionInfo = &eventgenpb.TransientDecisionInfo{}
		response.DecisionInfo.ScheduledEvent = scheduledEvent
		response.DecisionInfo.StartedEvent = startedEvent
	}
	currentBranchToken, err := msBuilder.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}
	response.BranchToken = currentBranchToken

	qr := msBuilder.GetQueryRegistry()
	buffered := qr.getBufferedIDs()
	queries := make(map[string]*querypb.WorkflowQuery)
	for _, id := range buffered {
		input, err := qr.getQueryInput(id)
		if err != nil {
			continue
		}
		queries[id] = input
	}
	response.Queries = queries
	return response, nil
}

func (handler *decisionHandlerImpl) handleBufferedQueries(msBuilder mutableState, queryResults map[string]*querypb.WorkflowQueryResult, createNewDecisionTask bool, namespaceEntry *cache.NamespaceCacheEntry, decisionHeartbeating bool) {
	queryRegistry := msBuilder.GetQueryRegistry()
	if !queryRegistry.hasBufferedQuery() {
		return
	}

	namespaceID := namespaceEntry.GetInfo().Id
	namespace := namespaceEntry.GetInfo().Name
	workflowID := msBuilder.GetExecutionInfo().WorkflowID
	runID := msBuilder.GetExecutionInfo().RunID

	scope := handler.metricsClient.Scope(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.NamespaceTag(namespaceEntry.GetInfo().Name),
		metrics.DecisionTypeTag("ConsistentQuery"))

	// if its a heartbeat decision it means local activities may still be running on the worker
	// which were started by an external event which happened before the query
	if decisionHeartbeating {
		return
	}

	sizeLimitError := handler.config.BlobSizeLimitError(namespace)
	sizeLimitWarn := handler.config.BlobSizeLimitWarn(namespace)

	// Complete or fail all queries we have results for
	for id, result := range queryResults {
		if err := common.CheckEventBlobSizeLimit(
			len(result.GetAnswer()),
			sizeLimitWarn,
			sizeLimitError,
			primitives.UUIDString(namespaceID),
			workflowID,
			runID,
			scope,
			handler.throttledLogger,
			tag.BlobSizeViolationOperation("ConsistentQuery"),
		); err != nil {
			handler.logger.Info("failing query because query result size is too large",
				tag.WorkflowNamespace(namespace),
				tag.WorkflowID(workflowID),
				tag.WorkflowRunID(runID),
				tag.QueryID(id),
				tag.Error(err))
			failedTerminationState := &queryTerminationState{
				queryTerminationType: queryTerminationTypeFailed,
				failure:              err,
			}
			if err := queryRegistry.setTerminationState(id, failedTerminationState); err != nil {
				handler.logger.Error(
					"failed to set query termination state to failed",
					tag.WorkflowNamespace(namespace),
					tag.WorkflowID(workflowID),
					tag.WorkflowRunID(runID),
					tag.QueryID(id),
					tag.Error(err))
				scope.IncCounter(metrics.QueryRegistryInvalidStateCount)
			}
		} else {
			completedTerminationState := &queryTerminationState{
				queryTerminationType: queryTerminationTypeCompleted,
				queryResult:          result,
			}
			if err := queryRegistry.setTerminationState(id, completedTerminationState); err != nil {
				handler.logger.Error(
					"failed to set query termination state to completed",
					tag.WorkflowNamespace(namespace),
					tag.WorkflowID(workflowID),
					tag.WorkflowRunID(runID),
					tag.QueryID(id),
					tag.Error(err))
				scope.IncCounter(metrics.QueryRegistryInvalidStateCount)
			}
		}
	}

	// If no decision task was created then it means no buffered events came in during this decision task's handling.
	// This means all unanswered buffered queries can be dispatched directly through matching at this point.
	if !createNewDecisionTask {
		buffered := queryRegistry.getBufferedIDs()
		for _, id := range buffered {
			unblockTerminationState := &queryTerminationState{
				queryTerminationType: queryTerminationTypeUnblocked,
			}
			if err := queryRegistry.setTerminationState(id, unblockTerminationState); err != nil {
				handler.logger.Error(
					"failed to set query termination state to unblocked",
					tag.WorkflowNamespace(namespace),
					tag.WorkflowID(workflowID),
					tag.WorkflowRunID(runID),
					tag.QueryID(id),
					tag.Error(err))
				scope.IncCounter(metrics.QueryRegistryInvalidStateCount)
			}
		}
	}
}
