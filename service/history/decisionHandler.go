// Copyright (c) 2017 Uber Technologies, Inc.
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
	ctx "context"
	"fmt"
	"time"

	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"go.uber.org/yarpc"
)

type (
	// decision business logic handler
	decisionHandler interface {
		handleDecisionTaskScheduled(ctx.Context, *h.ScheduleDecisionTaskRequest) error
		handleDecisionTaskStarted(ctx.Context,
			*h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error)
		handleDecisionTaskFailed(ctx.Context,
			*h.RespondDecisionTaskFailedRequest) error
		handleDecisionTaskCompleted(ctx.Context,
			*h.RespondDecisionTaskCompletedRequest) (*h.RespondDecisionTaskCompletedResponse, error)
		// TODO also include the handle of decision timeout here
	}

	decisionHandlerImpl struct {
		currentClusterName    string
		config                *Config
		shard                 ShardContext
		timeSource            clock.TimeSource
		historyEngine         *historyEngineImpl
		domainCache           cache.DomainCache
		historyCache          *historyCache
		txProcessor           transferQueueProcessor
		timerProcessor        timerQueueProcessor
		tokenSerializer       common.TaskTokenSerializer
		metricsClient         metrics.Client
		logger                log.Logger
		throttledLogger       log.Logger
		decisionAttrValidator *decisionAttrValidator
	}
)

func newDecisionHandler(historyEngine *historyEngineImpl) *decisionHandlerImpl {
	return &decisionHandlerImpl{
		currentClusterName: historyEngine.currentClusterName,
		config:             historyEngine.config,
		shard:              historyEngine.shard,
		timeSource:         historyEngine.shard.GetTimeSource(),
		historyEngine:      historyEngine,
		domainCache:        historyEngine.shard.GetDomainCache(),
		historyCache:       historyEngine.historyCache,
		txProcessor:        historyEngine.txProcessor,
		timerProcessor:     historyEngine.timerProcessor,
		tokenSerializer:    historyEngine.tokenSerializer,
		metricsClient:      historyEngine.metricsClient,
		logger:             historyEngine.logger,
		throttledLogger:    historyEngine.throttledLogger,
		decisionAttrValidator: newDecisionAttrValidator(
			historyEngine.shard.GetDomainCache(),
			historyEngine.config,
			historyEngine.logger,
		),
	}
}

func (handler *decisionHandlerImpl) handleDecisionTaskScheduled(
	ctx ctx.Context,
	req *h.ScheduleDecisionTaskRequest,
) error {

	domainEntry, err := handler.historyEngine.getActiveDomainEntry(req.DomainUUID)
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID

	execution := workflow.WorkflowExecution{
		WorkflowId: req.WorkflowExecution.WorkflowId,
		RunId:      req.WorkflowExecution.RunId,
	}

	return handler.historyEngine.updateWorkflowExecutionWithAction(ctx, domainID, execution,
		func(msBuilder mutableState, tBuilder *timerBuilder) (*updateWorkflowAction, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			if msBuilder.HasProcessedOrPendingDecision() {
				return &updateWorkflowAction{
					noop: true,
				}, nil
			}

			startEvent, err := msBuilder.GetStartEvent()
			if err != nil {
				return nil, err
			}
			if err := msBuilder.AddFirstDecisionTaskScheduled(
				startEvent,
			); err != nil {
				return nil, err
			}

			return &updateWorkflowAction{}, nil
		})
}

func (handler *decisionHandlerImpl) handleDecisionTaskStarted(
	ctx ctx.Context,
	req *h.RecordDecisionTaskStartedRequest,
) (*h.RecordDecisionTaskStartedResponse, error) {

	domainEntry, err := handler.historyEngine.getActiveDomainEntry(req.DomainUUID)
	if err != nil {
		return nil, err
	}
	domainID := domainEntry.GetInfo().ID

	execution := workflow.WorkflowExecution{
		WorkflowId: req.WorkflowExecution.WorkflowId,
		RunId:      req.WorkflowExecution.RunId,
	}

	scheduleID := req.GetScheduleId()
	requestID := req.GetRequestId()

	var resp *h.RecordDecisionTaskStartedResponse
	err = handler.historyEngine.updateWorkflowExecutionWithAction(ctx, domainID, execution,
		func(msBuilder mutableState, tBuilder *timerBuilder) (*updateWorkflowAction, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			decision, isRunning := msBuilder.GetDecisionInfo(scheduleID)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduleID >= msBuilder.GetNextEventID() {
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
				return nil, &workflow.EntityNotExistsError{Message: "Decision task not found."}
			}

			updateAction := &updateWorkflowAction{}

			if decision.StartedID != common.EmptyEventID {
				// If decision is started as part of the current request scope then return a positive response
				if decision.RequestID == requestID {
					resp, err = handler.createRecordDecisionTaskStartedResponse(domainID, msBuilder, decision, req.PollRequest.GetIdentity())
					if err != nil {
						return nil, err
					}
					updateAction.noop = true
					return updateAction, nil
				}

				// Looks like DecisionTask already started as a result of another call.
				// It is OK to drop the task at this point.
				return nil, &h.EventAlreadyStartedError{Message: "Decision task already started."}
			}

			_, decision, err = msBuilder.AddDecisionTaskStartedEvent(scheduleID, requestID, req.PollRequest)
			if err != nil {
				// Unable to add DecisionTaskStarted event to history
				return nil, &workflow.InternalServiceError{Message: "Unable to add DecisionTaskStarted event to history."}
			}

			resp, err = handler.createRecordDecisionTaskStartedResponse(domainID, msBuilder, decision, req.PollRequest.GetIdentity())
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
	ctx ctx.Context,
	req *h.RespondDecisionTaskFailedRequest,
) (retError error) {

	domainEntry, err := handler.historyEngine.getActiveDomainEntry(req.DomainUUID)
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID

	request := req.FailedRequest
	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return ErrDeserializingToken
	}

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(token.WorkflowID),
		RunId:      common.StringPtr(token.RunID),
	}

	return handler.historyEngine.updateWorkflowExecution(ctx, domainID, workflowExecution, true,
		func(msBuilder mutableState, tBuilder *timerBuilder) error {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return ErrWorkflowCompleted
			}

			scheduleID := token.ScheduleID
			decision, isRunning := msBuilder.GetDecisionInfo(scheduleID)
			if !isRunning || decision.Attempt != token.ScheduleAttempt || decision.StartedID == common.EmptyEventID {
				return &workflow.EntityNotExistsError{Message: "Decision task not found."}
			}

			_, err := msBuilder.AddDecisionTaskFailedEvent(decision.ScheduleID, decision.StartedID, request.GetCause(), request.Details,
				request.GetIdentity(), "", "", "", 0)
			return err
		})
}

func (handler *decisionHandlerImpl) handleDecisionTaskCompleted(
	ctx ctx.Context,
	req *h.RespondDecisionTaskCompletedRequest,
) (resp *h.RespondDecisionTaskCompletedResponse, retError error) {

	domainEntry, err := handler.historyEngine.getActiveDomainEntry(req.DomainUUID)
	if err != nil {
		return nil, err
	}
	domainID := domainEntry.GetInfo().ID

	request := req.CompleteRequest
	token, err0 := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return nil, ErrDeserializingToken
	}

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(token.WorkflowID),
		RunId:      common.StringPtr(token.RunID),
	}

	call := yarpc.CallFromContext(ctx)
	clientLibVersion := call.Header(common.LibraryVersionHeaderName)
	clientFeatureVersion := call.Header(common.FeatureVersionHeaderName)
	clientImpl := call.Header(common.ClientImplHeaderName)

	context, release, err := handler.historyCache.getOrCreateWorkflowExecution(ctx, domainID, workflowExecution)
	if err != nil {
		return nil, err
	}
	defer func() { release(retError) }()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err := context.loadWorkflowExecution()
		if err != nil {
			return nil, err
		}
		if !msBuilder.IsWorkflowExecutionRunning() {
			return nil, ErrWorkflowCompleted
		}
		executionStats, err := context.loadExecutionStats()
		if err != nil {
			return nil, err
		}

		executionInfo := msBuilder.GetExecutionInfo()
		timerBuilderProvider := func() *timerBuilder {
			return handler.historyEngine.getTimerBuilder(context.getExecution())
		}

		scheduleID := token.ScheduleID
		currentDecision, isRunning := msBuilder.GetDecisionInfo(scheduleID)

		// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
		// some extreme cassandra failure cases.
		if !isRunning && scheduleID >= msBuilder.GetNextEventID() {
			handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.StaleMutableStateCounter)
			// Reload workflow execution history
			context.clear()
			continue Update_History_Loop
		}

		if !msBuilder.IsWorkflowExecutionRunning() || !isRunning || currentDecision.Attempt != token.ScheduleAttempt ||
			currentDecision.StartedID == common.EmptyEventID {
			return nil, &workflow.EntityNotExistsError{Message: "Decision task not found."}
		}

		startedID := currentDecision.StartedID
		maxResetPoints := handler.config.MaxAutoResetPoints(domainEntry.GetInfo().Name)
		if msBuilder.GetExecutionInfo().AutoResetPoints != nil && maxResetPoints == len(msBuilder.GetExecutionInfo().AutoResetPoints.Points) {
			handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.AutoResetPointsLimitExceededCounter)
		}

		decisionHeartbeating := request.GetForceCreateNewDecisionTask() && len(request.Decisions) == 0
		var decisionHeartbeatTimeout bool
		var completedEvent *workflow.HistoryEvent
		if decisionHeartbeating {
			domainName := domainEntry.GetInfo().Name
			timeout := handler.config.DecisionHeartbeatTimeout(domainName)
			if currentDecision.OriginalScheduledTimestamp > 0 && handler.timeSource.Now().After(time.Unix(0, currentDecision.OriginalScheduledTimestamp).Add(timeout)) {
				decisionHeartbeatTimeout = true
				scope := handler.metricsClient.Scope(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.DomainTag(domainName))
				scope.IncCounter(metrics.DecisionHeartbeatTimeoutCounter)
				completedEvent, err = msBuilder.AddDecisionTaskTimedOutEvent(currentDecision.ScheduleID, currentDecision.StartedID)
				if err != nil {
					return nil, &workflow.InternalServiceError{Message: "Failed to add decision timeout event."}
				}
				msBuilder.ClearStickyness()
			} else {
				completedEvent, err = msBuilder.AddDecisionTaskCompletedEvent(scheduleID, startedID, request, maxResetPoints)
				if err != nil {
					return nil, &workflow.InternalServiceError{Message: "Unable to add DecisionTaskCompleted event to history."}
				}
			}
		} else {
			completedEvent, err = msBuilder.AddDecisionTaskCompletedEvent(scheduleID, startedID, request, maxResetPoints)
			if err != nil {
				return nil, &workflow.InternalServiceError{Message: "Unable to add DecisionTaskCompleted event to history."}
			}
		}

		var (
			failDecision                bool
			failCause                   workflow.DecisionTaskFailedCause
			failMessage                 string
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
		if _, ok := domainEntry.GetConfig().BadBinaries.Binaries[binChecksum]; ok {
			failDecision = true
			failCause = workflow.DecisionTaskFailedCauseBadBinary
			failMessage = fmt.Sprintf("binary %v is already marked as bad deployment", binChecksum)
		} else {

			domainName := domainEntry.GetInfo().Name
			workflowSizeChecker := newWorkflowSizeChecker(
				handler.config.BlobSizeLimitWarn(domainName),
				handler.config.BlobSizeLimitError(domainName),
				handler.config.HistorySizeLimitWarn(domainName),
				handler.config.HistorySizeLimitError(domainName),
				handler.config.HistoryCountLimitWarn(domainName),
				handler.config.HistoryCountLimitError(domainName),
				completedEvent.GetEventId(),
				msBuilder,
				executionStats,
				handler.metricsClient,
				handler.throttledLogger,
			)

			decisionTaskHandler := newDecisionTaskHandler(
				request.GetIdentity(),
				completedEvent.GetEventId(),
				domainEntry,
				msBuilder,
				handler.decisionAttrValidator,
				workflowSizeChecker,
				handler.logger,
				timerBuilderProvider,
				handler.domainCache,
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
			failDecision = decisionTaskHandler.failDecision
			if failDecision {
				failCause = *decisionTaskHandler.failDecisionCause
				failMessage = *decisionTaskHandler.failMessage
			}

			// failMessage is not used by decisionTaskHandler
			activityNotStartedCancelled = decisionTaskHandler.activityNotStartedCancelled
			// continueAsNewTimerTasks is not used by decisionTaskHandler

			continueAsNewBuilder = decisionTaskHandler.continueAsNewBuilder

			hasUnhandledEvents = decisionTaskHandler.hasUnhandledEventsBeforeDecisions
		}

		if failDecision {
			handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.FailedDecisionsCounter)
			handler.logger.Info("Failing the decision.", tag.WorkflowDecisionFailCause(int64(failCause)),
				tag.WorkflowID(token.WorkflowID),
				tag.WorkflowRunID(token.RunID),
				tag.WorkflowDomainID(domainID))
			msBuilder, err = handler.historyEngine.failDecision(context, scheduleID, startedID, failCause, []byte(failMessage), request)
			if err != nil {
				return nil, err
			}
			hasUnhandledEvents = true
			continueAsNewBuilder = nil
		}

		// Schedule another decision task if new events came in during this decision or if request forced to
		createNewDecisionTask := msBuilder.IsWorkflowExecutionRunning() &&
			(hasUnhandledEvents || request.GetForceCreateNewDecisionTask() || activityNotStartedCancelled)
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
				return nil, &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
			}

			newDecisionTaskScheduledID = newDecision.ScheduleID
			// skip transfer task for decision if request asking to return new decision task
			if request.GetReturnNewDecisionTask() {
				// start the new decision task if request asked to do so
				// TODO: replace the poll request
				_, _, err := msBuilder.AddDecisionTaskStartedEvent(newDecision.ScheduleID, "request-from-RespondDecisionTaskCompleted", &workflow.PollForDecisionTaskRequest{
					TaskList: &workflow.TaskList{Name: common.StringPtr(newDecision.TaskList)},
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
			updateErr = context.updateWorkflowExecutionWithNewAsActive(
				handler.shard.GetTimeSource().Now(),
				newWorkflowExecutionContext(
					continueAsNewExecutionInfo.DomainID,
					workflow.WorkflowExecution{
						WorkflowId: common.StringPtr(continueAsNewExecutionInfo.WorkflowID),
						RunId:      common.StringPtr(continueAsNewExecutionInfo.RunID),
					},
					handler.shard,
					handler.shard.GetExecutionManager(),
					handler.logger,
				),
				continueAsNewBuilder,
			)
		} else {
			updateErr = context.updateWorkflowExecutionAsActive(handler.shard.GetTimeSource().Now())
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
				msBuilder, err = context.loadWorkflowExecution()
				if err != nil {
					return nil, err
				}

				if _, err := msBuilder.AddWorkflowExecutionTerminatedEvent(
					common.FailureReasonTransactionSizeExceedsLimit,
					[]byte(updateErr.Error()),
					"cadence-history-server",
				); err != nil {
					return nil, err
				}
				if err := context.updateWorkflowExecutionAsActive(
					handler.shard.GetTimeSource().Now(),
				); err != nil {
					return nil, err
				}
			}

			return nil, updateErr
		}

		if decisionHeartbeatTimeout {
			// at this point, update is successful, but we still return an error to client so that the worker will give up this workflow
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("decision heartbeat timeout"),
			}
		}

		resp = &h.RespondDecisionTaskCompletedResponse{}
		if request.GetReturnNewDecisionTask() && createNewDecisionTask {
			decision, _ := msBuilder.GetDecisionInfo(newDecisionTaskScheduledID)
			resp.StartedResponse, err = handler.createRecordDecisionTaskStartedResponse(domainID, msBuilder, decision, request.GetIdentity())
			if err != nil {
				return nil, err
			}
			// sticky is always enabled when worker request for new decision task from RespondDecisionTaskCompleted
			resp.StartedResponse.StickyExecutionEnabled = common.BoolPtr(true)
		}

		return resp, nil
	}

	return nil, ErrMaxAttemptsExceeded
}

func (handler *decisionHandlerImpl) createRecordDecisionTaskStartedResponse(
	domainID string,
	msBuilder mutableState,
	decision *decisionInfo,
	identity string,
) (*h.RecordDecisionTaskStartedResponse, error) {

	response := &h.RecordDecisionTaskStartedResponse{}
	response.WorkflowType = msBuilder.GetWorkflowType()
	executionInfo := msBuilder.GetExecutionInfo()
	if executionInfo.LastProcessedEvent != common.EmptyEventID {
		response.PreviousStartedEventId = common.Int64Ptr(executionInfo.LastProcessedEvent)
	}

	// Starting decision could result in different scheduleID if decision was transient and new new events came in
	// before it was started.
	response.ScheduledEventId = common.Int64Ptr(decision.ScheduleID)
	response.StartedEventId = common.Int64Ptr(decision.StartedID)
	response.StickyExecutionEnabled = common.BoolPtr(msBuilder.IsStickyTaskListEnabled())
	response.NextEventId = common.Int64Ptr(msBuilder.GetNextEventID())
	response.Attempt = common.Int64Ptr(decision.Attempt)
	response.WorkflowExecutionTaskList = common.TaskListPtr(workflow.TaskList{
		Name: &executionInfo.TaskList,
		Kind: common.TaskListKindPtr(workflow.TaskListKindNormal),
	})
	response.ScheduledTimestamp = common.Int64Ptr(decision.ScheduledTimestamp)
	response.StartedTimestamp = common.Int64Ptr(decision.StartedTimestamp)

	if decision.Attempt > 0 {
		// This decision is retried from mutable state
		// Also return schedule and started which are not written to history yet
		scheduledEvent, startedEvent := msBuilder.CreateTransientDecisionEvents(decision, identity)
		response.DecisionInfo = &workflow.TransientDecisionInfo{}
		response.DecisionInfo.ScheduledEvent = scheduledEvent
		response.DecisionInfo.StartedEvent = startedEvent
	}
	currentBranchToken, err := msBuilder.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}
	response.BranchToken = currentBranchToken

	return response, nil
}
