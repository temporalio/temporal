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
		currentClusterName string
		config             *Config
		shard              ShardContext
		historyEngine      *historyEngineImpl
		domainCache        cache.DomainCache
		historyCache       *historyCache
		txProcessor        transferQueueProcessor
		timerProcessor     timerQueueProcessor
		tokenSerializer    common.TaskTokenSerializer
		metricsClient      metrics.Client
		logger             log.Logger
		throttledLogger    log.Logger
	}
)

func newDecisionHandler(historyEngine *historyEngineImpl) *decisionHandlerImpl {
	return &decisionHandlerImpl{
		currentClusterName: historyEngine.currentClusterName,
		config:             historyEngine.config,
		shard:              historyEngine.shard,
		historyEngine:      historyEngine,
		domainCache:        historyEngine.shard.GetDomainCache(),
		historyCache:       historyEngine.historyCache,
		txProcessor:        historyEngine.txProcessor,
		timerProcessor:     historyEngine.timerProcessor,
		tokenSerializer:    historyEngine.tokenSerializer,
		metricsClient:      historyEngine.metricsClient,
		logger:             historyEngine.logger,
		throttledLogger:    historyEngine.throttledLogger,
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

			postActions := &updateWorkflowAction{
				createDecision: true,
				transferTasks:  []persistence.Task{&persistence.RecordWorkflowStartedTask{}},
			}

			startEvent, found := msBuilder.GetStartEvent()
			if !found {
				return nil, &workflow.InternalServiceError{Message: "Failed to load start event."}
			}
			executionTimestamp := getWorkflowExecutionTimestamp(msBuilder, startEvent)
			if req.GetIsFirstDecision() && executionTimestamp.After(time.Now()) {
				postActions.timerTasks = append(postActions.timerTasks, &persistence.WorkflowBackoffTimerTask{
					VisibilityTimestamp: executionTimestamp,
					TimeoutType:         persistence.WorkflowBackoffTimeoutTypeCron,
				})
				postActions.createDecision = false
			}

			return postActions, nil
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

			di, isRunning := msBuilder.GetPendingDecision(scheduleID)

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

			updateAction := &updateWorkflowAction{
				noop:           false,
				deleteWorkflow: false,
				createDecision: false,
				timerTasks:     nil,
				transferTasks:  nil,
			}

			if di.StartedID != common.EmptyEventID {
				// If decision is started as part of the current request scope then return a positive response
				if di.RequestID == requestID {
					resp = handler.createRecordDecisionTaskStartedResponse(domainID, msBuilder, di, req.PollRequest.GetIdentity())
					updateAction.noop = true
					return updateAction, nil
				}

				// Looks like DecisionTask already started as a result of another call.
				// It is OK to drop the task at this point.
				return nil, &h.EventAlreadyStartedError{Message: "Decision task already started."}
			}

			_, di, err = msBuilder.AddDecisionTaskStartedEvent(scheduleID, requestID, req.PollRequest)
			if err != nil {
				// Unable to add DecisionTaskStarted event to history
				return nil, &workflow.InternalServiceError{Message: "Unable to add DecisionTaskStarted event to history."}
			}

			resp = handler.createRecordDecisionTaskStartedResponse(domainID, msBuilder, di, req.PollRequest.GetIdentity())
			updateAction.timerTasks = []persistence.Task{tBuilder.AddStartToCloseDecisionTimoutTask(
				di.ScheduleID,
				di.Attempt,
				di.DecisionTimeout,
			)}
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

	return handler.historyEngine.updateWorkflowExecution(ctx, domainID, workflowExecution, false, true,
		func(msBuilder mutableState, tBuilder *timerBuilder) ([]persistence.Task, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			scheduleID := token.ScheduleID
			di, isRunning := msBuilder.GetPendingDecision(scheduleID)
			if !isRunning || di.Attempt != token.ScheduleAttempt || di.StartedID == common.EmptyEventID {
				return nil, &workflow.EntityNotExistsError{Message: "Decision task not found."}
			}

			_, err := msBuilder.AddDecisionTaskFailedEvent(di.ScheduleID, di.StartedID, request.GetCause(), request.Details,
				request.GetIdentity(), "", "", "", 0)
			return nil, err
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

	var eventStoreVersion int32
	if handler.config.EnableEventsV2(domainEntry.GetInfo().Name) {
		eventStoreVersion = persistence.EventStoreVersionV2
	}
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

		executionInfo := msBuilder.GetExecutionInfo()
		timerBuilderProvider := func() *timerBuilder {
			return handler.historyEngine.getTimerBuilder(context.getExecution())
		}

		scheduleID := token.ScheduleID
		di, isRunning := msBuilder.GetPendingDecision(scheduleID)

		// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
		// some extreme cassandra failure cases.
		if !isRunning && scheduleID >= msBuilder.GetNextEventID() {
			handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.StaleMutableStateCounter)
			// Reload workflow execution history
			context.clear()
			continue Update_History_Loop
		}

		if !msBuilder.IsWorkflowExecutionRunning() || !isRunning || di.Attempt != token.ScheduleAttempt ||
			di.StartedID == common.EmptyEventID {
			return nil, &workflow.EntityNotExistsError{Message: "Decision task not found."}
		}

		startedID := di.StartedID
		maxResetPoints := handler.config.MaxAutoResetPoints(domainEntry.GetInfo().Name)
		if msBuilder.GetExecutionInfo().AutoResetPoints != nil && maxResetPoints == len(msBuilder.GetExecutionInfo().AutoResetPoints.Points) {
			handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.AutoResetPointsLimitExceededCounter)
			handler.throttledLogger.Warn("the number of auto-reset points is exceeding the limit, will do rotating.",
				tag.WorkflowDomainName(domainEntry.GetInfo().Name),
				tag.WorkflowDomainID(domainEntry.GetInfo().ID),
				tag.WorkflowID(workflowExecution.GetWorkflowId()),
				tag.WorkflowRunID(workflowExecution.GetRunId()),
				tag.Number(int64(maxResetPoints)))
		}
		completedEvent, err := msBuilder.AddDecisionTaskCompletedEvent(scheduleID, startedID, request, maxResetPoints)
		if err != nil {
			return nil, &workflow.InternalServiceError{Message: "Unable to add DecisionTaskCompleted event to history."}
		}

		var (
			failDecision                bool
			failCause                   workflow.DecisionTaskFailedCause
			failMessage                 string
			isComplete                  bool
			activityNotStartedCancelled bool
			transferTasks               []persistence.Task
			timerTasks                  []persistence.Task
			continueAsNewBuilder        mutableState
			continueAsNewTimerTasks     []persistence.Task

			tBuilder           *timerBuilder
			hasUnhandledEvents bool
		)
		tBuilder = timerBuilderProvider()
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

			decisionAttrValidator := newDecisionAttrValidator(
				handler.domainCache,
				handler.config.MaxIDLengthLimit(),
			)
			decisionBlobSizeChecker := newDecisionBlobSizeChecker(
				handler.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name),
				handler.config.BlobSizeLimitError(domainEntry.GetInfo().Name),
				completedEvent.GetEventId(),
				msBuilder,
				handler.metricsClient,
				handler.throttledLogger,
			)

			decisionTaskHandler := newDecisionTaskHandler(
				request.GetIdentity(),
				completedEvent.GetEventId(),
				eventStoreVersion,
				domainEntry,
				msBuilder,
				decisionAttrValidator,
				decisionBlobSizeChecker,
				handler.logger,
				timerBuilderProvider,
				handler.domainCache,
				handler.metricsClient,
			)

			err := decisionTaskHandler.handleDecisions(request.Decisions)
			if err != nil {
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
			isComplete = !msBuilder.IsWorkflowExecutionRunning()
			activityNotStartedCancelled = decisionTaskHandler.activityNotStartedCancelled
			// continueAsNewTimerTasks is not used by decisionTaskHandler

			transferTasks = append(transferTasks, decisionTaskHandler.transferTasks...)
			timerTasks = append(timerTasks, decisionTaskHandler.timerTasks...)

			continueAsNewBuilder = decisionTaskHandler.continueAsNewBuilder

			tBuilder = decisionTaskHandler.timerBuilder
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
			tBuilder = handler.historyEngine.getTimerBuilder(context.getExecution())
			isComplete = false
			hasUnhandledEvents = true
			continueAsNewBuilder = nil
		}

		if tt := tBuilder.GetUserTimerTaskIfNeeded(msBuilder); tt != nil {
			timerTasks = append(timerTasks, tt)
		}
		if tt := tBuilder.GetActivityTimerTaskIfNeeded(msBuilder); tt != nil {
			timerTasks = append(timerTasks, tt)
		}

		// Schedule another decision task if new events came in during this decision or if request forced to
		createNewDecisionTask := !isComplete && (hasUnhandledEvents ||
			request.GetForceCreateNewDecisionTask() || activityNotStartedCancelled)
		var newDecisionTaskScheduledID int64
		if createNewDecisionTask {
			di, err := msBuilder.AddDecisionTaskScheduledEvent()
			if err != nil {
				return nil, &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
			}
			newDecisionTaskScheduledID = di.ScheduleID
			// skip transfer task for decision if request asking to return new decision task
			if !request.GetReturnNewDecisionTask() {
				transferTasks = append(transferTasks, &persistence.DecisionTask{
					DomainID:   domainID,
					TaskList:   di.TaskList,
					ScheduleID: di.ScheduleID,
				})
				if msBuilder.IsStickyTaskListEnabled() {
					tBuilder := handler.historyEngine.getTimerBuilder(context.getExecution())
					stickyTaskTimeoutTimer := tBuilder.AddScheduleToStartDecisionTimoutTask(di.ScheduleID, di.Attempt,
						executionInfo.StickyScheduleToStartTimeout)
					timerTasks = append(timerTasks, stickyTaskTimeoutTimer)
				}
			} else {
				// start the new decision task if request asked to do so
				// TODO: replace the poll request
				_, _, err := msBuilder.AddDecisionTaskStartedEvent(di.ScheduleID, "request-from-RespondDecisionTaskCompleted", &workflow.PollForDecisionTaskRequest{
					TaskList: &workflow.TaskList{Name: common.StringPtr(di.TaskList)},
					Identity: request.Identity,
				})
				if err != nil {
					return nil, err
				}
				timeOutTask := tBuilder.AddStartToCloseDecisionTimoutTask(di.ScheduleID, di.Attempt, di.DecisionTimeout)
				timerTasks = append(timerTasks, timeOutTask)
			}
		}

		if isComplete {
			tranT, timerT, err := handler.historyEngine.getWorkflowHistoryCleanupTasks(
				domainID,
				workflowExecution.GetWorkflowId(),
				tBuilder)
			if err != nil {
				return nil, err
			}
			transferTasks = append(transferTasks, tranT)
			timerTasks = append(timerTasks, timerT)
		}

		// Generate a transaction ID for appending events to history
		transactionID, err := handler.shard.GetNextTransferTaskID()
		if err != nil {
			return nil, err
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict then reload
		// the history and try the operation again.
		var updateErr error
		if continueAsNewBuilder != nil {
			continueAsNewTimerTasks = msBuilder.GetContinueAsNew().TimerTasks
			updateErr = context.continueAsNewWorkflowExecution(request.ExecutionContext, continueAsNewBuilder,
				transferTasks, timerTasks, transactionID)
		} else {
			updateErr = context.updateWorkflowExecutionWithContext(request.ExecutionContext, transferTasks, timerTasks,
				transactionID)
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

				_, err := msBuilder.AddWorkflowExecutionTerminatedEvent(
					common.FailureReasonTransactionSizeExceedsLimit,
					[]byte(updateErr.Error()),
					"cadence-history-server",
				)
				if err != nil {
					return nil, err
				}
				tranT, timerT, err := handler.historyEngine.getWorkflowHistoryCleanupTasks(domainID, workflowExecution.GetWorkflowId(), tBuilder)
				if err != nil {
					return nil, err
				}
				transferTasks = []persistence.Task{tranT}
				timerTasks = []persistence.Task{timerT}
				transactionID, err = handler.shard.GetNextTransferTaskID()
				if err != nil {
					return nil, err
				}
				if err := context.updateWorkflowExecutionWithContext(request.ExecutionContext, transferTasks, timerTasks, transactionID); err != nil {
					return nil, err
				}
				handler.timerProcessor.NotifyNewTimers(handler.currentClusterName, handler.shard.GetCurrentTime(handler.currentClusterName), timerTasks)
			}
			return nil, updateErr
		}

		// add continueAsNewTimerTask
		timerTasks = append(timerTasks, continueAsNewTimerTasks...)
		// Inform timer about the new ones.
		handler.timerProcessor.NotifyNewTimers(handler.currentClusterName, handler.shard.GetCurrentTime(handler.currentClusterName), timerTasks)

		resp = &h.RespondDecisionTaskCompletedResponse{}
		if request.GetReturnNewDecisionTask() && createNewDecisionTask {
			di, _ := msBuilder.GetPendingDecision(newDecisionTaskScheduledID)
			resp.StartedResponse = handler.createRecordDecisionTaskStartedResponse(domainID, msBuilder, di, request.GetIdentity())
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
	di *decisionInfo,
	identity string,
) *h.RecordDecisionTaskStartedResponse {

	response := &h.RecordDecisionTaskStartedResponse{}
	response.WorkflowType = msBuilder.GetWorkflowType()
	executionInfo := msBuilder.GetExecutionInfo()
	if executionInfo.LastProcessedEvent != common.EmptyEventID {
		response.PreviousStartedEventId = common.Int64Ptr(executionInfo.LastProcessedEvent)
	}

	// Starting decision could result in different scheduleID if decision was transient and new new events came in
	// before it was started.
	response.ScheduledEventId = common.Int64Ptr(di.ScheduleID)
	response.StartedEventId = common.Int64Ptr(di.StartedID)
	response.StickyExecutionEnabled = common.BoolPtr(msBuilder.IsStickyTaskListEnabled())
	response.NextEventId = common.Int64Ptr(msBuilder.GetNextEventID())
	response.Attempt = common.Int64Ptr(di.Attempt)
	response.WorkflowExecutionTaskList = common.TaskListPtr(workflow.TaskList{
		Name: &executionInfo.TaskList,
		Kind: common.TaskListKindPtr(workflow.TaskListKindNormal),
	})
	response.ScheduledTimestamp = common.Int64Ptr(di.ScheduledTimestamp)
	response.StartedTimestamp = common.Int64Ptr(di.StartedTimestamp)

	if di.Attempt > 0 {
		// This decision is retried from mutable state
		// Also return schedule and started which are not written to history yet
		scheduledEvent, startedEvent := msBuilder.CreateTransientDecisionEvents(di, identity)
		response.DecisionInfo = &workflow.TransientDecisionInfo{}
		response.DecisionInfo.ScheduledEvent = scheduledEvent
		response.DecisionInfo.StartedEvent = startedEvent
	}
	response.EventStoreVersion = common.Int32Ptr(msBuilder.GetEventStoreVersion())
	response.BranchToken = msBuilder.GetCurrentBranch()

	return response
}
