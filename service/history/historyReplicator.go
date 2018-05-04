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
	"errors"
	"fmt"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/persistence"
)

type (
	historyReplicator struct {
		shard             ShardContext
		historyEngine     *historyEngineImpl
		historyCache      *historyCache
		domainCache       cache.DomainCache
		historyMgr        persistence.HistoryManager
		historySerializer persistence.HistorySerializer
		logger            bark.Logger
	}
)

func newHistoryReplicator(shard ShardContext, historyEngine *historyEngineImpl, historyCache *historyCache, domainCache cache.DomainCache,
	historyMgr persistence.HistoryManager, logger bark.Logger) *historyReplicator {
	replicator := &historyReplicator{
		shard:             shard,
		historyEngine:     historyEngine,
		historyCache:      historyCache,
		domainCache:       domainCache,
		historyMgr:        historyMgr,
		historySerializer: persistence.NewJSONHistorySerializer(),
		logger:            logger,
	}

	return replicator
}

func (r *historyReplicator) ApplyEvents(request *h.ReplicateEventsRequest) (retError error) {
	if request == nil || request.History == nil || len(request.History.Events) == 0 {
		return nil
	}

	domainID, err := validateDomainUUID(request.DomainUUID)
	if err != nil {
		return err
	}
	execution := *request.WorkflowExecution

	var context *workflowExecutionContext
	var msBuilder *mutableStateBuilder
	firstEvent := request.History.Events[0]
	switch firstEvent.GetEventType() {
	case shared.EventTypeWorkflowExecutionStarted:
		msBuilder = newMutableStateBuilderWithReplicationState(r.shard.GetConfig(), r.logger, request.GetVersion())

	default:
		var release releaseWorkflowExecutionFunc
		context, release, err = r.historyCache.getOrCreateWorkflowExecution(domainID, execution)
		if err != nil {
			return err
		}
		defer func() { release(retError) }()

		msBuilder, err = context.loadWorkflowExecution()
		if err != nil {
			return err
		}

		// Check for out of order replication task and store it in the buffer
		if firstEvent.GetEventId() > msBuilder.GetNextEventID() {
			if t := msBuilder.BufferReplicationTask(request); t == nil {
				return errors.New("failed to add buffered replication task")
			}

			return nil
		}
	}

	// First check if there are events which needs to be flushed before applying the update
	err = r.FlushBuffer(context, msBuilder, request)
	if err != nil {
		return err
	}

	// Apply the replication task
	err = r.ApplyReplicationTask(context, msBuilder, request)
	if err != nil {
		return err
	}

	// Flush buffered replication tasks after applying the update
	err = r.FlushBuffer(context, msBuilder, request)

	return err
}

func (r *historyReplicator) ApplyReplicationTask(context *workflowExecutionContext, msBuilder *mutableStateBuilder,
	request *h.ReplicateEventsRequest) error {

	domainID, err := validateDomainUUID(request.DomainUUID)
	if err != nil {
		return err
	}
	if len(request.History.Events) == 0 {
		return nil
	}

	execution := *request.WorkflowExecution

	transferTasks := []persistence.Task{}
	timerTasks := []persistence.Task{}

	var lastEvent *shared.HistoryEvent
	decisionVersionID := emptyVersion
	decisionScheduleID := emptyEventID
	decisionStartID := emptyEventID
	decisionTimeout := int32(0)
	var requestID string
	for _, event := range request.History.Events {
		lastEvent = event
		switch event.GetEventType() {
		case shared.EventTypeWorkflowExecutionStarted:
			attributes := event.WorkflowExecutionStartedEventAttributes
			requestID = uuid.New()
			var parentDomainID *string
			if attributes.ParentWorkflowDomain != nil {
				parentDomainEntry, err := r.shard.GetDomainCache().GetDomain(attributes.GetParentWorkflowDomain())
				if err != nil {
					return err
				}
				parentDomainID = &parentDomainEntry.GetInfo().ID
			}
			msBuilder.ReplicateWorkflowExecutionStartedEvent(domainID, parentDomainID, execution, requestID, attributes)

			timerTasks = append(timerTasks, r.scheduleWorkflowTimerTask(event, msBuilder))

		case shared.EventTypeDecisionTaskScheduled:
			attributes := event.DecisionTaskScheduledEventAttributes
			di := msBuilder.ReplicateDecisionTaskScheduledEvent(event.GetVersion(), event.GetEventId(), attributes.TaskList.GetName(),
				attributes.GetStartToCloseTimeoutSeconds())

			decisionVersionID = di.Version
			decisionScheduleID = di.ScheduleID
			decisionStartID = di.StartedID
			decisionTimeout = di.DecisionTimeout

			transferTasks = append(transferTasks, r.scheduleDecisionTransferTask(domainID, r.getTaskList(msBuilder), di.ScheduleID))
			// since we do not use stickyness on the standby side, there shall be no decision schedule to start timeout

		case shared.EventTypeDecisionTaskStarted:
			attributes := event.DecisionTaskStartedEventAttributes
			di := msBuilder.ReplicateDecisionTaskStartedEvent(nil, event.GetVersion(), attributes.GetScheduledEventId(), event.GetEventId(),
				attributes.GetRequestId(), event.GetTimestamp())

			decisionVersionID = di.Version
			decisionScheduleID = di.ScheduleID
			decisionStartID = di.StartedID
			decisionTimeout = di.DecisionTimeout

			timerTasks = append(timerTasks, r.scheduleDecisionTimerTask(event, di.ScheduleID, di.Attempt, di.DecisionTimeout))

		case shared.EventTypeDecisionTaskCompleted:
			attributes := event.DecisionTaskCompletedEventAttributes
			msBuilder.ReplicateDecisionTaskCompletedEvent(attributes.GetScheduledEventId(),
				attributes.GetStartedEventId())

		case shared.EventTypeDecisionTaskTimedOut:
			attributes := event.DecisionTaskTimedOutEventAttributes
			msBuilder.ReplicateDecisionTaskTimedOutEvent(attributes.GetScheduledEventId(),
				attributes.GetStartedEventId())

		case shared.EventTypeDecisionTaskFailed:
			attributes := event.DecisionTaskFailedEventAttributes
			msBuilder.ReplicateDecisionTaskFailedEvent(attributes.GetScheduledEventId(),
				attributes.GetStartedEventId())

		case shared.EventTypeActivityTaskScheduled:
			ai := msBuilder.ReplicateActivityTaskScheduledEvent(event)

			transferTasks = append(transferTasks, r.scheduleActivityTransferTask(domainID, r.getTaskList(msBuilder), ai.ScheduleID))
			if timerTask := r.scheduleActivityTimerTask(event, msBuilder); timerTask != nil {
				timerTasks = append(timerTasks, timerTask)
			}

		case shared.EventTypeActivityTaskStarted:
			msBuilder.ReplicateActivityTaskStartedEvent(event)

		case shared.EventTypeActivityTaskCompleted:
			if err := msBuilder.ReplicateActivityTaskCompletedEvent(event); err != nil {
				return err
			}

		case shared.EventTypeActivityTaskFailed:
			msBuilder.ReplicateActivityTaskFailedEvent(event)

		case shared.EventTypeActivityTaskTimedOut:
			msBuilder.ReplicateActivityTaskTimedOutEvent(event)

		case shared.EventTypeActivityTaskCancelRequested:
			msBuilder.ReplicateActivityTaskCancelRequestedEvent(event)

		case shared.EventTypeActivityTaskCanceled:
			msBuilder.ReplicateActivityTaskCanceledEvent(event)

		case shared.EventTypeRequestCancelActivityTaskFailed:
			// No mutable state action is needed

		case shared.EventTypeTimerStarted:
			msBuilder.ReplicateTimerStartedEvent(event)
			if timerTask := r.scheduleUserTimerTask(event, msBuilder); timerTask != nil {
				timerTasks = append(timerTasks, timerTask)
			}

		case shared.EventTypeTimerFired:
			msBuilder.ReplicateTimerFiredEvent(event)

		case shared.EventTypeTimerCanceled:
			msBuilder.ReplicateTimerCanceledEvent(event)

		case shared.EventTypeCancelTimerFailed:
			// No mutable state action is needed

		case shared.EventTypeStartChildWorkflowExecutionInitiated:
			// Create a new request ID which is used by transfer queue processor if domain is failed over at this point
			createRequestID := uuid.New()
			cei := msBuilder.ReplicateStartChildWorkflowExecutionInitiatedEvent(event, createRequestID)

			attributes := event.StartChildWorkflowExecutionInitiatedEventAttributes
			childDomainEntry, err := r.shard.GetDomainCache().GetDomain(attributes.GetDomain())
			if err != nil {
				return err
			}
			transferTasks = append(transferTasks, r.scheduleStartChildWorkflowTransferTask(childDomainEntry.GetInfo().ID, attributes.GetWorkflowId(), cei.InitiatedID))

		case shared.EventTypeStartChildWorkflowExecutionFailed:
			msBuilder.ReplicateStartChildWorkflowExecutionFailedEvent(event)

		case shared.EventTypeChildWorkflowExecutionStarted:
			msBuilder.ReplicateChildWorkflowExecutionStartedEvent(event)

		case shared.EventTypeChildWorkflowExecutionCompleted:
			msBuilder.ReplicateChildWorkflowExecutionCompletedEvent(event)

		case shared.EventTypeChildWorkflowExecutionFailed:
			msBuilder.ReplicateChildWorkflowExecutionFailedEvent(event)

		case shared.EventTypeChildWorkflowExecutionCanceled:
			msBuilder.ReplicateChildWorkflowExecutionCanceledEvent(event)

		case shared.EventTypeChildWorkflowExecutionTimedOut:
			msBuilder.ReplicateChildWorkflowExecutionTimedOutEvent(event)

		case shared.EventTypeChildWorkflowExecutionTerminated:
			msBuilder.ReplicateChildWorkflowExecutionTerminatedEvent(event)

		case shared.EventTypeRequestCancelExternalWorkflowExecutionInitiated:
			// Create a new request ID which is used by transfer queue processor if domain is failed over at this point
			cancelRequestID := uuid.New()
			rci := msBuilder.ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(event, cancelRequestID)

			attributes := event.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes
			transferTasks = append(transferTasks, r.scheduleCancelExternalWorkflowTransferTask(
				attributes.GetDomain(),
				attributes.WorkflowExecution.GetWorkflowId(),
				attributes.WorkflowExecution.GetRunId(),
				attributes.GetChildWorkflowOnly(),
				rci.InitiatedID,
			))

		case shared.EventTypeRequestCancelExternalWorkflowExecutionFailed:
			msBuilder.ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(event)

		case shared.EventTypeExternalWorkflowExecutionCancelRequested:
			msBuilder.ReplicateExternalWorkflowExecutionCancelRequested(event)

		case shared.EventTypeSignalExternalWorkflowExecutionInitiated:
			// Create a new request ID which is used by transfer queue processor if domain is failed over at this point
			signalRequestID := uuid.New()
			si := msBuilder.ReplicateSignalExternalWorkflowExecutionInitiatedEvent(event, signalRequestID)

			attributes := event.SignalExternalWorkflowExecutionInitiatedEventAttributes
			transferTasks = append(transferTasks, r.scheduleSignalWorkflowTransferTask(
				attributes.GetDomain(),
				attributes.WorkflowExecution.GetWorkflowId(),
				attributes.WorkflowExecution.GetRunId(),
				attributes.GetChildWorkflowOnly(),
				si.InitiatedID,
			))

		case shared.EventTypeSignalExternalWorkflowExecutionFailed:
			msBuilder.ReplicateSignalExternalWorkflowExecutionFailedEvent(event)

		case shared.EventTypeExternalWorkflowExecutionSignaled:
			msBuilder.ReplicateExternalWorkflowExecutionSignaled(event)

		case shared.EventTypeMarkerRecorded:
			// No mutable state action is needed

		case shared.EventTypeWorkflowExecutionSignaled:
			// No mutable state action is needed

		case shared.EventTypeWorkflowExecutionCancelRequested:
			msBuilder.ReplicateWorkflowExecutionCancelRequestedEvent(event)

		case shared.EventTypeWorkflowExecutionCompleted:
			msBuilder.ReplicateWorkflowExecutionCompletedEvent(event)
			transferTasks = append(transferTasks, r.scheduleDeleteHistoryTransferTask())
			timerTask, err := r.scheduleDeleteHistoryTimerTask(event, domainID)
			if err != nil {
				return err
			}
			timerTasks = append(timerTasks, timerTask)

		case shared.EventTypeWorkflowExecutionFailed:
			msBuilder.ReplicateWorkflowExecutionFailedEvent(event)
			transferTasks = append(transferTasks, r.scheduleDeleteHistoryTransferTask())
			timerTask, err := r.scheduleDeleteHistoryTimerTask(event, domainID)
			if err != nil {
				return err
			}
			timerTasks = append(timerTasks, timerTask)

		case shared.EventTypeWorkflowExecutionTimedOut:
			msBuilder.ReplicateWorkflowExecutionTimedoutEvent(event)
			transferTasks = append(transferTasks, r.scheduleDeleteHistoryTransferTask())
			timerTask, err := r.scheduleDeleteHistoryTimerTask(event, domainID)
			if err != nil {
				return err
			}
			timerTasks = append(timerTasks, timerTask)

		case shared.EventTypeWorkflowExecutionCanceled:
			msBuilder.ReplicateWorkflowExecutionCanceledEvent(event)
			transferTasks = append(transferTasks, r.scheduleDeleteHistoryTransferTask())
			timerTask, err := r.scheduleDeleteHistoryTimerTask(event, domainID)
			if err != nil {
				return err
			}
			timerTasks = append(timerTasks, timerTask)

		case shared.EventTypeWorkflowExecutionTerminated:
			msBuilder.ReplicateWorkflowExecutionTerminatedEvent(event)
			transferTasks = append(transferTasks, r.scheduleDeleteHistoryTransferTask())
			timerTask, err := r.scheduleDeleteHistoryTimerTask(event, domainID)
			if err != nil {
				return err
			}
			timerTasks = append(timerTasks, timerTask)

		case shared.EventTypeWorkflowExecutionContinuedAsNew:
			// ContinuedAsNew event also has history for first 2 events for next run as they are created transactionally
			newTransferTasks := []persistence.Task{}
			newTimerTasks := []persistence.Task{}
			newRunHistory := request.NewRunHistory
			startedEvent := newRunHistory.Events[0]
			startedAttributes := startedEvent.WorkflowExecutionStartedEventAttributes
			dtScheduledEvent := newRunHistory.Events[1]

			// History event only have the parentDomainName.  Lookup the domain ID from cache
			var parentDomainID *string
			if startedAttributes.ParentWorkflowDomain != nil {
				parentDomainEntry, err := r.shard.GetDomainCache().GetDomain(startedAttributes.GetParentWorkflowDomain())
				if err != nil {
					return err
				}
				parentDomainID = &parentDomainEntry.GetInfo().ID
			}

			newRunID := event.WorkflowExecutionContinuedAsNewEventAttributes.GetNewExecutionRunId()

			newExecution := shared.WorkflowExecution{
				WorkflowId: request.WorkflowExecution.WorkflowId,
				RunId:      common.StringPtr(newRunID),
			}

			// Create mutable state updates for the new run
			newStateBuilder := newMutableStateBuilderWithReplicationState(r.shard.GetConfig(), r.logger, request.GetVersion())
			newStateBuilder.ReplicateWorkflowExecutionStartedEvent(domainID, parentDomainID, newExecution, uuid.New(),
				startedAttributes)
			di := newStateBuilder.ReplicateDecisionTaskScheduledEvent(
				dtScheduledEvent.GetVersion(),
				dtScheduledEvent.GetEventId(),
				dtScheduledEvent.DecisionTaskScheduledEventAttributes.TaskList.GetName(),
				dtScheduledEvent.DecisionTaskScheduledEventAttributes.GetStartToCloseTimeoutSeconds(),
			)
			nextEventID := di.ScheduleID + 1
			newStateBuilder.executionInfo.NextEventID = nextEventID
			newStateBuilder.executionInfo.LastFirstEventID = startedEvent.GetEventId()
			// Set the history from replication task on the newStateBuilder
			newStateBuilder.hBuilder = newHistoryBuilderFromEvents(newRunHistory.Events, r.logger)

			newTransferTasks = append(newTransferTasks, r.scheduleDecisionTransferTask(domainID, r.getTaskList(newStateBuilder), di.ScheduleID))
			newTimerTasks = append(newTimerTasks, r.scheduleWorkflowTimerTask(event, newStateBuilder))

			msBuilder.ReplicateWorkflowExecutionContinuedAsNewEvent(request.GetSourceCluster(), domainID, event, startedEvent, di, newStateBuilder)

			// Generate a transaction ID for appending events to history
			transactionID, err := r.shard.GetNextTransferTaskID()
			if err != nil {
				return err
			}
			err = context.replicateContinueAsNewWorkflowExecution(newStateBuilder, newTransferTasks, newTimerTasks, transactionID)
			if err != nil {
				return err
			}
		}
	}

	firstEvent := request.History.Events[0]
	switch firstEvent.GetEventType() {
	case shared.EventTypeWorkflowExecutionStarted:
		// TODO: Support for child execution
		var parentExecution *shared.WorkflowExecution
		initiatedID := emptyEventID
		parentDomainID := ""

		// Serialize the history
		serializedHistory, serializedError := r.Serialize(request.History)
		if serializedError != nil {
			logging.LogHistorySerializationErrorEvent(r.logger, serializedError, fmt.Sprintf(
				"HistoryEventBatch serialization error on start workflow.  WorkflowID: %v, RunID: %v",
				execution.GetWorkflowId(), execution.GetRunId()))
			return serializedError
		}

		// Generate a transaction ID for appending events to history
		transactionID, err2 := r.shard.GetNextTransferTaskID()
		if err2 != nil {
			return err2
		}

		err = r.shard.AppendHistoryEvents(&persistence.AppendHistoryEventsRequest{
			DomainID:      domainID,
			Execution:     execution,
			TransactionID: transactionID,
			FirstEventID:  firstEvent.GetEventId(),
			Events:        serializedHistory,
		})
		if err != nil {
			return err
		}

		nextEventID := lastEvent.GetEventId() + 1
		msBuilder.executionInfo.NextEventID = nextEventID
		msBuilder.executionInfo.LastFirstEventID = firstEvent.GetEventId()

		failoverVersion := request.GetVersion()
		replicationState := &persistence.ReplicationState{
			CurrentVersion:   failoverVersion,
			StartVersion:     failoverVersion,
			LastWriteVersion: failoverVersion,
			LastWriteEventID: lastEvent.GetEventId(),
		}

		createWorkflow := func(isBrandNew bool, prevRunID string) (string, error) {
			_, err = r.shard.CreateWorkflowExecution(&persistence.CreateWorkflowExecutionRequest{
				RequestID:                   requestID,
				DomainID:                    domainID,
				Execution:                   execution,
				ParentDomainID:              parentDomainID,
				ParentExecution:             parentExecution,
				InitiatedID:                 initiatedID,
				TaskList:                    msBuilder.executionInfo.TaskList,
				WorkflowTypeName:            msBuilder.executionInfo.WorkflowTypeName,
				WorkflowTimeout:             msBuilder.executionInfo.WorkflowTimeout,
				DecisionTimeoutValue:        msBuilder.executionInfo.DecisionTimeoutValue,
				ExecutionContext:            nil,
				NextEventID:                 msBuilder.GetNextEventID(),
				LastProcessedEvent:          emptyEventID,
				TransferTasks:               transferTasks, // TODO: Generate transfer task
				DecisionVersion:             decisionVersionID,
				DecisionScheduleID:          decisionScheduleID,
				DecisionStartedID:           decisionStartID,
				DecisionStartToCloseTimeout: decisionTimeout,
				TimerTasks:                  timerTasks, // TODO: Generate workflow timeout task
				ContinueAsNew:               !isBrandNew,
				PreviousRunID:               prevRunID,
				ReplicationState:            replicationState,
			})

			if err != nil {
				return "", err
			}

			return execution.GetRunId(), nil
		}

		// try to create the workflow execution
		isBrandNew := true
		_, err = createWorkflow(isBrandNew, "")
		// if err still non nil, see if retry
		/*if errExist, ok := err.(*persistence.WorkflowExecutionAlreadyStartedError); ok {
			if err = workflowExistsErrHandler(errExist); err == nil {
				isBrandNew = false
				_, err = createWorkflow(isBrandNew, errExist.RunID)
			}
		}*/

	default:
		// Generate a transaction ID for appending events to history
		transactionID, err2 := r.shard.GetNextTransferTaskID()
		if err2 != nil {
			return err2
		}
		err = context.replicateWorkflowExecution(request, transferTasks, timerTasks, lastEvent.GetEventId(), transactionID)
	}

	if err == nil {
		now := time.Unix(0, lastEvent.GetTimestamp())
		r.notify(request.GetSourceCluster(), now, transferTasks, timerTasks)
	}

	return err
}

func (r *historyReplicator) FlushBuffer(context *workflowExecutionContext, msBuilder *mutableStateBuilder,
	request *h.ReplicateEventsRequest) error {

	// Keep on applying on applying buffered replication tasks in a loop
	for msBuilder.HasBufferedReplicationTasks() {
		nextEventID := msBuilder.GetNextEventID()
		bt, ok := msBuilder.GetBufferedReplicationTask(nextEventID)
		if !ok {
			// Bail out if nextEventID is not in the buffer
			return nil
		}

		// We need to delete the task from buffer first to make sure delete update is queued up
		// Applying replication task commits the transaction along with the delete
		msBuilder.DeleteBufferedReplicationTask(nextEventID)

		req := &h.ReplicateEventsRequest{
			SourceCluster:     request.SourceCluster,
			DomainUUID:        request.DomainUUID,
			WorkflowExecution: request.WorkflowExecution,
			FirstEventId:      common.Int64Ptr(bt.FirstEventID),
			NextEventId:       common.Int64Ptr(bt.NextEventID),
			Version:           common.Int64Ptr(bt.Version),
			History:           msBuilder.GetBufferedHistory(bt.History),
			NewRunHistory:     msBuilder.GetBufferedHistory(bt.NewRunHistory),
		}

		// Apply replication task to workflow execution
		if err := r.ApplyReplicationTask(context, msBuilder, req); err != nil {
			return err
		}
	}

	return nil
}

func (r *historyReplicator) Serialize(history *shared.History) (*persistence.SerializedHistoryEventBatch, error) {
	eventBatch := persistence.NewHistoryEventBatch(persistence.GetDefaultHistoryVersion(), history.Events)
	h, err := r.historySerializer.Serialize(eventBatch)
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (r *historyReplicator) scheduleDecisionTransferTask(domainID string, tasklist string, scheduleID int64) persistence.Task {
	return &persistence.DecisionTask{
		DomainID:   domainID,
		TaskList:   tasklist,
		ScheduleID: scheduleID,
	}
}

func (r *historyReplicator) scheduleActivityTransferTask(domainID string, tasklist string, scheduleID int64) persistence.Task {
	return &persistence.ActivityTask{
		DomainID:   domainID,
		TaskList:   tasklist,
		ScheduleID: scheduleID,
	}
}

func (r *historyReplicator) scheduleStartChildWorkflowTransferTask(domainID string, workflowID string, initiatedID int64) persistence.Task {
	return &persistence.StartChildExecutionTask{
		TargetDomainID:   domainID,
		TargetWorkflowID: workflowID,
		InitiatedID:      initiatedID,
	}
}

func (r *historyReplicator) scheduleCancelExternalWorkflowTransferTask(domainID string, workflowID string,
	runID string, childWorkflowOnly bool, initiatedID int64) persistence.Task {
	return &persistence.CancelExecutionTask{
		TargetDomainID:          domainID,
		TargetWorkflowID:        workflowID,
		TargetRunID:             runID,
		TargetChildWorkflowOnly: childWorkflowOnly,
		InitiatedID:             initiatedID,
	}
}

func (r *historyReplicator) scheduleSignalWorkflowTransferTask(domainID string, workflowID string,
	runID string, childWorkflowOnly bool, initiatedID int64) persistence.Task {
	return &persistence.SignalExecutionTask{
		TargetDomainID:          domainID,
		TargetWorkflowID:        workflowID,
		TargetRunID:             runID,
		TargetChildWorkflowOnly: childWorkflowOnly,
		InitiatedID:             initiatedID,
	}
}

func (r *historyReplicator) scheduleDeleteHistoryTransferTask() persistence.Task {
	return &persistence.CloseExecutionTask{}
}

func (r *historyReplicator) scheduleDecisionTimerTask(event *shared.HistoryEvent, scheduleID int64, attempt int64, timeoutSecond int32) persistence.Task {
	return r.getTimerBuilder(event).AddStartToCloseDecisionTimoutTask(scheduleID, attempt, timeoutSecond)
}

func (r *historyReplicator) scheduleUserTimerTask(event *shared.HistoryEvent, msBuilder *mutableStateBuilder) persistence.Task {
	return r.getTimerBuilder(event).GetUserTimerTaskIfNeeded(msBuilder)
}

func (r *historyReplicator) scheduleActivityTimerTask(event *shared.HistoryEvent, msBuilder *mutableStateBuilder) persistence.Task {
	return r.getTimerBuilder(event).GetActivityTimerTaskIfNeeded(msBuilder)
}

func (r *historyReplicator) scheduleWorkflowTimerTask(event *shared.HistoryEvent, msBuilder *mutableStateBuilder) persistence.Task {
	now := time.Unix(0, event.GetTimestamp())
	timeout := now.Add(time.Duration(msBuilder.executionInfo.WorkflowTimeout) * time.Second)
	return &persistence.WorkflowTimeoutTask{VisibilityTimestamp: timeout}
}

func (r *historyReplicator) scheduleDeleteHistoryTimerTask(event *shared.HistoryEvent, domainID string) (persistence.Task, error) {
	var retentionInDays int32
	domainEntry, err := r.shard.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		if _, ok := err.(*shared.EntityNotExistsError); !ok {
			return nil, err
		}
	}
	retentionInDays = domainEntry.GetConfig().Retention
	return r.getTimerBuilder(event).createDeleteHistoryEventTimerTask(time.Duration(retentionInDays) * time.Hour * 24), nil
}

func (r *historyReplicator) getTaskList(msBuilder *mutableStateBuilder) string {
	// on the standby side, sticky tasklist is meaningless, so always use the normal tasklist
	return msBuilder.executionInfo.TaskList
}

func (r *historyReplicator) getTimerBuilder(event *shared.HistoryEvent) *timerBuilder {
	timeSource := common.NewFakeTimeSource()
	now := time.Unix(0, event.GetTimestamp())
	timeSource.Update(now)
	return newTimerBuilder(r.shard.GetConfig(), r.logger, timeSource)
}

func (r *historyReplicator) notify(clusterName string, now time.Time, transferTasks []persistence.Task, timerTasks []persistence.Task) {
	r.shard.SetCurrentTime(clusterName, now)
	r.historyEngine.txProcessor.NotifyNewTask(clusterName, now, transferTasks)
	r.historyEngine.timerProcessor.NotifyNewTimers(clusterName, now, timerTasks)
}
