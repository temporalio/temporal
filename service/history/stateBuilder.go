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
	"time"

	"github.com/pborman/uuid"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/errors"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
)

type (
	stateBuilder interface {
		applyEvents(
			domainID string,
			requestID string,
			execution shared.WorkflowExecution,
			history []*shared.HistoryEvent,
			newRunHistory []*shared.HistoryEvent,
			newRunNDC bool,
		) (*shared.HistoryEvent, *decisionInfo, mutableState, error)
		getTransferTasks() []persistence.Task
		getTimerTasks() []persistence.Task
		getNewRunTransferTasks() []persistence.Task
		getNewRunTimerTasks() []persistence.Task
		getMutableState() mutableState
	}

	stateBuilderImpl struct {
		shard           ShardContext
		clusterMetadata cluster.Metadata
		msBuilder       mutableState
		domainCache     cache.DomainCache
		logger          log.Logger

		transferTasks       []persistence.Task
		timerTasks          []persistence.Task
		newRunTransferTasks []persistence.Task
		newRunTimerTasks    []persistence.Task
	}
)

const (
	// ErrMessageHistorySizeZero indicate that history is empty
	ErrMessageHistorySizeZero = "encounter history size being zero"
	// ErrMessageNewRunHistorySizeZero indicate that new run history is empty
	ErrMessageNewRunHistorySizeZero = "encounter new run history size being zero"
)

var _ stateBuilder = (*stateBuilderImpl)(nil)

func newStateBuilder(
	shard ShardContext,
	msBuilder mutableState,
	logger log.Logger,
) *stateBuilderImpl {

	return &stateBuilderImpl{
		shard:           shard,
		clusterMetadata: shard.GetService().GetClusterMetadata(),
		msBuilder:       msBuilder,
		domainCache:     shard.GetDomainCache(),
		logger:          logger,
	}
}

func (b *stateBuilderImpl) getMutableState() mutableState {
	return b.msBuilder
}

func (b *stateBuilderImpl) getTransferTasks() []persistence.Task {
	return b.transferTasks
}

func (b *stateBuilderImpl) getTimerTasks() []persistence.Task {
	return b.timerTasks
}

func (b *stateBuilderImpl) getNewRunTransferTasks() []persistence.Task {
	return b.newRunTransferTasks
}

func (b *stateBuilderImpl) getNewRunTimerTasks() []persistence.Task {
	return b.newRunTimerTasks
}

func (b *stateBuilderImpl) applyEvents(
	domainID string,
	requestID string,
	execution shared.WorkflowExecution,
	history []*shared.HistoryEvent,
	newRunHistory []*shared.HistoryEvent,
	newRunNDC bool,
) (*shared.HistoryEvent, *decisionInfo, mutableState, error) {

	if len(history) == 0 {
		return nil, nil, nil, errors.NewInternalFailureError(ErrMessageHistorySizeZero)
	}
	firstEvent := history[0]
	lastEvent := history[len(history)-1]
	var lastDecision *decisionInfo
	var newRunMutableStateBuilder mutableState

	// need to clear the stickiness since workflow turned to passive
	b.msBuilder.ClearStickyness()

	for _, event := range history {
		// NOTE: stateBuilder is also being used in the active side
		if b.msBuilder.GetReplicationState() != nil {
			// this function must be called within the for loop, in case
			// history event version changed during for loop
			b.msBuilder.UpdateReplicationStateVersion(event.GetVersion(), true)
			b.msBuilder.UpdateReplicationStateLastEventID(lastEvent.GetVersion(), lastEvent.GetEventId())
		} else if b.msBuilder.GetVersionHistories() != nil {
			if err := b.msBuilder.UpdateCurrentVersion(event.GetVersion(), true); err != nil {
				return nil, nil, nil, err
			}
			versionHistories := b.msBuilder.GetVersionHistories()
			versionHistory, err := versionHistories.GetCurrentVersionHistory()
			if err != nil {
				return nil, nil, nil, err
			}
			if err := versionHistory.AddOrUpdateItem(persistence.NewVersionHistoryItem(
				event.GetEventId(),
				event.GetVersion(),
			)); err != nil {
				return nil, nil, nil, err
			}
		}
		b.msBuilder.GetExecutionInfo().LastEventTaskID = event.GetTaskId()

		switch event.GetEventType() {
		case shared.EventTypeWorkflowExecutionStarted:
			attributes := event.WorkflowExecutionStartedEventAttributes
			var parentDomainID *string
			if attributes.ParentWorkflowDomain != nil {
				parentDomainEntry, err := b.domainCache.GetDomain(attributes.GetParentWorkflowDomain())
				if err != nil {
					return nil, nil, nil, err
				}
				parentDomainID = &parentDomainEntry.GetInfo().ID
			}
			err := b.msBuilder.ReplicateWorkflowExecutionStartedEvent(parentDomainID, execution, requestID, event)
			if err != nil {
				return nil, nil, nil, err
			}

			b.timerTasks = append(b.timerTasks, b.scheduleWorkflowTimerTask(event, b.msBuilder)...)
			b.transferTasks = append(b.transferTasks, b.scheduleWorkflowStartTransferTask())

			err = b.msBuilder.SetHistoryTree(execution.GetRunId())
			if err != nil {
				return nil, nil, nil, err
			}

			// TODO remove after NDC is fully migrated
			if b.msBuilder.GetReplicationState() != nil {
				b.msBuilder.GetReplicationState().StartVersion = event.GetVersion()
			}

		case shared.EventTypeDecisionTaskScheduled:
			attributes := event.DecisionTaskScheduledEventAttributes
			// use event.GetTimestamp() as DecisionOriginalScheduledTimestamp, because the heartbeat is not happening here.
			decision, err := b.msBuilder.ReplicateDecisionTaskScheduledEvent(event.GetVersion(), event.GetEventId(),
				attributes.TaskList.GetName(), attributes.GetStartToCloseTimeoutSeconds(), attributes.GetAttempt(),
				event.GetTimestamp(), event.GetTimestamp())
			if err != nil {
				return nil, nil, nil, err
			}

			b.transferTasks = append(b.transferTasks, b.scheduleDecisionTransferTask(domainID, b.getTaskList(b.msBuilder),
				decision.ScheduleID))
			// since we do not use stickiness on the standby side, there shall be no decision schedule to start timeout

			lastDecision = decision

		case shared.EventTypeDecisionTaskStarted:
			attributes := event.DecisionTaskStartedEventAttributes
			decision, err := b.msBuilder.ReplicateDecisionTaskStartedEvent(nil, event.GetVersion(), attributes.GetScheduledEventId(),
				event.GetEventId(), attributes.GetRequestId(), event.GetTimestamp())
			if err != nil {
				return nil, nil, nil, err
			}

			b.timerTasks = append(b.timerTasks, b.scheduleDecisionTimerTask(event, decision.ScheduleID, decision.Attempt,
				decision.DecisionTimeout))

			lastDecision = decision

		case shared.EventTypeDecisionTaskCompleted:
			err := b.msBuilder.ReplicateDecisionTaskCompletedEvent(event)
			if err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeDecisionTaskTimedOut:
			attributes := event.DecisionTaskTimedOutEventAttributes
			err := b.msBuilder.ReplicateDecisionTaskTimedOutEvent(attributes.GetTimeoutType())
			if err != nil {
				return nil, nil, nil, err
			}

			// this is for transient decision
			decision, err := b.msBuilder.ReplicateTransientDecisionTaskScheduled()
			if err != nil {
				return nil, nil, nil, err
			}

			if decision != nil {
				b.transferTasks = append(b.transferTasks, b.scheduleDecisionTransferTask(domainID, b.getTaskList(b.msBuilder),
					decision.ScheduleID))
				// since we do not use stickiness on the standby side, there shall be no decision schedule to start timeout
				lastDecision = decision
			}

		case shared.EventTypeDecisionTaskFailed:
			if err := b.msBuilder.ReplicateDecisionTaskFailedEvent(); err != nil {
				return nil, nil, nil, err
			}

			// this is for transient decision
			decision, err := b.msBuilder.ReplicateTransientDecisionTaskScheduled()
			if err != nil {
				return nil, nil, nil, err
			}

			if decision != nil {
				b.transferTasks = append(b.transferTasks, b.scheduleDecisionTransferTask(domainID, b.getTaskList(b.msBuilder),
					decision.ScheduleID))
				// since we do not use stickiness on the standby side, there shall be no decision schedule to start timeout
				lastDecision = decision
			}

		case shared.EventTypeActivityTaskScheduled:
			ai, err := b.msBuilder.ReplicateActivityTaskScheduledEvent(firstEvent.GetEventId(), event)
			if err != nil {
				return nil, nil, nil, err
			}

			b.transferTasks = append(b.transferTasks, b.scheduleActivityTransferTask(domainID, b.getTaskList(b.msBuilder),
				ai.ScheduleID))
			if timerTask := b.scheduleActivityTimerTask(event, b.msBuilder); timerTask != nil {
				b.timerTasks = append(b.timerTasks, timerTask)
			}

		case shared.EventTypeActivityTaskStarted:
			if err := b.msBuilder.ReplicateActivityTaskStartedEvent(event); err != nil {
				return nil, nil, nil, err
			}

			if timerTask := b.scheduleActivityTimerTask(event, b.msBuilder); timerTask != nil {
				b.timerTasks = append(b.timerTasks, timerTask)
			}

		case shared.EventTypeActivityTaskCompleted:
			if err := b.msBuilder.ReplicateActivityTaskCompletedEvent(event); err != nil {
				return nil, nil, nil, err
			}

			if timerTask := b.scheduleActivityTimerTask(event, b.msBuilder); timerTask != nil {
				b.timerTasks = append(b.timerTasks, timerTask)
			}

		case shared.EventTypeActivityTaskFailed:
			if err := b.msBuilder.ReplicateActivityTaskFailedEvent(event); err != nil {
				return nil, nil, nil, err
			}

			if timerTask := b.scheduleActivityTimerTask(event, b.msBuilder); timerTask != nil {
				b.timerTasks = append(b.timerTasks, timerTask)
			}

		case shared.EventTypeActivityTaskTimedOut:
			if err := b.msBuilder.ReplicateActivityTaskTimedOutEvent(event); err != nil {
				return nil, nil, nil, err
			}

			if timerTask := b.scheduleActivityTimerTask(event, b.msBuilder); timerTask != nil {
				b.timerTasks = append(b.timerTasks, timerTask)
			}

		case shared.EventTypeActivityTaskCancelRequested:
			if err := b.msBuilder.ReplicateActivityTaskCancelRequestedEvent(event); err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeActivityTaskCanceled:
			if err := b.msBuilder.ReplicateActivityTaskCanceledEvent(event); err != nil {
				return nil, nil, nil, err
			}

			if timerTask := b.scheduleActivityTimerTask(event, b.msBuilder); timerTask != nil {
				b.timerTasks = append(b.timerTasks, timerTask)
			}

		case shared.EventTypeRequestCancelActivityTaskFailed:
			// No mutable state action is needed

		case shared.EventTypeTimerStarted:
			ti, err := b.msBuilder.ReplicateTimerStartedEvent(event)
			if err != nil {
				return nil, nil, nil, err
			}

			if timerTask := b.scheduleUserTimerTask(event, ti, b.msBuilder); timerTask != nil {
				b.timerTasks = append(b.timerTasks, timerTask)
			}

		case shared.EventTypeTimerFired:
			if err := b.msBuilder.ReplicateTimerFiredEvent(event); err != nil {
				return nil, nil, nil, err
			}

			if timerTask := b.refreshUserTimerTask(event, b.msBuilder); timerTask != nil {
				b.timerTasks = append(b.timerTasks, timerTask)
			}

		case shared.EventTypeTimerCanceled:
			if err := b.msBuilder.ReplicateTimerCanceledEvent(event); err != nil {
				return nil, nil, nil, err
			}

			if timerTask := b.refreshUserTimerTask(event, b.msBuilder); timerTask != nil {
				b.timerTasks = append(b.timerTasks, timerTask)
			}

		case shared.EventTypeCancelTimerFailed:
			// No mutable state action is needed

		case shared.EventTypeStartChildWorkflowExecutionInitiated:
			// Create a new request ID which is used by transfer queue processor if domain is failed over at this point
			createRequestID := uuid.New()
			cei, err := b.msBuilder.ReplicateStartChildWorkflowExecutionInitiatedEvent(firstEvent.GetEventId(), event,
				createRequestID)
			if err != nil {
				return nil, nil, nil, err
			}

			attributes := event.StartChildWorkflowExecutionInitiatedEventAttributes
			childDomainEntry, err := b.shard.GetDomainCache().GetDomain(attributes.GetDomain())
			if err != nil {
				return nil, nil, nil, err
			}

			b.transferTasks = append(b.transferTasks, b.scheduleStartChildWorkflowTransferTask(childDomainEntry.GetInfo().ID,
				attributes.GetWorkflowId(), cei.InitiatedID))

		case shared.EventTypeStartChildWorkflowExecutionFailed:
			if err := b.msBuilder.ReplicateStartChildWorkflowExecutionFailedEvent(event); err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeChildWorkflowExecutionStarted:
			if err := b.msBuilder.ReplicateChildWorkflowExecutionStartedEvent(event); err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeChildWorkflowExecutionCompleted:
			if err := b.msBuilder.ReplicateChildWorkflowExecutionCompletedEvent(event); err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeChildWorkflowExecutionFailed:
			if err := b.msBuilder.ReplicateChildWorkflowExecutionFailedEvent(event); err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeChildWorkflowExecutionCanceled:
			if err := b.msBuilder.ReplicateChildWorkflowExecutionCanceledEvent(event); err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeChildWorkflowExecutionTimedOut:
			if err := b.msBuilder.ReplicateChildWorkflowExecutionTimedOutEvent(event); err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeChildWorkflowExecutionTerminated:
			if err := b.msBuilder.ReplicateChildWorkflowExecutionTerminatedEvent(event); err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeRequestCancelExternalWorkflowExecutionInitiated:
			// Create a new request ID which is used by transfer queue processor if domain is failed over at this point
			cancelRequestID := uuid.New()
			rci, err := b.msBuilder.ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(firstEvent.GetEventId(), event, cancelRequestID)
			if err != nil {
				return nil, nil, nil, err
			}

			attributes := event.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes
			targetDomainEntry, err := b.shard.GetDomainCache().GetDomain(attributes.GetDomain())
			if err != nil {
				return nil, nil, nil, err
			}
			b.transferTasks = append(b.transferTasks, b.scheduleCancelExternalWorkflowTransferTask(
				targetDomainEntry.GetInfo().ID,
				attributes.WorkflowExecution.GetWorkflowId(),
				attributes.WorkflowExecution.GetRunId(),
				attributes.GetChildWorkflowOnly(),
				rci.InitiatedID,
			))

		case shared.EventTypeRequestCancelExternalWorkflowExecutionFailed:
			if err := b.msBuilder.ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(event); err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeExternalWorkflowExecutionCancelRequested:
			if err := b.msBuilder.ReplicateExternalWorkflowExecutionCancelRequested(event); err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeSignalExternalWorkflowExecutionInitiated:
			// Create a new request ID which is used by transfer queue processor if domain is failed over at this point
			signalRequestID := uuid.New()
			si, err := b.msBuilder.ReplicateSignalExternalWorkflowExecutionInitiatedEvent(firstEvent.GetEventId(), event, signalRequestID)
			if err != nil {
				return nil, nil, nil, err
			}

			attributes := event.SignalExternalWorkflowExecutionInitiatedEventAttributes
			targetDomainEntry, err := b.shard.GetDomainCache().GetDomain(attributes.GetDomain())
			if err != nil {
				return nil, nil, nil, err
			}
			b.transferTasks = append(b.transferTasks, b.scheduleSignalWorkflowTransferTask(
				targetDomainEntry.GetInfo().ID,
				attributes.WorkflowExecution.GetWorkflowId(),
				attributes.WorkflowExecution.GetRunId(),
				attributes.GetChildWorkflowOnly(),
				si.InitiatedID,
			))

		case shared.EventTypeSignalExternalWorkflowExecutionFailed:
			if err := b.msBuilder.ReplicateSignalExternalWorkflowExecutionFailedEvent(event); err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeExternalWorkflowExecutionSignaled:
			if err := b.msBuilder.ReplicateExternalWorkflowExecutionSignaled(event); err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeMarkerRecorded:
			// No mutable state action is needed

		case shared.EventTypeWorkflowExecutionSignaled:
			if err := b.msBuilder.ReplicateWorkflowExecutionSignaled(event); err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeWorkflowExecutionCancelRequested:
			if err := b.msBuilder.ReplicateWorkflowExecutionCancelRequestedEvent(event); err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeWorkflowExecutionCompleted:
			if err := b.msBuilder.ReplicateWorkflowExecutionCompletedEvent(firstEvent.GetEventId(), event); err != nil {
				return nil, nil, nil, err
			}

			err := b.appendTasksForFinishedExecutions(event, domainID, execution.GetWorkflowId())
			if err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeWorkflowExecutionFailed:
			if err := b.msBuilder.ReplicateWorkflowExecutionFailedEvent(firstEvent.GetEventId(), event); err != nil {
				return nil, nil, nil, err
			}

			err := b.appendTasksForFinishedExecutions(event, domainID, execution.GetWorkflowId())
			if err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeWorkflowExecutionTimedOut:
			if err := b.msBuilder.ReplicateWorkflowExecutionTimedoutEvent(firstEvent.GetEventId(), event); err != nil {
				return nil, nil, nil, err
			}

			err := b.appendTasksForFinishedExecutions(event, domainID, execution.GetWorkflowId())
			if err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeWorkflowExecutionCanceled:
			if err := b.msBuilder.ReplicateWorkflowExecutionCanceledEvent(firstEvent.GetEventId(), event); err != nil {
				return nil, nil, nil, err
			}

			err := b.appendTasksForFinishedExecutions(event, domainID, execution.GetWorkflowId())
			if err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeWorkflowExecutionTerminated:
			if err := b.msBuilder.ReplicateWorkflowExecutionTerminatedEvent(firstEvent.GetEventId(), event); err != nil {
				return nil, nil, nil, err
			}

			err := b.appendTasksForFinishedExecutions(event, domainID, execution.GetWorkflowId())
			if err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeUpsertWorkflowSearchAttributes:
			b.msBuilder.ReplicateUpsertWorkflowSearchAttributesEvent(event)
			b.transferTasks = append(b.transferTasks, b.scheduleUpsertSearchAttributesTask())

		case shared.EventTypeWorkflowExecutionContinuedAsNew:
			if len(newRunHistory) == 0 {
				return nil, nil, nil, errors.NewInternalFailureError(ErrMessageNewRunHistorySizeZero)
			}

			if newRunNDC {
				newRunMutableStateBuilder = newMutableStateBuilderWithVersionHistories(
					b.shard,
					b.shard.GetEventsCache(),
					b.logger,
					b.msBuilder.GetDomainEntry(),
				)
			} else {
				newRunMutableStateBuilder = newMutableStateBuilderWithReplicationState(
					b.shard,
					b.shard.GetEventsCache(),
					b.logger,
					b.msBuilder.GetDomainEntry(),
				)
			}
			newRunStateBuilder := newStateBuilder(b.shard, newRunMutableStateBuilder, b.logger)

			newRunID := event.WorkflowExecutionContinuedAsNewEventAttributes.GetNewExecutionRunId()
			newExecution := shared.WorkflowExecution{
				WorkflowId: execution.WorkflowId,
				RunId:      common.StringPtr(newRunID),
			}
			_, _, _, err := newRunStateBuilder.applyEvents(
				domainID,
				uuid.New(),
				newExecution,
				newRunHistory,
				nil,
				false,
			)
			if err != nil {
				return nil, nil, nil, err
			}

			b.newRunTransferTasks = append(b.newRunTransferTasks, newRunStateBuilder.getTransferTasks()...)
			b.newRunTimerTasks = append(b.newRunTimerTasks, newRunStateBuilder.getTimerTasks()...)

			err = b.msBuilder.ReplicateWorkflowExecutionContinuedAsNewEvent(
				firstEvent.GetEventId(),
				domainID,
				event,
			)
			if err != nil {
				return nil, nil, nil, err
			}

			// TODO the continue as new logic (task generation) is broken (including the active / existing code)
			// we should merge all task generation & persistence into one place
			// BTW, the newRunTransferTasks and newRunTimerTasks are not used

			err = b.appendTasksForFinishedExecutions(event, domainID, execution.GetWorkflowId())
			if err != nil {
				return nil, nil, nil, err
			}

		default:
			err := &shared.BadRequestError{Message: "Unknown event type"}
			return nil, nil, nil, err
		}
	}

	b.msBuilder.GetExecutionInfo().SetLastFirstEventID(firstEvent.GetEventId())
	b.msBuilder.GetExecutionInfo().SetNextEventID(lastEvent.GetEventId() + 1)

	b.msBuilder.SetHistoryBuilder(newHistoryBuilderFromEvents(history, b.logger))
	b.msBuilder.AddTransferTasks(b.transferTasks...)
	b.msBuilder.AddTimerTasks(b.timerTasks...)

	return lastEvent, lastDecision, newRunMutableStateBuilder, nil
}

// Transfer tasks

func (b *stateBuilderImpl) scheduleWorkflowStartTransferTask() persistence.Task {
	return &persistence.RecordWorkflowStartedTask{}
}

func (b *stateBuilderImpl) scheduleDecisionTransferTask(
	domainID string,
	tasklist string,
	scheduleID int64,
) persistence.Task {
	return &persistence.DecisionTask{
		DomainID:   domainID,
		TaskList:   tasklist,
		ScheduleID: scheduleID,
	}
}

func (b *stateBuilderImpl) scheduleActivityTransferTask(
	domainID string,
	tasklist string,
	scheduleID int64,
) persistence.Task {
	return &persistence.ActivityTask{
		DomainID:   domainID,
		TaskList:   tasklist,
		ScheduleID: scheduleID,
	}
}

func (b *stateBuilderImpl) scheduleStartChildWorkflowTransferTask(
	domainID string,
	workflowID string,
	initiatedID int64,
) persistence.Task {
	return &persistence.StartChildExecutionTask{
		TargetDomainID:   domainID,
		TargetWorkflowID: workflowID,
		InitiatedID:      initiatedID,
	}
}

func (b *stateBuilderImpl) scheduleCancelExternalWorkflowTransferTask(
	domainID string,
	workflowID string,
	runID string,
	childWorkflowOnly bool,
	initiatedID int64,
) persistence.Task {
	return &persistence.CancelExecutionTask{
		TargetDomainID:          domainID,
		TargetWorkflowID:        workflowID,
		TargetRunID:             runID,
		TargetChildWorkflowOnly: childWorkflowOnly,
		InitiatedID:             initiatedID,
	}
}

func (b *stateBuilderImpl) scheduleSignalWorkflowTransferTask(
	domainID string,
	workflowID string,
	runID string,
	childWorkflowOnly bool,
	initiatedID int64,
) persistence.Task {
	return &persistence.SignalExecutionTask{
		TargetDomainID:          domainID,
		TargetWorkflowID:        workflowID,
		TargetRunID:             runID,
		TargetChildWorkflowOnly: childWorkflowOnly,
		InitiatedID:             initiatedID,
	}
}

func (b *stateBuilderImpl) scheduleUpsertSearchAttributesTask() persistence.Task {
	return &persistence.UpsertWorkflowSearchAttributesTask{}
}

func (b *stateBuilderImpl) scheduleDeleteHistoryTransferTask() persistence.Task {
	return &persistence.CloseExecutionTask{}
}

// Timer tasks

func (b *stateBuilderImpl) scheduleDecisionTimerTask(
	event *shared.HistoryEvent,
	scheduleID int64,
	attempt int64,
	timeoutSecond int32,
) persistence.Task {
	return b.getTimerBuilder(event).AddStartToCloseDecisionTimoutTask(scheduleID, attempt, timeoutSecond)
}

func (b *stateBuilderImpl) scheduleUserTimerTask(
	event *shared.HistoryEvent,
	ti *persistence.TimerInfo,
	msBuilder mutableState,
) persistence.Task {
	timerBuilder := b.getTimerBuilder(event)
	return timerBuilder.GetUserTimerTaskIfNeeded(msBuilder)
}

func (b *stateBuilderImpl) refreshUserTimerTask(
	event *shared.HistoryEvent,
	msBuilder mutableState,
) persistence.Task {
	timerBuilder := b.getTimerBuilder(event)
	timerBuilder.loadUserTimers(msBuilder)
	return timerBuilder.GetUserTimerTaskIfNeeded(msBuilder)
}

func (b *stateBuilderImpl) scheduleActivityTimerTask(
	event *shared.HistoryEvent,
	msBuilder mutableState,
) persistence.Task {
	return b.getTimerBuilder(event).GetActivityTimerTaskIfNeeded(msBuilder)
}

func (b *stateBuilderImpl) scheduleWorkflowTimerTask(
	event *shared.HistoryEvent,
	msBuilder mutableState,
) []persistence.Task {
	timerTasks := []persistence.Task{}
	now := time.Unix(0, event.GetTimestamp())

	workflowTimeout := now.Add(time.Duration(msBuilder.GetExecutionInfo().WorkflowTimeout) * time.Second)

	startWorkflowAttribute := event.GetWorkflowExecutionStartedEventAttributes()
	firstDecisionTaskBackoffDuration := time.Duration(startWorkflowAttribute.GetFirstDecisionTaskBackoffSeconds()) * time.Second
	if firstDecisionTaskBackoffDuration != 0 {
		workflowTimeout = workflowTimeout.Add(firstDecisionTaskBackoffDuration)
		timeoutType := persistence.WorkflowBackoffTimeoutTypeRetry
		if startWorkflowAttribute.GetInitiator().Equals(shared.ContinueAsNewInitiatorCronSchedule) {
			timeoutType = persistence.WorkflowBackoffTimeoutTypeCron
		}
		timerTasks = append(timerTasks, &persistence.WorkflowBackoffTimerTask{
			VisibilityTimestamp: now.Add(firstDecisionTaskBackoffDuration),
			TimeoutType:         timeoutType,
		})
	}

	timerTasks = append(timerTasks, &persistence.WorkflowTimeoutTask{VisibilityTimestamp: workflowTimeout})
	return timerTasks
}

func (b *stateBuilderImpl) scheduleDeleteHistoryTimerTask(
	event *shared.HistoryEvent,
	domainID string,
	workflowID string,
) (persistence.Task, error) {
	var retentionInDays int32
	domainEntry, err := b.shard.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		if _, ok := err.(*shared.EntityNotExistsError); !ok {
			return nil, err
		}
	} else {
		retentionInDays = domainEntry.GetRetentionDays(workflowID)
	}
	return b.getTimerBuilder(event).createDeleteHistoryEventTimerTask(time.Duration(retentionInDays) * time.Hour * 24), nil
}

func (b *stateBuilderImpl) appendTasksForFinishedExecutions(
	event *shared.HistoryEvent,
	domainID string,
	workflowID string,
) error {
	b.transferTasks = append(b.transferTasks, b.scheduleDeleteHistoryTransferTask())
	timerTask, err := b.scheduleDeleteHistoryTimerTask(event, domainID, workflowID)
	if err != nil {
		return err
	}
	b.timerTasks = append(b.timerTasks, timerTask)
	return nil
}

func (b *stateBuilderImpl) getTaskList(
	msBuilder mutableState,
) string {
	// on the standby side, sticky tasklist is meaningless, so always use the normal tasklist
	return msBuilder.GetExecutionInfo().TaskList
}

func (b *stateBuilderImpl) getTimerBuilder(
	event *shared.HistoryEvent,
) *timerBuilder {
	timeSource := clock.NewEventTimeSource()
	now := time.Unix(0, event.GetTimestamp())
	timeSource.Update(now)

	return newTimerBuilder(timeSource)
}
