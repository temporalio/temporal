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
	"github.com/uber-common/bark"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/persistence"
)

type (
	stateBuilder interface {
		applyEvents(domainID, requestID string, execution shared.WorkflowExecution,
			history *shared.History, newRunHistory *shared.History) (*shared.HistoryEvent,
			*decisionInfo, mutableState, error)
		getTransferTasks() []persistence.Task
		getTimerTasks() []persistence.Task
		getNewRunTransferTasks() []persistence.Task
		getNewRunTimerTasks() []persistence.Task
	}

	stateBuilderImpl struct {
		shard           ShardContext
		clusterMetadata cluster.Metadata
		msBuilder       mutableState
		domainCache     cache.DomainCache
		logger          bark.Logger

		transferTasks       []persistence.Task
		timerTasks          []persistence.Task
		newRunTransferTasks []persistence.Task
		newRunTimerTasks    []persistence.Task
	}
)

func newStateBuilder(shard ShardContext, msBuilder mutableState, logger bark.Logger) *stateBuilderImpl {

	return &stateBuilderImpl{
		shard:           shard,
		clusterMetadata: shard.GetService().GetClusterMetadata(),
		msBuilder:       msBuilder,
		domainCache:     shard.GetDomainCache(),
		logger:          logger,
	}
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

func (b *stateBuilderImpl) applyEvents(domainID, requestID string, execution shared.WorkflowExecution,
	history *shared.History, newRunHistory *shared.History) (*shared.HistoryEvent,
	*decisionInfo, mutableState, error) {
	var lastEvent *shared.HistoryEvent
	var lastDecision *decisionInfo
	var newRunStateBuilder mutableState
	for _, event := range history.Events {
		lastEvent = event
		// must set the current version, since this is standby here, not active
		b.msBuilder.UpdateReplicationStateVersion(event.GetVersion(), true)
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
			b.msBuilder.ReplicateWorkflowExecutionStartedEvent(domainID, parentDomainID, execution, requestID, attributes)

			b.timerTasks = append(b.timerTasks, b.scheduleWorkflowTimerTask(event, b.msBuilder))

		case shared.EventTypeDecisionTaskScheduled:
			attributes := event.DecisionTaskScheduledEventAttributes
			di := b.msBuilder.ReplicateDecisionTaskScheduledEvent(event.GetVersion(), event.GetEventId(),
				attributes.TaskList.GetName(), attributes.GetStartToCloseTimeoutSeconds(), attributes.GetAttempt())

			b.transferTasks = append(b.transferTasks, b.scheduleDecisionTransferTask(domainID, b.getTaskList(b.msBuilder),
				di.ScheduleID))
			// since we do not use stickyness on the standby side, there shall be no decision schedule to start timeout

			lastDecision = di

		case shared.EventTypeDecisionTaskStarted:
			attributes := event.DecisionTaskStartedEventAttributes
			di := b.msBuilder.ReplicateDecisionTaskStartedEvent(nil, event.GetVersion(), attributes.GetScheduledEventId(),
				event.GetEventId(), attributes.GetRequestId(), event.GetTimestamp())

			b.timerTasks = append(b.timerTasks, b.scheduleDecisionTimerTask(event, di.ScheduleID, di.Attempt,
				di.DecisionTimeout))

			lastDecision = di

		case shared.EventTypeDecisionTaskCompleted:
			attributes := event.DecisionTaskCompletedEventAttributes
			b.msBuilder.ReplicateDecisionTaskCompletedEvent(attributes.GetScheduledEventId(),
				attributes.GetStartedEventId())

		case shared.EventTypeDecisionTaskTimedOut:
			attributes := event.DecisionTaskTimedOutEventAttributes
			b.msBuilder.ReplicateDecisionTaskTimedOutEvent(attributes.GetScheduledEventId(),
				attributes.GetStartedEventId())

		case shared.EventTypeDecisionTaskFailed:
			attributes := event.DecisionTaskFailedEventAttributes
			b.msBuilder.ReplicateDecisionTaskFailedEvent(attributes.GetScheduledEventId(),
				attributes.GetStartedEventId())

		case shared.EventTypeActivityTaskScheduled:
			ai := b.msBuilder.ReplicateActivityTaskScheduledEvent(event)

			b.transferTasks = append(b.transferTasks, b.scheduleActivityTransferTask(domainID, b.getTaskList(b.msBuilder),
				ai.ScheduleID))
			if timerTask := b.scheduleActivityTimerTask(event, b.msBuilder); timerTask != nil {
				b.timerTasks = append(b.timerTasks, timerTask)
			}

		case shared.EventTypeActivityTaskStarted:
			b.msBuilder.ReplicateActivityTaskStartedEvent(event)
			if timerTask := b.scheduleActivityTimerTask(event, b.msBuilder); timerTask != nil {
				b.timerTasks = append(b.timerTasks, timerTask)
			}

		case shared.EventTypeActivityTaskCompleted:
			if err := b.msBuilder.ReplicateActivityTaskCompletedEvent(event); err != nil {
				return nil, nil, nil, err
			}

		case shared.EventTypeActivityTaskFailed:
			b.msBuilder.ReplicateActivityTaskFailedEvent(event)

		case shared.EventTypeActivityTaskTimedOut:
			b.msBuilder.ReplicateActivityTaskTimedOutEvent(event)

		case shared.EventTypeActivityTaskCancelRequested:
			b.msBuilder.ReplicateActivityTaskCancelRequestedEvent(event)

		case shared.EventTypeActivityTaskCanceled:
			b.msBuilder.ReplicateActivityTaskCanceledEvent(event)

		case shared.EventTypeRequestCancelActivityTaskFailed:
			// No mutable state action is needed

		case shared.EventTypeTimerStarted:
			ti := b.msBuilder.ReplicateTimerStartedEvent(event)
			if timerTask := b.scheduleUserTimerTask(event, ti, b.msBuilder); timerTask != nil {
				b.timerTasks = append(b.timerTasks, timerTask)
			}

		case shared.EventTypeTimerFired:
			b.msBuilder.ReplicateTimerFiredEvent(event)

		case shared.EventTypeTimerCanceled:
			b.msBuilder.ReplicateTimerCanceledEvent(event)
			if timerTask := b.refreshUserTimerTask(event, b.msBuilder); timerTask != nil {
				b.timerTasks = append(b.timerTasks, timerTask)
			}

		case shared.EventTypeCancelTimerFailed:
			// No mutable state action is needed

		case shared.EventTypeStartChildWorkflowExecutionInitiated:
			// Create a new request ID which is used by transfer queue processor if domain is failed over at this point
			createRequestID := uuid.New()
			cei := b.msBuilder.ReplicateStartChildWorkflowExecutionInitiatedEvent(event, createRequestID)

			attributes := event.StartChildWorkflowExecutionInitiatedEventAttributes
			childDomainEntry, err := b.shard.GetDomainCache().GetDomain(attributes.GetDomain())
			if err != nil {
				return nil, nil, nil, err
			}
			b.transferTasks = append(b.transferTasks, b.scheduleStartChildWorkflowTransferTask(childDomainEntry.GetInfo().ID,
				attributes.GetWorkflowId(), cei.InitiatedID))

		case shared.EventTypeStartChildWorkflowExecutionFailed:
			b.msBuilder.ReplicateStartChildWorkflowExecutionFailedEvent(event)

		case shared.EventTypeChildWorkflowExecutionStarted:
			b.msBuilder.ReplicateChildWorkflowExecutionStartedEvent(event)

		case shared.EventTypeChildWorkflowExecutionCompleted:
			b.msBuilder.ReplicateChildWorkflowExecutionCompletedEvent(event)

		case shared.EventTypeChildWorkflowExecutionFailed:
			b.msBuilder.ReplicateChildWorkflowExecutionFailedEvent(event)

		case shared.EventTypeChildWorkflowExecutionCanceled:
			b.msBuilder.ReplicateChildWorkflowExecutionCanceledEvent(event)

		case shared.EventTypeChildWorkflowExecutionTimedOut:
			b.msBuilder.ReplicateChildWorkflowExecutionTimedOutEvent(event)

		case shared.EventTypeChildWorkflowExecutionTerminated:
			b.msBuilder.ReplicateChildWorkflowExecutionTerminatedEvent(event)

		case shared.EventTypeRequestCancelExternalWorkflowExecutionInitiated:
			// Create a new request ID which is used by transfer queue processor if domain is failed over at this point
			cancelRequestID := uuid.New()
			rci := b.msBuilder.ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(event, cancelRequestID)

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
			b.msBuilder.ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(event)

		case shared.EventTypeExternalWorkflowExecutionCancelRequested:
			b.msBuilder.ReplicateExternalWorkflowExecutionCancelRequested(event)

		case shared.EventTypeSignalExternalWorkflowExecutionInitiated:
			// Create a new request ID which is used by transfer queue processor if domain is failed over at this point
			signalRequestID := uuid.New()
			si := b.msBuilder.ReplicateSignalExternalWorkflowExecutionInitiatedEvent(event, signalRequestID)

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
			b.msBuilder.ReplicateSignalExternalWorkflowExecutionFailedEvent(event)

		case shared.EventTypeExternalWorkflowExecutionSignaled:
			b.msBuilder.ReplicateExternalWorkflowExecutionSignaled(event)

		case shared.EventTypeMarkerRecorded:
			// No mutable state action is needed

		case shared.EventTypeWorkflowExecutionSignaled:
			// No mutable state action is needed

		case shared.EventTypeWorkflowExecutionCancelRequested:
			b.msBuilder.ReplicateWorkflowExecutionCancelRequestedEvent(event)

		case shared.EventTypeWorkflowExecutionCompleted:
			b.msBuilder.ReplicateWorkflowExecutionCompletedEvent(event)
			b.transferTasks = append(b.transferTasks, b.scheduleDeleteHistoryTransferTask())
			timerTask, err := b.scheduleDeleteHistoryTimerTask(event, domainID)
			if err != nil {
				return nil, nil, nil, err
			}
			b.timerTasks = append(b.timerTasks, timerTask)

		case shared.EventTypeWorkflowExecutionFailed:
			b.msBuilder.ReplicateWorkflowExecutionFailedEvent(event)
			b.transferTasks = append(b.transferTasks, b.scheduleDeleteHistoryTransferTask())
			timerTask, err := b.scheduleDeleteHistoryTimerTask(event, domainID)
			if err != nil {
				return nil, nil, nil, err
			}
			b.timerTasks = append(b.timerTasks, timerTask)

		case shared.EventTypeWorkflowExecutionTimedOut:
			b.msBuilder.ReplicateWorkflowExecutionTimedoutEvent(event)
			b.transferTasks = append(b.transferTasks, b.scheduleDeleteHistoryTransferTask())
			timerTask, err := b.scheduleDeleteHistoryTimerTask(event, domainID)
			if err != nil {
				return nil, nil, nil, err
			}
			b.timerTasks = append(b.timerTasks, timerTask)

		case shared.EventTypeWorkflowExecutionCanceled:
			b.msBuilder.ReplicateWorkflowExecutionCanceledEvent(event)
			b.transferTasks = append(b.transferTasks, b.scheduleDeleteHistoryTransferTask())
			timerTask, err := b.scheduleDeleteHistoryTimerTask(event, domainID)
			if err != nil {
				return nil, nil, nil, err
			}
			b.timerTasks = append(b.timerTasks, timerTask)

		case shared.EventTypeWorkflowExecutionTerminated:
			b.msBuilder.ReplicateWorkflowExecutionTerminatedEvent(event)
			b.transferTasks = append(b.transferTasks, b.scheduleDeleteHistoryTransferTask())
			timerTask, err := b.scheduleDeleteHistoryTimerTask(event, domainID)
			if err != nil {
				return nil, nil, nil, err
			}
			b.timerTasks = append(b.timerTasks, timerTask)

		case shared.EventTypeWorkflowExecutionContinuedAsNew:
			// ContinuedAsNew event also has history for first 2 events for next run as they are created transactionally
			startedEvent := newRunHistory.Events[0]
			startedAttributes := startedEvent.WorkflowExecutionStartedEventAttributes

			// History event only have the parentDomainName.  Lookup the domain ID from cache
			var parentDomainID *string
			if startedAttributes.ParentWorkflowDomain != nil {
				parentDomainEntry, err := b.domainCache.GetDomain(startedAttributes.GetParentWorkflowDomain())
				if err != nil {
					return nil, nil, nil, err
				}
				parentDomainID = &parentDomainEntry.GetInfo().ID
			}

			newRunID := event.WorkflowExecutionContinuedAsNewEventAttributes.GetNewExecutionRunId()

			newExecution := shared.WorkflowExecution{
				WorkflowId: execution.WorkflowId,
				RunId:      common.StringPtr(newRunID),
			}
			// Create mutable state updates for the new run
			newRunStateBuilder = newMutableStateBuilderWithReplicationState(
				b.clusterMetadata.GetCurrentClusterName(),
				b.shard.GetConfig(),
				b.logger,
				startedEvent.GetVersion(),
			)
			newRunStateBuilder.ReplicateWorkflowExecutionStartedEvent(domainID, parentDomainID, newExecution, uuid.New(),
				startedAttributes)

			var di *decisionInfo
			nextEventID := startedEvent.GetEventId() + 1
			if len(newRunHistory.Events) > 1 {
				dtScheduledEvent := newRunHistory.Events[1]
				di = newRunStateBuilder.ReplicateDecisionTaskScheduledEvent(
					dtScheduledEvent.GetVersion(),
					dtScheduledEvent.GetEventId(),
					dtScheduledEvent.DecisionTaskScheduledEventAttributes.TaskList.GetName(),
					dtScheduledEvent.DecisionTaskScheduledEventAttributes.GetStartToCloseTimeoutSeconds(),
					dtScheduledEvent.DecisionTaskScheduledEventAttributes.GetAttempt(),
				)
				nextEventID = di.ScheduleID + 1
			}
			newRunExecutionInfo := newRunStateBuilder.GetExecutionInfo()
			newRunExecutionInfo.NextEventID = nextEventID
			newRunExecutionInfo.LastFirstEventID = startedEvent.GetEventId()
			// Set the history from replication task on the newStateBuilder
			newRunStateBuilder.SetHistoryBuilder(newHistoryBuilderFromEvents(newRunHistory.Events, b.logger))
			sourceClusterName := b.clusterMetadata.ClusterNameForFailoverVersion(startedEvent.GetVersion())
			newRunStateBuilder.UpdateReplicationStateLastEventID(sourceClusterName, startedEvent.GetVersion(), nextEventID-1)

			if startedAttributes.GetAttempt() == 0 {
				b.newRunTransferTasks = append(b.newRunTransferTasks, b.scheduleDecisionTransferTask(domainID,
					b.getTaskList(newRunStateBuilder), nextEventID-1))
			}
			b.newRunTimerTasks = append(b.newRunTimerTasks, b.scheduleWorkflowTimerTask(event, newRunStateBuilder))

			b.msBuilder.ReplicateWorkflowExecutionContinuedAsNewEvent(sourceClusterName, domainID, event,
				startedEvent, di, newRunStateBuilder)

			// TODO the continue as new logic (task generation) is broken (including the active / existing code)
			// we should merge all task generation & persistence into one place
			// BTW, the newRunTransferTasks and newRunTimerTasks are not used

			b.transferTasks = append(b.transferTasks, b.scheduleDeleteHistoryTransferTask())
			timerTask, err := b.scheduleDeleteHistoryTimerTask(event, domainID)
			if err != nil {
				return nil, nil, nil, err
			}
			b.timerTasks = append(b.timerTasks, timerTask)
		}
	}

	return lastEvent, lastDecision, newRunStateBuilder, nil
}

func (b *stateBuilderImpl) scheduleDecisionTransferTask(domainID string, tasklist string,
	scheduleID int64) persistence.Task {
	return &persistence.DecisionTask{
		DomainID:   domainID,
		TaskList:   tasklist,
		ScheduleID: scheduleID,
	}
}

func (b *stateBuilderImpl) scheduleActivityTransferTask(domainID string, tasklist string,
	scheduleID int64) persistence.Task {
	return &persistence.ActivityTask{
		DomainID:   domainID,
		TaskList:   tasklist,
		ScheduleID: scheduleID,
	}
}

func (b *stateBuilderImpl) scheduleStartChildWorkflowTransferTask(domainID string, workflowID string,
	initiatedID int64) persistence.Task {
	return &persistence.StartChildExecutionTask{
		TargetDomainID:   domainID,
		TargetWorkflowID: workflowID,
		InitiatedID:      initiatedID,
	}
}

func (b *stateBuilderImpl) scheduleCancelExternalWorkflowTransferTask(domainID string, workflowID string,
	runID string, childWorkflowOnly bool, initiatedID int64) persistence.Task {
	return &persistence.CancelExecutionTask{
		TargetDomainID:          domainID,
		TargetWorkflowID:        workflowID,
		TargetRunID:             runID,
		TargetChildWorkflowOnly: childWorkflowOnly,
		InitiatedID:             initiatedID,
	}
}

func (b *stateBuilderImpl) scheduleSignalWorkflowTransferTask(domainID string, workflowID string,
	runID string, childWorkflowOnly bool, initiatedID int64) persistence.Task {
	return &persistence.SignalExecutionTask{
		TargetDomainID:          domainID,
		TargetWorkflowID:        workflowID,
		TargetRunID:             runID,
		TargetChildWorkflowOnly: childWorkflowOnly,
		InitiatedID:             initiatedID,
	}
}

func (b *stateBuilderImpl) scheduleDeleteHistoryTransferTask() persistence.Task {
	return &persistence.CloseExecutionTask{}
}

func (b *stateBuilderImpl) scheduleDecisionTimerTask(event *shared.HistoryEvent, scheduleID int64, attempt int64,
	timeoutSecond int32) persistence.Task {
	return b.getTimerBuilder(event).AddStartToCloseDecisionTimoutTask(scheduleID, attempt, timeoutSecond)
}

func (b *stateBuilderImpl) scheduleUserTimerTask(event *shared.HistoryEvent,
	ti *persistence.TimerInfo, msBuilder mutableState) persistence.Task {
	timerBuilder := b.getTimerBuilder(event)
	timerBuilder.AddUserTimer(ti, msBuilder)
	return timerBuilder.GetUserTimerTaskIfNeeded(msBuilder)
}

func (b *stateBuilderImpl) refreshUserTimerTask(event *shared.HistoryEvent, msBuilder mutableState) persistence.Task {
	timerBuilder := b.getTimerBuilder(event)
	timerBuilder.loadUserTimers(msBuilder)
	return timerBuilder.GetUserTimerTaskIfNeeded(msBuilder)
}

func (b *stateBuilderImpl) scheduleActivityTimerTask(event *shared.HistoryEvent,
	msBuilder mutableState) persistence.Task {
	return b.getTimerBuilder(event).GetActivityTimerTaskIfNeeded(msBuilder)
}

func (b *stateBuilderImpl) scheduleWorkflowTimerTask(event *shared.HistoryEvent,
	msBuilder mutableState) persistence.Task {
	now := time.Unix(0, event.GetTimestamp())
	timeout := now.Add(time.Duration(msBuilder.GetExecutionInfo().WorkflowTimeout) * time.Second)
	return &persistence.WorkflowTimeoutTask{VisibilityTimestamp: timeout}
}

func (b *stateBuilderImpl) scheduleDeleteHistoryTimerTask(event *shared.HistoryEvent, domainID string) (persistence.Task, error) {
	var retentionInDays int32
	domainEntry, err := b.shard.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		if _, ok := err.(*shared.EntityNotExistsError); !ok {
			return nil, err
		}
	} else {
		retentionInDays = domainEntry.GetConfig().Retention
	}
	return b.getTimerBuilder(event).createDeleteHistoryEventTimerTask(time.Duration(retentionInDays) * time.Hour * 24), nil
}

func (b *stateBuilderImpl) getTaskList(msBuilder mutableState) string {
	// on the standby side, sticky tasklist is meaningless, so always use the normal tasklist
	return msBuilder.GetExecutionInfo().TaskList
}

func (b *stateBuilderImpl) getTimerBuilder(event *shared.HistoryEvent) *timerBuilder {
	timeSource := common.NewEventTimeSource()
	now := time.Unix(0, event.GetTimestamp())
	timeSource.Update(now)
	return newTimerBuilder(b.shard.GetConfig(), b.logger, timeSource)
}
