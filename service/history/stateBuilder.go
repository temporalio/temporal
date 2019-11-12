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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination stateBuilder_mock.go

package history

import (
	"time"

	"github.com/pborman/uuid"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/errors"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
)

type (
	taskGeneratorProvider func(mutableState) mutableStateTaskGenerator

	stateBuilder interface {
		applyEvents(
			domainID string,
			requestID string,
			execution shared.WorkflowExecution,
			history []*shared.HistoryEvent,
			newRunHistory []*shared.HistoryEvent,
			newRunNDC bool,
		) (mutableState, error)
	}

	stateBuilderImpl struct {
		shard           ShardContext
		clusterMetadata cluster.Metadata
		domainCache     cache.DomainCache
		logger          log.Logger

		mutableState          mutableState
		taskGeneratorProvider taskGeneratorProvider
	}
)

const (
	// ErrMessageHistorySizeZero indicate that history is empty
	ErrMessageHistorySizeZero = "encounter history size being zero"
)

var _ stateBuilder = (*stateBuilderImpl)(nil)

func newStateBuilder(
	shard ShardContext,
	logger log.Logger,
	mutableState mutableState,
	taskGeneratorProvider taskGeneratorProvider,
) *stateBuilderImpl {

	return &stateBuilderImpl{
		shard:                 shard,
		clusterMetadata:       shard.GetService().GetClusterMetadata(),
		domainCache:           shard.GetDomainCache(),
		logger:                logger,
		mutableState:          mutableState,
		taskGeneratorProvider: taskGeneratorProvider,
	}
}

func (b *stateBuilderImpl) applyEvents(
	domainID string,
	requestID string,
	execution shared.WorkflowExecution,
	history []*shared.HistoryEvent,
	newRunHistory []*shared.HistoryEvent,
	newRunNDC bool,
) (mutableState, error) {

	if len(history) == 0 {
		return nil, errors.NewInternalFailureError(ErrMessageHistorySizeZero)
	}
	firstEvent := history[0]
	lastEvent := history[len(history)-1]
	var newRunMutableStateBuilder mutableState

	taskGenerator := b.taskGeneratorProvider(b.mutableState)

	// need to clear the stickiness since workflow turned to passive
	b.mutableState.ClearStickyness()

	for _, event := range history {
		// NOTE: stateBuilder is also being used in the active side
		if b.mutableState.GetReplicationState() != nil {
			// this function must be called within the for loop, in case
			// history event version changed during for loop
			b.mutableState.UpdateReplicationStateVersion(event.GetVersion(), true)
			b.mutableState.UpdateReplicationStateLastEventID(lastEvent.GetVersion(), lastEvent.GetEventId())
		} else if b.mutableState.GetVersionHistories() != nil {
			if err := b.mutableState.UpdateCurrentVersion(event.GetVersion(), true); err != nil {
				return nil, err
			}
			versionHistories := b.mutableState.GetVersionHistories()
			versionHistory, err := versionHistories.GetCurrentVersionHistory()
			if err != nil {
				return nil, err
			}
			if err := versionHistory.AddOrUpdateItem(persistence.NewVersionHistoryItem(
				event.GetEventId(),
				event.GetVersion(),
			)); err != nil {
				return nil, err
			}
		}
		b.mutableState.GetExecutionInfo().LastEventTaskID = event.GetTaskId()

		switch event.GetEventType() {
		case shared.EventTypeWorkflowExecutionStarted:
			attributes := event.WorkflowExecutionStartedEventAttributes
			var parentDomainID *string
			if attributes.ParentWorkflowDomain != nil {
				parentDomainEntry, err := b.domainCache.GetDomain(
					attributes.GetParentWorkflowDomain(),
				)
				if err != nil {
					return nil, err
				}
				parentDomainID = &parentDomainEntry.GetInfo().ID
			}

			if err := b.mutableState.ReplicateWorkflowExecutionStartedEvent(
				parentDomainID,
				execution,
				requestID,
				event,
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.generateRecordWorkflowStartedTasks(
				b.unixNanoToTime(event.GetTimestamp()),
				event,
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.generateWorkflowStartTasks(
				b.unixNanoToTime(event.GetTimestamp()),
				event,
			); err != nil {
				return nil, err
			}

			if attributes.GetFirstDecisionTaskBackoffSeconds() > 0 {
				if err := taskGenerator.generateDelayedDecisionTasks(
					b.unixNanoToTime(event.GetTimestamp()),
					event,
				); err != nil {
					return nil, err
				}
			}

			if err := b.mutableState.SetHistoryTree(
				execution.GetRunId(),
			); err != nil {
				return nil, err
			}

			// TODO remove after NDC is fully migrated
			if b.mutableState.GetReplicationState() != nil {
				b.mutableState.GetReplicationState().StartVersion = event.GetVersion()
			}

		case shared.EventTypeDecisionTaskScheduled:
			attributes := event.DecisionTaskScheduledEventAttributes
			// use event.GetTimestamp() as DecisionOriginalScheduledTimestamp, because the heartbeat is not happening here.
			decision, err := b.mutableState.ReplicateDecisionTaskScheduledEvent(
				event.GetVersion(),
				event.GetEventId(),
				attributes.TaskList.GetName(),
				attributes.GetStartToCloseTimeoutSeconds(),
				attributes.GetAttempt(),
				event.GetTimestamp(),
				event.GetTimestamp(),
			)
			if err != nil {
				return nil, err
			}

			// since we do not use stickiness on the standby side
			// there shall be no decision schedule to start timeout
			// NOTE: at the beginning of the loop, stickyness is cleared
			if err := taskGenerator.generateDecisionScheduleTasks(
				b.unixNanoToTime(event.GetTimestamp()),
				decision.ScheduleID,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeDecisionTaskStarted:
			attributes := event.DecisionTaskStartedEventAttributes
			decision, err := b.mutableState.ReplicateDecisionTaskStartedEvent(
				nil,
				event.GetVersion(),
				attributes.GetScheduledEventId(),
				event.GetEventId(),
				attributes.GetRequestId(),
				event.GetTimestamp(),
			)
			if err != nil {
				return nil, err
			}

			if err := taskGenerator.generateDecisionStartTasks(
				b.unixNanoToTime(event.GetTimestamp()),
				decision.ScheduleID,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeDecisionTaskCompleted:
			if err := b.mutableState.ReplicateDecisionTaskCompletedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeDecisionTaskTimedOut:
			if err := b.mutableState.ReplicateDecisionTaskTimedOutEvent(
				event.DecisionTaskTimedOutEventAttributes.GetTimeoutType(),
			); err != nil {
				return nil, err
			}

			// this is for transient decision
			decision, err := b.mutableState.ReplicateTransientDecisionTaskScheduled()
			if err != nil {
				return nil, err
			}

			if decision != nil {
				// since we do not use stickiness on the standby side
				// there shall be no decision schedule to start timeout
				// NOTE: at the beginning of the loop, stickyness is cleared
				if err := taskGenerator.generateDecisionScheduleTasks(
					b.unixNanoToTime(event.GetTimestamp()),
					decision.ScheduleID,
				); err != nil {
					return nil, err
				}
			}

		case shared.EventTypeDecisionTaskFailed:
			if err := b.mutableState.ReplicateDecisionTaskFailedEvent(); err != nil {
				return nil, err
			}

			// this is for transient decision
			decision, err := b.mutableState.ReplicateTransientDecisionTaskScheduled()
			if err != nil {
				return nil, err
			}

			if decision != nil {
				// since we do not use stickiness on the standby side
				// there shall be no decision schedule to start timeout
				// NOTE: at the beginning of the loop, stickyness is cleared
				if err := taskGenerator.generateDecisionScheduleTasks(
					b.unixNanoToTime(event.GetTimestamp()),
					decision.ScheduleID,
				); err != nil {
					return nil, err
				}
			}

		case shared.EventTypeActivityTaskScheduled:
			if _, err := b.mutableState.ReplicateActivityTaskScheduledEvent(
				firstEvent.GetEventId(),
				event,
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.generateActivityTransferTasks(
				b.unixNanoToTime(event.GetTimestamp()),
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeActivityTaskStarted:
			if err := b.mutableState.ReplicateActivityTaskStartedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeActivityTaskCompleted:
			if err := b.mutableState.ReplicateActivityTaskCompletedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeActivityTaskFailed:
			if err := b.mutableState.ReplicateActivityTaskFailedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeActivityTaskTimedOut:
			if err := b.mutableState.ReplicateActivityTaskTimedOutEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeActivityTaskCancelRequested:
			if err := b.mutableState.ReplicateActivityTaskCancelRequestedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeActivityTaskCanceled:
			if err := b.mutableState.ReplicateActivityTaskCanceledEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeRequestCancelActivityTaskFailed:
			// No mutable state action is needed

		case shared.EventTypeTimerStarted:
			if _, err := b.mutableState.ReplicateTimerStartedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeTimerFired:
			if err := b.mutableState.ReplicateTimerFiredEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeTimerCanceled:
			if err := b.mutableState.ReplicateTimerCanceledEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeCancelTimerFailed:
			// no mutable state action is needed

		case shared.EventTypeStartChildWorkflowExecutionInitiated:
			if _, err := b.mutableState.ReplicateStartChildWorkflowExecutionInitiatedEvent(
				firstEvent.GetEventId(),
				event,
				// create a new request ID which is used by transfer queue processor
				// if domain is failed over at this point
				uuid.New(),
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.generateChildWorkflowTasks(
				b.unixNanoToTime(event.GetTimestamp()),
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeStartChildWorkflowExecutionFailed:
			if err := b.mutableState.ReplicateStartChildWorkflowExecutionFailedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeChildWorkflowExecutionStarted:
			if err := b.mutableState.ReplicateChildWorkflowExecutionStartedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeChildWorkflowExecutionCompleted:
			if err := b.mutableState.ReplicateChildWorkflowExecutionCompletedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeChildWorkflowExecutionFailed:
			if err := b.mutableState.ReplicateChildWorkflowExecutionFailedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeChildWorkflowExecutionCanceled:
			if err := b.mutableState.ReplicateChildWorkflowExecutionCanceledEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeChildWorkflowExecutionTimedOut:
			if err := b.mutableState.ReplicateChildWorkflowExecutionTimedOutEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeChildWorkflowExecutionTerminated:
			if err := b.mutableState.ReplicateChildWorkflowExecutionTerminatedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeRequestCancelExternalWorkflowExecutionInitiated:
			if _, err := b.mutableState.ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(
				firstEvent.GetEventId(),
				event,
				// create a new request ID which is used by transfer queue processor
				// if domain is failed over at this point
				uuid.New(),
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.generateRequestCancelExternalTasks(
				b.unixNanoToTime(event.GetTimestamp()),
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeRequestCancelExternalWorkflowExecutionFailed:
			if err := b.mutableState.ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeExternalWorkflowExecutionCancelRequested:
			if err := b.mutableState.ReplicateExternalWorkflowExecutionCancelRequested(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeSignalExternalWorkflowExecutionInitiated:
			// Create a new request ID which is used by transfer queue processor if domain is failed over at this point
			signalRequestID := uuid.New()
			if _, err := b.mutableState.ReplicateSignalExternalWorkflowExecutionInitiatedEvent(
				firstEvent.GetEventId(),
				event,
				signalRequestID,
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.generateSignalExternalTasks(
				b.unixNanoToTime(event.GetTimestamp()),
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeSignalExternalWorkflowExecutionFailed:
			if err := b.mutableState.ReplicateSignalExternalWorkflowExecutionFailedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeExternalWorkflowExecutionSignaled:
			if err := b.mutableState.ReplicateExternalWorkflowExecutionSignaled(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeMarkerRecorded:
			// No mutable state action is needed

		case shared.EventTypeWorkflowExecutionSignaled:
			if err := b.mutableState.ReplicateWorkflowExecutionSignaled(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeWorkflowExecutionCancelRequested:
			if err := b.mutableState.ReplicateWorkflowExecutionCancelRequestedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case shared.EventTypeUpsertWorkflowSearchAttributes:
			b.mutableState.ReplicateUpsertWorkflowSearchAttributesEvent(event)
			if err := taskGenerator.generateWorkflowSearchAttrTasks(
				b.unixNanoToTime(event.GetTimestamp()),
			); err != nil {
				return nil, err
			}

		case shared.EventTypeWorkflowExecutionCompleted:
			if err := b.mutableState.ReplicateWorkflowExecutionCompletedEvent(
				firstEvent.GetEventId(),
				event,
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.generateWorkflowCloseTasks(
				b.unixNanoToTime(event.GetTimestamp()),
			); err != nil {
				return nil, err
			}

		case shared.EventTypeWorkflowExecutionFailed:
			if err := b.mutableState.ReplicateWorkflowExecutionFailedEvent(
				firstEvent.GetEventId(),
				event,
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.generateWorkflowCloseTasks(
				b.unixNanoToTime(event.GetTimestamp()),
			); err != nil {
				return nil, err
			}

		case shared.EventTypeWorkflowExecutionTimedOut:
			if err := b.mutableState.ReplicateWorkflowExecutionTimedoutEvent(
				firstEvent.GetEventId(),
				event,
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.generateWorkflowCloseTasks(
				b.unixNanoToTime(event.GetTimestamp()),
			); err != nil {
				return nil, err
			}

		case shared.EventTypeWorkflowExecutionCanceled:
			if err := b.mutableState.ReplicateWorkflowExecutionCanceledEvent(
				firstEvent.GetEventId(),
				event,
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.generateWorkflowCloseTasks(
				b.unixNanoToTime(event.GetTimestamp()),
			); err != nil {
				return nil, err
			}

		case shared.EventTypeWorkflowExecutionTerminated:
			if err := b.mutableState.ReplicateWorkflowExecutionTerminatedEvent(
				firstEvent.GetEventId(),
				event,
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.generateWorkflowCloseTasks(
				b.unixNanoToTime(event.GetTimestamp()),
			); err != nil {
				return nil, err
			}

		case shared.EventTypeWorkflowExecutionContinuedAsNew:

			// The length of newRunHistory can be zero in resend case
			if len(newRunHistory) != 0 {
				if newRunNDC {
					newRunMutableStateBuilder = newMutableStateBuilderWithVersionHistories(
						b.shard,
						b.shard.GetEventsCache(),
						b.logger,
						b.mutableState.GetDomainEntry(),
					)
				} else {
					newRunMutableStateBuilder = newMutableStateBuilderWithReplicationState(
						b.shard,
						b.shard.GetEventsCache(),
						b.logger,
						b.mutableState.GetDomainEntry(),
					)
				}
				newRunStateBuilder := newStateBuilder(b.shard, b.logger, newRunMutableStateBuilder, b.taskGeneratorProvider)

				newRunID := event.WorkflowExecutionContinuedAsNewEventAttributes.GetNewExecutionRunId()
				newExecution := shared.WorkflowExecution{
					WorkflowId: execution.WorkflowId,
					RunId:      common.StringPtr(newRunID),
				}
				_, err := newRunStateBuilder.applyEvents(
					domainID,
					uuid.New(),
					newExecution,
					newRunHistory,
					nil,
					false,
				)
				if err != nil {
					return nil, err
				}
			}

			err := b.mutableState.ReplicateWorkflowExecutionContinuedAsNewEvent(
				firstEvent.GetEventId(),
				domainID,
				event,
			)
			if err != nil {
				return nil, err
			}

			if err := taskGenerator.generateWorkflowCloseTasks(
				b.unixNanoToTime(event.GetTimestamp()),
			); err != nil {
				return nil, err
			}

		default:
			return nil, &shared.BadRequestError{Message: "Unknown event type"}
		}
	}

	// must generate the activity timer / user timer at the very end
	if err := taskGenerator.generateActivityTimerTasks(
		b.unixNanoToTime(lastEvent.GetTimestamp()),
	); err != nil {
		return nil, err
	}
	if err := taskGenerator.generateUserTimerTasks(
		b.unixNanoToTime(lastEvent.GetTimestamp()),
	); err != nil {
		return nil, err
	}

	b.mutableState.GetExecutionInfo().SetLastFirstEventID(firstEvent.GetEventId())
	b.mutableState.GetExecutionInfo().SetNextEventID(lastEvent.GetEventId() + 1)

	b.mutableState.SetHistoryBuilder(newHistoryBuilderFromEvents(history, b.logger))

	return newRunMutableStateBuilder, nil
}

func (b *stateBuilderImpl) unixNanoToTime(
	unixNano int64,
) time.Time {

	return time.Unix(0, unixNano)
}
