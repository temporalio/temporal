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

// Code generated: TODO put <- here to avoid linter, this file need to be rewritten

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination mutable_state_rebuilder_mock.go

package workflow

import (
	"context"
	"fmt"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/historybuilder"
	"go.temporal.io/server/service/history/shard"
)

type (
	MutableStateRebuilder interface {
		ApplyEvents(
			ctx context.Context,
			namespaceID namespace.ID,
			requestID string,
			execution *commonpb.WorkflowExecution,
			history [][]*historypb.HistoryEvent,
			newRunHistory []*historypb.HistoryEvent,
			newRunID string,
		) (MutableState, error)
	}

	MutableStateRebuilderImpl struct {
		shard             shard.Context
		clusterMetadata   cluster.Metadata
		namespaceRegistry namespace.Registry
		logger            log.Logger

		mutableState MutableState
	}
)

const (
	// ErrMessageHistorySizeZero indicate that history is empty
	ErrMessageHistorySizeZero = "encounter history size being zero"
)

var _ MutableStateRebuilder = (*MutableStateRebuilderImpl)(nil)

func NewMutableStateRebuilder(
	shard shard.Context,
	logger log.Logger,
	mutableState MutableState,
) *MutableStateRebuilderImpl {

	return &MutableStateRebuilderImpl{
		shard:             shard,
		clusterMetadata:   shard.GetClusterMetadata(),
		namespaceRegistry: shard.GetNamespaceRegistry(),
		logger:            logger,
		mutableState:      mutableState,
	}
}

func (b *MutableStateRebuilderImpl) ApplyEvents(
	ctx context.Context,
	namespaceID namespace.ID,
	requestID string,
	execution *commonpb.WorkflowExecution,
	history [][]*historypb.HistoryEvent,
	newRunHistory []*historypb.HistoryEvent,
	newRunID string,
) (MutableState, error) {
	for i := 0; i < len(history)-1; i++ {
		_, err := b.applyEvents(ctx, namespaceID, requestID, execution, history[i], nil, "")
		if err != nil {
			return nil, err
		}
	}
	newMutableState, err := b.applyEvents(ctx, namespaceID, requestID, execution, history[len(history)-1], newRunHistory, newRunID)
	if err != nil {
		return nil, err
	}
	// must generate the activity timer / user timer at the very end
	taskGenerator := taskGeneratorProvider.NewTaskGenerator(b.shard, b.mutableState)
	if err := taskGenerator.GenerateActivityTimerTasks(); err != nil {
		return nil, err
	}
	if err := taskGenerator.GenerateUserTimerTasks(); err != nil {
		return nil, err
	}
	b.mutableState.SetHistoryBuilder(historybuilder.NewImmutable(history...))
	return newMutableState, nil
}

func (b *MutableStateRebuilderImpl) applyEvents(
	ctx context.Context,
	namespaceID namespace.ID,
	requestID string,
	execution *commonpb.WorkflowExecution,
	history []*historypb.HistoryEvent,
	newRunHistory []*historypb.HistoryEvent,
	newRunID string,
) (MutableState, error) {

	if len(history) == 0 {
		return nil, serviceerror.NewInternal(ErrMessageHistorySizeZero)
	}
	firstEvent := history[0]
	lastEvent := history[len(history)-1]

	taskGenerator := taskGeneratorProvider.NewTaskGenerator(b.shard, b.mutableState)

	// Need to clear the sticky task queue because workflow turned to passive.
	b.mutableState.ClearStickyTaskQueue()
	executionInfo := b.mutableState.GetExecutionInfo()
	executionInfo.LastFirstEventId = firstEvent.GetEventId()

	// NOTE: stateRebuilder is also being used in the active side
	if err := b.mutableState.UpdateCurrentVersion(lastEvent.GetVersion(), true); err != nil {
		return nil, err
	}
	versionHistories := executionInfo.GetVersionHistories()
	versionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	if err != nil {
		return nil, err
	}
	if err := versionhistory.AddOrUpdateVersionHistoryItem(versionHistory, versionhistory.NewVersionHistoryItem(
		lastEvent.GetEventId(),
		lastEvent.GetVersion(),
	)); err != nil {
		return nil, err
	}
	executionInfo.LastEventTaskId = lastEvent.GetTaskId()

	for _, event := range history {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
			attributes := event.GetWorkflowExecutionStartedEventAttributes()
			// TODO (alex): ParentWorkflowNamespaceId is back filled. Backward compatibility: old event doesn't have ParentNamespaceId set.
			if attributes.GetParentWorkflowNamespaceId() == "" && attributes.GetParentWorkflowNamespace() != "" {
				parentNamespaceEntry, err := b.namespaceRegistry.GetNamespace(namespace.Name(attributes.GetParentWorkflowNamespace()))
				if err != nil {
					return nil, err
				}
				attributes.ParentWorkflowNamespaceId = parentNamespaceEntry.ID().String()
			}

			if err := b.mutableState.ApplyWorkflowExecutionStartedEvent(
				nil, // shard clock is local to cluster
				execution,
				requestID,
				event,
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.GenerateRecordWorkflowStartedTasks(
				event,
			); err != nil {
				return nil, err
			}

			var err error
			executionInfo.WorkflowExecutionTimerTaskStatus, err = taskGenerator.GenerateWorkflowStartTasks(
				event,
			)
			if err != nil {
				return nil, err
			}

			if timestamp.DurationValue(attributes.GetFirstWorkflowTaskBackoff()) > 0 {
				if err := taskGenerator.GenerateDelayedWorkflowTasks(
					event,
				); err != nil {
					return nil, err
				}
			}

			if err := b.mutableState.SetHistoryTree(
				executionInfo.WorkflowExecutionTimeout,
				executionInfo.WorkflowRunTimeout,
				execution.GetRunId(),
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
			attributes := event.GetWorkflowTaskScheduledEventAttributes()
			// use event.GetEventTime() as WorkflowTaskOriginalScheduledTimestamp, because the heartbeat is not happening here.
			workflowTask, err := b.mutableState.ApplyWorkflowTaskScheduledEvent(
				event.GetVersion(),
				event.GetEventId(),
				attributes.TaskQueue,
				attributes.GetStartToCloseTimeout(),
				attributes.GetAttempt(),
				event.GetEventTime(),
				event.GetEventTime(),
				enumsspb.WORKFLOW_TASK_TYPE_NORMAL, // speculative workflow tasks are not replicated.
			)
			if err != nil {
				return nil, err
			}

			// since we do not use stickiness on the standby side
			// there shall be no workflowTask schedule to start timeout
			// NOTE: at the beginning of the loop, stickyness is cleared
			if err := taskGenerator.GenerateScheduleWorkflowTaskTasks(
				workflowTask.ScheduledEventID,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED:
			attributes := event.GetWorkflowTaskStartedEventAttributes()
			workflowTask, err := b.mutableState.ApplyWorkflowTaskStartedEvent(
				nil,
				event.GetVersion(),
				attributes.GetScheduledEventId(),
				event.GetEventId(),
				attributes.GetRequestId(),
				timestamp.TimeValue(event.GetEventTime()),
				attributes.GetSuggestContinueAsNew(),
				attributes.GetHistorySizeBytes(),
				attributes.GetWorkerVersion(),
				attributes.GetBuildIdRedirectCounter(),
			)
			if err != nil {
				return nil, err
			}

			if err := taskGenerator.GenerateStartWorkflowTaskTasks(
				workflowTask.ScheduledEventID,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
			if err := b.mutableState.ApplyWorkflowTaskCompletedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
			if err := b.mutableState.ApplyWorkflowTaskTimedOutEvent(
				event.GetWorkflowTaskTimedOutEventAttributes().GetTimeoutType(),
			); err != nil {
				return nil, err
			}

			// this is for transient workflowTask
			workflowTask, err := b.mutableState.ApplyTransientWorkflowTaskScheduled()
			if err != nil {
				return nil, err
			}

			if workflowTask != nil {
				// since we do not use stickiness on the standby side
				// there shall be no workflowTask schedule to start timeout
				// NOTE: at the beginning of the loop, stickyness is cleared
				if err := taskGenerator.GenerateScheduleWorkflowTaskTasks(
					workflowTask.ScheduledEventID,
				); err != nil {
					return nil, err
				}
			}

		case enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED:
			if err := b.mutableState.ApplyWorkflowTaskFailedEvent(); err != nil {
				return nil, err
			}

			// this is for transient workflowTask
			workflowTask, err := b.mutableState.ApplyTransientWorkflowTaskScheduled()
			if err != nil {
				return nil, err
			}

			if workflowTask != nil {
				// since we do not use stickiness on the standby side
				// there shall be no workflowTask schedule to start timeout
				// NOTE: at the beginning of the loop, stickyness is cleared
				if err := taskGenerator.GenerateScheduleWorkflowTaskTasks(
					workflowTask.ScheduledEventID,
				); err != nil {
					return nil, err
				}
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			if _, err := b.mutableState.ApplyActivityTaskScheduledEvent(
				firstEvent.GetEventId(),
				event,
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.GenerateActivityTasks(
				event.GetEventId(),
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			if err := b.mutableState.ApplyActivityTaskStartedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			if err := b.mutableState.ApplyActivityTaskCompletedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
			if err := b.mutableState.ApplyActivityTaskFailedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
			if err := b.mutableState.ApplyActivityTaskTimedOutEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:
			if err := b.mutableState.ApplyActivityTaskCancelRequestedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
			if err := b.mutableState.ApplyActivityTaskCanceledEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_TIMER_STARTED:
			if _, err := b.mutableState.ApplyTimerStartedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_TIMER_FIRED:
			if err := b.mutableState.ApplyTimerFiredEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_TIMER_CANCELED:
			if err := b.mutableState.ApplyTimerCanceledEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
			if _, err := b.mutableState.ApplyStartChildWorkflowExecutionInitiatedEvent(
				firstEvent.GetEventId(),
				event,
				// create a new request ID which is used by transfer queue processor
				// if namespace is failed over at this point
				uuid.New(),
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.GenerateChildWorkflowTasks(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED:
			if err := b.mutableState.ApplyStartChildWorkflowExecutionFailedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
			if err := b.mutableState.ApplyChildWorkflowExecutionStartedEvent(
				event,
				nil, // shard clock is local to cluster
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
			if err := b.mutableState.ApplyChildWorkflowExecutionCompletedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
			if err := b.mutableState.ApplyChildWorkflowExecutionFailedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
			if err := b.mutableState.ApplyChildWorkflowExecutionCanceledEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
			if err := b.mutableState.ApplyChildWorkflowExecutionTimedOutEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
			if err := b.mutableState.ApplyChildWorkflowExecutionTerminatedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
			if _, err := b.mutableState.ApplyRequestCancelExternalWorkflowExecutionInitiatedEvent(
				firstEvent.GetEventId(),
				event,
				// create a new request ID which is used by transfer queue processor
				// if namespace is failed over at this point
				uuid.New(),
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.GenerateRequestCancelExternalTasks(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
			if err := b.mutableState.ApplyRequestCancelExternalWorkflowExecutionFailedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
			if err := b.mutableState.ApplyExternalWorkflowExecutionCancelRequested(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:
			// Create a new request ID which is used by transfer queue processor if namespace is failed over at this point
			signalRequestID := uuid.New()
			if _, err := b.mutableState.ApplySignalExternalWorkflowExecutionInitiatedEvent(
				firstEvent.GetEventId(),
				event,
				signalRequestID,
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.GenerateSignalExternalTasks(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
			if err := b.mutableState.ApplySignalExternalWorkflowExecutionFailedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED:
			if err := b.mutableState.ApplyExternalWorkflowExecutionSignaled(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_MARKER_RECORDED:
			// No mutable state action is needed

		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			if err := b.mutableState.ApplyWorkflowExecutionSignaled(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
			if err := b.mutableState.ApplyWorkflowExecutionCancelRequestedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
			b.mutableState.ApplyUpsertWorkflowSearchAttributesEvent(event)
			if err := taskGenerator.GenerateUpsertVisibilityTask(); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED:
			b.mutableState.ApplyWorkflowPropertiesModifiedEvent(event)
			if err := taskGenerator.GenerateUpsertVisibilityTask(); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
			if err := b.mutableState.ApplyWorkflowExecutionCompletedEvent(
				firstEvent.GetEventId(),
				event,
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.GenerateWorkflowCloseTasks(
				event.GetEventTime().AsTime(),
				false,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
			if err := b.mutableState.ApplyWorkflowExecutionFailedEvent(
				firstEvent.GetEventId(),
				event,
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.GenerateWorkflowCloseTasks(
				event.GetEventTime().AsTime(),
				false,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
			if err := b.mutableState.ApplyWorkflowExecutionTimedoutEvent(
				firstEvent.GetEventId(),
				event,
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.GenerateWorkflowCloseTasks(
				event.GetEventTime().AsTime(),
				false,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
			if err := b.mutableState.ApplyWorkflowExecutionCanceledEvent(
				firstEvent.GetEventId(),
				event,
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.GenerateWorkflowCloseTasks(
				event.GetEventTime().AsTime(),
				false,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
			if err := b.mutableState.ApplyWorkflowExecutionTerminatedEvent(
				firstEvent.GetEventId(),
				event,
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.GenerateWorkflowCloseTasks(
				event.GetEventTime().AsTime(),
				false,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
			// This is for backward compatibility, old replication tasks generated for continuedAsNew case
			// doesn't have newRunID field set. In that case, we need to get newRunID from the last event.
			continuedAsNewRunID := event.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId()
			if newRunID == "" {
				newRunID = continuedAsNewRunID
			} else if newRunID != continuedAsNewRunID {
				return nil, serviceerror.NewInternal(fmt.Sprintf(
					"ApplyEvents encounted newRunID mismatch for continuedAsNew event, task newRunID: %v, event newRunID: %v",
					newRunID,
					continuedAsNewRunID,
				))
			}

			if err := b.mutableState.ApplyWorkflowExecutionContinuedAsNewEvent(
				firstEvent.GetEventId(),
				event,
			); err != nil {
				return nil, err
			}

			if err := taskGenerator.GenerateWorkflowCloseTasks(
				event.GetEventTime().AsTime(),
				false,
			); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED:
			if err := b.mutableState.ApplyWorkflowExecutionUpdateAdmittedEvent(event, firstEvent.GetEventId()); err != nil {
				return nil, err
			}
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED:
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
			if err := b.mutableState.ApplyWorkflowExecutionUpdateAcceptedEvent(event); err != nil {
				return nil, err
			}
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED:
			if err := b.mutableState.ApplyWorkflowExecutionUpdateCompletedEvent(event, firstEvent.GetEventId()); err != nil {
				return nil, err
			}

		case enumspb.EVENT_TYPE_ACTIVITY_PROPERTIES_MODIFIED_EXTERNALLY,
			enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED_EXTERNALLY:
			return nil, serviceerror.NewUnimplemented("Workflow/activity property modification not implemented")

		default:
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Unknown event type: %v", event.GetEventType()))
		}
	}

	// The length of newRunHistory can be zero in resend case
	if len(newRunHistory) == 0 {
		return nil, nil
	}

	if b.mutableState.GetExecutionState().Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil, serviceerror.NewInternal("Cannot apply events for new run when current run is still running")
	}

	return b.applyNewRunHistory(
		ctx,
		namespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: execution.WorkflowId,
			RunId:      newRunID,
		},
		newRunHistory,
	)
}

func (b *MutableStateRebuilderImpl) applyNewRunHistory(
	ctx context.Context,
	namespaceID namespace.ID,
	newExecution *commonpb.WorkflowExecution,
	newRunHistory []*historypb.HistoryEvent,
) (MutableState, error) {

	// TODO: replication task should contain enough information to determine whether the new run is part of the same chain
	// and not relying on a specific event type to make that decision
	sameWorkflowChain := false
	newRunFirstEvent := newRunHistory[0]
	if newRunFirstEvent.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
		newRunFirstRunID := newRunFirstEvent.GetWorkflowExecutionStartedEventAttributes().FirstExecutionRunId
		sameWorkflowChain = newRunFirstRunID == b.mutableState.GetExecutionInfo().FirstExecutionRunId
	}

	var newRunMutableState MutableState
	if sameWorkflowChain {
		newRunMutableState = NewMutableStateInChain(
			b.shard,
			b.shard.GetEventsCache(),
			b.logger,
			b.mutableState.GetNamespaceEntry(),
			newExecution.WorkflowId,
			newExecution.RunId,
			timestamp.TimeValue(newRunHistory[0].GetEventTime()),
			b.mutableState,
		)
	} else {
		newRunMutableState = NewMutableState(
			b.shard,
			b.shard.GetEventsCache(),
			b.logger,
			b.mutableState.GetNamespaceEntry(),
			newExecution.WorkflowId,
			newExecution.RunId,
			timestamp.TimeValue(newRunHistory[0].GetEventTime()),
		)
	}

	newRunStateBuilder := NewMutableStateRebuilder(b.shard, b.logger, newRunMutableState)

	_, err := newRunStateBuilder.ApplyEvents(
		ctx,
		namespaceID,
		uuid.New(),
		newExecution,
		[][]*historypb.HistoryEvent{newRunHistory},
		nil,
		"",
	)
	if err != nil {
		return nil, err
	}

	return newRunMutableState, nil
}
