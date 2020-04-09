//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination stateBuilder_mock.go

package history

import (
	"time"

	"github.com/pborman/uuid"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
)

type (
	taskGeneratorProvider func(mutableState) mutableStateTaskGenerator

	stateBuilder interface {
		applyEvents(
			namespaceID string,
			requestID string,
			execution executionpb.WorkflowExecution,
			history []*eventpb.HistoryEvent,
			newRunHistory []*eventpb.HistoryEvent,
			newRunNDC bool,
		) (mutableState, error)
	}

	stateBuilderImpl struct {
		shard           ShardContext
		clusterMetadata cluster.Metadata
		namespaceCache  cache.NamespaceCache
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
		namespaceCache:        shard.GetNamespaceCache(),
		logger:                logger,
		mutableState:          mutableState,
		taskGeneratorProvider: taskGeneratorProvider,
	}
}

func (b *stateBuilderImpl) applyEvents(
	namespaceID string,
	requestID string,
	execution executionpb.WorkflowExecution,
	history []*eventpb.HistoryEvent,
	newRunHistory []*eventpb.HistoryEvent,
	newRunNDC bool,
) (mutableState, error) {

	if len(history) == 0 {
		return nil, serviceerror.NewInternal(ErrMessageHistorySizeZero)
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
		case eventpb.EventType_WorkflowExecutionStarted:
			attributes := event.GetWorkflowExecutionStartedEventAttributes()
			var parentNamespaceID string
			if attributes.GetParentWorkflowNamespace() != "" {
				parentNamespaceEntry, err := b.namespaceCache.GetNamespace(
					attributes.GetParentWorkflowNamespace(),
				)
				if err != nil {
					return nil, err
				}
				parentNamespaceID = parentNamespaceEntry.GetInfo().ID
			}

			if err := b.mutableState.ReplicateWorkflowExecutionStartedEvent(
				parentNamespaceID,
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
				primitives.MustParseUUID(execution.GetRunId()),
			); err != nil {
				return nil, err
			}

			// TODO remove after NDC is fully migrated
			if b.mutableState.GetReplicationState() != nil {
				b.mutableState.GetReplicationState().StartVersion = event.GetVersion()
			}

		case eventpb.EventType_DecisionTaskScheduled:
			attributes := event.GetDecisionTaskScheduledEventAttributes()
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

		case eventpb.EventType_DecisionTaskStarted:
			attributes := event.GetDecisionTaskStartedEventAttributes()
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

		case eventpb.EventType_DecisionTaskCompleted:
			if err := b.mutableState.ReplicateDecisionTaskCompletedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_DecisionTaskTimedOut:
			if err := b.mutableState.ReplicateDecisionTaskTimedOutEvent(
				event.GetDecisionTaskTimedOutEventAttributes().GetTimeoutType(),
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

		case eventpb.EventType_DecisionTaskFailed:
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

		case eventpb.EventType_ActivityTaskScheduled:
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

		case eventpb.EventType_ActivityTaskStarted:
			if err := b.mutableState.ReplicateActivityTaskStartedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_ActivityTaskCompleted:
			if err := b.mutableState.ReplicateActivityTaskCompletedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_ActivityTaskFailed:
			if err := b.mutableState.ReplicateActivityTaskFailedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_ActivityTaskTimedOut:
			if err := b.mutableState.ReplicateActivityTaskTimedOutEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_ActivityTaskCancelRequested:
			if err := b.mutableState.ReplicateActivityTaskCancelRequestedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_ActivityTaskCanceled:
			if err := b.mutableState.ReplicateActivityTaskCanceledEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_RequestCancelActivityTaskFailed:
			// No mutable state action is needed

		case eventpb.EventType_TimerStarted:
			if _, err := b.mutableState.ReplicateTimerStartedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_TimerFired:
			if err := b.mutableState.ReplicateTimerFiredEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_TimerCanceled:
			if err := b.mutableState.ReplicateTimerCanceledEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_CancelTimerFailed:
			// no mutable state action is needed

		case eventpb.EventType_StartChildWorkflowExecutionInitiated:
			if _, err := b.mutableState.ReplicateStartChildWorkflowExecutionInitiatedEvent(
				firstEvent.GetEventId(),
				event,
				// create a new request ID which is used by transfer queue processor
				// if namespace is failed over at this point
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

		case eventpb.EventType_StartChildWorkflowExecutionFailed:
			if err := b.mutableState.ReplicateStartChildWorkflowExecutionFailedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_ChildWorkflowExecutionStarted:
			if err := b.mutableState.ReplicateChildWorkflowExecutionStartedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_ChildWorkflowExecutionCompleted:
			if err := b.mutableState.ReplicateChildWorkflowExecutionCompletedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_ChildWorkflowExecutionFailed:
			if err := b.mutableState.ReplicateChildWorkflowExecutionFailedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_ChildWorkflowExecutionCanceled:
			if err := b.mutableState.ReplicateChildWorkflowExecutionCanceledEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_ChildWorkflowExecutionTimedOut:
			if err := b.mutableState.ReplicateChildWorkflowExecutionTimedOutEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_ChildWorkflowExecutionTerminated:
			if err := b.mutableState.ReplicateChildWorkflowExecutionTerminatedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_RequestCancelExternalWorkflowExecutionInitiated:
			if _, err := b.mutableState.ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(
				firstEvent.GetEventId(),
				event,
				// create a new request ID which is used by transfer queue processor
				// if namespace is failed over at this point
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

		case eventpb.EventType_RequestCancelExternalWorkflowExecutionFailed:
			if err := b.mutableState.ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_ExternalWorkflowExecutionCancelRequested:
			if err := b.mutableState.ReplicateExternalWorkflowExecutionCancelRequested(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_SignalExternalWorkflowExecutionInitiated:
			// Create a new request ID which is used by transfer queue processor if namespace is failed over at this point
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

		case eventpb.EventType_SignalExternalWorkflowExecutionFailed:
			if err := b.mutableState.ReplicateSignalExternalWorkflowExecutionFailedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_ExternalWorkflowExecutionSignaled:
			if err := b.mutableState.ReplicateExternalWorkflowExecutionSignaled(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_MarkerRecorded:
			// No mutable state action is needed

		case eventpb.EventType_WorkflowExecutionSignaled:
			if err := b.mutableState.ReplicateWorkflowExecutionSignaled(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_WorkflowExecutionCancelRequested:
			if err := b.mutableState.ReplicateWorkflowExecutionCancelRequestedEvent(
				event,
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_UpsertWorkflowSearchAttributes:
			b.mutableState.ReplicateUpsertWorkflowSearchAttributesEvent(event)
			if err := taskGenerator.generateWorkflowSearchAttrTasks(
				b.unixNanoToTime(event.GetTimestamp()),
			); err != nil {
				return nil, err
			}

		case eventpb.EventType_WorkflowExecutionCompleted:
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

		case eventpb.EventType_WorkflowExecutionFailed:
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

		case eventpb.EventType_WorkflowExecutionTimedOut:
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

		case eventpb.EventType_WorkflowExecutionCanceled:
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

		case eventpb.EventType_WorkflowExecutionTerminated:
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

		case eventpb.EventType_WorkflowExecutionContinuedAsNew:

			// The length of newRunHistory can be zero in resend case
			if len(newRunHistory) != 0 {
				if newRunNDC {
					newRunMutableStateBuilder = newMutableStateBuilderWithVersionHistories(
						b.shard,
						b.shard.GetEventsCache(),
						b.logger,
						b.mutableState.GetNamespaceEntry(),
					)
				} else {
					newRunMutableStateBuilder = newMutableStateBuilderWithReplicationState(
						b.shard,
						b.shard.GetEventsCache(),
						b.logger,
						b.mutableState.GetNamespaceEntry(),
					)
				}
				newRunStateBuilder := newStateBuilder(b.shard, b.logger, newRunMutableStateBuilder, b.taskGeneratorProvider)

				newRunID := event.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId()
				newExecution := executionpb.WorkflowExecution{
					WorkflowId: execution.WorkflowId,
					RunId:      newRunID,
				}
				_, err := newRunStateBuilder.applyEvents(
					namespaceID,
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
				namespaceID,
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
			return nil, serviceerror.NewInvalidArgument("Unknown event type")
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
