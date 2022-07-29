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

package workflow

import (
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	HistoryBuilderStateMutable HistoryBuilderState = iota
	HistoryBuilderStateImmutable
	HistoryBuilderStateSealed
)

// TODO should the reorderFunc functionality be ported?
type (
	HistoryBuilderState int

	HistoryMutation struct {
		// events to be persist to events table
		DBEventsBatches [][]*historypb.HistoryEvent
		// events to be buffer in execution table
		DBBufferBatch []*historypb.HistoryEvent
		// whether to clear buffer events on DB
		DBClearBuffer bool
		// accumulated buffered events, equal to all buffer events from execution table
		MemBufferBatch []*historypb.HistoryEvent
		// scheduled to started event ID mapping for flushed buffered event
		ScheduledIDToStartedID map[int64]int64
	}

	TaskIDGenerator func(number int) ([]int64, error)

	HistoryBuilder struct {
		state           HistoryBuilderState
		timeSource      clock.TimeSource
		taskIDGenerator TaskIDGenerator

		version     int64
		nextEventID int64

		// workflow finished
		workflowFinished bool

		// buffer events in DB
		dbBufferBatch []*historypb.HistoryEvent
		dbClearBuffer bool

		// in mem events
		memEventsBatches [][]*historypb.HistoryEvent
		memLatestBatch   []*historypb.HistoryEvent
		memBufferBatch   []*historypb.HistoryEvent

		// scheduled to started event ID mapping
		scheduledIDToStartedID map[int64]int64
	}
)

func NewMutableHistoryBuilder(
	timeSource clock.TimeSource,
	taskIDGenerator TaskIDGenerator,
	version int64,
	nextEventID int64,
	dbBufferBatch []*historypb.HistoryEvent,
) *HistoryBuilder {
	return &HistoryBuilder{
		state:           HistoryBuilderStateMutable,
		timeSource:      timeSource,
		taskIDGenerator: taskIDGenerator,

		version:     version,
		nextEventID: nextEventID,

		workflowFinished: false,

		dbBufferBatch:          dbBufferBatch,
		dbClearBuffer:          false,
		memEventsBatches:       nil,
		memLatestBatch:         nil,
		memBufferBatch:         nil,
		scheduledIDToStartedID: make(map[int64]int64),
	}
}

func NewImmutableHistoryBuilder(
	history []*historypb.HistoryEvent,
) *HistoryBuilder {
	lastEvent := history[len(history)-1]
	return &HistoryBuilder{
		state:           HistoryBuilderStateImmutable,
		timeSource:      nil,
		taskIDGenerator: nil,

		version:     lastEvent.GetVersion(),
		nextEventID: lastEvent.GetEventId() + 1,

		workflowFinished: false,

		dbBufferBatch:          nil,
		dbClearBuffer:          false,
		memEventsBatches:       nil,
		memLatestBatch:         history,
		memBufferBatch:         nil,
		scheduledIDToStartedID: nil,
	}
}

// NOTE:
// originalRunID is the runID when the WorkflowExecutionStarted event is written
// firstRunID is the very first runID along the chain of ContinueAsNew and Reset

func (b *HistoryBuilder) AddWorkflowExecutionStartedEvent(
	startTime time.Time,
	request *historyservice.StartWorkflowExecutionRequest,
	resetPoints *workflowpb.ResetPoints,
	prevRunID string,
	firstRunID string,
	originalRunID string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, startTime)
	req := request.StartRequest
	attributes := &historypb.WorkflowExecutionStartedEventAttributes{
		WorkflowType:                    req.WorkflowType,
		TaskQueue:                       req.TaskQueue,
		Header:                          req.Header,
		Input:                           req.Input,
		WorkflowRunTimeout:              req.WorkflowRunTimeout,
		WorkflowExecutionTimeout:        req.WorkflowExecutionTimeout,
		WorkflowTaskTimeout:             req.WorkflowTaskTimeout,
		ContinuedExecutionRunId:         prevRunID,
		PrevAutoResetPoints:             resetPoints,
		Identity:                        req.Identity,
		RetryPolicy:                     req.RetryPolicy,
		Attempt:                         request.GetAttempt(),
		WorkflowExecutionExpirationTime: request.WorkflowExecutionExpirationTime,
		CronSchedule:                    req.CronSchedule,
		LastCompletionResult:            request.LastCompletionResult,
		ContinuedFailure:                request.GetContinuedFailure(),
		Initiator:                       request.ContinueAsNewInitiator,
		FirstWorkflowTaskBackoff:        request.FirstWorkflowTaskBackoff,
		FirstExecutionRunId:             firstRunID,
		OriginalExecutionRunId:          originalRunID,
		Memo:                            req.Memo,
		SearchAttributes:                req.SearchAttributes,
	}
	parentInfo := request.ParentExecutionInfo
	if parentInfo != nil {
		attributes.ParentWorkflowNamespaceId = parentInfo.NamespaceId
		attributes.ParentWorkflowNamespace = parentInfo.Namespace
		attributes.ParentWorkflowExecution = parentInfo.Execution
		attributes.ParentInitiatedEventId = parentInfo.InitiatedId
		attributes.ParentInitiatedEventVersion = parentInfo.InitiatedVersion
	}
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
		WorkflowExecutionStartedEventAttributes: attributes,
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowTaskScheduledEvent(
	taskQueue *taskqueuepb.TaskQueue,
	startToCloseTimeout *time.Duration,
	attempt int32,
	now time.Time,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, now)
	event.Attributes = &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
		WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
			TaskQueue:           taskQueue,
			StartToCloseTimeout: startToCloseTimeout,
			Attempt:             attempt,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowTaskStartedEvent(
	scheduledEventID int64,
	requestID string,
	identity string,
	now time.Time,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, now)
	event.Attributes = &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
		WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
			ScheduledEventId: scheduledEventID,
			Identity:         identity,
			RequestId:        requestID,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowTaskCompletedEvent(
	scheduledEventID int64,
	startedEventID int64,
	identity string,
	checksum string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
		WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
			Identity:         identity,
			BinaryChecksum:   checksum,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowTaskTimedOutEvent(
	scheduledEventID int64,
	startedEventID int64,
	timeoutType enumspb.TimeoutType,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowTaskTimedOutEventAttributes{
		WorkflowTaskTimedOutEventAttributes: &historypb.WorkflowTaskTimedOutEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
			TimeoutType:      timeoutType,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowTaskFailedEvent(
	scheduledEventID int64,
	startedEventID int64,
	cause enumspb.WorkflowTaskFailedCause,
	failure *failurepb.Failure,
	identity string,
	baseRunID string,
	newRunID string,
	forkEventVersion int64,
	checksum string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{
		WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
			Cause:            cause,
			Failure:          failure,
			Identity:         identity,
			BaseRunId:        baseRunID,
			NewRunId:         newRunID,
			ForkEventVersion: forkEventVersion,
			BinaryChecksum:   checksum,
		},
	}
	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddActivityTaskScheduledEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.ScheduleActivityTaskCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
		ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			ActivityId:                   command.ActivityId,
			ActivityType:                 command.ActivityType,
			TaskQueue:                    command.TaskQueue,
			Header:                       command.Header,
			Input:                        command.Input,
			ScheduleToCloseTimeout:       command.ScheduleToCloseTimeout,
			ScheduleToStartTimeout:       command.ScheduleToStartTimeout,
			StartToCloseTimeout:          command.StartToCloseTimeout,
			HeartbeatTimeout:             command.HeartbeatTimeout,
			RetryPolicy:                  command.RetryPolicy,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddActivityTaskStartedEvent(
	scheduledEventID int64,
	attempt int32,
	requestID string,
	identity string,
	lastFailure *failurepb.Failure,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
		ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
			ScheduledEventId: scheduledEventID,
			Attempt:          attempt,
			Identity:         identity,
			RequestId:        requestID,
			LastFailure:      lastFailure,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddActivityTaskCompletedEvent(
	scheduledEventID int64,
	startedEventID int64,
	identity string,
	result *commonpb.Payloads,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
		ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
			Result:           result,
			Identity:         identity,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddActivityTaskFailedEvent(
	scheduledEventID int64,
	startedEventID int64,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
	identity string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{
		ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
			Failure:          failure,
			RetryState:       retryState,
			Identity:         identity,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddActivityTaskTimedOutEvent(
	scheduledEventID,
	startedEventID int64,
	timeoutFailure *failurepb.Failure,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{
		ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
			Failure:          timeoutFailure,
			RetryState:       retryState,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddCompletedWorkflowEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.CompleteWorkflowExecutionCommandAttributes,
	newExecutionRunID string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
		WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Result:                       command.Result,
			NewExecutionRunId:            newExecutionRunID,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddFailWorkflowEvent(
	workflowTaskCompletedEventID int64,
	retryState enumspb.RetryState,
	command *commandpb.FailWorkflowExecutionCommandAttributes,
	newExecutionRunID string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
		WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Failure:                      command.Failure,
			RetryState:                   retryState,
			NewExecutionRunId:            newExecutionRunID,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddTimeoutWorkflowEvent(
	retryState enumspb.RetryState,
	newExecutionRunID string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
		WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{
			RetryState:        retryState,
			NewExecutionRunId: newExecutionRunID,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowExecutionTerminatedEvent(
	reason string,
	details *commonpb.Payloads,
	identity string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
		WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
			Reason:   reason,
			Details:  details,
			Identity: identity,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddContinuedAsNewEvent(
	workflowTaskCompletedEventID int64,
	newRunID string,
	command *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, b.timeSource.Now())
	attributes := &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		NewExecutionRunId:            newRunID,
		WorkflowType:                 command.WorkflowType,
		TaskQueue:                    command.TaskQueue,
		Header:                       command.Header,
		Input:                        command.Input,
		WorkflowRunTimeout:           command.WorkflowRunTimeout,
		WorkflowTaskTimeout:          command.WorkflowTaskTimeout,
		BackoffStartInterval:         command.BackoffStartInterval,
		Initiator:                    command.Initiator,
		Failure:                      command.Failure,
		LastCompletionResult:         command.LastCompletionResult,
		Memo:                         command.Memo,
		SearchAttributes:             command.SearchAttributes,
	}
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{
		WorkflowExecutionContinuedAsNewEventAttributes: attributes,
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddTimerStartedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.StartTimerCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_TIMER_STARTED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_TimerStartedEventAttributes{
		TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			TimerId:                      command.TimerId,
			StartToFireTimeout:           command.StartToFireTimeout,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddTimerFiredEvent(
	startedEventID int64,
	timerID string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_TIMER_FIRED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_TimerFiredEventAttributes{
		TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{
			TimerId:        timerID,
			StartedEventId: startedEventID,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddActivityTaskCancelRequestedEvent(
	workflowTaskCompletedEventID int64,
	scheduledEventID int64,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{
		ActivityTaskCancelRequestedEventAttributes: &historypb.ActivityTaskCancelRequestedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			ScheduledEventId:             scheduledEventID,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddActivityTaskCanceledEvent(
	scheduledEventID int64,
	startedEventID int64,
	latestCancelRequestedEventID int64,
	details *commonpb.Payloads,
	identity string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{
		ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{
			ScheduledEventId:             scheduledEventID,
			StartedEventId:               startedEventID,
			LatestCancelRequestedEventId: latestCancelRequestedEventID,
			Details:                      details,
			Identity:                     identity,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddTimerCanceledEvent(
	workflowTaskCompletedEventID int64,
	startedEventID int64,
	timerID string,
	identity string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_TIMER_CANCELED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_TimerCanceledEventAttributes{
		TimerCanceledEventAttributes: &historypb.TimerCanceledEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			StartedEventId:               startedEventID,
			TimerId:                      timerID,
			Identity:                     identity,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowExecutionCancelRequestedEvent(
	request *historyservice.RequestCancelWorkflowExecutionRequest,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{
		WorkflowExecutionCancelRequestedEventAttributes: &historypb.WorkflowExecutionCancelRequestedEventAttributes{
			Cause:                     request.CancelRequest.Reason,
			Identity:                  request.CancelRequest.Identity,
			ExternalInitiatedEventId:  request.ExternalInitiatedEventId,
			ExternalWorkflowExecution: request.ExternalWorkflowExecution,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowExecutionCanceledEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.CancelWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{
		WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Details:                      command.Details,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
		RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Namespace:                    command.Namespace,
			NamespaceId:                  targetNamespaceID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: command.WorkflowId,
				RunId:      command.RunId,
			},
			Control:           command.Control,
			ChildWorkflowOnly: command.ChildWorkflowOnly,
			Reason:            command.Reason,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddRequestCancelExternalWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
	cause enumspb.CancelExternalWorkflowExecutionFailedCause,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{
		RequestCancelExternalWorkflowExecutionFailedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionFailedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			InitiatedEventId:             initiatedEventID,
			Namespace:                    targetNamespace.String(),
			NamespaceId:                  targetNamespaceID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			Cause:   cause,
			Control: "",
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddExternalWorkflowExecutionCancelRequested(
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{
		ExternalWorkflowExecutionCancelRequestedEventAttributes: &historypb.ExternalWorkflowExecutionCancelRequestedEventAttributes{
			InitiatedEventId: initiatedEventID,
			Namespace:        targetNamespace.String(),
			NamespaceId:      targetNamespaceID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddSignalExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.SignalExternalWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{
		SignalExternalWorkflowExecutionInitiatedEventAttributes: &historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Namespace:                    command.Namespace,
			NamespaceId:                  targetNamespaceID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: command.Execution.WorkflowId,
				RunId:      command.Execution.RunId,
			},
			SignalName:        command.SignalName,
			Input:             command.Input,
			Control:           command.Control,
			ChildWorkflowOnly: command.ChildWorkflowOnly,
			Header:            command.Header,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddUpsertWorkflowSearchAttributesEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{
		UpsertWorkflowSearchAttributesEventAttributes: &historypb.UpsertWorkflowSearchAttributesEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			SearchAttributes:             command.SearchAttributes,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddSignalExternalWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
	control string,
	cause enumspb.SignalExternalWorkflowExecutionFailedCause,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{
		SignalExternalWorkflowExecutionFailedEventAttributes: &historypb.SignalExternalWorkflowExecutionFailedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			InitiatedEventId:             initiatedEventID,
			Namespace:                    targetNamespace.String(),
			NamespaceId:                  targetNamespaceID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			Cause:   cause,
			Control: control,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddExternalWorkflowExecutionSignaled(
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
	control string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{
		ExternalWorkflowExecutionSignaledEventAttributes: &historypb.ExternalWorkflowExecutionSignaledEventAttributes{
			InitiatedEventId: initiatedEventID,
			Namespace:        targetNamespace.String(),
			NamespaceId:      targetNamespaceID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			Control: control,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddMarkerRecordedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.RecordMarkerCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_MARKER_RECORDED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_MarkerRecordedEventAttributes{
		MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
			MarkerName:                   command.MarkerName,
			Details:                      command.Details,
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Header:                       command.Header,
			Failure:                      command.Failure,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowExecutionSignaledEvent(
	signalName string,
	input *commonpb.Payloads,
	identity string,
	header *commonpb.Header,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
		WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: signalName,
			Input:      input,
			Identity:   identity,
			Header:     header,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddStartChildWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.StartChildWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{
		StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Namespace:                    command.Namespace,
			NamespaceId:                  targetNamespaceID.String(),
			WorkflowId:                   command.WorkflowId,
			WorkflowType:                 command.WorkflowType,
			TaskQueue:                    command.TaskQueue,
			Header:                       command.Header,
			Input:                        command.Input,
			WorkflowExecutionTimeout:     command.WorkflowExecutionTimeout,
			WorkflowRunTimeout:           command.WorkflowRunTimeout,
			WorkflowTaskTimeout:          command.WorkflowTaskTimeout,
			Control:                      command.Control,
			WorkflowIdReusePolicy:        command.WorkflowIdReusePolicy,
			RetryPolicy:                  command.RetryPolicy,
			CronSchedule:                 command.CronSchedule,
			Memo:                         command.Memo,
			SearchAttributes:             command.SearchAttributes,
			ParentClosePolicy:            command.GetParentClosePolicy(),
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddChildWorkflowExecutionStartedEvent(
	initiatedID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	header *commonpb.Header,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{
		ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{
			InitiatedEventId:  initiatedID,
			Namespace:         targetNamespace.String(),
			NamespaceId:       targetNamespaceID.String(),
			WorkflowExecution: execution,
			WorkflowType:      workflowType,
			Header:            header,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddStartChildWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedID int64,
	cause enumspb.StartChildWorkflowExecutionFailedCause,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	workflowType *commonpb.WorkflowType,
	control string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{
		StartChildWorkflowExecutionFailedEventAttributes: &historypb.StartChildWorkflowExecutionFailedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			InitiatedEventId:             initiatedID,
			Namespace:                    targetNamespace.String(),
			NamespaceId:                  targetNamespaceID.String(),
			WorkflowId:                   workflowID,
			WorkflowType:                 workflowType,
			Control:                      control,
			Cause:                        cause,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddChildWorkflowExecutionCompletedEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	result *commonpb.Payloads,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{
		ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{
			InitiatedEventId:  initiatedID,
			StartedEventId:    startedEventID,
			Namespace:         targetNamespace.String(),
			NamespaceId:       targetNamespaceID.String(),
			WorkflowExecution: execution,
			WorkflowType:      workflowType,
			Result:            result,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddChildWorkflowExecutionFailedEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{
		ChildWorkflowExecutionFailedEventAttributes: &historypb.ChildWorkflowExecutionFailedEventAttributes{
			InitiatedEventId:  initiatedID,
			StartedEventId:    startedEventID,
			Namespace:         targetNamespace.String(),
			NamespaceId:       targetNamespaceID.String(),
			WorkflowExecution: execution,
			WorkflowType:      workflowType,
			Failure:           failure,
			RetryState:        retryState,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddChildWorkflowExecutionCanceledEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	details *commonpb.Payloads,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{
		ChildWorkflowExecutionCanceledEventAttributes: &historypb.ChildWorkflowExecutionCanceledEventAttributes{
			InitiatedEventId:  initiatedID,
			StartedEventId:    startedEventID,
			Namespace:         targetNamespace.String(),
			NamespaceId:       targetNamespaceID.String(),
			WorkflowExecution: execution,
			WorkflowType:      workflowType,
			Details:           details,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddChildWorkflowExecutionTerminatedEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{
		ChildWorkflowExecutionTerminatedEventAttributes: &historypb.ChildWorkflowExecutionTerminatedEventAttributes{
			InitiatedEventId:  initiatedID,
			StartedEventId:    startedEventID,
			Namespace:         targetNamespace.String(),
			NamespaceId:       targetNamespaceID.String(),
			WorkflowExecution: execution,
			WorkflowType:      workflowType,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddChildWorkflowExecutionTimedOutEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{
		ChildWorkflowExecutionTimedOutEventAttributes: &historypb.ChildWorkflowExecutionTimedOutEventAttributes{
			InitiatedEventId:  initiatedID,
			StartedEventId:    startedEventID,
			Namespace:         targetNamespace.String(),
			NamespaceId:       targetNamespaceID.String(),
			WorkflowExecution: execution,
			WorkflowType:      workflowType,
			RetryState:        retryState,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) appendEvents(
	event *historypb.HistoryEvent,
) *historypb.HistoryEvent {
	b.assertMutable()
	if b.bufferEvent(event.GetEventType()) {
		b.memBufferBatch = append(b.memBufferBatch, event)
	} else {
		b.memLatestBatch = append(b.memLatestBatch, event)
	}
	return event
}

func (b *HistoryBuilder) HasBufferEvents() bool {
	return len(b.dbBufferBatch) > 0 || len(b.memBufferBatch) > 0
}

func (b *HistoryBuilder) BufferEventSize() int {
	return len(b.dbBufferBatch) + len(b.memBufferBatch)
}

func (b *HistoryBuilder) NextEventID() int64 {
	return b.nextEventID
}

func (b *HistoryBuilder) FlushBufferToCurrentBatch() map[int64]int64 {
	if len(b.dbBufferBatch) == 0 && len(b.memBufferBatch) == 0 {
		return b.scheduledIDToStartedID
	}

	b.assertMutable()

	if b.workflowFinished {
		// in case this case happen
		// 1. request cancel activity
		// 2. workflow task complete
		// above will generate 2 then 1
		b.dbBufferBatch = nil
		b.memBufferBatch = nil
		return b.scheduledIDToStartedID
	}

	b.dbClearBuffer = b.dbClearBuffer || len(b.dbBufferBatch) > 0
	bufferBatch := append(b.dbBufferBatch, b.memBufferBatch...)
	b.dbBufferBatch = nil
	b.memBufferBatch = nil

	// 0th reorder events in case casandra reorder the buffered events
	// TODO eventually remove this ordering
	bufferBatch = b.reorderBuffer(bufferBatch)

	// 1st assign event ID
	for _, event := range bufferBatch {
		event.EventId = b.nextEventID
		b.nextEventID += 1
	}

	// 2nd wire event ID, e.g. activity, child workflow
	b.wireEventIDs(bufferBatch)

	b.memLatestBatch = append(b.memLatestBatch, bufferBatch...)

	return b.scheduledIDToStartedID
}

func (b *HistoryBuilder) FlushAndCreateNewBatch() {
	b.assertNotSealed()
	if len(b.memLatestBatch) == 0 {
		return
	}

	b.memEventsBatches = append(b.memEventsBatches, b.memLatestBatch)
	b.memLatestBatch = nil
}

func (b *HistoryBuilder) Finish(
	flushBufferEvent bool,
) (*HistoryMutation, error) {
	defer func() {
		b.state = HistoryBuilderStateSealed
	}()

	if flushBufferEvent {
		_ = b.FlushBufferToCurrentBatch()
	}
	b.FlushAndCreateNewBatch()

	dbEventsBatches := b.memEventsBatches
	dbClearBuffer := b.dbClearBuffer
	dbBufferBatch := b.memBufferBatch
	memBufferBatch := b.dbBufferBatch
	memBufferBatch = append(memBufferBatch, dbBufferBatch...)
	scheduledIDToStartedID := b.scheduledIDToStartedID

	b.memEventsBatches = nil
	b.memBufferBatch = nil
	b.memLatestBatch = nil
	b.dbClearBuffer = false
	b.dbBufferBatch = nil
	b.scheduledIDToStartedID = nil

	if err := b.assignTaskIDs(dbEventsBatches); err != nil {
		return nil, err
	}

	return &HistoryMutation{
		DBEventsBatches:        dbEventsBatches,
		DBClearBuffer:          dbClearBuffer,
		DBBufferBatch:          dbBufferBatch,
		MemBufferBatch:         memBufferBatch,
		ScheduledIDToStartedID: scheduledIDToStartedID,
	}, nil
}

func (b *HistoryBuilder) assignTaskIDs(
	dbEventsBatches [][]*historypb.HistoryEvent,
) error {
	b.assertNotSealed()

	if b.state == HistoryBuilderStateImmutable {
		return nil
	}

	taskIDCount := 0
	for i := 0; i < len(dbEventsBatches); i++ {
		taskIDCount += len(dbEventsBatches[i])
	}
	taskIDs, err := b.taskIDGenerator(taskIDCount)
	if err != nil {
		return err
	}

	taskIDPointer := 0
	height := len(dbEventsBatches)
	for i := 0; i < height; i++ {
		width := len(dbEventsBatches[i])
		for j := 0; j < width; j++ {
			dbEventsBatches[i][j].TaskId = taskIDs[taskIDPointer]
			taskIDPointer++
		}
	}
	return nil
}

func (b *HistoryBuilder) assertMutable() {
	if b.state != HistoryBuilderStateMutable {
		panic("history builder is mutated while not in mutable state")
	}
}

func (b *HistoryBuilder) assertNotSealed() {
	if b.state == HistoryBuilderStateSealed {
		panic("history builder is in sealed state")
	}
}

func (b *HistoryBuilder) createNewHistoryEvent(
	eventType enumspb.EventType,
	time time.Time,
) *historypb.HistoryEvent {
	b.assertMutable()

	if b.workflowFinished {
		panic("history builder unable to create new event after workflow finish")
	}
	if b.finishEvent(eventType) {
		b.workflowFinished = true
	}

	historyEvent := &historypb.HistoryEvent{}
	historyEvent.EventTime = timestamp.TimePtr(time.UTC())
	historyEvent.EventType = eventType
	historyEvent.Version = b.version

	if b.bufferEvent(eventType) {
		historyEvent.EventId = common.BufferedEventID
		historyEvent.TaskId = common.EmptyEventTaskID
	} else {
		historyEvent.EventId = b.nextEventID
		historyEvent.TaskId = common.EmptyEventTaskID
		b.nextEventID += 1
	}

	return historyEvent
}

func (b *HistoryBuilder) bufferEvent(
	eventType enumspb.EventType,
) bool {
	switch eventType {
	case // do not buffer for workflow state change
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		return false

	case // workflow task event should not be buffered
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
		return false

	case // events generated directly from commands should not be buffered
		// workflow complete, failed, cancelled and continue-as-new events are duplication of above
		// just put is here for reference
		// workflow.EventTypeWorkflowExecutionCompleted,
		// workflow.EventTypeWorkflowExecutionFailed,
		// workflow.EventTypeWorkflowExecutionCanceled,
		// workflow.EventTypeWorkflowExecutionContinuedAsNew,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_TIMER_STARTED,
		// CommandTypeCancelTimer is an exception. This command will be mapped
		// to workflow.EventTypeTimerCanceled.
		// This event should not be buffered. Ref: historyEngine, search for "workflow.CommandTypeCancelTimer"
		enumspb.EVENT_TYPE_TIMER_CANCELED,
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		enumspb.EVENT_TYPE_MARKER_RECORDED,
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
		enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
		enumspb.EVENT_TYPE_WORKFLOW_UPDATE_ACCEPTED,
		enumspb.EVENT_TYPE_WORKFLOW_UPDATE_COMPLETED:
		// do not buffer event if event is directly generated from a corresponding command
		return false

	default:
		return true
	}
}

func (b *HistoryBuilder) finishEvent(
	eventType enumspb.EventType,
) bool {
	switch eventType {
	case
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		return true

	default:
		return false
	}
}

func (b *HistoryBuilder) wireEventIDs(
	bufferEvents []*historypb.HistoryEvent,
) {
	for _, event := range bufferEvents {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			attributes := event.GetActivityTaskStartedEventAttributes()
			scheduledEventID := attributes.GetScheduledEventId()
			b.scheduledIDToStartedID[scheduledEventID] = event.GetEventId()
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			attributes := event.GetActivityTaskCompletedEventAttributes()
			if startedEventID, ok := b.scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = startedEventID
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
			attributes := event.GetActivityTaskFailedEventAttributes()
			if startedEventID, ok := b.scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = startedEventID
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
			attributes := event.GetActivityTaskTimedOutEventAttributes()
			if startedEventID, ok := b.scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = startedEventID
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
			attributes := event.GetActivityTaskCanceledEventAttributes()
			if startedEventID, ok := b.scheduledIDToStartedID[attributes.GetScheduledEventId()]; ok {
				attributes.StartedEventId = startedEventID
			}

		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
			attributes := event.GetChildWorkflowExecutionStartedEventAttributes()
			initiatedEventID := attributes.GetInitiatedEventId()
			b.scheduledIDToStartedID[initiatedEventID] = event.GetEventId()
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
			attributes := event.GetChildWorkflowExecutionCompletedEventAttributes()
			if startedEventID, ok := b.scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedEventID
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
			attributes := event.GetChildWorkflowExecutionFailedEventAttributes()
			if startedEventID, ok := b.scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedEventID
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
			attributes := event.GetChildWorkflowExecutionTimedOutEventAttributes()
			if startedEventID, ok := b.scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedEventID
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
			attributes := event.GetChildWorkflowExecutionCanceledEventAttributes()
			if startedEventID, ok := b.scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedEventID
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
			attributes := event.GetChildWorkflowExecutionTerminatedEventAttributes()
			if startedEventID, ok := b.scheduledIDToStartedID[attributes.GetInitiatedEventId()]; ok {
				attributes.StartedEventId = startedEventID
			}
		}
	}
}

// TODO remove this function once we keep all info in DB, e.g. activity / timer / child workflow
//  to deprecate
//  * HasActivityFinishEvent
//  * hasActivityFinishEvent

func (b *HistoryBuilder) reorderBuffer(
	bufferEvents []*historypb.HistoryEvent,
) []*historypb.HistoryEvent {
	reorderBuffer := make([]*historypb.HistoryEvent, 0, len(bufferEvents))
	reorderEvents := make([]*historypb.HistoryEvent, 0, len(bufferEvents))
	for _, event := range bufferEvents {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
			enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
			enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
			reorderBuffer = append(reorderBuffer, event)
		default:
			reorderEvents = append(reorderEvents, event)
		}
	}

	return append(reorderEvents, reorderBuffer...)
}

func (b *HistoryBuilder) HasActivityFinishEvent(
	scheduledEventID int64,
) bool {

	if hasActivityFinishEvent(scheduledEventID, b.dbBufferBatch) {
		return true
	}

	if hasActivityFinishEvent(scheduledEventID, b.memBufferBatch) {
		return true
	}

	if hasActivityFinishEvent(scheduledEventID, b.memLatestBatch) {
		return true
	}

	for _, batch := range b.memEventsBatches {
		if hasActivityFinishEvent(scheduledEventID, batch) {
			return true
		}
	}

	return false
}

func hasActivityFinishEvent(
	scheduledEventID int64,
	events []*historypb.HistoryEvent,
) bool {
	for _, event := range events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			if event.GetActivityTaskCompletedEventAttributes().GetScheduledEventId() == scheduledEventID {
				return true
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
			if event.GetActivityTaskFailedEventAttributes().GetScheduledEventId() == scheduledEventID {
				return true
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
			if event.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId() == scheduledEventID {
				return true
			}

		case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
			if event.GetActivityTaskCanceledEventAttributes().GetScheduledEventId() == scheduledEventID {
				return true
			}
		}
	}

	return false
}

func (b *HistoryBuilder) GetAndRemoveTimerFireEvent(
	timerID string,
) *historypb.HistoryEvent {
	var timerFireEvent *historypb.HistoryEvent

	b.dbBufferBatch, timerFireEvent = deleteTimerFiredEvent(timerID, b.dbBufferBatch)
	if timerFireEvent != nil {
		b.dbClearBuffer = true
		return timerFireEvent
	}

	b.memBufferBatch, timerFireEvent = deleteTimerFiredEvent(timerID, b.memBufferBatch)
	if timerFireEvent != nil {
		b.dbClearBuffer = true
		return timerFireEvent
	}

	return nil
}

func deleteTimerFiredEvent(
	timerID string,
	events []*historypb.HistoryEvent,
) ([]*historypb.HistoryEvent, *historypb.HistoryEvent) {
	// go over all history events. if we find a timer fired event for the given
	// timerID, clear it
	timerFiredIdx := -1
	for idx, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_TIMER_FIRED &&
			event.GetTimerFiredEventAttributes().GetTimerId() == timerID {
			timerFiredIdx = idx
			break
		}
	}
	if timerFiredIdx == -1 {
		return events, nil
	}

	timerEvent := events[timerFiredIdx]
	return append(events[:timerFiredIdx], events[timerFiredIdx+1:]...), timerEvent
}
