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

package mutablestate

import (
	"fmt"
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
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	HistoryBuilderStateMutable   HistoryBuilderState = 0
	HistoryBuilderStateImmutable                     = 1
)

type (
	HistoryBuilderState int

	HistoryBuilder struct {
		state       HistoryBuilderState
		TimeSource  clock.TimeSource
		Version     int64
		NextEventID int64

		eventsBatch [][]*historypb.HistoryEvent
		latestBatch []*historypb.HistoryEvent
	}
)

func NewHistoryBuilder(
	timeSource clock.TimeSource,
	version int64,
	nextEventID int64,
) *HistoryBuilder {
	return &HistoryBuilder{
		state:       HistoryBuilderStateMutable,
		TimeSource:  timeSource,
		Version:     version,
		NextEventID: nextEventID,

		eventsBatch: nil,
		latestBatch: nil,
	}
}

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
		attributes.ParentWorkflowNamespace = parentInfo.Namespace
		attributes.ParentWorkflowExecution = parentInfo.Execution
		attributes.ParentInitiatedEventId = parentInfo.InitiatedId
	}
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
		WorkflowExecutionStartedEventAttributes: attributes,
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowTaskScheduledEvent(
	taskQueue *taskqueuepb.TaskQueue,
	startToCloseTimeoutSeconds int32,
	attempt int32,
	now time.Time,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, now.UTC())
	event.Attributes = &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
		WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
			TaskQueue:           taskQueue,
			StartToCloseTimeout: timestamp.DurationPtr(time.Duration(startToCloseTimeoutSeconds) * time.Second),
			Attempt:             attempt,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowTaskStartedEvent(
	scheduleEventID int64,
	requestID string,
	identity string,
	now time.Time,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, now.UTC())
	event.Attributes = &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
		WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
			ScheduledEventId: scheduleEventID,
			Identity:         identity,
			RequestId:        requestID,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowTaskCompletedEvent(
	scheduleEventID int64,
	startedEventID int64,
	identity string,
	checksum string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
		WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
			ScheduledEventId: scheduleEventID,
			StartedEventId:   startedEventID,
			Identity:         identity,
			BinaryChecksum:   checksum,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowTaskTimedOutEvent(
	scheduleEventID int64,
	startedEventID int64,
	timeoutType enumspb.TimeoutType,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowTaskTimedOutEventAttributes{
		WorkflowTaskTimedOutEventAttributes: &historypb.WorkflowTaskTimedOutEventAttributes{
			ScheduledEventId: scheduleEventID,
			StartedEventId:   startedEventID,
			TimeoutType:      timeoutType,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowTaskFailedEvent(
	scheduleEventID int64,
	startedEventID int64,
	cause enumspb.WorkflowTaskFailedCause,
	failure *failurepb.Failure,
	identity string,
	baseRunID string,
	newRunID string,
	forkEventVersion int64,
	checksum string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{
		WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
			ScheduledEventId: scheduleEventID,
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
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, b.TimeSource.Now())
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
			Namespace:                    command.Namespace,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddActivityTaskStartedEvent(
	scheduleEventID int64,
	attempt int32,
	requestID string,
	identity string,
	lastFailure *failurepb.Failure,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
		ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
			ScheduledEventId: scheduleEventID,
			Attempt:          attempt,
			Identity:         identity,
			RequestId:        requestID,
			LastFailure:      lastFailure,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddActivityTaskCompletedEvent(
	scheduleEventID int64,
	startedEventID int64,
	identity string,
	result *commonpb.Payloads,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
		ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
			ScheduledEventId: scheduleEventID,
			StartedEventId:   startedEventID,
			Result:           result,
			Identity:         identity,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddActivityTaskFailedEvent(
	scheduleEventID int64,
	startedEventID int64,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
	identity string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{
		ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
			ScheduledEventId: scheduleEventID,
			StartedEventId:   startedEventID,
			Failure:          failure,
			RetryState:       retryState,
			Identity:         identity,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddActivityTaskTimedOutEvent(
	scheduleEventID,
	startedEventID int64,
	timeoutFailure *failurepb.Failure,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{
		ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
			ScheduledEventId: scheduleEventID,
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
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
		WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Result:                       command.Result,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddFailWorkflowEvent(
	workflowTaskCompletedEventID int64,
	retryState enumspb.RetryState,
	command *commandpb.FailWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
		WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Failure:                      command.Failure,
			RetryState:                   retryState,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddTimeoutWorkflowEvent(
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
		WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{
			RetryState: retryState,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowExecutionTerminatedEvent(
	reason string,
	details *commonpb.Payloads,
	identity string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, b.TimeSource.Now())
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
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, b.TimeSource.Now())
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
		Initiator:                    enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW,
		Failure:                      command.Failure,
		LastCompletionResult:         command.LastCompletionResult,
		Memo:                         command.Memo,
		SearchAttributes:             command.SearchAttributes,
	}
	if len(command.CronSchedule) != 0 {
		attributes.Initiator = enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE
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
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_TIMER_STARTED, b.TimeSource.Now())
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
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_TIMER_FIRED, b.TimeSource.Now())
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
	scheduleID int64,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{
		ActivityTaskCancelRequestedEventAttributes: &historypb.ActivityTaskCancelRequestedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			ScheduledEventId:             scheduleID,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddActivityTaskCanceledEvent(
	scheduleEventID int64,
	startedEventID int64,
	latestCancelRequestedEventID int64,
	details *commonpb.Payloads,
	identity string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{
		ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{
			ScheduledEventId:             scheduleEventID,
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
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_TIMER_CANCELED, b.TimeSource.Now())
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
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{
		WorkflowExecutionCancelRequestedEventAttributes: &historypb.WorkflowExecutionCancelRequestedEventAttributes{
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
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED, b.TimeSource.Now())
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
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
		RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Namespace:                    command.Namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: command.WorkflowId,
				RunId:      command.RunId,
			},
			Control:           command.Control,
			ChildWorkflowOnly: command.ChildWorkflowOnly,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddRequestCancelExternalWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedEventID int64,
	namespace string,
	workflowID string,
	runID string,
	cause enumspb.CancelExternalWorkflowExecutionFailedCause,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{
		RequestCancelExternalWorkflowExecutionFailedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionFailedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			InitiatedEventId:             initiatedEventID,
			Namespace:                    namespace,
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
	namespace string,
	workflowID string,
	runID string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{
		ExternalWorkflowExecutionCancelRequestedEventAttributes: &historypb.ExternalWorkflowExecutionCancelRequestedEventAttributes{
			InitiatedEventId: initiatedEventID,
			Namespace:        namespace,
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
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{
		SignalExternalWorkflowExecutionInitiatedEventAttributes: &historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Namespace:                    command.Namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: command.Execution.WorkflowId,
				RunId:      command.Execution.RunId,
			},
			SignalName:        command.SignalName,
			Input:             command.Input,
			Control:           command.Control,
			ChildWorkflowOnly: command.ChildWorkflowOnly,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddUpsertWorkflowSearchAttributesEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES, b.TimeSource.Now())
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
	namespace string,
	workflowID string,
	runID string,
	control string,
	cause enumspb.SignalExternalWorkflowExecutionFailedCause,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{
		SignalExternalWorkflowExecutionFailedEventAttributes: &historypb.SignalExternalWorkflowExecutionFailedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			InitiatedEventId:             initiatedEventID,
			Namespace:                    namespace,
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
	namespace string,
	workflowID string,
	runID string,
	control string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{
		ExternalWorkflowExecutionSignaledEventAttributes: &historypb.ExternalWorkflowExecutionSignaledEventAttributes{
			InitiatedEventId: initiatedEventID,
			Namespace:        namespace,
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
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_MARKER_RECORDED, b.TimeSource.Now())
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
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
		WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: signalName,
			Input:      input,
			Identity:   identity,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddStartChildWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.StartChildWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{
		StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Namespace:                    command.Namespace,
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
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	header *commonpb.Header,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{
		ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{
			InitiatedEventId:  initiatedID,
			Namespace:         namespace,
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
	namespace string,
	workflowID string,
	workflowType *commonpb.WorkflowType,
	control string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{
		StartChildWorkflowExecutionFailedEventAttributes: &historypb.StartChildWorkflowExecutionFailedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			InitiatedEventId:             initiatedID,
			Namespace:                    namespace,
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
	startedID int64,
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	result *commonpb.Payloads,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{
		ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{
			InitiatedEventId:  initiatedID,
			StartedEventId:    startedID,
			Namespace:         namespace,
			WorkflowExecution: execution,
			WorkflowType:      workflowType,
			Result:            result,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddChildWorkflowExecutionFailedEvent(
	initiatedID int64,
	startedID int64,
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{
		ChildWorkflowExecutionFailedEventAttributes: &historypb.ChildWorkflowExecutionFailedEventAttributes{
			InitiatedEventId:  initiatedID,
			StartedEventId:    startedID,
			Namespace:         namespace,
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
	startedID int64,
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	details *commonpb.Payloads,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{
		ChildWorkflowExecutionCanceledEventAttributes: &historypb.ChildWorkflowExecutionCanceledEventAttributes{
			InitiatedEventId:  initiatedID,
			StartedEventId:    startedID,
			Namespace:         namespace,
			WorkflowExecution: execution,
			WorkflowType:      workflowType,
			Details:           details,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddChildWorkflowExecutionTerminatedEvent(
	initiatedID int64,
	startedID int64,
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{
		ChildWorkflowExecutionTerminatedEventAttributes: &historypb.ChildWorkflowExecutionTerminatedEventAttributes{
			InitiatedEventId:  initiatedID,
			StartedEventId:    startedID,
			Namespace:         namespace,
			WorkflowExecution: execution,
			WorkflowType:      workflowType,
		},
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddChildWorkflowExecutionTimedOutEvent(
	initiatedID int64,
	startedID int64,
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{
		ChildWorkflowExecutionTimedOutEventAttributes: &historypb.ChildWorkflowExecutionTimedOutEventAttributes{
			InitiatedEventId:  initiatedID,
			StartedEventId:    startedID,
			Namespace:         namespace,
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
	b.latestBatch = append(b.latestBatch, event)
	return event
}

func (b *HistoryBuilder) FlushEvents() {
	b.assertMutable()
	if len(b.latestBatch) == 0 {
		return
	}

	b.eventsBatch = append(b.eventsBatch, b.latestBatch)
	b.latestBatch = nil
}

func (b *HistoryBuilder) Finish() [][]*historypb.HistoryEvent {
	b.FlushEvents()
	b.state = HistoryBuilderStateImmutable
	return b.eventsBatch
}

func (b *HistoryBuilder) assertMutable() {
	if b.state != HistoryBuilderStateMutable {
		panic(fmt.Sprintf("history builder is mutated while in immutable state"))
	}
}

func (b *HistoryBuilder) createNewHistoryEvent(
	eventType enumspb.EventType,
	time time.Time,
) *historypb.HistoryEvent {
	b.assertMutable()

	historyEvent := &historypb.HistoryEvent{}
	historyEvent.EventTime = timestamp.TimePtr(time.UTC())
	historyEvent.EventType = eventType
	historyEvent.Version = b.Version

	if b.bufferEvent(eventType) {
		historyEvent.EventId = common.BufferedEventID
		historyEvent.TaskId = common.EmptyEventTaskID
	} else {
		historyEvent.EventId = b.NextEventID
		b.NextEventID += 1
		// TODO historyEvent.TaskId
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
		enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
		// do not buffer event if event is directly generated from a corresponding command
		return false

	default:
		return true
	}
}
