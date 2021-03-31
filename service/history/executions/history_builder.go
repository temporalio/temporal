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

package executions

import (
	"fmt"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
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
	previousExecution *persistencespb.WorkflowExecutionInfo,
	previousExecutionState *persistencespb.WorkflowExecutionState,
	firstRunID string,
	originalRunID string,
) *historypb.HistoryEvent {
	event := b.newWorkflowExecutionStartedEvent(
		startTime,
		request,
		previousExecution,
		previousExecutionState,
		firstRunID,
		originalRunID,
	)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowTaskScheduledEvent(
	taskQueue *taskqueuepb.TaskQueue,
	startToCloseTimeoutSeconds int32,
	attempt int32,
	now time.Time,
) *historypb.HistoryEvent {
	event := b.newWorkflowTaskScheduledEvent(taskQueue, startToCloseTimeoutSeconds, attempt, now)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowTaskStartedEvent(
	scheduleEventID int64,
	requestID string,
	identity string,
	now time.Time,
) *historypb.HistoryEvent {
	event := b.newWorkflowTaskStartedEvent(scheduleEventID, requestID, identity, now)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowTaskCompletedEvent(
	scheduleEventID int64,
	startedEventID int64,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) *historypb.HistoryEvent {
	event := b.newWorkflowTaskCompletedEvent(scheduleEventID, startedEventID, request)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowTaskTimedOutEvent(
	scheduleEventID int64,
	startedEventID int64,
	timeoutType enumspb.TimeoutType,
) *historypb.HistoryEvent {
	event := b.newWorkflowTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutType)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowTaskFailedEvent(
	attr *historypb.WorkflowTaskFailedEventAttributes,
) *historypb.HistoryEvent {
	event := b.newWorkflowTaskFailedEvent(attr)
	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddActivityTaskScheduledEvent(
	workflowTaskCompletedEventID int64,
	attributes *commandpb.ScheduleActivityTaskCommandAttributes,
) *historypb.HistoryEvent {
	event := b.newActivityTaskScheduledEvent(workflowTaskCompletedEventID, attributes)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddActivityTaskStartedEvent(
	scheduleEventID int64,
	attempt int32,
	requestID string,
	identity string,
	lastFailure *failurepb.Failure,
) *historypb.HistoryEvent {
	event := b.newActivityTaskStartedEvent(scheduleEventID, attempt, requestID, identity, lastFailure)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddActivityTaskCompletedEvent(
	scheduleEventID int64,
	startedEventID int64,
	request *workflowservice.RespondActivityTaskCompletedRequest,
) *historypb.HistoryEvent {
	event := b.newActivityTaskCompletedEvent(scheduleEventID, startedEventID, request)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddActivityTaskFailedEvent(
	scheduleEventID int64,
	startedEventID int64,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
	identity string,
) *historypb.HistoryEvent {
	event := b.newActivityTaskFailedEvent(scheduleEventID, startedEventID, failure, retryState, identity)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddActivityTaskTimedOutEvent(
	scheduleEventID,
	startedEventID int64,
	timeoutFailure *failurepb.Failure,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.newActivityTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutFailure, retryState)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddCompletedWorkflowEvent(
	workflowTaskCompletedEventID int64,
	attributes *commandpb.CompleteWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.newCompleteWorkflowExecutionEvent(workflowTaskCompletedEventID, attributes)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddFailWorkflowEvent(
	workflowTaskCompletedEventID int64,
	retryState enumspb.RetryState,
	attributes *commandpb.FailWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.newFailWorkflowExecutionEvent(workflowTaskCompletedEventID, retryState, attributes)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddTimeoutWorkflowEvent(
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.newTimeoutWorkflowExecutionEvent(retryState)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowExecutionTerminatedEvent(
	reason string,
	details *commonpb.Payloads,
	identity string,
) *historypb.HistoryEvent {
	event := b.newWorkflowExecutionTerminatedEvent(reason, details, identity)
	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddContinuedAsNewEvent(
	workflowTaskCompletedEventID int64,
	newRunID string,
	attributes *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.newWorkflowExecutionContinuedAsNewEvent(workflowTaskCompletedEventID, newRunID, attributes)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddTimerStartedEvent(
	workflowTaskCompletedEventID int64,
	request *commandpb.StartTimerCommandAttributes,
) *historypb.HistoryEvent {

	attributes := &historypb.TimerStartedEventAttributes{
		TimerId:                      request.TimerId,
		StartToFireTimeout:           request.StartToFireTimeout,
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
	}
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_TIMER_STARTED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_TimerStartedEventAttributes{
		TimerStartedEventAttributes: attributes,
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddTimerFiredEvent(
	startedEventID int64,
	timerID string,
) *historypb.HistoryEvent {

	attributes := &historypb.TimerFiredEventAttributes{
		TimerId:        timerID,
		StartedEventId: startedEventID,
	}
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_TIMER_FIRED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: attributes}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddActivityTaskCancelRequestedEvent(
	workflowTaskCompletedEventID int64,
	scheduleID int64,
) *historypb.HistoryEvent {

	attributes := &historypb.ActivityTaskCancelRequestedEventAttributes{
		ScheduledEventId:             scheduleID,
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
	}
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{
		ActivityTaskCancelRequestedEventAttributes: attributes,
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

	attributes := &historypb.ActivityTaskCanceledEventAttributes{
		ScheduledEventId:             scheduleEventID,
		StartedEventId:               startedEventID,
		LatestCancelRequestedEventId: latestCancelRequestedEventID,
		Details:                      details,
		Identity:                     identity,
	}
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{
		ActivityTaskCanceledEventAttributes: attributes,
	}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddTimerCanceledEvent(
	startedEventID int64,
	workflowTaskCompletedEventID int64,
	timerID string,
	identity string,
) *historypb.HistoryEvent {

	attributes := &historypb.TimerCanceledEventAttributes{
		StartedEventId:               startedEventID,
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		TimerId:                      timerID,
		Identity:                     identity,
	}
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_TIMER_CANCELED, b.TimeSource.Now())
	event.Attributes = &historypb.HistoryEvent_TimerCanceledEventAttributes{TimerCanceledEventAttributes: attributes}

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowExecutionCancelRequestedEvent(
	request *historyservice.RequestCancelWorkflowExecutionRequest,
) *historypb.HistoryEvent {
	event := b.newWorkflowExecutionCancelRequestedEvent(request)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowExecutionCanceledEvent(
	workflowTaskCompletedEventID int64,
	attributes *commandpb.CancelWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.newWorkflowExecutionCanceledEvent(workflowTaskCompletedEventID, attributes)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	request *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.newRequestCancelExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, request)

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
	event := b.newRequestCancelExternalWorkflowExecutionFailedEvent(workflowTaskCompletedEventID, initiatedEventID,
		namespace, workflowID, runID, cause)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddExternalWorkflowExecutionCancelRequested(
	initiatedEventID int64,
	namespace string,
	workflowID string,
	runID string,
) *historypb.HistoryEvent {
	event := b.newExternalWorkflowExecutionCancelRequestedEvent(initiatedEventID,
		namespace, workflowID, runID)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddSignalExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	attributes *commandpb.SignalExternalWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.newSignalExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, attributes)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddUpsertWorkflowSearchAttributesEvent(
	workflowTaskCompletedEventID int64,
	attributes *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
) *historypb.HistoryEvent {
	event := b.newUpsertWorkflowSearchAttributesEvent(workflowTaskCompletedEventID, attributes)

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
	event := b.newSignalExternalWorkflowExecutionFailedEvent(workflowTaskCompletedEventID, initiatedEventID,
		namespace, workflowID, runID, control, cause)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddExternalWorkflowExecutionSignaled(
	initiatedEventID int64,
	namespace string,
	workflowID string,
	runID string,
	control string,
) *historypb.HistoryEvent {
	event := b.newExternalWorkflowExecutionSignaledEvent(initiatedEventID,
		namespace, workflowID, runID, control)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddMarkerRecordedEvent(
	workflowTaskCompletedEventID int64,
	attributes *commandpb.RecordMarkerCommandAttributes,
) *historypb.HistoryEvent {
	event := b.newMarkerRecordedEventAttributes(workflowTaskCompletedEventID, attributes)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddWorkflowExecutionSignaledEvent(
	signalName string,
	input *commonpb.Payloads,
	identity string,
) *historypb.HistoryEvent {
	event := b.newWorkflowExecutionSignaledEvent(signalName, input, identity)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddStartChildWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	attributes *commandpb.StartChildWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.newStartChildWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, attributes)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddChildWorkflowExecutionStartedEvent(
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	header *commonpb.Header,
) *historypb.HistoryEvent {
	event := b.newChildWorkflowExecutionStartedEvent(namespace, execution, workflowType, initiatedID, header)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddStartChildWorkflowExecutionFailedEvent(
	initiatedID int64,
	cause enumspb.StartChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes,
) *historypb.HistoryEvent {
	event := b.newStartChildWorkflowExecutionFailedEvent(initiatedID, cause, initiatedEventAttributes)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddChildWorkflowExecutionCompletedEvent(
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	startedID int64,
	completedAttributes *historypb.WorkflowExecutionCompletedEventAttributes,
) *historypb.HistoryEvent {
	event := b.newChildWorkflowExecutionCompletedEvent(namespace, execution, workflowType, initiatedID, startedID,
		completedAttributes)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddChildWorkflowExecutionFailedEvent(
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	startedID int64,
	failedAttributes *historypb.WorkflowExecutionFailedEventAttributes,
) *historypb.HistoryEvent {
	event := b.newChildWorkflowExecutionFailedEvent(namespace, execution, workflowType, initiatedID, startedID,
		failedAttributes)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddChildWorkflowExecutionCanceledEvent(
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	startedID int64,
	canceledAttributes *historypb.WorkflowExecutionCanceledEventAttributes,
) *historypb.HistoryEvent {
	event := b.newChildWorkflowExecutionCanceledEvent(namespace, execution, workflowType, initiatedID, startedID,
		canceledAttributes)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddChildWorkflowExecutionTerminatedEvent(
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	startedID int64,
	terminatedAttributes *historypb.WorkflowExecutionTerminatedEventAttributes,
) *historypb.HistoryEvent {
	event := b.newChildWorkflowExecutionTerminatedEvent(namespace, execution, workflowType, initiatedID, startedID,
		terminatedAttributes)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) AddChildWorkflowExecutionTimedOutEvent(
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	startedID int64,
	timedOutAttributes *historypb.WorkflowExecutionTimedOutEventAttributes,
) *historypb.HistoryEvent {
	event := b.newChildWorkflowExecutionTimedOutEvent(namespace, execution, workflowType, initiatedID, startedID,
		timedOutAttributes)

	return b.appendEvents(event)
}

func (b *HistoryBuilder) newWorkflowExecutionStartedEvent(
	startTime time.Time,
	startRequest *historyservice.StartWorkflowExecutionRequest,
	previousExecution *persistencespb.WorkflowExecutionInfo,
	previousExecutionState *persistencespb.WorkflowExecutionState,
	firstRunID string,
	originalRunID string,
) *historypb.HistoryEvent {
	prevRunID := previousExecutionState.GetRunId()
	resetPoints := previousExecution.GetAutoResetPoints()
	request := startRequest.StartRequest
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, b.TimeSource.Now())
	// need to override the start event timestamp
	// since workflow start time is set by mutable state initialization
	historyEvent.EventTime = timestamp.TimePtr(startTime)
	attributes := &historypb.WorkflowExecutionStartedEventAttributes{
		WorkflowType:                    request.WorkflowType,
		TaskQueue:                       request.TaskQueue,
		Header:                          request.Header,
		Input:                           request.Input,
		WorkflowRunTimeout:              request.WorkflowRunTimeout,
		WorkflowExecutionTimeout:        request.WorkflowExecutionTimeout,
		WorkflowTaskTimeout:             request.WorkflowTaskTimeout,
		ContinuedExecutionRunId:         prevRunID,
		PrevAutoResetPoints:             resetPoints,
		Identity:                        request.Identity,
		RetryPolicy:                     request.RetryPolicy,
		Attempt:                         startRequest.GetAttempt(),
		WorkflowExecutionExpirationTime: startRequest.WorkflowExecutionExpirationTime,
		CronSchedule:                    request.CronSchedule,
		LastCompletionResult:            startRequest.LastCompletionResult,
		ContinuedFailure:                startRequest.GetContinuedFailure(),
		Initiator:                       startRequest.ContinueAsNewInitiator,
		FirstWorkflowTaskBackoff:        startRequest.FirstWorkflowTaskBackoff,
		FirstExecutionRunId:             firstRunID,
		OriginalExecutionRunId:          originalRunID,
		Memo:                            request.Memo,
		SearchAttributes:                request.SearchAttributes,
	}
	parentInfo := startRequest.ParentExecutionInfo
	if parentInfo != nil {
		attributes.ParentWorkflowNamespace = parentInfo.Namespace
		attributes.ParentWorkflowExecution = parentInfo.Execution
		attributes.ParentInitiatedEventId = parentInfo.InitiatedId
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
		WorkflowExecutionStartedEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newWorkflowTaskScheduledEvent(
	taskQueue *taskqueuepb.TaskQueue,
	startToCloseTimeoutSeconds int32,
	attempt int32,
	time time.Time,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, time.UTC())

	return setWorkflowTaskScheduledEventInfo(historyEvent, taskQueue, startToCloseTimeoutSeconds, attempt)
}

func (b *HistoryBuilder) newWorkflowTaskStartedEvent(
	scheduledEventID int64,
	requestID string,
	identity string,
	time time.Time,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, time.UTC())

	return setWorkflowTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func (b *HistoryBuilder) newWorkflowTaskCompletedEvent(
	scheduleEventID int64,
	startedEventID int64,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, b.TimeSource.Now())
	attributes := &historypb.WorkflowTaskCompletedEventAttributes{
		ScheduledEventId: scheduleEventID,
		StartedEventId:   startedEventID,
		Identity:         request.Identity,
		BinaryChecksum:   request.BinaryChecksum,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
		WorkflowTaskCompletedEventAttributes: attributes,
	}
	return historyEvent
}

func (b *HistoryBuilder) newWorkflowTaskTimedOutEvent(
	scheduleEventID int64,
	startedEventID int64,
	timeoutType enumspb.TimeoutType,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT, b.TimeSource.Now())
	attributes := &historypb.WorkflowTaskTimedOutEventAttributes{
		ScheduledEventId: scheduleEventID,
		StartedEventId:   startedEventID,
		TimeoutType:      timeoutType,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowTaskTimedOutEventAttributes{
		WorkflowTaskTimedOutEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newWorkflowTaskFailedEvent(
	attr *historypb.WorkflowTaskFailedEventAttributes,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, b.TimeSource.Now())
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{
		WorkflowTaskFailedEventAttributes: attr,
	}
	return historyEvent
}

func (b *HistoryBuilder) newActivityTaskScheduledEvent(
	workflowTaskCompletedEventID int64,
	scheduleAttributes *commandpb.ScheduleActivityTaskCommandAttributes,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, b.TimeSource.Now())
	attributes := &historypb.ActivityTaskScheduledEventAttributes{
		ActivityId:                   scheduleAttributes.ActivityId,
		ActivityType:                 scheduleAttributes.ActivityType,
		TaskQueue:                    scheduleAttributes.TaskQueue,
		Header:                       scheduleAttributes.Header,
		Input:                        scheduleAttributes.Input,
		ScheduleToCloseTimeout:       scheduleAttributes.ScheduleToCloseTimeout,
		ScheduleToStartTimeout:       scheduleAttributes.ScheduleToStartTimeout,
		StartToCloseTimeout:          scheduleAttributes.StartToCloseTimeout,
		HeartbeatTimeout:             scheduleAttributes.HeartbeatTimeout,
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		RetryPolicy:                  scheduleAttributes.RetryPolicy,
		Namespace:                    scheduleAttributes.Namespace,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
		ActivityTaskScheduledEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newActivityTaskStartedEvent(
	scheduledEventID int64,
	attempt int32,
	requestID string,
	identity string,
	lastFailure *failurepb.Failure,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, b.TimeSource.Now())
	attributes := &historypb.ActivityTaskStartedEventAttributes{
		ScheduledEventId: scheduledEventID,
		Attempt:          attempt,
		Identity:         identity,
		RequestId:        requestID,
		LastFailure:      lastFailure,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
		ActivityTaskStartedEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newActivityTaskCompletedEvent(
	scheduleEventID int64,
	startedEventID int64,
	request *workflowservice.RespondActivityTaskCompletedRequest,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, b.TimeSource.Now())
	attributes := &historypb.ActivityTaskCompletedEventAttributes{
		Result:           request.Result,
		ScheduledEventId: scheduleEventID,
		StartedEventId:   startedEventID,
		Identity:         request.Identity,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
		ActivityTaskCompletedEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newActivityTaskTimedOutEvent(
	scheduleEventID, startedEventID int64,
	timeoutFailure *failurepb.Failure,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT, b.TimeSource.Now())
	attributes := &historypb.ActivityTaskTimedOutEventAttributes{
		ScheduledEventId: scheduleEventID,
		StartedEventId:   startedEventID,
		Failure:          timeoutFailure,
		RetryState:       retryState,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{
		ActivityTaskTimedOutEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newActivityTaskFailedEvent(
	scheduleEventID int64,
	startedEventID int64,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
	identity string,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, b.TimeSource.Now())
	attributes := &historypb.ActivityTaskFailedEventAttributes{
		Failure:          failure,
		RetryState:       retryState,
		ScheduledEventId: scheduleEventID,
		StartedEventId:   startedEventID,
		Identity:         identity,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{
		ActivityTaskFailedEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newCompleteWorkflowExecutionEvent(
	workflowTaskCompletedEventID int64,
	request *commandpb.CompleteWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, b.TimeSource.Now())
	attributes := &historypb.WorkflowExecutionCompletedEventAttributes{
		Result:                       request.Result,
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
		WorkflowExecutionCompletedEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newFailWorkflowExecutionEvent(
	workflowTaskCompletedEventID int64,
	retryState enumspb.RetryState,
	request *commandpb.FailWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, b.TimeSource.Now())
	attributes := &historypb.WorkflowExecutionFailedEventAttributes{
		Failure:                      request.GetFailure(),
		RetryState:                   retryState,
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
		WorkflowExecutionFailedEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newTimeoutWorkflowExecutionEvent(
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT, b.TimeSource.Now())
	attributes := &historypb.WorkflowExecutionTimedOutEventAttributes{
		RetryState: retryState,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
		WorkflowExecutionTimedOutEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newWorkflowExecutionSignaledEvent(
	signalName string,
	input *commonpb.Payloads,
	identity string,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, b.TimeSource.Now())
	attributes := &historypb.WorkflowExecutionSignaledEventAttributes{
		SignalName: signalName,
		Input:      input,
		Identity:   identity,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
		WorkflowExecutionSignaledEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newWorkflowExecutionTerminatedEvent(
	reason string,
	details *commonpb.Payloads,
	identity string,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, b.TimeSource.Now())
	attributes := &historypb.WorkflowExecutionTerminatedEventAttributes{
		Reason:   reason,
		Details:  details,
		Identity: identity,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
		WorkflowExecutionTerminatedEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newMarkerRecordedEventAttributes(
	workflowTaskCompletedEventID int64,
	request *commandpb.RecordMarkerCommandAttributes,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_MARKER_RECORDED, b.TimeSource.Now())
	attributes := &historypb.MarkerRecordedEventAttributes{
		MarkerName:                   request.MarkerName,
		Details:                      request.Details,
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		Header:                       request.Header,
		Failure:                      request.Failure,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_MarkerRecordedEventAttributes{
		MarkerRecordedEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newWorkflowExecutionCancelRequestedEvent(
	request *historyservice.RequestCancelWorkflowExecutionRequest,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED, b.TimeSource.Now())
	attributes := &historypb.WorkflowExecutionCancelRequestedEventAttributes{
		Identity:                  request.CancelRequest.Identity,
		ExternalInitiatedEventId:  request.ExternalInitiatedEventId,
		ExternalWorkflowExecution: request.ExternalWorkflowExecution,
	}
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{
		WorkflowExecutionCancelRequestedEventAttributes: attributes,
	}

	return event
}

func (b *HistoryBuilder) newWorkflowExecutionCanceledEvent(
	workflowTaskCompletedEventID int64,
	request *commandpb.CancelWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED, b.TimeSource.Now())
	attributes := &historypb.WorkflowExecutionCanceledEventAttributes{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		Details:                      request.GetDetails(),
	}
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{
		WorkflowExecutionCanceledEventAttributes: attributes,
	}

	return event
}

func (b *HistoryBuilder) newRequestCancelExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	request *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED, b.TimeSource.Now())
	attributes := &historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		Namespace:                    request.Namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: request.WorkflowId,
			RunId:      request.RunId,
		},
		Control:           request.Control,
		ChildWorkflowOnly: request.ChildWorkflowOnly,
	}
	event.Attributes = &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
		RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: attributes,
	}

	return event
}

func (b *HistoryBuilder) newRequestCancelExternalWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedEventID int64,
	namespace string,
	workflowID string,
	runID string,
	cause enumspb.CancelExternalWorkflowExecutionFailedCause,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED, b.TimeSource.Now())
	attributes := &historypb.RequestCancelExternalWorkflowExecutionFailedEventAttributes{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		InitiatedEventId:             initiatedEventID,
		Namespace:                    namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Cause:   cause,
		Control: "",
	}
	event.Attributes = &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{
		RequestCancelExternalWorkflowExecutionFailedEventAttributes: attributes,
	}

	return event
}

func (b *HistoryBuilder) newExternalWorkflowExecutionCancelRequestedEvent(
	initiatedEventID int64,
	namespace string,
	workflowID string,
	runID string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED, b.TimeSource.Now())
	attributes := &historypb.ExternalWorkflowExecutionCancelRequestedEventAttributes{
		InitiatedEventId: initiatedEventID,
		Namespace:        namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	}
	event.Attributes = &historypb.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{
		ExternalWorkflowExecutionCancelRequestedEventAttributes: attributes,
	}

	return event
}

func (b *HistoryBuilder) newSignalExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	request *commandpb.SignalExternalWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED, b.TimeSource.Now())
	attributes := &historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		Namespace:                    request.Namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: request.Execution.WorkflowId,
			RunId:      request.Execution.RunId,
		},
		SignalName:        request.GetSignalName(),
		Input:             request.Input,
		Control:           request.Control,
		ChildWorkflowOnly: request.ChildWorkflowOnly,
	}
	event.Attributes = &historypb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{
		SignalExternalWorkflowExecutionInitiatedEventAttributes: attributes,
	}

	return event
}

func (b *HistoryBuilder) newUpsertWorkflowSearchAttributesEvent(
	workflowTaskCompletedEventID int64,
	request *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES, b.TimeSource.Now())
	attributes := &historypb.UpsertWorkflowSearchAttributesEventAttributes{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		SearchAttributes:             request.GetSearchAttributes(),
	}
	event.Attributes = &historypb.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{
		UpsertWorkflowSearchAttributesEventAttributes: attributes,
	}

	return event
}

func (b *HistoryBuilder) newSignalExternalWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedEventID int64,
	namespace string,
	workflowID string,
	runID string,
	control string,
	cause enumspb.SignalExternalWorkflowExecutionFailedCause,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED, b.TimeSource.Now())
	attributes := &historypb.SignalExternalWorkflowExecutionFailedEventAttributes{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		InitiatedEventId:             initiatedEventID,
		Namespace:                    namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Cause:   cause,
		Control: control,
	}
	event.Attributes = &historypb.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{
		SignalExternalWorkflowExecutionFailedEventAttributes: attributes,
	}

	return event
}

func (b *HistoryBuilder) newExternalWorkflowExecutionSignaledEvent(
	initiatedEventID int64,
	namespace string,
	workflowID string,
	runID string,
	control string,
) *historypb.HistoryEvent {
	event := b.createNewHistoryEvent(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED, b.TimeSource.Now())
	attributes := &historypb.ExternalWorkflowExecutionSignaledEventAttributes{
		InitiatedEventId: initiatedEventID,
		Namespace:        namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Control: control,
	}
	event.Attributes = &historypb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{
		ExternalWorkflowExecutionSignaledEventAttributes: attributes,
	}

	return event
}

func (b *HistoryBuilder) newWorkflowExecutionContinuedAsNewEvent(
	workflowTaskCompletedEventID int64,
	newRunID string,
	request *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, b.TimeSource.Now())
	attributes := &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
		NewExecutionRunId:            newRunID,
		WorkflowType:                 request.WorkflowType,
		TaskQueue:                    request.TaskQueue,
		Header:                       request.Header,
		Input:                        request.Input,
		WorkflowRunTimeout:           request.WorkflowRunTimeout,
		WorkflowTaskTimeout:          request.WorkflowTaskTimeout,
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		BackoffStartInterval:         request.GetBackoffStartInterval(),
		Initiator:                    enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW,
		Failure:                      request.GetFailure(),
		LastCompletionResult:         request.LastCompletionResult,
		Memo:                         request.Memo,
		SearchAttributes:             request.SearchAttributes,
	}

	if len(request.CronSchedule) != 0 {
		attributes.Initiator = enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{
		WorkflowExecutionContinuedAsNewEventAttributes: attributes,
	}
	return historyEvent
}

func (b *HistoryBuilder) newStartChildWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	startAttributes *commandpb.StartChildWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED, b.TimeSource.Now())
	attributes := &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
		Namespace:                    startAttributes.Namespace,
		WorkflowId:                   startAttributes.WorkflowId,
		WorkflowType:                 startAttributes.WorkflowType,
		TaskQueue:                    startAttributes.TaskQueue,
		Header:                       startAttributes.Header,
		Input:                        startAttributes.Input,
		WorkflowExecutionTimeout:     startAttributes.WorkflowExecutionTimeout,
		WorkflowRunTimeout:           startAttributes.WorkflowRunTimeout,
		WorkflowTaskTimeout:          startAttributes.WorkflowTaskTimeout,
		Control:                      startAttributes.Control,
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		WorkflowIdReusePolicy:        startAttributes.WorkflowIdReusePolicy,
		RetryPolicy:                  startAttributes.RetryPolicy,
		CronSchedule:                 startAttributes.CronSchedule,
		Memo:                         startAttributes.Memo,
		SearchAttributes:             startAttributes.SearchAttributes,
		ParentClosePolicy:            startAttributes.GetParentClosePolicy(),
	}
	historyEvent.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{
		StartChildWorkflowExecutionInitiatedEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newChildWorkflowExecutionStartedEvent(
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	header *commonpb.Header,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED, b.TimeSource.Now())
	attributes := &historypb.ChildWorkflowExecutionStartedEventAttributes{
		Namespace:         namespace,
		WorkflowExecution: execution,
		WorkflowType:      workflowType,
		InitiatedEventId:  initiatedID,
		Header:            header,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{
		ChildWorkflowExecutionStartedEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newStartChildWorkflowExecutionFailedEvent(
	initiatedID int64,
	cause enumspb.StartChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED, b.TimeSource.Now())
	attributes := &historypb.StartChildWorkflowExecutionFailedEventAttributes{
		Namespace:                    initiatedEventAttributes.Namespace,
		WorkflowId:                   initiatedEventAttributes.WorkflowId,
		WorkflowType:                 initiatedEventAttributes.WorkflowType,
		InitiatedEventId:             initiatedID,
		WorkflowTaskCompletedEventId: initiatedEventAttributes.WorkflowTaskCompletedEventId,
		Control:                      initiatedEventAttributes.Control,
		Cause:                        cause,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{
		StartChildWorkflowExecutionFailedEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newChildWorkflowExecutionCompletedEvent(
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	startedID int64,
	completedAttributes *historypb.WorkflowExecutionCompletedEventAttributes,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED, b.TimeSource.Now())
	attributes := &historypb.ChildWorkflowExecutionCompletedEventAttributes{
		Namespace:         namespace,
		WorkflowExecution: execution,
		WorkflowType:      workflowType,
		InitiatedEventId:  initiatedID,
		StartedEventId:    startedID,
		Result:            completedAttributes.Result,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{
		ChildWorkflowExecutionCompletedEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newChildWorkflowExecutionFailedEvent(
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	startedID int64,
	failedAttributes *historypb.WorkflowExecutionFailedEventAttributes,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED, b.TimeSource.Now())
	attributes := &historypb.ChildWorkflowExecutionFailedEventAttributes{
		Namespace:         namespace,
		WorkflowExecution: execution,
		WorkflowType:      workflowType,
		InitiatedEventId:  initiatedID,
		StartedEventId:    startedID,
		Failure:           failedAttributes.GetFailure(),
		RetryState:        failedAttributes.GetRetryState(),
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{
		ChildWorkflowExecutionFailedEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newChildWorkflowExecutionCanceledEvent(
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	startedID int64,
	canceledAttributes *historypb.WorkflowExecutionCanceledEventAttributes,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED, b.TimeSource.Now())

	attributes := &historypb.ChildWorkflowExecutionCanceledEventAttributes{
		Namespace:         namespace,
		WorkflowExecution: execution,
		WorkflowType:      workflowType,
		InitiatedEventId:  initiatedID,
		StartedEventId:    startedID,
		Details:           canceledAttributes.Details,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{
		ChildWorkflowExecutionCanceledEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newChildWorkflowExecutionTerminatedEvent(
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	startedID int64,
	terminatedAttributes *historypb.WorkflowExecutionTerminatedEventAttributes,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED, b.TimeSource.Now())
	attributes := &historypb.ChildWorkflowExecutionTerminatedEventAttributes{
		Namespace:         namespace,
		WorkflowExecution: execution,
		WorkflowType:      workflowType,
		InitiatedEventId:  initiatedID,
		StartedEventId:    startedID,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{
		ChildWorkflowExecutionTerminatedEventAttributes: attributes,
	}

	return historyEvent
}

func (b *HistoryBuilder) newChildWorkflowExecutionTimedOutEvent(
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	startedID int64,
	timedOutAttributes *historypb.WorkflowExecutionTimedOutEventAttributes,
) *historypb.HistoryEvent {
	historyEvent := b.createNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT, b.TimeSource.Now())
	attributes := &historypb.ChildWorkflowExecutionTimedOutEventAttributes{
		Namespace:         namespace,
		WorkflowExecution: execution,
		WorkflowType:      workflowType,
		InitiatedEventId:  initiatedID,
		StartedEventId:    startedID,
		RetryState:        timedOutAttributes.GetRetryState(),
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{
		ChildWorkflowExecutionTimedOutEventAttributes: attributes,
	}

	return historyEvent
}

func newWorkflowTaskScheduledEventWithInfo(
	eventID int64,
	timestamp *time.Time,
	taskQueue *taskqueuepb.TaskQueue,
	startToCloseTimeoutSeconds int32,
	attempt int32,
) *historypb.HistoryEvent {
	historyEvent := createNewHistoryEvent(eventID, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, timestamp)

	return setWorkflowTaskScheduledEventInfo(historyEvent, taskQueue, startToCloseTimeoutSeconds, attempt)
}

func newWorkflowTaskStartedEventWithInfo(
	eventID int64,
	timestamp *time.Time,
	scheduledEventID int64,
	requestID string,
	identity string,
) *historypb.HistoryEvent {
	historyEvent := createNewHistoryEvent(eventID, enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, timestamp)

	return setWorkflowTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func createNewHistoryEvent(
	eventID int64,
	eventType enumspb.EventType,
	eventTime *time.Time,
) *historypb.HistoryEvent {
	historyEvent := &historypb.HistoryEvent{
		EventId:   eventID,
		EventTime: eventTime,
		EventType: eventType,
	}

	return historyEvent
}

func setWorkflowTaskScheduledEventInfo(
	historyEvent *historypb.HistoryEvent,
	taskQueue *taskqueuepb.TaskQueue,
	startToCloseTimeoutSeconds int32,
	attempt int32,
) *historypb.HistoryEvent {
	attributes := &historypb.WorkflowTaskScheduledEventAttributes{
		TaskQueue:           taskQueue,
		StartToCloseTimeout: timestamp.DurationPtr(time.Duration(startToCloseTimeoutSeconds) * time.Second),
		Attempt:             attempt,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
		WorkflowTaskScheduledEventAttributes: attributes,
	}
	return historyEvent
}

func setWorkflowTaskStartedEventInfo(
	historyEvent *historypb.HistoryEvent,
	scheduledEventID int64,
	requestID string,
	identity string,
) *historypb.HistoryEvent {
	attributes := &historypb.WorkflowTaskStartedEventAttributes{
		ScheduledEventId: scheduledEventID,
		Identity:         identity,
		RequestId:        requestID,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
		WorkflowTaskStartedEventAttributes: attributes,
	}
	return historyEvent
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

func (b *HistoryBuilder) assertMutable() {
	if b.state != HistoryBuilderStateMutable {
		panic(fmt.Sprintf("history builder is mutated while in immutable state"))
	}
}

func (b *HistoryBuilder) Finish() [][]*historypb.HistoryEvent {
	b.assertMutable()
	b.FlushEvents()
	b.state = HistoryBuilderStateImmutable
	return b.eventsBatch
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
