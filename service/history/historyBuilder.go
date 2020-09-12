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

package history

import (
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	historyBuilder struct {
		transientHistory []*historypb.HistoryEvent
		history          []*historypb.HistoryEvent
		msBuilder        mutableState
	}
)

func newHistoryBuilder(msBuilder mutableState, logger log.Logger) *historyBuilder {
	return &historyBuilder{
		transientHistory: []*historypb.HistoryEvent{},
		history:          []*historypb.HistoryEvent{},
		msBuilder:        msBuilder,
	}
}

func newHistoryBuilderFromEvents(history []*historypb.HistoryEvent, logger log.Logger) *historyBuilder {
	return &historyBuilder{
		history: history,
	}
}

func (b *historyBuilder) GetFirstEvent() *historypb.HistoryEvent {
	// Transient workflow task events are always written before other events
	if b.transientHistory != nil && len(b.transientHistory) > 0 {
		return b.transientHistory[0]
	}

	if b.history != nil && len(b.history) > 0 {
		return b.history[0]
	}

	return nil
}

func (b *historyBuilder) HasTransientEvents() bool {
	return b.transientHistory != nil && len(b.transientHistory) > 0
}

// originalRunID is the runID when the WorkflowExecutionStarted event is written
// firstRunID is the very first runID along the chain of ContinueAsNew and Reset
func (b *historyBuilder) AddWorkflowExecutionStartedEvent(request *historyservice.StartWorkflowExecutionRequest,
	previousExecution *persistence.WorkflowExecutionInfo, firstRunID, originalRunID string) *historypb.HistoryEvent {
	event := b.newWorkflowExecutionStartedEvent(request, previousExecution, firstRunID, originalRunID)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowTaskScheduledEvent(taskQueue *taskqueuepb.TaskQueue, startToCloseTimeoutSeconds int32, attempt int32) *historypb.HistoryEvent {
	event := b.newWorkflowTaskScheduledEvent(taskQueue, startToCloseTimeoutSeconds, attempt)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTransientWorkflowTaskScheduledEvent(taskQueue *taskqueuepb.TaskQueue, startToCloseTimeoutSeconds int32, attempt int32, time time.Time) *historypb.HistoryEvent {
	event := b.newTransientWorkflowTaskScheduledEvent(taskQueue, startToCloseTimeoutSeconds, attempt, time)

	return b.addTransientEvent(event)
}

func (b *historyBuilder) AddWorkflowTaskStartedEvent(scheduleEventID int64, requestID string,
	identity string) *historypb.HistoryEvent {
	event := b.newWorkflowTaskStartedEvent(scheduleEventID, requestID, identity)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTransientWorkflowTaskStartedEvent(scheduleEventID int64, requestID string,
	identity string, time time.Time) *historypb.HistoryEvent {
	event := b.newTransientWorkflowTaskStartedEvent(scheduleEventID, requestID, identity, time)

	return b.addTransientEvent(event)
}

func (b *historyBuilder) AddWorkflowTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondWorkflowTaskCompletedRequest) *historypb.HistoryEvent {
	event := b.newWorkflowTaskCompletedEvent(scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowTaskTimedOutEvent(scheduleEventID int64,
	startedEventID int64, timeoutType enumspb.TimeoutType) *historypb.HistoryEvent {
	event := b.newWorkflowTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutType)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowTaskFailedEvent(attr *historypb.WorkflowTaskFailedEventAttributes) *historypb.HistoryEvent {
	event := b.newWorkflowTaskFailedEvent(attr)
	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskScheduledEvent(workflowTaskCompletedEventID int64,
	attributes *commandpb.ScheduleActivityTaskCommandAttributes) *historypb.HistoryEvent {
	event := b.newActivityTaskScheduledEvent(workflowTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskStartedEvent(
	scheduleEventID int64,
	attempt int32,
	requestID string,
	identity string,
	lastFailure *failurepb.Failure,
) *historypb.HistoryEvent {
	event := b.newActivityTaskStartedEvent(scheduleEventID, attempt, requestID, identity, lastFailure)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondActivityTaskCompletedRequest) *historypb.HistoryEvent {
	event := b.newActivityTaskCompletedEvent(scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskFailedEvent(scheduleEventID, startedEventID int64,
	failure *failurepb.Failure, retryState enumspb.RetryState, identity string) *historypb.HistoryEvent {
	event := b.newActivityTaskFailedEvent(scheduleEventID, startedEventID, failure, retryState, identity)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskTimedOutEvent(
	scheduleEventID,
	startedEventID int64,
	timeoutFailure *failurepb.Failure,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.newActivityTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutFailure, retryState)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddCompletedWorkflowEvent(workflowTaskCompletedEventID int64,
	attributes *commandpb.CompleteWorkflowExecutionCommandAttributes) *historypb.HistoryEvent {
	event := b.newCompleteWorkflowExecutionEvent(workflowTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddFailWorkflowEvent(workflowTaskCompletedEventID int64, retryState enumspb.RetryState,
	attributes *commandpb.FailWorkflowExecutionCommandAttributes) *historypb.HistoryEvent {
	event := b.newFailWorkflowExecutionEvent(workflowTaskCompletedEventID, retryState, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimeoutWorkflowEvent(retryState enumspb.RetryState) *historypb.HistoryEvent {
	event := b.newTimeoutWorkflowExecutionEvent(retryState)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionTerminatedEvent(
	reason string,
	details *commonpb.Payloads,
	identity string,
) *historypb.HistoryEvent {
	event := b.newWorkflowExecutionTerminatedEvent(reason, details, identity)
	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddContinuedAsNewEvent(workflowTaskCompletedEventID int64, newRunID string,
	attributes *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes) *historypb.HistoryEvent {
	event := b.newWorkflowExecutionContinuedAsNewEvent(workflowTaskCompletedEventID, newRunID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerStartedEvent(workflowTaskCompletedEventID int64,
	request *commandpb.StartTimerCommandAttributes) *historypb.HistoryEvent {

	attributes := &historypb.TimerStartedEventAttributes{
		TimerId:                      request.TimerId,
		StartToFireTimeout:           request.StartToFireTimeout,
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
	}
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_TIMER_STARTED)
	event.Attributes = &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerFiredEvent(
	startedEventID int64,
	timerID string,
) *historypb.HistoryEvent {

	attributes := &historypb.TimerFiredEventAttributes{
		TimerId:        timerID,
		StartedEventId: startedEventID,
	}
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_TIMER_FIRED)
	event.Attributes = &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCancelRequestedEvent(workflowTaskCompletedEventID int64,
	scheduleID int64) *historypb.HistoryEvent {

	attributes := &historypb.ActivityTaskCancelRequestedEventAttributes{
		ScheduledEventId:             scheduleID,
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
	}
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED)
	event.Attributes = &historypb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{ActivityTaskCancelRequestedEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCanceledEvent(scheduleEventID, startedEventID int64,
	latestCancelRequestedEventID int64, details *commonpb.Payloads, identity string) *historypb.HistoryEvent {

	attributes := &historypb.ActivityTaskCanceledEventAttributes{
		ScheduledEventId:             scheduleEventID,
		StartedEventId:               startedEventID,
		LatestCancelRequestedEventId: latestCancelRequestedEventID,
		Details:                      details,
		Identity:                     identity,
	}
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED)
	event.Attributes = &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{ActivityTaskCanceledEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerCanceledEvent(startedEventID int64,
	workflowTaskCompletedEventID int64, timerID string, identity string) *historypb.HistoryEvent {

	attributes := &historypb.TimerCanceledEventAttributes{
		StartedEventId:               startedEventID,
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		TimerId:                      timerID,
		Identity:                     identity,
	}
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_TIMER_CANCELED)
	event.Attributes = &historypb.HistoryEvent_TimerCanceledEventAttributes{TimerCanceledEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionCancelRequestedEvent(cause string,
	request *historyservice.RequestCancelWorkflowExecutionRequest) *historypb.HistoryEvent {
	event := b.newWorkflowExecutionCancelRequestedEvent(cause, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionCanceledEvent(workflowTaskCompletedEventID int64,
	attributes *commandpb.CancelWorkflowExecutionCommandAttributes) *historypb.HistoryEvent {
	event := b.newWorkflowExecutionCanceledEvent(workflowTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddRequestCancelExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID int64,
	request *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes) *historypb.HistoryEvent {
	event := b.newRequestCancelExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddRequestCancelExternalWorkflowExecutionFailedEvent(workflowTaskCompletedEventID, initiatedEventID int64,
	namespace, workflowID, runID string, cause enumspb.CancelExternalWorkflowExecutionFailedCause) *historypb.HistoryEvent {
	event := b.newRequestCancelExternalWorkflowExecutionFailedEvent(workflowTaskCompletedEventID, initiatedEventID,
		namespace, workflowID, runID, cause)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddExternalWorkflowExecutionCancelRequested(initiatedEventID int64,
	namespace, workflowID, runID string) *historypb.HistoryEvent {
	event := b.newExternalWorkflowExecutionCancelRequestedEvent(initiatedEventID,
		namespace, workflowID, runID)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddSignalExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID int64,
	attributes *commandpb.SignalExternalWorkflowExecutionCommandAttributes) *historypb.HistoryEvent {
	event := b.newSignalExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddUpsertWorkflowSearchAttributesEvent(
	workflowTaskCompletedEventID int64,
	attributes *commandpb.UpsertWorkflowSearchAttributesCommandAttributes) *historypb.HistoryEvent {
	event := b.newUpsertWorkflowSearchAttributesEvent(workflowTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddSignalExternalWorkflowExecutionFailedEvent(workflowTaskCompletedEventID, initiatedEventID int64,
	namespace, workflowID, runID, control string, cause enumspb.SignalExternalWorkflowExecutionFailedCause) *historypb.HistoryEvent {
	event := b.newSignalExternalWorkflowExecutionFailedEvent(workflowTaskCompletedEventID, initiatedEventID,
		namespace, workflowID, runID, control, cause)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddExternalWorkflowExecutionSignaled(initiatedEventID int64,
	namespace, workflowID, runID, control string) *historypb.HistoryEvent {
	event := b.newExternalWorkflowExecutionSignaledEvent(initiatedEventID,
		namespace, workflowID, runID, control)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddMarkerRecordedEvent(workflowTaskCompletedEventID int64,
	attributes *commandpb.RecordMarkerCommandAttributes) *historypb.HistoryEvent {
	event := b.newMarkerRecordedEventAttributes(workflowTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionSignaledEvent(
	signalName string, input *commonpb.Payloads, identity string) *historypb.HistoryEvent {
	event := b.newWorkflowExecutionSignaledEvent(signalName, input, identity)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddStartChildWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID int64,
	attributes *commandpb.StartChildWorkflowExecutionCommandAttributes) *historypb.HistoryEvent {
	event := b.newStartChildWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionStartedEvent(
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	header *commonpb.Header,
) *historypb.HistoryEvent {
	event := b.newChildWorkflowExecutionStartedEvent(namespace, execution, workflowType, initiatedID, header)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddStartChildWorkflowExecutionFailedEvent(initiatedID int64,
	cause enumspb.StartChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes) *historypb.HistoryEvent {
	event := b.newStartChildWorkflowExecutionFailedEvent(initiatedID, cause, initiatedEventAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionCompletedEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	completedAttributes *historypb.WorkflowExecutionCompletedEventAttributes) *historypb.HistoryEvent {
	event := b.newChildWorkflowExecutionCompletedEvent(namespace, execution, workflowType, initiatedID, startedID,
		completedAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionFailedEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	failedAttributes *historypb.WorkflowExecutionFailedEventAttributes) *historypb.HistoryEvent {
	event := b.newChildWorkflowExecutionFailedEvent(namespace, execution, workflowType, initiatedID, startedID,
		failedAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionCanceledEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	canceledAttributes *historypb.WorkflowExecutionCanceledEventAttributes) *historypb.HistoryEvent {
	event := b.newChildWorkflowExecutionCanceledEvent(namespace, execution, workflowType, initiatedID, startedID,
		canceledAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionTerminatedEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	terminatedAttributes *historypb.WorkflowExecutionTerminatedEventAttributes) *historypb.HistoryEvent {
	event := b.newChildWorkflowExecutionTerminatedEvent(namespace, execution, workflowType, initiatedID, startedID,
		terminatedAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionTimedOutEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	timedOutAttributes *historypb.WorkflowExecutionTimedOutEventAttributes) *historypb.HistoryEvent {
	event := b.newChildWorkflowExecutionTimedOutEvent(namespace, execution, workflowType, initiatedID, startedID,
		timedOutAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) addEventToHistory(event *historypb.HistoryEvent) *historypb.HistoryEvent {
	b.history = append(b.history, event)
	return event
}

func (b *historyBuilder) addTransientEvent(event *historypb.HistoryEvent) *historypb.HistoryEvent {
	b.transientHistory = append(b.transientHistory, event)
	return event
}

func (b *historyBuilder) newWorkflowExecutionStartedEvent(
	startRequest *historyservice.StartWorkflowExecutionRequest, previousExecution *persistence.WorkflowExecutionInfo, firstRunID, originalRunID string) *historypb.HistoryEvent {
	var prevRunID string
	var resetPoints *workflowpb.ResetPoints
	if previousExecution != nil {
		prevRunID = previousExecution.ExecutionState.RunId
		resetPoints = previousExecution.AutoResetPoints
	}
	request := startRequest.StartRequest
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
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
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newWorkflowTaskScheduledEvent(taskQueue *taskqueuepb.TaskQueue, startToCloseTimeoutSeconds int32, attempt int32) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)

	return setWorkflowTaskScheduledEventInfo(historyEvent, taskQueue, startToCloseTimeoutSeconds, attempt)
}

func (b *historyBuilder) newTransientWorkflowTaskScheduledEvent(taskQueue *taskqueuepb.TaskQueue, startToCloseTimeoutSeconds int32, attempt int32, time time.Time) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEventWithTime(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, time)

	return setWorkflowTaskScheduledEventInfo(historyEvent, taskQueue, startToCloseTimeoutSeconds, attempt)
}

func (b *historyBuilder) newWorkflowTaskStartedEvent(scheduledEventID int64, requestID string,
	identity string) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED)

	return setWorkflowTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func (b *historyBuilder) newTransientWorkflowTaskStartedEvent(scheduledEventID int64, requestID string,
	identity string, time time.Time) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEventWithTime(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, time)

	return setWorkflowTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func (b *historyBuilder) newWorkflowTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondWorkflowTaskCompletedRequest) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)
	attributes := &historypb.WorkflowTaskCompletedEventAttributes{
		ScheduledEventId: scheduleEventID,
		StartedEventId:   startedEventID,
		Identity:         request.Identity,
		BinaryChecksum:   request.BinaryChecksum,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: attributes}
	return historyEvent
}

func (b *historyBuilder) newWorkflowTaskTimedOutEvent(scheduleEventID int64, startedEventID int64, timeoutType enumspb.TimeoutType) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT)
	attributes := &historypb.WorkflowTaskTimedOutEventAttributes{
		ScheduledEventId: scheduleEventID,
		StartedEventId:   startedEventID,
		TimeoutType:      timeoutType,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowTaskTimedOutEventAttributes{WorkflowTaskTimedOutEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newWorkflowTaskFailedEvent(attr *historypb.WorkflowTaskFailedEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED)
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{WorkflowTaskFailedEventAttributes: attr}
	return historyEvent
}

func (b *historyBuilder) newActivityTaskScheduledEvent(workflowTaskCompletedEventID int64,
	scheduleAttributes *commandpb.ScheduleActivityTaskCommandAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
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
	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newActivityTaskStartedEvent(
	scheduledEventID int64,
	attempt int32,
	requestID string,
	identity string,
	lastFailure *failurepb.Failure,
) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED)
	attributes := &historypb.ActivityTaskStartedEventAttributes{
		ScheduledEventId: scheduledEventID,
		Attempt:          attempt,
		Identity:         identity,
		RequestId:        requestID,
		LastFailure:      lastFailure,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newActivityTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondActivityTaskCompletedRequest) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED)
	attributes := &historypb.ActivityTaskCompletedEventAttributes{
		Result:           request.Result,
		ScheduledEventId: scheduleEventID,
		StartedEventId:   startedEventID,
		Identity:         request.Identity,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newActivityTaskTimedOutEvent(
	scheduleEventID, startedEventID int64,
	timeoutFailure *failurepb.Failure,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT)
	attributes := &historypb.ActivityTaskTimedOutEventAttributes{
		ScheduledEventId: scheduleEventID,
		StartedEventId:   startedEventID,
		Failure:          timeoutFailure,
		RetryState:       retryState,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newActivityTaskFailedEvent(scheduleEventID, startedEventID int64,
	failure *failurepb.Failure, retryState enumspb.RetryState, identity string) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED)
	attributes := &historypb.ActivityTaskFailedEventAttributes{
		Failure:          failure,
		RetryState:       retryState,
		ScheduledEventId: scheduleEventID,
		StartedEventId:   startedEventID,
		Identity:         identity,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newCompleteWorkflowExecutionEvent(workflowTaskCompletedEventID int64,
	request *commandpb.CompleteWorkflowExecutionCommandAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED)
	attributes := &historypb.WorkflowExecutionCompletedEventAttributes{
		Result:                       request.Result,
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newFailWorkflowExecutionEvent(workflowTaskCompletedEventID int64, retryState enumspb.RetryState,
	request *commandpb.FailWorkflowExecutionCommandAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED)
	attributes := &historypb.WorkflowExecutionFailedEventAttributes{
		Failure:                      request.GetFailure(),
		RetryState:                   retryState,
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newTimeoutWorkflowExecutionEvent(retryState enumspb.RetryState) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT)
	attributes := &historypb.WorkflowExecutionTimedOutEventAttributes{
		RetryState: retryState,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{WorkflowExecutionTimedOutEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionSignaledEvent(
	signalName string, input *commonpb.Payloads, identity string) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
	attributes := &historypb.WorkflowExecutionSignaledEventAttributes{
		SignalName: signalName,
		Input:      input,
		Identity:   identity,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionTerminatedEvent(
	reason string, details *commonpb.Payloads, identity string) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED)
	attributes := &historypb.WorkflowExecutionTerminatedEventAttributes{
		Reason:   reason,
		Details:  details,
		Identity: identity,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newMarkerRecordedEventAttributes(workflowTaskCompletedEventID int64,
	request *commandpb.RecordMarkerCommandAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_MARKER_RECORDED)
	attributes := &historypb.MarkerRecordedEventAttributes{
		MarkerName:                   request.MarkerName,
		Details:                      request.Details,
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		Header:                       request.Header,
		Failure:                      request.Failure,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionCancelRequestedEvent(cause string,
	request *historyservice.RequestCancelWorkflowExecutionRequest) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED)
	attributes := &historypb.WorkflowExecutionCancelRequestedEventAttributes{
		Cause:                     cause,
		Identity:                  request.CancelRequest.Identity,
		ExternalInitiatedEventId:  request.ExternalInitiatedEventId,
		ExternalWorkflowExecution: request.ExternalWorkflowExecution,
	}
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{WorkflowExecutionCancelRequestedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newWorkflowExecutionCanceledEvent(workflowTaskCompletedEventID int64,
	request *commandpb.CancelWorkflowExecutionCommandAttributes) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED)
	attributes := &historypb.WorkflowExecutionCanceledEventAttributes{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		Details:                      request.GetDetails(),
	}
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{WorkflowExecutionCanceledEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newRequestCancelExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID int64,
	request *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED)
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
	event.Attributes = &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newRequestCancelExternalWorkflowExecutionFailedEvent(workflowTaskCompletedEventID, initiatedEventID int64,
	namespace, workflowID, runID string, cause enumspb.CancelExternalWorkflowExecutionFailedCause) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED)
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
	event.Attributes = &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{RequestCancelExternalWorkflowExecutionFailedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newExternalWorkflowExecutionCancelRequestedEvent(initiatedEventID int64,
	namespace, workflowID, runID string) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED)
	attributes := &historypb.ExternalWorkflowExecutionCancelRequestedEventAttributes{
		InitiatedEventId: initiatedEventID,
		Namespace:        namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	}
	event.Attributes = &historypb.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{ExternalWorkflowExecutionCancelRequestedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newSignalExternalWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID int64,
	request *commandpb.SignalExternalWorkflowExecutionCommandAttributes) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED)
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
	event.Attributes = &historypb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{SignalExternalWorkflowExecutionInitiatedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newUpsertWorkflowSearchAttributesEvent(workflowTaskCompletedEventID int64,
	request *commandpb.UpsertWorkflowSearchAttributesCommandAttributes) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES)
	attributes := &historypb.UpsertWorkflowSearchAttributesEventAttributes{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		SearchAttributes:             request.GetSearchAttributes(),
	}
	event.Attributes = &historypb.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{UpsertWorkflowSearchAttributesEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newSignalExternalWorkflowExecutionFailedEvent(workflowTaskCompletedEventID, initiatedEventID int64,
	namespace, workflowID, runID, control string, cause enumspb.SignalExternalWorkflowExecutionFailedCause) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED)
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
	event.Attributes = &historypb.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{SignalExternalWorkflowExecutionFailedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newExternalWorkflowExecutionSignaledEvent(initiatedEventID int64,
	namespace, workflowID, runID, control string) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED)
	attributes := &historypb.ExternalWorkflowExecutionSignaledEventAttributes{
		InitiatedEventId: initiatedEventID,
		Namespace:        namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Control: control,
	}
	event.Attributes = &historypb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{ExternalWorkflowExecutionSignaledEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newWorkflowExecutionContinuedAsNewEvent(workflowTaskCompletedEventID int64,
	newRunID string, request *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW)
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
		Initiator:                    request.Initiator,
		Failure:                      request.GetFailure(),
		LastCompletionResult:         request.LastCompletionResult,
		Memo:                         request.Memo,
		SearchAttributes:             request.SearchAttributes,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newStartChildWorkflowExecutionInitiatedEvent(workflowTaskCompletedEventID int64,
	startAttributes *commandpb.StartChildWorkflowExecutionCommandAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED)
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
	historyEvent.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{StartChildWorkflowExecutionInitiatedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionStartedEvent(
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	header *commonpb.Header,
) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED)
	attributes := &historypb.ChildWorkflowExecutionStartedEventAttributes{
		Namespace:         namespace,
		WorkflowExecution: execution,
		WorkflowType:      workflowType,
		InitiatedEventId:  initiatedID,
		Header:            header,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newStartChildWorkflowExecutionFailedEvent(initiatedID int64,
	cause enumspb.StartChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED)
	attributes := &historypb.StartChildWorkflowExecutionFailedEventAttributes{
		Namespace:                    initiatedEventAttributes.Namespace,
		WorkflowId:                   initiatedEventAttributes.WorkflowId,
		WorkflowType:                 initiatedEventAttributes.WorkflowType,
		InitiatedEventId:             initiatedID,
		WorkflowTaskCompletedEventId: initiatedEventAttributes.WorkflowTaskCompletedEventId,
		Control:                      initiatedEventAttributes.Control,
		Cause:                        cause,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{StartChildWorkflowExecutionFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionCompletedEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	completedAttributes *historypb.WorkflowExecutionCompletedEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED)
	attributes := &historypb.ChildWorkflowExecutionCompletedEventAttributes{
		Namespace:         namespace,
		WorkflowExecution: execution,
		WorkflowType:      workflowType,
		InitiatedEventId:  initiatedID,
		StartedEventId:    startedID,
		Result:            completedAttributes.Result,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{ChildWorkflowExecutionCompletedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionFailedEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	failedAttributes *historypb.WorkflowExecutionFailedEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED)
	attributes := &historypb.ChildWorkflowExecutionFailedEventAttributes{
		Namespace:         namespace,
		WorkflowExecution: execution,
		WorkflowType:      workflowType,
		InitiatedEventId:  initiatedID,
		StartedEventId:    startedID,
		Failure:           failedAttributes.GetFailure(),
		RetryState:        failedAttributes.GetRetryState(),
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{ChildWorkflowExecutionFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionCanceledEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	canceledAttributes *historypb.WorkflowExecutionCanceledEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED)

	attributes := &historypb.ChildWorkflowExecutionCanceledEventAttributes{
		Namespace:         namespace,
		WorkflowExecution: execution,
		WorkflowType:      workflowType,
		InitiatedEventId:  initiatedID,
		StartedEventId:    startedID,
		Details:           canceledAttributes.Details,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{ChildWorkflowExecutionCanceledEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionTerminatedEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	terminatedAttributes *historypb.WorkflowExecutionTerminatedEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED)
	attributes := &historypb.ChildWorkflowExecutionTerminatedEventAttributes{
		Namespace:         namespace,
		WorkflowExecution: execution,
		WorkflowType:      workflowType,
		InitiatedEventId:  initiatedID,
		StartedEventId:    startedID,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{ChildWorkflowExecutionTerminatedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionTimedOutEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	timedOutAttributes *historypb.WorkflowExecutionTimedOutEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT)
	attributes := &historypb.ChildWorkflowExecutionTimedOutEventAttributes{
		Namespace:         namespace,
		WorkflowExecution: execution,
		WorkflowType:      workflowType,
		InitiatedEventId:  initiatedID,
		StartedEventId:    startedID,
		RetryState:        timedOutAttributes.GetRetryState(),
	}
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{ChildWorkflowExecutionTimedOutEventAttributes: attributes}

	return historyEvent
}

func newWorkflowTaskScheduledEventWithInfo(eventID int64, timestamp *time.Time, taskQueue *taskqueuepb.TaskQueue, startToCloseTimeoutSeconds, attempt int32) *historypb.HistoryEvent {
	historyEvent := createNewHistoryEvent(eventID, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, timestamp)

	return setWorkflowTaskScheduledEventInfo(historyEvent, taskQueue, startToCloseTimeoutSeconds, attempt)
}

func newWorkflowTaskStartedEventWithInfo(eventID int64, timestamp *time.Time, scheduledEventID int64, requestID string,
	identity string) *historypb.HistoryEvent {
	historyEvent := createNewHistoryEvent(eventID, enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, timestamp)

	return setWorkflowTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func createNewHistoryEvent(eventID int64, eventType enumspb.EventType, eventTime *time.Time) *historypb.HistoryEvent {
	historyEvent := &historypb.HistoryEvent{
		EventId:   eventID,
		EventTime: eventTime,
		EventType: eventType,
	}

	return historyEvent
}

func setWorkflowTaskScheduledEventInfo(historyEvent *historypb.HistoryEvent, taskQueue *taskqueuepb.TaskQueue, startToCloseTimeoutSeconds int32, attempt int32) *historypb.HistoryEvent {
	attributes := &historypb.WorkflowTaskScheduledEventAttributes{
		TaskQueue:           taskQueue,
		StartToCloseTimeout: timestamp.DurationPtr(time.Duration(startToCloseTimeoutSeconds) * time.Second),
		Attempt:             attempt,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: attributes}

	return historyEvent
}

func setWorkflowTaskStartedEventInfo(historyEvent *historypb.HistoryEvent, scheduledEventID int64, requestID string,
	identity string) *historypb.HistoryEvent {
	attributes := &historypb.WorkflowTaskStartedEventAttributes{
		ScheduledEventId: scheduledEventID,
		Identity:         identity,
		RequestId:        requestID,
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) GetHistory() *historypb.History {
	history := historypb.History{Events: b.history}
	return &history
}
