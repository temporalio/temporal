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
	commonpb "go.temporal.io/temporal-proto/common/v1"
	decisionpb "go.temporal.io/temporal-proto/decision/v1"
	enumspb "go.temporal.io/temporal-proto/enums/v1"
	failurepb "go.temporal.io/temporal-proto/failure/v1"
	historypb "go.temporal.io/temporal-proto/history/v1"
	taskqueuepb "go.temporal.io/temporal-proto/taskqueue/v1"
	workflowpb "go.temporal.io/temporal-proto/workflow/v1"
	"go.temporal.io/temporal-proto/workflowservice/v1"

	"github.com/temporalio/temporal/.gen/proto/historyservice/v1"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/persistence"
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
	// Transient decision events are always written before other events
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

func (b *historyBuilder) AddDecisionTaskScheduledEvent(taskQueue string,
	startToCloseTimeoutSeconds int32, attempt int64) *historypb.HistoryEvent {
	event := b.newDecisionTaskScheduledEvent(taskQueue, startToCloseTimeoutSeconds, attempt)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTransientDecisionTaskScheduledEvent(taskQueue string,
	startToCloseTimeoutSeconds int32, attempt int64, timestamp int64) *historypb.HistoryEvent {
	event := b.newTransientDecisionTaskScheduledEvent(taskQueue, startToCloseTimeoutSeconds, attempt, timestamp)

	return b.addTransientEvent(event)
}

func (b *historyBuilder) AddDecisionTaskStartedEvent(scheduleEventID int64, requestID string,
	identity string) *historypb.HistoryEvent {
	event := b.newDecisionTaskStartedEvent(scheduleEventID, requestID, identity)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTransientDecisionTaskStartedEvent(scheduleEventID int64, requestID string,
	identity string, timestamp int64) *historypb.HistoryEvent {
	event := b.newTransientDecisionTaskStartedEvent(scheduleEventID, requestID, identity, timestamp)

	return b.addTransientEvent(event)
}

func (b *historyBuilder) AddDecisionTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondDecisionTaskCompletedRequest) *historypb.HistoryEvent {
	event := b.newDecisionTaskCompletedEvent(scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskTimedOutEvent(scheduleEventID int64,
	startedEventID int64, timeoutType enumspb.TimeoutType) *historypb.HistoryEvent {
	event := b.newDecisionTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutType)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskFailedEvent(attr *historypb.DecisionTaskFailedEventAttributes) *historypb.HistoryEvent {
	event := b.newDecisionTaskFailedEvent(attr)
	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskScheduledEvent(decisionCompletedEventID int64,
	attributes *decisionpb.ScheduleActivityTaskDecisionAttributes) *historypb.HistoryEvent {
	event := b.newActivityTaskScheduledEvent(decisionCompletedEventID, attributes)

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
	failure *failurepb.Failure, retryStatus enumspb.RetryStatus, identity string) *historypb.HistoryEvent {
	event := b.newActivityTaskFailedEvent(scheduleEventID, startedEventID, failure, retryStatus, identity)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskTimedOutEvent(
	scheduleEventID,
	startedEventID int64,
	timeoutFailure *failurepb.Failure,
	retryStatus enumspb.RetryStatus,
) *historypb.HistoryEvent {
	event := b.newActivityTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutFailure, retryStatus)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddCompletedWorkflowEvent(decisionCompletedEventID int64,
	attributes *decisionpb.CompleteWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	event := b.newCompleteWorkflowExecutionEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddFailWorkflowEvent(decisionCompletedEventID int64, retryStatus enumspb.RetryStatus,
	attributes *decisionpb.FailWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	event := b.newFailWorkflowExecutionEvent(decisionCompletedEventID, retryStatus, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimeoutWorkflowEvent(retryStatus enumspb.RetryStatus) *historypb.HistoryEvent {
	event := b.newTimeoutWorkflowExecutionEvent(retryStatus)

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

func (b *historyBuilder) AddContinuedAsNewEvent(decisionCompletedEventID int64, newRunID string,
	attributes *decisionpb.ContinueAsNewWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	event := b.newWorkflowExecutionContinuedAsNewEvent(decisionCompletedEventID, newRunID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerStartedEvent(decisionCompletedEventID int64,
	request *decisionpb.StartTimerDecisionAttributes) *historypb.HistoryEvent {

	attributes := &historypb.TimerStartedEventAttributes{}
	attributes.TimerId = request.TimerId
	attributes.StartToFireTimeoutSeconds = request.StartToFireTimeoutSeconds
	attributes.DecisionTaskCompletedEventId = decisionCompletedEventID

	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_TIMER_STARTED)
	event.Attributes = &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerFiredEvent(
	startedEventID int64,
	timerID string,
) *historypb.HistoryEvent {

	attributes := &historypb.TimerFiredEventAttributes{}
	attributes.TimerId = timerID
	attributes.StartedEventId = startedEventID

	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_TIMER_FIRED)
	event.Attributes = &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCancelRequestedEvent(decisionCompletedEventID int64,
	scheduleID int64) *historypb.HistoryEvent {

	attributes := &historypb.ActivityTaskCancelRequestedEventAttributes{}
	attributes.ScheduledEventId = scheduleID
	attributes.DecisionTaskCompletedEventId = decisionCompletedEventID

	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED)
	event.Attributes = &historypb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{ActivityTaskCancelRequestedEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCanceledEvent(scheduleEventID, startedEventID int64,
	latestCancelRequestedEventID int64, details *commonpb.Payloads, identity string) *historypb.HistoryEvent {

	attributes := &historypb.ActivityTaskCanceledEventAttributes{}
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.LatestCancelRequestedEventId = latestCancelRequestedEventID
	attributes.Details = details
	attributes.Identity = identity

	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED)
	event.Attributes = &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{ActivityTaskCanceledEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerCanceledEvent(startedEventID int64,
	decisionTaskCompletedEventID int64, timerID string, identity string) *historypb.HistoryEvent {

	attributes := &historypb.TimerCanceledEventAttributes{}
	attributes.StartedEventId = startedEventID
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.TimerId = timerID
	attributes.Identity = identity

	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_TIMER_CANCELED)
	event.Attributes = &historypb.HistoryEvent_TimerCanceledEventAttributes{TimerCanceledEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddCancelTimerFailedEvent(timerID string, decisionTaskCompletedEventID int64,
	cause string, identity string) *historypb.HistoryEvent {

	attributes := &historypb.CancelTimerFailedEventAttributes{}
	attributes.TimerId = timerID
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.Cause = cause
	attributes.Identity = identity

	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_CANCEL_TIMER_FAILED)
	event.Attributes = &historypb.HistoryEvent_CancelTimerFailedEventAttributes{CancelTimerFailedEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionCancelRequestedEvent(cause string,
	request *historyservice.RequestCancelWorkflowExecutionRequest) *historypb.HistoryEvent {
	event := b.newWorkflowExecutionCancelRequestedEvent(cause, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID int64,
	attributes *decisionpb.CancelWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	event := b.newWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	request *decisionpb.RequestCancelExternalWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	event := b.newRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	namespace, workflowID, runID string, cause enumspb.CancelExternalWorkflowExecutionFailedCause) *historypb.HistoryEvent {
	event := b.newRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID,
		namespace, workflowID, runID, cause)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddExternalWorkflowExecutionCancelRequested(initiatedEventID int64,
	namespace, workflowID, runID string) *historypb.HistoryEvent {
	event := b.newExternalWorkflowExecutionCancelRequestedEvent(initiatedEventID,
		namespace, workflowID, runID)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddSignalExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	attributes *decisionpb.SignalExternalWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	event := b.newSignalExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddUpsertWorkflowSearchAttributesEvent(
	decisionTaskCompletedEventID int64,
	attributes *decisionpb.UpsertWorkflowSearchAttributesDecisionAttributes) *historypb.HistoryEvent {
	event := b.newUpsertWorkflowSearchAttributesEvent(decisionTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	namespace, workflowID, runID, control string, cause enumspb.SignalExternalWorkflowExecutionFailedCause) *historypb.HistoryEvent {
	event := b.newSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID,
		namespace, workflowID, runID, control, cause)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddExternalWorkflowExecutionSignaled(initiatedEventID int64,
	namespace, workflowID, runID, control string) *historypb.HistoryEvent {
	event := b.newExternalWorkflowExecutionSignaledEvent(initiatedEventID,
		namespace, workflowID, runID, control)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddMarkerRecordedEvent(decisionCompletedEventID int64,
	attributes *decisionpb.RecordMarkerDecisionAttributes) *historypb.HistoryEvent {
	event := b.newMarkerRecordedEventAttributes(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionSignaledEvent(
	signalName string, input *commonpb.Payloads, identity string) *historypb.HistoryEvent {
	event := b.newWorkflowExecutionSignaledEvent(signalName, input, identity)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddStartChildWorkflowExecutionInitiatedEvent(decisionCompletedEventID int64,
	attributes *decisionpb.StartChildWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	event := b.newStartChildWorkflowExecutionInitiatedEvent(decisionCompletedEventID, attributes)

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
		prevRunID = previousExecution.RunID
		resetPoints = previousExecution.AutoResetPoints
	}
	request := startRequest.StartRequest
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
	attributes := &historypb.WorkflowExecutionStartedEventAttributes{}
	attributes.WorkflowType = request.WorkflowType
	attributes.TaskQueue = request.TaskQueue
	attributes.Header = request.Header
	attributes.Input = request.Input
	attributes.WorkflowRunTimeoutSeconds = request.WorkflowRunTimeoutSeconds
	attributes.WorkflowExecutionTimeoutSeconds = request.WorkflowExecutionTimeoutSeconds
	attributes.WorkflowTaskTimeoutSeconds = request.WorkflowTaskTimeoutSeconds
	attributes.ContinuedExecutionRunId = prevRunID
	attributes.PrevAutoResetPoints = resetPoints
	attributes.Identity = request.Identity
	attributes.RetryPolicy = request.RetryPolicy
	attributes.Attempt = startRequest.GetAttempt()
	attributes.WorkflowExecutionExpirationTimestamp = startRequest.WorkflowExecutionExpirationTimestamp
	attributes.CronSchedule = request.CronSchedule
	attributes.LastCompletionResult = startRequest.LastCompletionResult
	attributes.ContinuedFailure = startRequest.GetContinuedFailure()
	attributes.Initiator = startRequest.ContinueAsNewInitiator
	attributes.FirstDecisionTaskBackoffSeconds = startRequest.FirstDecisionTaskBackoffSeconds
	attributes.FirstExecutionRunId = firstRunID
	attributes.OriginalExecutionRunId = originalRunID
	attributes.Memo = request.Memo
	attributes.SearchAttributes = request.SearchAttributes

	parentInfo := startRequest.ParentExecutionInfo
	if parentInfo != nil {
		attributes.ParentWorkflowNamespace = parentInfo.Namespace
		attributes.ParentWorkflowExecution = parentInfo.Execution
		attributes.ParentInitiatedEventId = parentInfo.InitiatedId
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskScheduledEvent(taskQueue string, startToCloseTimeoutSeconds int32,
	attempt int64) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED)

	return setDecisionTaskScheduledEventInfo(historyEvent, taskQueue, startToCloseTimeoutSeconds, attempt)
}

func (b *historyBuilder) newTransientDecisionTaskScheduledEvent(taskQueue string, startToCloseTimeoutSeconds int32,
	attempt int64, timestamp int64) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEventWithTimestamp(enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED, timestamp)

	return setDecisionTaskScheduledEventInfo(historyEvent, taskQueue, startToCloseTimeoutSeconds, attempt)
}

func (b *historyBuilder) newDecisionTaskStartedEvent(scheduledEventID int64, requestID string,
	identity string) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_DECISION_TASK_STARTED)

	return setDecisionTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func (b *historyBuilder) newTransientDecisionTaskStartedEvent(scheduledEventID int64, requestID string,
	identity string, timestamp int64) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEventWithTimestamp(enumspb.EVENT_TYPE_DECISION_TASK_STARTED, timestamp)

	return setDecisionTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func (b *historyBuilder) newDecisionTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondDecisionTaskCompletedRequest) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_DECISION_TASK_COMPLETED)
	attributes := &historypb.DecisionTaskCompletedEventAttributes{}
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.Identity = request.Identity
	attributes.BinaryChecksum = request.BinaryChecksum
	historyEvent.Attributes = &historypb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskTimedOutEvent(scheduleEventID int64, startedEventID int64, timeoutType enumspb.TimeoutType) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_DECISION_TASK_TIMED_OUT)
	attributes := &historypb.DecisionTaskTimedOutEventAttributes{}
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.TimeoutType = timeoutType
	historyEvent.Attributes = &historypb.HistoryEvent_DecisionTaskTimedOutEventAttributes{DecisionTaskTimedOutEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskFailedEvent(attr *historypb.DecisionTaskFailedEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_DECISION_TASK_FAILED)
	historyEvent.Attributes = &historypb.HistoryEvent_DecisionTaskFailedEventAttributes{DecisionTaskFailedEventAttributes: attr}
	return historyEvent
}

func (b *historyBuilder) newActivityTaskScheduledEvent(decisionTaskCompletedEventID int64,
	scheduleAttributes *decisionpb.ScheduleActivityTaskDecisionAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
	attributes := &historypb.ActivityTaskScheduledEventAttributes{}
	attributes.ActivityId = scheduleAttributes.ActivityId
	attributes.ActivityType = scheduleAttributes.ActivityType
	attributes.TaskQueue = scheduleAttributes.TaskQueue
	attributes.Header = scheduleAttributes.Header
	attributes.Input = scheduleAttributes.Input
	attributes.ScheduleToCloseTimeoutSeconds = scheduleAttributes.ScheduleToCloseTimeoutSeconds
	attributes.ScheduleToStartTimeoutSeconds = scheduleAttributes.ScheduleToStartTimeoutSeconds
	attributes.StartToCloseTimeoutSeconds = scheduleAttributes.StartToCloseTimeoutSeconds
	attributes.HeartbeatTimeoutSeconds = scheduleAttributes.HeartbeatTimeoutSeconds
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.RetryPolicy = scheduleAttributes.RetryPolicy
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
	attributes := &historypb.ActivityTaskStartedEventAttributes{}
	attributes.ScheduledEventId = scheduledEventID
	attributes.Attempt = attempt
	attributes.Identity = identity
	attributes.RequestId = requestID
	attributes.LastFailure = lastFailure
	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newActivityTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondActivityTaskCompletedRequest) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED)
	attributes := &historypb.ActivityTaskCompletedEventAttributes{}
	attributes.Result = request.Result
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.Identity = request.Identity
	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newActivityTaskTimedOutEvent(
	scheduleEventID, startedEventID int64,
	timeoutFailure *failurepb.Failure,
	retryStatus enumspb.RetryStatus,
) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT)
	attributes := &historypb.ActivityTaskTimedOutEventAttributes{}
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.Failure = timeoutFailure
	attributes.RetryStatus = retryStatus

	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newActivityTaskFailedEvent(scheduleEventID, startedEventID int64,
	failure *failurepb.Failure, retryStatus enumspb.RetryStatus, identity string) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED)
	attributes := &historypb.ActivityTaskFailedEventAttributes{}
	attributes.Failure = failure
	attributes.RetryStatus = retryStatus
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.Identity = identity
	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newCompleteWorkflowExecutionEvent(decisionTaskCompletedEventID int64,
	request *decisionpb.CompleteWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED)
	attributes := &historypb.WorkflowExecutionCompletedEventAttributes{}
	attributes.Result = request.Result
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newFailWorkflowExecutionEvent(decisionTaskCompletedEventID int64, retryStatus enumspb.RetryStatus,
	request *decisionpb.FailWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED)
	attributes := &historypb.WorkflowExecutionFailedEventAttributes{}
	attributes.Failure = request.GetFailure()
	attributes.RetryStatus = retryStatus
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newTimeoutWorkflowExecutionEvent(retryStatus enumspb.RetryStatus) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT)
	attributes := &historypb.WorkflowExecutionTimedOutEventAttributes{}
	attributes.RetryStatus = retryStatus
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{WorkflowExecutionTimedOutEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionSignaledEvent(
	signalName string, input *commonpb.Payloads, identity string) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
	attributes := &historypb.WorkflowExecutionSignaledEventAttributes{}
	attributes.SignalName = signalName
	attributes.Input = input
	attributes.Identity = identity
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionTerminatedEvent(
	reason string, details *commonpb.Payloads, identity string) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED)
	attributes := &historypb.WorkflowExecutionTerminatedEventAttributes{}
	attributes.Reason = reason
	attributes.Details = details
	attributes.Identity = identity
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newMarkerRecordedEventAttributes(decisionTaskCompletedEventID int64,
	request *decisionpb.RecordMarkerDecisionAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_MARKER_RECORDED)
	attributes := &historypb.MarkerRecordedEventAttributes{}
	attributes.MarkerName = request.MarkerName
	attributes.Details = request.Details
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.Header = request.Header
	historyEvent.Attributes = &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionCancelRequestedEvent(cause string,
	request *historyservice.RequestCancelWorkflowExecutionRequest) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED)
	attributes := &historypb.WorkflowExecutionCancelRequestedEventAttributes{}
	attributes.Cause = cause
	attributes.Identity = request.CancelRequest.Identity
	attributes.ExternalInitiatedEventId = request.ExternalInitiatedEventId
	attributes.ExternalWorkflowExecution = request.ExternalWorkflowExecution
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{WorkflowExecutionCancelRequestedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID int64,
	request *decisionpb.CancelWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED)
	attributes := &historypb.WorkflowExecutionCanceledEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.Details = request.GetDetails()
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{WorkflowExecutionCanceledEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	request *decisionpb.RequestCancelExternalWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED)
	attributes := &historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.Namespace = request.Namespace
	attributes.WorkflowExecution = &commonpb.WorkflowExecution{
		WorkflowId: request.WorkflowId,
		RunId:      request.RunId,
	}
	attributes.Control = request.Control
	attributes.ChildWorkflowOnly = request.ChildWorkflowOnly
	event.Attributes = &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	namespace, workflowID, runID string, cause enumspb.CancelExternalWorkflowExecutionFailedCause) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED)
	attributes := &historypb.RequestCancelExternalWorkflowExecutionFailedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.InitiatedEventId = initiatedEventID
	attributes.Namespace = namespace
	attributes.WorkflowExecution = &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	attributes.Cause = cause
	event.Attributes = &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{RequestCancelExternalWorkflowExecutionFailedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newExternalWorkflowExecutionCancelRequestedEvent(initiatedEventID int64,
	namespace, workflowID, runID string) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED)
	attributes := &historypb.ExternalWorkflowExecutionCancelRequestedEventAttributes{}
	attributes.InitiatedEventId = initiatedEventID
	attributes.Namespace = namespace
	attributes.WorkflowExecution = &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	event.Attributes = &historypb.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{ExternalWorkflowExecutionCancelRequestedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newSignalExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	request *decisionpb.SignalExternalWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED)
	attributes := &historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.Namespace = request.Namespace
	attributes.WorkflowExecution = &commonpb.WorkflowExecution{
		WorkflowId: request.Execution.WorkflowId,
		RunId:      request.Execution.RunId,
	}
	attributes.SignalName = request.GetSignalName()
	attributes.Input = request.Input
	attributes.Control = request.Control
	attributes.ChildWorkflowOnly = request.ChildWorkflowOnly
	event.Attributes = &historypb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{SignalExternalWorkflowExecutionInitiatedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newUpsertWorkflowSearchAttributesEvent(decisionTaskCompletedEventID int64,
	request *decisionpb.UpsertWorkflowSearchAttributesDecisionAttributes) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES)
	attributes := &historypb.UpsertWorkflowSearchAttributesEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.SearchAttributes = request.GetSearchAttributes()
	event.Attributes = &historypb.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{UpsertWorkflowSearchAttributesEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	namespace, workflowID, runID, control string, cause enumspb.SignalExternalWorkflowExecutionFailedCause) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED)
	attributes := &historypb.SignalExternalWorkflowExecutionFailedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.InitiatedEventId = initiatedEventID
	attributes.Namespace = namespace
	attributes.WorkflowExecution = &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	attributes.Cause = cause
	attributes.Control = control
	event.Attributes = &historypb.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{SignalExternalWorkflowExecutionFailedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newExternalWorkflowExecutionSignaledEvent(initiatedEventID int64,
	namespace, workflowID, runID, control string) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED)
	attributes := &historypb.ExternalWorkflowExecutionSignaledEventAttributes{}
	attributes.InitiatedEventId = initiatedEventID
	attributes.Namespace = namespace
	attributes.WorkflowExecution = &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	attributes.Control = control
	event.Attributes = &historypb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{ExternalWorkflowExecutionSignaledEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newWorkflowExecutionContinuedAsNewEvent(decisionTaskCompletedEventID int64,
	newRunID string, request *decisionpb.ContinueAsNewWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW)
	attributes := &historypb.WorkflowExecutionContinuedAsNewEventAttributes{}
	attributes.NewExecutionRunId = newRunID
	attributes.WorkflowType = request.WorkflowType
	attributes.TaskQueue = request.TaskQueue
	attributes.Header = request.Header
	attributes.Input = request.Input
	attributes.WorkflowRunTimeoutSeconds = request.WorkflowRunTimeoutSeconds
	attributes.WorkflowTaskTimeoutSeconds = request.WorkflowTaskTimeoutSeconds
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.BackoffStartIntervalInSeconds = request.GetBackoffStartIntervalInSeconds()
	attributes.Initiator = request.Initiator
	attributes.Failure = request.GetFailure()
	attributes.LastCompletionResult = request.LastCompletionResult
	attributes.Memo = request.Memo
	attributes.SearchAttributes = request.SearchAttributes
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newStartChildWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	startAttributes *decisionpb.StartChildWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED)
	attributes := &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{}
	attributes.Namespace = startAttributes.Namespace
	attributes.WorkflowId = startAttributes.WorkflowId
	attributes.WorkflowType = startAttributes.WorkflowType
	attributes.TaskQueue = startAttributes.TaskQueue
	attributes.Header = startAttributes.Header
	attributes.Input = startAttributes.Input
	attributes.WorkflowExecutionTimeoutSeconds = startAttributes.WorkflowExecutionTimeoutSeconds
	attributes.WorkflowRunTimeoutSeconds = startAttributes.WorkflowRunTimeoutSeconds
	attributes.WorkflowTaskTimeoutSeconds = startAttributes.WorkflowTaskTimeoutSeconds
	attributes.Control = startAttributes.Control
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.WorkflowIdReusePolicy = startAttributes.WorkflowIdReusePolicy
	attributes.RetryPolicy = startAttributes.RetryPolicy
	attributes.CronSchedule = startAttributes.CronSchedule
	attributes.Memo = startAttributes.Memo
	attributes.SearchAttributes = startAttributes.SearchAttributes
	attributes.ParentClosePolicy = startAttributes.GetParentClosePolicy()
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
	attributes := &historypb.ChildWorkflowExecutionStartedEventAttributes{}
	attributes.Namespace = namespace
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.Header = header
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newStartChildWorkflowExecutionFailedEvent(initiatedID int64,
	cause enumspb.StartChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED)
	attributes := &historypb.StartChildWorkflowExecutionFailedEventAttributes{}
	attributes.Namespace = initiatedEventAttributes.Namespace
	attributes.WorkflowId = initiatedEventAttributes.WorkflowId
	attributes.WorkflowType = initiatedEventAttributes.WorkflowType
	attributes.InitiatedEventId = initiatedID
	attributes.DecisionTaskCompletedEventId = initiatedEventAttributes.DecisionTaskCompletedEventId
	attributes.Control = initiatedEventAttributes.Control
	attributes.Cause = cause
	historyEvent.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{StartChildWorkflowExecutionFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionCompletedEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	completedAttributes *historypb.WorkflowExecutionCompletedEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED)
	attributes := &historypb.ChildWorkflowExecutionCompletedEventAttributes{}
	attributes.Namespace = namespace
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.StartedEventId = startedID
	attributes.Result = completedAttributes.Result
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{ChildWorkflowExecutionCompletedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionFailedEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	failedAttributes *historypb.WorkflowExecutionFailedEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED)
	attributes := &historypb.ChildWorkflowExecutionFailedEventAttributes{}
	attributes.Namespace = namespace
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.StartedEventId = startedID
	attributes.Failure = failedAttributes.GetFailure()
	attributes.RetryStatus = failedAttributes.GetRetryStatus()
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{ChildWorkflowExecutionFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionCanceledEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	canceledAttributes *historypb.WorkflowExecutionCanceledEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED)
	attributes := &historypb.ChildWorkflowExecutionCanceledEventAttributes{}
	attributes.Namespace = namespace
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.StartedEventId = startedID
	attributes.Details = canceledAttributes.Details
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{ChildWorkflowExecutionCanceledEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionTerminatedEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	terminatedAttributes *historypb.WorkflowExecutionTerminatedEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED)
	attributes := &historypb.ChildWorkflowExecutionTerminatedEventAttributes{}
	attributes.Namespace = namespace
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.StartedEventId = startedID
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{ChildWorkflowExecutionTerminatedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionTimedOutEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	timedOutAttributes *historypb.WorkflowExecutionTimedOutEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT)
	attributes := &historypb.ChildWorkflowExecutionTimedOutEventAttributes{}
	attributes.Namespace = namespace
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.StartedEventId = startedID
	attributes.RetryStatus = timedOutAttributes.GetRetryStatus()
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{ChildWorkflowExecutionTimedOutEventAttributes: attributes}

	return historyEvent
}

func newDecisionTaskScheduledEventWithInfo(eventID, timestamp int64, taskQueue string, startToCloseTimeoutSeconds int32,
	attempt int64) *historypb.HistoryEvent {
	historyEvent := createNewHistoryEvent(eventID, enumspb.EVENT_TYPE_DECISION_TASK_SCHEDULED, timestamp)

	return setDecisionTaskScheduledEventInfo(historyEvent, taskQueue, startToCloseTimeoutSeconds, attempt)
}

func newDecisionTaskStartedEventWithInfo(eventID, timestamp int64, scheduledEventID int64, requestID string,
	identity string) *historypb.HistoryEvent {
	historyEvent := createNewHistoryEvent(eventID, enumspb.EVENT_TYPE_DECISION_TASK_STARTED, timestamp)

	return setDecisionTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func createNewHistoryEvent(eventID int64, eventType enumspb.EventType, timestamp int64) *historypb.HistoryEvent {
	historyEvent := &historypb.HistoryEvent{}
	historyEvent.EventId = eventID
	historyEvent.Timestamp = timestamp
	historyEvent.EventType = eventType

	return historyEvent
}

func setDecisionTaskScheduledEventInfo(historyEvent *historypb.HistoryEvent, taskQueue string,
	startToCloseTimeoutSeconds int32, attempt int64) *historypb.HistoryEvent {
	attributes := &historypb.DecisionTaskScheduledEventAttributes{}
	attributes.TaskQueue = &taskqueuepb.TaskQueue{}
	attributes.TaskQueue.Name = taskQueue
	attributes.StartToCloseTimeoutSeconds = startToCloseTimeoutSeconds
	attributes.Attempt = attempt
	historyEvent.Attributes = &historypb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: attributes}

	return historyEvent
}

func setDecisionTaskStartedEventInfo(historyEvent *historypb.HistoryEvent, scheduledEventID int64, requestID string,
	identity string) *historypb.HistoryEvent {
	attributes := &historypb.DecisionTaskStartedEventAttributes{}
	attributes.ScheduledEventId = scheduledEventID
	attributes.Identity = identity
	attributes.RequestId = requestID
	historyEvent.Attributes = &historypb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) GetHistory() *historypb.History {
	history := historypb.History{Events: b.history}
	return &history
}
