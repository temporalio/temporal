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
	commonpb "go.temporal.io/temporal-proto/common"
	decisionpb "go.temporal.io/temporal-proto/decision"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	failurepb "go.temporal.io/temporal-proto/failure"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	historyBuilder struct {
		transientHistory []*eventpb.HistoryEvent
		history          []*eventpb.HistoryEvent
		msBuilder        mutableState
	}
)

func newHistoryBuilder(msBuilder mutableState, logger log.Logger) *historyBuilder {
	return &historyBuilder{
		transientHistory: []*eventpb.HistoryEvent{},
		history:          []*eventpb.HistoryEvent{},
		msBuilder:        msBuilder,
	}
}

func newHistoryBuilderFromEvents(history []*eventpb.HistoryEvent, logger log.Logger) *historyBuilder {
	return &historyBuilder{
		history: history,
	}
}

func (b *historyBuilder) GetFirstEvent() *eventpb.HistoryEvent {
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
	previousExecution *persistence.WorkflowExecutionInfo, firstRunID, originalRunID string) *eventpb.HistoryEvent {
	event := b.newWorkflowExecutionStartedEvent(request, previousExecution, firstRunID, originalRunID)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskScheduledEvent(taskList string,
	startToCloseTimeoutSeconds int32, attempt int64) *eventpb.HistoryEvent {
	event := b.newDecisionTaskScheduledEvent(taskList, startToCloseTimeoutSeconds, attempt)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTransientDecisionTaskScheduledEvent(taskList string,
	startToCloseTimeoutSeconds int32, attempt int64, timestamp int64) *eventpb.HistoryEvent {
	event := b.newTransientDecisionTaskScheduledEvent(taskList, startToCloseTimeoutSeconds, attempt, timestamp)

	return b.addTransientEvent(event)
}

func (b *historyBuilder) AddDecisionTaskStartedEvent(scheduleEventID int64, requestID string,
	identity string) *eventpb.HistoryEvent {
	event := b.newDecisionTaskStartedEvent(scheduleEventID, requestID, identity)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTransientDecisionTaskStartedEvent(scheduleEventID int64, requestID string,
	identity string, timestamp int64) *eventpb.HistoryEvent {
	event := b.newTransientDecisionTaskStartedEvent(scheduleEventID, requestID, identity, timestamp)

	return b.addTransientEvent(event)
}

func (b *historyBuilder) AddDecisionTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondDecisionTaskCompletedRequest) *eventpb.HistoryEvent {
	event := b.newDecisionTaskCompletedEvent(scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskTimedOutEvent(scheduleEventID int64,
	startedEventID int64, timeoutType commonpb.TimeoutType) *eventpb.HistoryEvent {
	event := b.newDecisionTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutType)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskFailedEvent(attr *eventpb.DecisionTaskFailedEventAttributes) *eventpb.HistoryEvent {
	event := b.newDecisionTaskFailedEvent(attr)
	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskScheduledEvent(decisionCompletedEventID int64,
	attributes *decisionpb.ScheduleActivityTaskDecisionAttributes) *eventpb.HistoryEvent {
	event := b.newActivityTaskScheduledEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskStartedEvent(
	scheduleEventID int64,
	attempt int32,
	requestID string,
	identity string,
	lastFailure *failurepb.Failure,
) *eventpb.HistoryEvent {
	event := b.newActivityTaskStartedEvent(scheduleEventID, attempt, requestID, identity, lastFailure)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondActivityTaskCompletedRequest) *eventpb.HistoryEvent {
	event := b.newActivityTaskCompletedEvent(scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskFailedEvent(scheduleEventID, startedEventID int64,
	failure *failurepb.Failure, retryStatus commonpb.RetryStatus, identity string) *eventpb.HistoryEvent {
	event := b.newActivityTaskFailedEvent(scheduleEventID, startedEventID, failure, retryStatus, identity)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskTimedOutEvent(
	scheduleEventID,
	startedEventID int64,
	timeoutFailure *failurepb.Failure,
	retryStatus commonpb.RetryStatus,
) *eventpb.HistoryEvent {
	event := b.newActivityTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutFailure, retryStatus)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddCompletedWorkflowEvent(decisionCompletedEventID int64,
	attributes *decisionpb.CompleteWorkflowExecutionDecisionAttributes) *eventpb.HistoryEvent {
	event := b.newCompleteWorkflowExecutionEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddFailWorkflowEvent(decisionCompletedEventID int64, retryStatus commonpb.RetryStatus,
	attributes *decisionpb.FailWorkflowExecutionDecisionAttributes) *eventpb.HistoryEvent {
	event := b.newFailWorkflowExecutionEvent(decisionCompletedEventID, retryStatus, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimeoutWorkflowEvent(retryStatus commonpb.RetryStatus) *eventpb.HistoryEvent {
	event := b.newTimeoutWorkflowExecutionEvent(retryStatus)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionTerminatedEvent(
	reason string,
	details *commonpb.Payloads,
	identity string,
) *eventpb.HistoryEvent {
	event := b.newWorkflowExecutionTerminatedEvent(reason, details, identity)
	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddContinuedAsNewEvent(decisionCompletedEventID int64, newRunID string,
	attributes *decisionpb.ContinueAsNewWorkflowExecutionDecisionAttributes) *eventpb.HistoryEvent {
	event := b.newWorkflowExecutionContinuedAsNewEvent(decisionCompletedEventID, newRunID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerStartedEvent(decisionCompletedEventID int64,
	request *decisionpb.StartTimerDecisionAttributes) *eventpb.HistoryEvent {

	attributes := &eventpb.TimerStartedEventAttributes{}
	attributes.TimerId = request.TimerId
	attributes.StartToFireTimeoutSeconds = request.StartToFireTimeoutSeconds
	attributes.DecisionTaskCompletedEventId = decisionCompletedEventID

	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_TIMER_STARTED)
	event.Attributes = &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerFiredEvent(
	startedEventID int64,
	timerID string,
) *eventpb.HistoryEvent {

	attributes := &eventpb.TimerFiredEventAttributes{}
	attributes.TimerId = timerID
	attributes.StartedEventId = startedEventID

	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_TIMER_FIRED)
	event.Attributes = &eventpb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCancelRequestedEvent(decisionCompletedEventID int64,
	scheduleID int64) *eventpb.HistoryEvent {

	attributes := &eventpb.ActivityTaskCancelRequestedEventAttributes{}
	attributes.ScheduledEventId = scheduleID
	attributes.DecisionTaskCompletedEventId = decisionCompletedEventID

	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED)
	event.Attributes = &eventpb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{ActivityTaskCancelRequestedEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCanceledEvent(scheduleEventID, startedEventID int64,
	latestCancelRequestedEventID int64, details *commonpb.Payloads, identity string) *eventpb.HistoryEvent {

	attributes := &eventpb.ActivityTaskCanceledEventAttributes{}
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.LatestCancelRequestedEventId = latestCancelRequestedEventID
	attributes.Details = details
	attributes.Identity = identity

	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_ACTIVITY_TASK_CANCELED)
	event.Attributes = &eventpb.HistoryEvent_ActivityTaskCanceledEventAttributes{ActivityTaskCanceledEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerCanceledEvent(startedEventID int64,
	decisionTaskCompletedEventID int64, timerID string, identity string) *eventpb.HistoryEvent {

	attributes := &eventpb.TimerCanceledEventAttributes{}
	attributes.StartedEventId = startedEventID
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.TimerId = timerID
	attributes.Identity = identity

	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_TIMER_CANCELED)
	event.Attributes = &eventpb.HistoryEvent_TimerCanceledEventAttributes{TimerCanceledEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddCancelTimerFailedEvent(timerID string, decisionTaskCompletedEventID int64,
	cause string, identity string) *eventpb.HistoryEvent {

	attributes := &eventpb.CancelTimerFailedEventAttributes{}
	attributes.TimerId = timerID
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.Cause = cause
	attributes.Identity = identity

	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_CANCEL_TIMER_FAILED)
	event.Attributes = &eventpb.HistoryEvent_CancelTimerFailedEventAttributes{CancelTimerFailedEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionCancelRequestedEvent(cause string,
	request *historyservice.RequestCancelWorkflowExecutionRequest) *eventpb.HistoryEvent {
	event := b.newWorkflowExecutionCancelRequestedEvent(cause, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID int64,
	attributes *decisionpb.CancelWorkflowExecutionDecisionAttributes) *eventpb.HistoryEvent {
	event := b.newWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	request *decisionpb.RequestCancelExternalWorkflowExecutionDecisionAttributes) *eventpb.HistoryEvent {
	event := b.newRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	namespace, workflowID, runID string, cause eventpb.CancelExternalWorkflowExecutionFailedCause) *eventpb.HistoryEvent {
	event := b.newRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID,
		namespace, workflowID, runID, cause)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddExternalWorkflowExecutionCancelRequested(initiatedEventID int64,
	namespace, workflowID, runID string) *eventpb.HistoryEvent {
	event := b.newExternalWorkflowExecutionCancelRequestedEvent(initiatedEventID,
		namespace, workflowID, runID)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddSignalExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	attributes *decisionpb.SignalExternalWorkflowExecutionDecisionAttributes) *eventpb.HistoryEvent {
	event := b.newSignalExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddUpsertWorkflowSearchAttributesEvent(
	decisionTaskCompletedEventID int64,
	attributes *decisionpb.UpsertWorkflowSearchAttributesDecisionAttributes) *eventpb.HistoryEvent {
	event := b.newUpsertWorkflowSearchAttributesEvent(decisionTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	namespace, workflowID, runID, control string, cause eventpb.SignalExternalWorkflowExecutionFailedCause) *eventpb.HistoryEvent {
	event := b.newSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID,
		namespace, workflowID, runID, control, cause)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddExternalWorkflowExecutionSignaled(initiatedEventID int64,
	namespace, workflowID, runID, control string) *eventpb.HistoryEvent {
	event := b.newExternalWorkflowExecutionSignaledEvent(initiatedEventID,
		namespace, workflowID, runID, control)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddMarkerRecordedEvent(decisionCompletedEventID int64,
	attributes *decisionpb.RecordMarkerDecisionAttributes) *eventpb.HistoryEvent {
	event := b.newMarkerRecordedEventAttributes(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionSignaledEvent(
	signalName string, input *commonpb.Payloads, identity string) *eventpb.HistoryEvent {
	event := b.newWorkflowExecutionSignaledEvent(signalName, input, identity)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddStartChildWorkflowExecutionInitiatedEvent(decisionCompletedEventID int64,
	attributes *decisionpb.StartChildWorkflowExecutionDecisionAttributes) *eventpb.HistoryEvent {
	event := b.newStartChildWorkflowExecutionInitiatedEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionStartedEvent(
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	header *commonpb.Header,
) *eventpb.HistoryEvent {
	event := b.newChildWorkflowExecutionStartedEvent(namespace, execution, workflowType, initiatedID, header)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddStartChildWorkflowExecutionFailedEvent(initiatedID int64,
	cause eventpb.StartChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *eventpb.StartChildWorkflowExecutionInitiatedEventAttributes) *eventpb.HistoryEvent {
	event := b.newStartChildWorkflowExecutionFailedEvent(initiatedID, cause, initiatedEventAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionCompletedEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	completedAttributes *eventpb.WorkflowExecutionCompletedEventAttributes) *eventpb.HistoryEvent {
	event := b.newChildWorkflowExecutionCompletedEvent(namespace, execution, workflowType, initiatedID, startedID,
		completedAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionFailedEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	failedAttributes *eventpb.WorkflowExecutionFailedEventAttributes) *eventpb.HistoryEvent {
	event := b.newChildWorkflowExecutionFailedEvent(namespace, execution, workflowType, initiatedID, startedID,
		failedAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionCanceledEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	canceledAttributes *eventpb.WorkflowExecutionCanceledEventAttributes) *eventpb.HistoryEvent {
	event := b.newChildWorkflowExecutionCanceledEvent(namespace, execution, workflowType, initiatedID, startedID,
		canceledAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionTerminatedEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	terminatedAttributes *eventpb.WorkflowExecutionTerminatedEventAttributes) *eventpb.HistoryEvent {
	event := b.newChildWorkflowExecutionTerminatedEvent(namespace, execution, workflowType, initiatedID, startedID,
		terminatedAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionTimedOutEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	timedOutAttributes *eventpb.WorkflowExecutionTimedOutEventAttributes) *eventpb.HistoryEvent {
	event := b.newChildWorkflowExecutionTimedOutEvent(namespace, execution, workflowType, initiatedID, startedID,
		timedOutAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) addEventToHistory(event *eventpb.HistoryEvent) *eventpb.HistoryEvent {
	b.history = append(b.history, event)
	return event
}

func (b *historyBuilder) addTransientEvent(event *eventpb.HistoryEvent) *eventpb.HistoryEvent {
	b.transientHistory = append(b.transientHistory, event)
	return event
}

func (b *historyBuilder) newWorkflowExecutionStartedEvent(
	startRequest *historyservice.StartWorkflowExecutionRequest, previousExecution *persistence.WorkflowExecutionInfo, firstRunID, originalRunID string) *eventpb.HistoryEvent {
	var prevRunID string
	var resetPoints *executionpb.ResetPoints
	if previousExecution != nil {
		prevRunID = previousExecution.RunID
		resetPoints = previousExecution.AutoResetPoints
	}
	request := startRequest.StartRequest
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
	attributes := &eventpb.WorkflowExecutionStartedEventAttributes{}
	attributes.WorkflowType = request.WorkflowType
	attributes.TaskList = request.TaskList
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
	historyEvent.Attributes = &eventpb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskScheduledEvent(taskList string, startToCloseTimeoutSeconds int32,
	attempt int64) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_DECISION_TASK_SCHEDULED)

	return setDecisionTaskScheduledEventInfo(historyEvent, taskList, startToCloseTimeoutSeconds, attempt)
}

func (b *historyBuilder) newTransientDecisionTaskScheduledEvent(taskList string, startToCloseTimeoutSeconds int32,
	attempt int64, timestamp int64) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEventWithTimestamp(eventpb.EVENT_TYPE_DECISION_TASK_SCHEDULED, timestamp)

	return setDecisionTaskScheduledEventInfo(historyEvent, taskList, startToCloseTimeoutSeconds, attempt)
}

func (b *historyBuilder) newDecisionTaskStartedEvent(scheduledEventID int64, requestID string,
	identity string) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_DECISION_TASK_STARTED)

	return setDecisionTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func (b *historyBuilder) newTransientDecisionTaskStartedEvent(scheduledEventID int64, requestID string,
	identity string, timestamp int64) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEventWithTimestamp(eventpb.EVENT_TYPE_DECISION_TASK_STARTED, timestamp)

	return setDecisionTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func (b *historyBuilder) newDecisionTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondDecisionTaskCompletedRequest) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_DECISION_TASK_COMPLETED)
	attributes := &eventpb.DecisionTaskCompletedEventAttributes{}
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.Identity = request.Identity
	attributes.BinaryChecksum = request.BinaryChecksum
	historyEvent.Attributes = &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskTimedOutEvent(scheduleEventID int64, startedEventID int64, timeoutType commonpb.TimeoutType) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_DECISION_TASK_TIMED_OUT)
	attributes := &eventpb.DecisionTaskTimedOutEventAttributes{}
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.TimeoutType = timeoutType
	historyEvent.Attributes = &eventpb.HistoryEvent_DecisionTaskTimedOutEventAttributes{DecisionTaskTimedOutEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskFailedEvent(attr *eventpb.DecisionTaskFailedEventAttributes) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_DECISION_TASK_FAILED)
	historyEvent.Attributes = &eventpb.HistoryEvent_DecisionTaskFailedEventAttributes{DecisionTaskFailedEventAttributes: attr}
	return historyEvent
}

func (b *historyBuilder) newActivityTaskScheduledEvent(decisionTaskCompletedEventID int64,
	scheduleAttributes *decisionpb.ScheduleActivityTaskDecisionAttributes) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
	attributes := &eventpb.ActivityTaskScheduledEventAttributes{}
	attributes.ActivityId = scheduleAttributes.ActivityId
	attributes.ActivityType = scheduleAttributes.ActivityType
	attributes.TaskList = scheduleAttributes.TaskList
	attributes.Header = scheduleAttributes.Header
	attributes.Input = scheduleAttributes.Input
	attributes.ScheduleToCloseTimeoutSeconds = scheduleAttributes.ScheduleToCloseTimeoutSeconds
	attributes.ScheduleToStartTimeoutSeconds = scheduleAttributes.ScheduleToStartTimeoutSeconds
	attributes.StartToCloseTimeoutSeconds = scheduleAttributes.StartToCloseTimeoutSeconds
	attributes.HeartbeatTimeoutSeconds = scheduleAttributes.HeartbeatTimeoutSeconds
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.RetryPolicy = scheduleAttributes.RetryPolicy
	historyEvent.Attributes = &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newActivityTaskStartedEvent(
	scheduledEventID int64,
	attempt int32,
	requestID string,
	identity string,
	lastFailure *failurepb.Failure,
) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_ACTIVITY_TASK_STARTED)
	attributes := &eventpb.ActivityTaskStartedEventAttributes{}
	attributes.ScheduledEventId = scheduledEventID
	attributes.Attempt = attempt
	attributes.Identity = identity
	attributes.RequestId = requestID
	attributes.LastFailure = lastFailure
	historyEvent.Attributes = &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newActivityTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondActivityTaskCompletedRequest) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED)
	attributes := &eventpb.ActivityTaskCompletedEventAttributes{}
	attributes.Result = request.Result
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.Identity = request.Identity
	historyEvent.Attributes = &eventpb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newActivityTaskTimedOutEvent(
	scheduleEventID, startedEventID int64,
	timeoutFailure *failurepb.Failure,
	retryStatus commonpb.RetryStatus,
) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT)
	attributes := &eventpb.ActivityTaskTimedOutEventAttributes{}
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.Failure = timeoutFailure
	attributes.RetryStatus = retryStatus

	historyEvent.Attributes = &eventpb.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newActivityTaskFailedEvent(scheduleEventID, startedEventID int64,
	failure *failurepb.Failure, retryStatus commonpb.RetryStatus, identity string) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_ACTIVITY_TASK_FAILED)
	attributes := &eventpb.ActivityTaskFailedEventAttributes{}
	attributes.Failure = failure
	attributes.RetryStatus = retryStatus
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.Identity = identity
	historyEvent.Attributes = &eventpb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newCompleteWorkflowExecutionEvent(decisionTaskCompletedEventID int64,
	request *decisionpb.CompleteWorkflowExecutionDecisionAttributes) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED)
	attributes := &eventpb.WorkflowExecutionCompletedEventAttributes{}
	attributes.Result = request.Result
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	historyEvent.Attributes = &eventpb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newFailWorkflowExecutionEvent(decisionTaskCompletedEventID int64, retryStatus commonpb.RetryStatus,
	request *decisionpb.FailWorkflowExecutionDecisionAttributes) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED)
	attributes := &eventpb.WorkflowExecutionFailedEventAttributes{}
	attributes.Failure = request.GetFailure()
	attributes.RetryStatus = retryStatus
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	historyEvent.Attributes = &eventpb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newTimeoutWorkflowExecutionEvent(retryStatus commonpb.RetryStatus) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT)
	attributes := &eventpb.WorkflowExecutionTimedOutEventAttributes{}
	attributes.RetryStatus = retryStatus
	historyEvent.Attributes = &eventpb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{WorkflowExecutionTimedOutEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionSignaledEvent(
	signalName string, input *commonpb.Payloads, identity string) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
	attributes := &eventpb.WorkflowExecutionSignaledEventAttributes{}
	attributes.SignalName = signalName
	attributes.Input = input
	attributes.Identity = identity
	historyEvent.Attributes = &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionTerminatedEvent(
	reason string, details *commonpb.Payloads, identity string) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED)
	attributes := &eventpb.WorkflowExecutionTerminatedEventAttributes{}
	attributes.Reason = reason
	attributes.Details = details
	attributes.Identity = identity
	historyEvent.Attributes = &eventpb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newMarkerRecordedEventAttributes(decisionTaskCompletedEventID int64,
	request *decisionpb.RecordMarkerDecisionAttributes) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_MARKER_RECORDED)
	attributes := &eventpb.MarkerRecordedEventAttributes{}
	attributes.MarkerName = request.MarkerName
	attributes.Details = request.Details
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.Header = request.Header
	historyEvent.Attributes = &eventpb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionCancelRequestedEvent(cause string,
	request *historyservice.RequestCancelWorkflowExecutionRequest) *eventpb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED)
	attributes := &eventpb.WorkflowExecutionCancelRequestedEventAttributes{}
	attributes.Cause = cause
	attributes.Identity = request.CancelRequest.Identity
	attributes.ExternalInitiatedEventId = request.ExternalInitiatedEventId
	attributes.ExternalWorkflowExecution = request.ExternalWorkflowExecution
	event.Attributes = &eventpb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{WorkflowExecutionCancelRequestedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID int64,
	request *decisionpb.CancelWorkflowExecutionDecisionAttributes) *eventpb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED)
	attributes := &eventpb.WorkflowExecutionCanceledEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.Details = request.GetDetails()
	event.Attributes = &eventpb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{WorkflowExecutionCanceledEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	request *decisionpb.RequestCancelExternalWorkflowExecutionDecisionAttributes) *eventpb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED)
	attributes := &eventpb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.Namespace = request.Namespace
	attributes.WorkflowExecution = &commonpb.WorkflowExecution{
		WorkflowId: request.WorkflowId,
		RunId:      request.RunId,
	}
	attributes.Control = request.Control
	attributes.ChildWorkflowOnly = request.ChildWorkflowOnly
	event.Attributes = &eventpb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	namespace, workflowID, runID string, cause eventpb.CancelExternalWorkflowExecutionFailedCause) *eventpb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED)
	attributes := &eventpb.RequestCancelExternalWorkflowExecutionFailedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.InitiatedEventId = initiatedEventID
	attributes.Namespace = namespace
	attributes.WorkflowExecution = &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	attributes.Cause = cause
	event.Attributes = &eventpb.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{RequestCancelExternalWorkflowExecutionFailedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newExternalWorkflowExecutionCancelRequestedEvent(initiatedEventID int64,
	namespace, workflowID, runID string) *eventpb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED)
	attributes := &eventpb.ExternalWorkflowExecutionCancelRequestedEventAttributes{}
	attributes.InitiatedEventId = initiatedEventID
	attributes.Namespace = namespace
	attributes.WorkflowExecution = &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	event.Attributes = &eventpb.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{ExternalWorkflowExecutionCancelRequestedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newSignalExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	request *decisionpb.SignalExternalWorkflowExecutionDecisionAttributes) *eventpb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED)
	attributes := &eventpb.SignalExternalWorkflowExecutionInitiatedEventAttributes{}
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
	event.Attributes = &eventpb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{SignalExternalWorkflowExecutionInitiatedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newUpsertWorkflowSearchAttributesEvent(decisionTaskCompletedEventID int64,
	request *decisionpb.UpsertWorkflowSearchAttributesDecisionAttributes) *eventpb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES)
	attributes := &eventpb.UpsertWorkflowSearchAttributesEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.SearchAttributes = request.GetSearchAttributes()
	event.Attributes = &eventpb.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{UpsertWorkflowSearchAttributesEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	namespace, workflowID, runID, control string, cause eventpb.SignalExternalWorkflowExecutionFailedCause) *eventpb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED)
	attributes := &eventpb.SignalExternalWorkflowExecutionFailedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.InitiatedEventId = initiatedEventID
	attributes.Namespace = namespace
	attributes.WorkflowExecution = &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	attributes.Cause = cause
	attributes.Control = control
	event.Attributes = &eventpb.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{SignalExternalWorkflowExecutionFailedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newExternalWorkflowExecutionSignaledEvent(initiatedEventID int64,
	namespace, workflowID, runID, control string) *eventpb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED)
	attributes := &eventpb.ExternalWorkflowExecutionSignaledEventAttributes{}
	attributes.InitiatedEventId = initiatedEventID
	attributes.Namespace = namespace
	attributes.WorkflowExecution = &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	attributes.Control = control
	event.Attributes = &eventpb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{ExternalWorkflowExecutionSignaledEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newWorkflowExecutionContinuedAsNewEvent(decisionTaskCompletedEventID int64,
	newRunID string, request *decisionpb.ContinueAsNewWorkflowExecutionDecisionAttributes) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW)
	attributes := &eventpb.WorkflowExecutionContinuedAsNewEventAttributes{}
	attributes.NewExecutionRunId = newRunID
	attributes.WorkflowType = request.WorkflowType
	attributes.TaskList = request.TaskList
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
	historyEvent.Attributes = &eventpb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newStartChildWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	startAttributes *decisionpb.StartChildWorkflowExecutionDecisionAttributes) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED)
	attributes := &eventpb.StartChildWorkflowExecutionInitiatedEventAttributes{}
	attributes.Namespace = startAttributes.Namespace
	attributes.WorkflowId = startAttributes.WorkflowId
	attributes.WorkflowType = startAttributes.WorkflowType
	attributes.TaskList = startAttributes.TaskList
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
	historyEvent.Attributes = &eventpb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{StartChildWorkflowExecutionInitiatedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionStartedEvent(
	namespace string,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	header *commonpb.Header,
) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED)
	attributes := &eventpb.ChildWorkflowExecutionStartedEventAttributes{}
	attributes.Namespace = namespace
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.Header = header
	historyEvent.Attributes = &eventpb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newStartChildWorkflowExecutionFailedEvent(initiatedID int64,
	cause eventpb.StartChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *eventpb.StartChildWorkflowExecutionInitiatedEventAttributes) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED)
	attributes := &eventpb.StartChildWorkflowExecutionFailedEventAttributes{}
	attributes.Namespace = initiatedEventAttributes.Namespace
	attributes.WorkflowId = initiatedEventAttributes.WorkflowId
	attributes.WorkflowType = initiatedEventAttributes.WorkflowType
	attributes.InitiatedEventId = initiatedID
	attributes.DecisionTaskCompletedEventId = initiatedEventAttributes.DecisionTaskCompletedEventId
	attributes.Control = initiatedEventAttributes.Control
	attributes.Cause = cause
	historyEvent.Attributes = &eventpb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{StartChildWorkflowExecutionFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionCompletedEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	completedAttributes *eventpb.WorkflowExecutionCompletedEventAttributes) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED)
	attributes := &eventpb.ChildWorkflowExecutionCompletedEventAttributes{}
	attributes.Namespace = namespace
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.StartedEventId = startedID
	attributes.Result = completedAttributes.Result
	historyEvent.Attributes = &eventpb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{ChildWorkflowExecutionCompletedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionFailedEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	failedAttributes *eventpb.WorkflowExecutionFailedEventAttributes) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED)
	attributes := &eventpb.ChildWorkflowExecutionFailedEventAttributes{}
	attributes.Namespace = namespace
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.StartedEventId = startedID
	attributes.Failure = failedAttributes.GetFailure()
	attributes.RetryStatus = failedAttributes.GetRetryStatus()
	historyEvent.Attributes = &eventpb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{ChildWorkflowExecutionFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionCanceledEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	canceledAttributes *eventpb.WorkflowExecutionCanceledEventAttributes) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED)
	attributes := &eventpb.ChildWorkflowExecutionCanceledEventAttributes{}
	attributes.Namespace = namespace
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.StartedEventId = startedID
	attributes.Details = canceledAttributes.Details
	historyEvent.Attributes = &eventpb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{ChildWorkflowExecutionCanceledEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionTerminatedEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	terminatedAttributes *eventpb.WorkflowExecutionTerminatedEventAttributes) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED)
	attributes := &eventpb.ChildWorkflowExecutionTerminatedEventAttributes{}
	attributes.Namespace = namespace
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.StartedEventId = startedID
	historyEvent.Attributes = &eventpb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{ChildWorkflowExecutionTerminatedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionTimedOutEvent(namespace string, execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	timedOutAttributes *eventpb.WorkflowExecutionTimedOutEventAttributes) *eventpb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT)
	attributes := &eventpb.ChildWorkflowExecutionTimedOutEventAttributes{}
	attributes.Namespace = namespace
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.StartedEventId = startedID
	attributes.RetryStatus = timedOutAttributes.GetRetryStatus()
	historyEvent.Attributes = &eventpb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{ChildWorkflowExecutionTimedOutEventAttributes: attributes}

	return historyEvent
}

func newDecisionTaskScheduledEventWithInfo(eventID, timestamp int64, taskList string, startToCloseTimeoutSeconds int32,
	attempt int64) *eventpb.HistoryEvent {
	historyEvent := createNewHistoryEvent(eventID, eventpb.EVENT_TYPE_DECISION_TASK_SCHEDULED, timestamp)

	return setDecisionTaskScheduledEventInfo(historyEvent, taskList, startToCloseTimeoutSeconds, attempt)
}

func newDecisionTaskStartedEventWithInfo(eventID, timestamp int64, scheduledEventID int64, requestID string,
	identity string) *eventpb.HistoryEvent {
	historyEvent := createNewHistoryEvent(eventID, eventpb.EVENT_TYPE_DECISION_TASK_STARTED, timestamp)

	return setDecisionTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func createNewHistoryEvent(eventID int64, eventType eventpb.EventType, timestamp int64) *eventpb.HistoryEvent {
	historyEvent := &eventpb.HistoryEvent{}
	historyEvent.EventId = eventID
	historyEvent.Timestamp = timestamp
	historyEvent.EventType = eventType

	return historyEvent
}

func setDecisionTaskScheduledEventInfo(historyEvent *eventpb.HistoryEvent, taskList string,
	startToCloseTimeoutSeconds int32, attempt int64) *eventpb.HistoryEvent {
	attributes := &eventpb.DecisionTaskScheduledEventAttributes{}
	attributes.TaskList = &tasklistpb.TaskList{}
	attributes.TaskList.Name = taskList
	attributes.StartToCloseTimeoutSeconds = startToCloseTimeoutSeconds
	attributes.Attempt = attempt
	historyEvent.Attributes = &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: attributes}

	return historyEvent
}

func setDecisionTaskStartedEventInfo(historyEvent *eventpb.HistoryEvent, scheduledEventID int64, requestID string,
	identity string) *eventpb.HistoryEvent {
	attributes := &eventpb.DecisionTaskStartedEventAttributes{}
	attributes.ScheduledEventId = scheduledEventID
	attributes.Identity = identity
	attributes.RequestId = requestID
	historyEvent.Attributes = &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) GetHistory() *eventpb.History {
	history := eventpb.History{Events: b.history}
	return &history
}
