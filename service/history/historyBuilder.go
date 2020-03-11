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
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	historyBuilder struct {
		transientHistory []*commonproto.HistoryEvent
		history          []*commonproto.HistoryEvent
		msBuilder        mutableState
	}
)

func newHistoryBuilder(msBuilder mutableState, logger log.Logger) *historyBuilder {
	return &historyBuilder{
		transientHistory: []*commonproto.HistoryEvent{},
		history:          []*commonproto.HistoryEvent{},
		msBuilder:        msBuilder,
	}
}

func newHistoryBuilderFromEvents(history []*commonproto.HistoryEvent, logger log.Logger) *historyBuilder {
	return &historyBuilder{
		history: history,
	}
}

func (b *historyBuilder) GetFirstEvent() *commonproto.HistoryEvent {
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
	previousExecution *persistence.WorkflowExecutionInfo, firstRunID, originalRunID string) *commonproto.HistoryEvent {
	event := b.newWorkflowExecutionStartedEvent(request, previousExecution, firstRunID, originalRunID)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskScheduledEvent(taskList string,
	startToCloseTimeoutSeconds int32, attempt int64) *commonproto.HistoryEvent {
	event := b.newDecisionTaskScheduledEvent(taskList, startToCloseTimeoutSeconds, attempt)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTransientDecisionTaskScheduledEvent(taskList string,
	startToCloseTimeoutSeconds int32, attempt int64, timestamp int64) *commonproto.HistoryEvent {
	event := b.newTransientDecisionTaskScheduledEvent(taskList, startToCloseTimeoutSeconds, attempt, timestamp)

	return b.addTransientEvent(event)
}

func (b *historyBuilder) AddDecisionTaskStartedEvent(scheduleEventID int64, requestID string,
	identity string) *commonproto.HistoryEvent {
	event := b.newDecisionTaskStartedEvent(scheduleEventID, requestID, identity)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTransientDecisionTaskStartedEvent(scheduleEventID int64, requestID string,
	identity string, timestamp int64) *commonproto.HistoryEvent {
	event := b.newTransientDecisionTaskStartedEvent(scheduleEventID, requestID, identity, timestamp)

	return b.addTransientEvent(event)
}

func (b *historyBuilder) AddDecisionTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondDecisionTaskCompletedRequest) *commonproto.HistoryEvent {
	event := b.newDecisionTaskCompletedEvent(scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskTimedOutEvent(scheduleEventID int64,
	startedEventID int64, timeoutType enums.TimeoutType) *commonproto.HistoryEvent {
	event := b.newDecisionTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutType)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskFailedEvent(attr *commonproto.DecisionTaskFailedEventAttributes) *commonproto.HistoryEvent {
	event := b.newDecisionTaskFailedEvent(attr)
	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskScheduledEvent(decisionCompletedEventID int64,
	attributes *commonproto.ScheduleActivityTaskDecisionAttributes) *commonproto.HistoryEvent {
	event := b.newActivityTaskScheduledEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskStartedEvent(scheduleEventID int64, attempt int32, requestID string,
	identity string) *commonproto.HistoryEvent {
	event := b.newActivityTaskStartedEvent(scheduleEventID, attempt, requestID, identity)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondActivityTaskCompletedRequest) *commonproto.HistoryEvent {
	event := b.newActivityTaskCompletedEvent(scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskFailedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondActivityTaskFailedRequest) *commonproto.HistoryEvent {
	event := b.newActivityTaskFailedEvent(scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskTimedOutEvent(
	scheduleEventID,
	startedEventID int64,
	timeoutType enums.TimeoutType,
	lastHeartBeatDetails []byte,
	lastFailureReason string,
	lastFailureDetail []byte,
) *commonproto.HistoryEvent {
	event := b.newActivityTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutType, lastHeartBeatDetails,
		lastFailureReason, lastFailureDetail)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddCompletedWorkflowEvent(decisionCompletedEventID int64,
	attributes *commonproto.CompleteWorkflowExecutionDecisionAttributes) *commonproto.HistoryEvent {
	event := b.newCompleteWorkflowExecutionEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddFailWorkflowEvent(decisionCompletedEventID int64,
	attributes *commonproto.FailWorkflowExecutionDecisionAttributes) *commonproto.HistoryEvent {
	event := b.newFailWorkflowExecutionEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimeoutWorkflowEvent() *commonproto.HistoryEvent {
	event := b.newTimeoutWorkflowExecutionEvent()

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionTerminatedEvent(
	reason string,
	details []byte,
	identity string,
) *commonproto.HistoryEvent {
	event := b.newWorkflowExecutionTerminatedEvent(reason, details, identity)
	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddContinuedAsNewEvent(decisionCompletedEventID int64, newRunID string,
	attributes *commonproto.ContinueAsNewWorkflowExecutionDecisionAttributes) *commonproto.HistoryEvent {
	event := b.newWorkflowExecutionContinuedAsNewEvent(decisionCompletedEventID, newRunID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerStartedEvent(decisionCompletedEventID int64,
	request *commonproto.StartTimerDecisionAttributes) *commonproto.HistoryEvent {

	attributes := &commonproto.TimerStartedEventAttributes{}
	attributes.TimerId = request.TimerId
	attributes.StartToFireTimeoutSeconds = request.StartToFireTimeoutSeconds
	attributes.DecisionTaskCompletedEventId = decisionCompletedEventID

	event := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeTimerStarted)
	event.Attributes = &commonproto.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerFiredEvent(
	startedEventID int64,
	timerID string,
) *commonproto.HistoryEvent {

	attributes := &commonproto.TimerFiredEventAttributes{}
	attributes.TimerId = timerID
	attributes.StartedEventId = startedEventID

	event := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeTimerFired)
	event.Attributes = &commonproto.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCancelRequestedEvent(decisionCompletedEventID int64,
	activityID string) *commonproto.HistoryEvent {

	attributes := &commonproto.ActivityTaskCancelRequestedEventAttributes{}
	attributes.ActivityId = activityID
	attributes.DecisionTaskCompletedEventId = decisionCompletedEventID

	event := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeActivityTaskCancelRequested)
	event.Attributes = &commonproto.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{ActivityTaskCancelRequestedEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddRequestCancelActivityTaskFailedEvent(decisionCompletedEventID int64,
	activityID string, cause string) *commonproto.HistoryEvent {

	attributes := &commonproto.RequestCancelActivityTaskFailedEventAttributes{}
	attributes.ActivityId = activityID
	attributes.DecisionTaskCompletedEventId = decisionCompletedEventID
	attributes.Cause = cause

	event := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeRequestCancelActivityTaskFailed)
	event.Attributes = &commonproto.HistoryEvent_RequestCancelActivityTaskFailedEventAttributes{RequestCancelActivityTaskFailedEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCanceledEvent(scheduleEventID, startedEventID int64,
	latestCancelRequestedEventID int64, details []byte, identity string) *commonproto.HistoryEvent {

	attributes := &commonproto.ActivityTaskCanceledEventAttributes{}
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.LatestCancelRequestedEventId = latestCancelRequestedEventID
	attributes.Details = details
	attributes.Identity = identity

	event := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeActivityTaskCanceled)
	event.Attributes = &commonproto.HistoryEvent_ActivityTaskCanceledEventAttributes{ActivityTaskCanceledEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerCanceledEvent(startedEventID int64,
	decisionTaskCompletedEventID int64, timerID string, identity string) *commonproto.HistoryEvent {

	attributes := &commonproto.TimerCanceledEventAttributes{}
	attributes.StartedEventId = startedEventID
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.TimerId = timerID
	attributes.Identity = identity

	event := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeTimerCanceled)
	event.Attributes = &commonproto.HistoryEvent_TimerCanceledEventAttributes{TimerCanceledEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddCancelTimerFailedEvent(timerID string, decisionTaskCompletedEventID int64,
	cause string, identity string) *commonproto.HistoryEvent {

	attributes := &commonproto.CancelTimerFailedEventAttributes{}
	attributes.TimerId = timerID
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.Cause = cause
	attributes.Identity = identity

	event := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeCancelTimerFailed)
	event.Attributes = &commonproto.HistoryEvent_CancelTimerFailedEventAttributes{CancelTimerFailedEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionCancelRequestedEvent(cause string,
	request *historyservice.RequestCancelWorkflowExecutionRequest) *commonproto.HistoryEvent {
	event := b.newWorkflowExecutionCancelRequestedEvent(cause, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID int64,
	attributes *commonproto.CancelWorkflowExecutionDecisionAttributes) *commonproto.HistoryEvent {
	event := b.newWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	request *commonproto.RequestCancelExternalWorkflowExecutionDecisionAttributes) *commonproto.HistoryEvent {
	event := b.newRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	domain, workflowID, runID string, cause enums.CancelExternalWorkflowExecutionFailedCause) *commonproto.HistoryEvent {
	event := b.newRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID,
		domain, workflowID, runID, cause)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddExternalWorkflowExecutionCancelRequested(initiatedEventID int64,
	domain, workflowID, runID string) *commonproto.HistoryEvent {
	event := b.newExternalWorkflowExecutionCancelRequestedEvent(initiatedEventID,
		domain, workflowID, runID)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddSignalExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	attributes *commonproto.SignalExternalWorkflowExecutionDecisionAttributes) *commonproto.HistoryEvent {
	event := b.newSignalExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddUpsertWorkflowSearchAttributesEvent(
	decisionTaskCompletedEventID int64,
	attributes *commonproto.UpsertWorkflowSearchAttributesDecisionAttributes) *commonproto.HistoryEvent {
	event := b.newUpsertWorkflowSearchAttributesEvent(decisionTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	domain, workflowID, runID string, control []byte, cause enums.SignalExternalWorkflowExecutionFailedCause) *commonproto.HistoryEvent {
	event := b.newSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID,
		domain, workflowID, runID, control, cause)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddExternalWorkflowExecutionSignaled(initiatedEventID int64,
	domain, workflowID, runID string, control []byte) *commonproto.HistoryEvent {
	event := b.newExternalWorkflowExecutionSignaledEvent(initiatedEventID,
		domain, workflowID, runID, control)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddMarkerRecordedEvent(decisionCompletedEventID int64,
	attributes *commonproto.RecordMarkerDecisionAttributes) *commonproto.HistoryEvent {
	event := b.newMarkerRecordedEventAttributes(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionSignaledEvent(
	signalName string, input []byte, identity string) *commonproto.HistoryEvent {
	event := b.newWorkflowExecutionSignaledEvent(signalName, input, identity)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddStartChildWorkflowExecutionInitiatedEvent(decisionCompletedEventID int64,
	attributes *commonproto.StartChildWorkflowExecutionDecisionAttributes) *commonproto.HistoryEvent {
	event := b.newStartChildWorkflowExecutionInitiatedEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionStartedEvent(
	domain string,
	execution *commonproto.WorkflowExecution,
	workflowType *commonproto.WorkflowType,
	initiatedID int64,
	header *commonproto.Header,
) *commonproto.HistoryEvent {
	event := b.newChildWorkflowExecutionStartedEvent(domain, execution, workflowType, initiatedID, header)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddStartChildWorkflowExecutionFailedEvent(initiatedID int64,
	cause enums.ChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *commonproto.StartChildWorkflowExecutionInitiatedEventAttributes) *commonproto.HistoryEvent {
	event := b.newStartChildWorkflowExecutionFailedEvent(initiatedID, cause, initiatedEventAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionCompletedEvent(domain string, execution *commonproto.WorkflowExecution,
	workflowType *commonproto.WorkflowType, initiatedID, startedID int64,
	completedAttributes *commonproto.WorkflowExecutionCompletedEventAttributes) *commonproto.HistoryEvent {
	event := b.newChildWorkflowExecutionCompletedEvent(domain, execution, workflowType, initiatedID, startedID,
		completedAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionFailedEvent(domain string, execution *commonproto.WorkflowExecution,
	workflowType *commonproto.WorkflowType, initiatedID, startedID int64,
	failedAttributes *commonproto.WorkflowExecutionFailedEventAttributes) *commonproto.HistoryEvent {
	event := b.newChildWorkflowExecutionFailedEvent(domain, execution, workflowType, initiatedID, startedID,
		failedAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionCanceledEvent(domain string, execution *commonproto.WorkflowExecution,
	workflowType *commonproto.WorkflowType, initiatedID, startedID int64,
	canceledAttributes *commonproto.WorkflowExecutionCanceledEventAttributes) *commonproto.HistoryEvent {
	event := b.newChildWorkflowExecutionCanceledEvent(domain, execution, workflowType, initiatedID, startedID,
		canceledAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionTerminatedEvent(domain string, execution *commonproto.WorkflowExecution,
	workflowType *commonproto.WorkflowType, initiatedID, startedID int64,
	terminatedAttributes *commonproto.WorkflowExecutionTerminatedEventAttributes) *commonproto.HistoryEvent {
	event := b.newChildWorkflowExecutionTerminatedEvent(domain, execution, workflowType, initiatedID, startedID,
		terminatedAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionTimedOutEvent(domain string, execution *commonproto.WorkflowExecution,
	workflowType *commonproto.WorkflowType, initiatedID, startedID int64,
	timedOutAttributes *commonproto.WorkflowExecutionTimedOutEventAttributes) *commonproto.HistoryEvent {
	event := b.newChildWorkflowExecutionTimedOutEvent(domain, execution, workflowType, initiatedID, startedID,
		timedOutAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) addEventToHistory(event *commonproto.HistoryEvent) *commonproto.HistoryEvent {
	b.history = append(b.history, event)
	return event
}

func (b *historyBuilder) addTransientEvent(event *commonproto.HistoryEvent) *commonproto.HistoryEvent {
	b.transientHistory = append(b.transientHistory, event)
	return event
}

func (b *historyBuilder) newWorkflowExecutionStartedEvent(
	startRequest *historyservice.StartWorkflowExecutionRequest, previousExecution *persistence.WorkflowExecutionInfo, firstRunID, originalRunID string) *commonproto.HistoryEvent {
	var prevRunID string
	var resetPoints *commonproto.ResetPoints
	if previousExecution != nil {
		prevRunID = previousExecution.RunID
		resetPoints = previousExecution.AutoResetPoints
	}
	request := startRequest.StartRequest
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeWorkflowExecutionStarted)
	attributes := &commonproto.WorkflowExecutionStartedEventAttributes{}
	attributes.WorkflowType = request.WorkflowType
	attributes.TaskList = request.TaskList
	attributes.Header = request.Header
	attributes.Input = request.Input
	attributes.ExecutionStartToCloseTimeoutSeconds = request.ExecutionStartToCloseTimeoutSeconds
	attributes.TaskStartToCloseTimeoutSeconds = request.TaskStartToCloseTimeoutSeconds
	attributes.ContinuedExecutionRunId = prevRunID
	attributes.PrevAutoResetPoints = resetPoints
	attributes.Identity = request.Identity
	attributes.RetryPolicy = request.RetryPolicy
	attributes.Attempt = startRequest.GetAttempt()
	attributes.ExpirationTimestamp = startRequest.ExpirationTimestamp
	attributes.CronSchedule = request.CronSchedule
	attributes.LastCompletionResult = startRequest.LastCompletionResult
	attributes.ContinuedFailureReason = startRequest.ContinuedFailureReason
	attributes.ContinuedFailureDetails = startRequest.ContinuedFailureDetails
	attributes.Initiator = startRequest.ContinueAsNewInitiator
	attributes.FirstDecisionTaskBackoffSeconds = startRequest.FirstDecisionTaskBackoffSeconds
	attributes.FirstExecutionRunId = firstRunID
	attributes.OriginalExecutionRunId = originalRunID
	attributes.Memo = request.Memo
	attributes.SearchAttributes = request.SearchAttributes

	parentInfo := startRequest.ParentExecutionInfo
	if parentInfo != nil {
		attributes.ParentWorkflowDomain = parentInfo.Domain
		attributes.ParentWorkflowExecution = parentInfo.Execution
		attributes.ParentInitiatedEventId = parentInfo.InitiatedId
	}
	historyEvent.Attributes = &commonproto.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskScheduledEvent(taskList string, startToCloseTimeoutSeconds int32,
	attempt int64) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeDecisionTaskScheduled)

	return setDecisionTaskScheduledEventInfo(historyEvent, taskList, startToCloseTimeoutSeconds, attempt)
}

func (b *historyBuilder) newTransientDecisionTaskScheduledEvent(taskList string, startToCloseTimeoutSeconds int32,
	attempt int64, timestamp int64) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEventWithTimestamp(enums.EventTypeDecisionTaskScheduled, timestamp)

	return setDecisionTaskScheduledEventInfo(historyEvent, taskList, startToCloseTimeoutSeconds, attempt)
}

func (b *historyBuilder) newDecisionTaskStartedEvent(scheduledEventID int64, requestID string,
	identity string) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeDecisionTaskStarted)

	return setDecisionTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func (b *historyBuilder) newTransientDecisionTaskStartedEvent(scheduledEventID int64, requestID string,
	identity string, timestamp int64) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEventWithTimestamp(enums.EventTypeDecisionTaskStarted, timestamp)

	return setDecisionTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func (b *historyBuilder) newDecisionTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondDecisionTaskCompletedRequest) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeDecisionTaskCompleted)
	attributes := &commonproto.DecisionTaskCompletedEventAttributes{}
	attributes.ExecutionContext = request.ExecutionContext
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.Identity = request.Identity
	attributes.BinaryChecksum = request.BinaryChecksum
	historyEvent.Attributes = &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskTimedOutEvent(scheduleEventID int64, startedEventID int64, timeoutType enums.TimeoutType) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeDecisionTaskTimedOut)
	attributes := &commonproto.DecisionTaskTimedOutEventAttributes{}
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.TimeoutType = timeoutType
	historyEvent.Attributes = &commonproto.HistoryEvent_DecisionTaskTimedOutEventAttributes{DecisionTaskTimedOutEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskFailedEvent(attr *commonproto.DecisionTaskFailedEventAttributes) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeDecisionTaskFailed)
	historyEvent.Attributes = &commonproto.HistoryEvent_DecisionTaskFailedEventAttributes{DecisionTaskFailedEventAttributes: attr}
	return historyEvent
}

func (b *historyBuilder) newActivityTaskScheduledEvent(decisionTaskCompletedEventID int64,
	scheduleAttributes *commonproto.ScheduleActivityTaskDecisionAttributes) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeActivityTaskScheduled)
	attributes := &commonproto.ActivityTaskScheduledEventAttributes{}
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
	historyEvent.Attributes = &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newActivityTaskStartedEvent(scheduledEventID int64, attempt int32, requestID string,
	identity string) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeActivityTaskStarted)
	attributes := &commonproto.ActivityTaskStartedEventAttributes{}
	attributes.ScheduledEventId = scheduledEventID
	attributes.Attempt = attempt
	attributes.Identity = identity
	attributes.RequestId = requestID
	historyEvent.Attributes = &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newActivityTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondActivityTaskCompletedRequest) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeActivityTaskCompleted)
	attributes := &commonproto.ActivityTaskCompletedEventAttributes{}
	attributes.Result = request.Result
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.Identity = request.Identity
	historyEvent.Attributes = &commonproto.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newActivityTaskTimedOutEvent(
	scheduleEventID, startedEventID int64,
	timeoutType enums.TimeoutType,
	lastHeartBeatDetails []byte,
	lastFailureReason string,
	lastFailureDetail []byte,
) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeActivityTaskTimedOut)
	attributes := &commonproto.ActivityTaskTimedOutEventAttributes{}
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.TimeoutType = timeoutType
	attributes.Details = lastHeartBeatDetails
	attributes.LastFailureReason = lastFailureReason
	attributes.LastFailureDetails = lastFailureDetail

	historyEvent.Attributes = &commonproto.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newActivityTaskFailedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondActivityTaskFailedRequest) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeActivityTaskFailed)
	attributes := &commonproto.ActivityTaskFailedEventAttributes{}
	attributes.Reason = request.Reason
	attributes.Details = request.Details
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.Identity = request.Identity
	historyEvent.Attributes = &commonproto.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newCompleteWorkflowExecutionEvent(decisionTaskCompletedEventID int64,
	request *commonproto.CompleteWorkflowExecutionDecisionAttributes) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeWorkflowExecutionCompleted)
	attributes := &commonproto.WorkflowExecutionCompletedEventAttributes{}
	attributes.Result = request.Result
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	historyEvent.Attributes = &commonproto.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newFailWorkflowExecutionEvent(decisionTaskCompletedEventID int64,
	request *commonproto.FailWorkflowExecutionDecisionAttributes) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeWorkflowExecutionFailed)
	attributes := &commonproto.WorkflowExecutionFailedEventAttributes{}
	attributes.Reason = request.Reason
	attributes.Details = request.Details
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	historyEvent.Attributes = &commonproto.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newTimeoutWorkflowExecutionEvent() *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeWorkflowExecutionTimedOut)
	attributes := &commonproto.WorkflowExecutionTimedOutEventAttributes{}
	attributes.TimeoutType = enums.TimeoutTypeStartToClose
	historyEvent.Attributes = &commonproto.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{WorkflowExecutionTimedOutEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionSignaledEvent(
	signalName string, input []byte, identity string) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeWorkflowExecutionSignaled)
	attributes := &commonproto.WorkflowExecutionSignaledEventAttributes{}
	attributes.SignalName = signalName
	attributes.Input = input
	attributes.Identity = identity
	historyEvent.Attributes = &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionTerminatedEvent(
	reason string, details []byte, identity string) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeWorkflowExecutionTerminated)
	attributes := &commonproto.WorkflowExecutionTerminatedEventAttributes{}
	attributes.Reason = reason
	attributes.Details = details
	attributes.Identity = identity
	historyEvent.Attributes = &commonproto.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newMarkerRecordedEventAttributes(decisionTaskCompletedEventID int64,
	request *commonproto.RecordMarkerDecisionAttributes) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeMarkerRecorded)
	attributes := &commonproto.MarkerRecordedEventAttributes{}
	attributes.MarkerName = request.MarkerName
	attributes.Details = request.Details
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.Header = request.Header
	historyEvent.Attributes = &commonproto.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionCancelRequestedEvent(cause string,
	request *historyservice.RequestCancelWorkflowExecutionRequest) *commonproto.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeWorkflowExecutionCancelRequested)
	attributes := &commonproto.WorkflowExecutionCancelRequestedEventAttributes{}
	attributes.Cause = cause
	attributes.Identity = request.CancelRequest.Identity
	attributes.ExternalInitiatedEventId = request.ExternalInitiatedEventId
	attributes.ExternalWorkflowExecution = request.ExternalWorkflowExecution
	event.Attributes = &commonproto.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{WorkflowExecutionCancelRequestedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID int64,
	request *commonproto.CancelWorkflowExecutionDecisionAttributes) *commonproto.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeWorkflowExecutionCanceled)
	attributes := &commonproto.WorkflowExecutionCanceledEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.Details = request.Details
	event.Attributes = &commonproto.HistoryEvent_WorkflowExecutionCanceledEventAttributes{WorkflowExecutionCanceledEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	request *commonproto.RequestCancelExternalWorkflowExecutionDecisionAttributes) *commonproto.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeRequestCancelExternalWorkflowExecutionInitiated)
	attributes := &commonproto.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.Domain = request.Domain
	attributes.WorkflowExecution = &commonproto.WorkflowExecution{
		WorkflowId: request.WorkflowId,
		RunId:      request.RunId,
	}
	attributes.Control = request.Control
	attributes.ChildWorkflowOnly = request.ChildWorkflowOnly
	event.Attributes = &commonproto.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	domain, workflowID, runID string, cause enums.CancelExternalWorkflowExecutionFailedCause) *commonproto.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeRequestCancelExternalWorkflowExecutionFailed)
	attributes := &commonproto.RequestCancelExternalWorkflowExecutionFailedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.InitiatedEventId = initiatedEventID
	attributes.Domain = domain
	attributes.WorkflowExecution = &commonproto.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	attributes.Cause = cause
	event.Attributes = &commonproto.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{RequestCancelExternalWorkflowExecutionFailedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newExternalWorkflowExecutionCancelRequestedEvent(initiatedEventID int64,
	domain, workflowID, runID string) *commonproto.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeExternalWorkflowExecutionCancelRequested)
	attributes := &commonproto.ExternalWorkflowExecutionCancelRequestedEventAttributes{}
	attributes.InitiatedEventId = initiatedEventID
	attributes.Domain = domain
	attributes.WorkflowExecution = &commonproto.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	event.Attributes = &commonproto.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{ExternalWorkflowExecutionCancelRequestedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newSignalExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	request *commonproto.SignalExternalWorkflowExecutionDecisionAttributes) *commonproto.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeSignalExternalWorkflowExecutionInitiated)
	attributes := &commonproto.SignalExternalWorkflowExecutionInitiatedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.Domain = request.Domain
	attributes.WorkflowExecution = &commonproto.WorkflowExecution{
		WorkflowId: request.Execution.WorkflowId,
		RunId:      request.Execution.RunId,
	}
	attributes.SignalName = request.GetSignalName()
	attributes.Input = request.Input
	attributes.Control = request.Control
	attributes.ChildWorkflowOnly = request.ChildWorkflowOnly
	event.Attributes = &commonproto.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{SignalExternalWorkflowExecutionInitiatedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newUpsertWorkflowSearchAttributesEvent(decisionTaskCompletedEventID int64,
	request *commonproto.UpsertWorkflowSearchAttributesDecisionAttributes) *commonproto.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeUpsertWorkflowSearchAttributes)
	attributes := &commonproto.UpsertWorkflowSearchAttributesEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.SearchAttributes = request.GetSearchAttributes()
	event.Attributes = &commonproto.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{UpsertWorkflowSearchAttributesEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	domain, workflowID, runID string, control []byte, cause enums.SignalExternalWorkflowExecutionFailedCause) *commonproto.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeSignalExternalWorkflowExecutionFailed)
	attributes := &commonproto.SignalExternalWorkflowExecutionFailedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.InitiatedEventId = initiatedEventID
	attributes.Domain = domain
	attributes.WorkflowExecution = &commonproto.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	attributes.Cause = cause
	attributes.Control = control
	event.Attributes = &commonproto.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{SignalExternalWorkflowExecutionFailedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newExternalWorkflowExecutionSignaledEvent(initiatedEventID int64,
	domain, workflowID, runID string, control []byte) *commonproto.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeExternalWorkflowExecutionSignaled)
	attributes := &commonproto.ExternalWorkflowExecutionSignaledEventAttributes{}
	attributes.InitiatedEventId = initiatedEventID
	attributes.Domain = domain
	attributes.WorkflowExecution = &commonproto.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	attributes.Control = control
	event.Attributes = &commonproto.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{ExternalWorkflowExecutionSignaledEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newWorkflowExecutionContinuedAsNewEvent(decisionTaskCompletedEventID int64,
	newRunID string, request *commonproto.ContinueAsNewWorkflowExecutionDecisionAttributes) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeWorkflowExecutionContinuedAsNew)
	attributes := &commonproto.WorkflowExecutionContinuedAsNewEventAttributes{}
	attributes.NewExecutionRunId = newRunID
	attributes.WorkflowType = request.WorkflowType
	attributes.TaskList = request.TaskList
	attributes.Header = request.Header
	attributes.Input = request.Input
	attributes.ExecutionStartToCloseTimeoutSeconds = request.ExecutionStartToCloseTimeoutSeconds
	attributes.TaskStartToCloseTimeoutSeconds = request.TaskStartToCloseTimeoutSeconds
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.BackoffStartIntervalInSeconds = request.GetBackoffStartIntervalInSeconds()
	attributes.Initiator = request.Initiator
	attributes.FailureReason = request.FailureReason
	attributes.FailureDetails = request.FailureDetails
	attributes.LastCompletionResult = request.LastCompletionResult
	attributes.Memo = request.Memo
	attributes.SearchAttributes = request.SearchAttributes
	historyEvent.Attributes = &commonproto.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newStartChildWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	startAttributes *commonproto.StartChildWorkflowExecutionDecisionAttributes) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeStartChildWorkflowExecutionInitiated)
	attributes := &commonproto.StartChildWorkflowExecutionInitiatedEventAttributes{}
	attributes.Domain = startAttributes.Domain
	attributes.WorkflowId = startAttributes.WorkflowId
	attributes.WorkflowType = startAttributes.WorkflowType
	attributes.TaskList = startAttributes.TaskList
	attributes.Header = startAttributes.Header
	attributes.Input = startAttributes.Input
	attributes.ExecutionStartToCloseTimeoutSeconds = startAttributes.ExecutionStartToCloseTimeoutSeconds
	attributes.TaskStartToCloseTimeoutSeconds = startAttributes.TaskStartToCloseTimeoutSeconds
	attributes.Control = startAttributes.Control
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.WorkflowIdReusePolicy = startAttributes.WorkflowIdReusePolicy
	attributes.RetryPolicy = startAttributes.RetryPolicy
	attributes.CronSchedule = startAttributes.CronSchedule
	attributes.Memo = startAttributes.Memo
	attributes.SearchAttributes = startAttributes.SearchAttributes
	attributes.ParentClosePolicy = startAttributes.GetParentClosePolicy()
	historyEvent.Attributes = &commonproto.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{StartChildWorkflowExecutionInitiatedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionStartedEvent(
	domain string,
	execution *commonproto.WorkflowExecution,
	workflowType *commonproto.WorkflowType,
	initiatedID int64,
	header *commonproto.Header,
) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeChildWorkflowExecutionStarted)
	attributes := &commonproto.ChildWorkflowExecutionStartedEventAttributes{}
	attributes.Domain = domain
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.Header = header
	historyEvent.Attributes = &commonproto.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newStartChildWorkflowExecutionFailedEvent(initiatedID int64,
	cause enums.ChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *commonproto.StartChildWorkflowExecutionInitiatedEventAttributes) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeStartChildWorkflowExecutionFailed)
	attributes := &commonproto.StartChildWorkflowExecutionFailedEventAttributes{}
	attributes.Domain = initiatedEventAttributes.Domain
	attributes.WorkflowId = initiatedEventAttributes.WorkflowId
	attributes.WorkflowType = initiatedEventAttributes.WorkflowType
	attributes.InitiatedEventId = initiatedID
	attributes.DecisionTaskCompletedEventId = initiatedEventAttributes.DecisionTaskCompletedEventId
	attributes.Control = initiatedEventAttributes.Control
	attributes.Cause = cause
	historyEvent.Attributes = &commonproto.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{StartChildWorkflowExecutionFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionCompletedEvent(domain string, execution *commonproto.WorkflowExecution,
	workflowType *commonproto.WorkflowType, initiatedID, startedID int64,
	completedAttributes *commonproto.WorkflowExecutionCompletedEventAttributes) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeChildWorkflowExecutionCompleted)
	attributes := &commonproto.ChildWorkflowExecutionCompletedEventAttributes{}
	attributes.Domain = domain
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.StartedEventId = startedID
	attributes.Result = completedAttributes.Result
	historyEvent.Attributes = &commonproto.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{ChildWorkflowExecutionCompletedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionFailedEvent(domain string, execution *commonproto.WorkflowExecution,
	workflowType *commonproto.WorkflowType, initiatedID, startedID int64,
	failedAttributes *commonproto.WorkflowExecutionFailedEventAttributes) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeChildWorkflowExecutionFailed)
	attributes := &commonproto.ChildWorkflowExecutionFailedEventAttributes{}
	attributes.Domain = domain
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.StartedEventId = startedID
	attributes.Reason = failedAttributes.Reason
	attributes.Details = failedAttributes.Details
	historyEvent.Attributes = &commonproto.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{ChildWorkflowExecutionFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionCanceledEvent(domain string, execution *commonproto.WorkflowExecution,
	workflowType *commonproto.WorkflowType, initiatedID, startedID int64,
	canceledAttributes *commonproto.WorkflowExecutionCanceledEventAttributes) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeChildWorkflowExecutionCanceled)
	attributes := &commonproto.ChildWorkflowExecutionCanceledEventAttributes{}
	attributes.Domain = domain
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.StartedEventId = startedID
	attributes.Details = canceledAttributes.Details
	historyEvent.Attributes = &commonproto.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{ChildWorkflowExecutionCanceledEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionTerminatedEvent(domain string, execution *commonproto.WorkflowExecution,
	workflowType *commonproto.WorkflowType, initiatedID, startedID int64,
	terminatedAttributes *commonproto.WorkflowExecutionTerminatedEventAttributes) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeChildWorkflowExecutionTerminated)
	attributes := &commonproto.ChildWorkflowExecutionTerminatedEventAttributes{}
	attributes.Domain = domain
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.StartedEventId = startedID
	historyEvent.Attributes = &commonproto.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{ChildWorkflowExecutionTerminatedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionTimedOutEvent(domain string, execution *commonproto.WorkflowExecution,
	workflowType *commonproto.WorkflowType, initiatedID, startedID int64,
	timedOutAttributes *commonproto.WorkflowExecutionTimedOutEventAttributes) *commonproto.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(enums.EventTypeChildWorkflowExecutionTimedOut)
	attributes := &commonproto.ChildWorkflowExecutionTimedOutEventAttributes{}
	attributes.Domain = domain
	attributes.TimeoutType = timedOutAttributes.TimeoutType
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.StartedEventId = startedID
	historyEvent.Attributes = &commonproto.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{ChildWorkflowExecutionTimedOutEventAttributes: attributes}

	return historyEvent
}

func newDecisionTaskScheduledEventWithInfo(eventID, timestamp int64, taskList string, startToCloseTimeoutSeconds int32,
	attempt int64) *commonproto.HistoryEvent {
	historyEvent := createNewHistoryEvent(eventID, enums.EventTypeDecisionTaskScheduled, timestamp)

	return setDecisionTaskScheduledEventInfo(historyEvent, taskList, startToCloseTimeoutSeconds, attempt)
}

func newDecisionTaskStartedEventWithInfo(eventID, timestamp int64, scheduledEventID int64, requestID string,
	identity string) *commonproto.HistoryEvent {
	historyEvent := createNewHistoryEvent(eventID, enums.EventTypeDecisionTaskStarted, timestamp)

	return setDecisionTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func createNewHistoryEvent(eventID int64, eventType enums.EventType, timestamp int64) *commonproto.HistoryEvent {
	historyEvent := &commonproto.HistoryEvent{}
	historyEvent.EventId = eventID
	historyEvent.Timestamp = timestamp
	historyEvent.EventType = eventType

	return historyEvent
}

func setDecisionTaskScheduledEventInfo(historyEvent *commonproto.HistoryEvent, taskList string,
	startToCloseTimeoutSeconds int32, attempt int64) *commonproto.HistoryEvent {
	attributes := &commonproto.DecisionTaskScheduledEventAttributes{}
	attributes.TaskList = &commonproto.TaskList{}
	attributes.TaskList.Name = taskList
	attributes.StartToCloseTimeoutSeconds = startToCloseTimeoutSeconds
	attributes.Attempt = attempt
	historyEvent.Attributes = &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: attributes}

	return historyEvent
}

func setDecisionTaskStartedEventInfo(historyEvent *commonproto.HistoryEvent, scheduledEventID int64, requestID string,
	identity string) *commonproto.HistoryEvent {
	attributes := &commonproto.DecisionTaskStartedEventAttributes{}
	attributes.ScheduledEventId = scheduledEventID
	attributes.Identity = identity
	attributes.RequestId = requestID
	historyEvent.Attributes = &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) GetHistory() *commonproto.History {
	history := commonproto.History{Events: b.history}
	return &history
}
