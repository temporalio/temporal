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
	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
)

type (
	historyBuilder struct {
		transientHistory []*workflow.HistoryEvent
		history          []*workflow.HistoryEvent
		msBuilder        mutableState
	}
)

func newHistoryBuilder(msBuilder mutableState, logger log.Logger) *historyBuilder {
	return &historyBuilder{
		transientHistory: []*workflow.HistoryEvent{},
		history:          []*workflow.HistoryEvent{},
		msBuilder:        msBuilder,
	}
}

func newHistoryBuilderFromEvents(history []*workflow.HistoryEvent, logger log.Logger) *historyBuilder {
	return &historyBuilder{
		history: history,
	}
}

func (b *historyBuilder) GetFirstEvent() *workflow.HistoryEvent {
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
func (b *historyBuilder) AddWorkflowExecutionStartedEvent(request *h.StartWorkflowExecutionRequest,
	previousExecution *persistence.WorkflowExecutionInfo, firstRunID, originalRunID string) *workflow.HistoryEvent {
	event := b.newWorkflowExecutionStartedEvent(request, previousExecution, firstRunID, originalRunID)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskScheduledEvent(taskList string,
	startToCloseTimeoutSeconds int32, attempt int64) *workflow.HistoryEvent {
	event := b.newDecisionTaskScheduledEvent(taskList, startToCloseTimeoutSeconds, attempt)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTransientDecisionTaskScheduledEvent(taskList string,
	startToCloseTimeoutSeconds int32, attempt int64, timestamp int64) *workflow.HistoryEvent {
	event := b.newTransientDecisionTaskScheduledEvent(taskList, startToCloseTimeoutSeconds, attempt, timestamp)

	return b.addTransientEvent(event)
}

func (b *historyBuilder) AddDecisionTaskStartedEvent(scheduleEventID int64, requestID string,
	identity string) *workflow.HistoryEvent {
	event := b.newDecisionTaskStartedEvent(scheduleEventID, requestID, identity)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTransientDecisionTaskStartedEvent(scheduleEventID int64, requestID string,
	identity string, timestamp int64) *workflow.HistoryEvent {
	event := b.newTransientDecisionTaskStartedEvent(scheduleEventID, requestID, identity, timestamp)

	return b.addTransientEvent(event)
}

func (b *historyBuilder) AddDecisionTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondDecisionTaskCompletedRequest) *workflow.HistoryEvent {
	event := b.newDecisionTaskCompletedEvent(scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskTimedOutEvent(scheduleEventID int64,
	startedEventID int64, timeoutType workflow.TimeoutType) *workflow.HistoryEvent {
	event := b.newDecisionTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutType)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskFailedEvent(attr workflow.DecisionTaskFailedEventAttributes) *workflow.HistoryEvent {
	event := b.newDecisionTaskFailedEvent(attr)
	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskScheduledEvent(decisionCompletedEventID int64,
	attributes *workflow.ScheduleActivityTaskDecisionAttributes) *workflow.HistoryEvent {
	event := b.newActivityTaskScheduledEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskStartedEvent(scheduleEventID int64, attempt int32, requestID string,
	identity string) *workflow.HistoryEvent {
	event := b.newActivityTaskStartedEvent(scheduleEventID, attempt, requestID, identity)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondActivityTaskCompletedRequest) *workflow.HistoryEvent {
	event := b.newActivityTaskCompletedEvent(scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskFailedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondActivityTaskFailedRequest) *workflow.HistoryEvent {
	event := b.newActivityTaskFailedEvent(scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskTimedOutEvent(
	scheduleEventID,
	startedEventID int64,
	timeoutType workflow.TimeoutType,
	lastHeartBeatDetails []byte,
	lastFailureReason string,
	lastFailureDetail []byte,
) *workflow.HistoryEvent {
	event := b.newActivityTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutType, lastHeartBeatDetails,
		lastFailureReason, lastFailureDetail)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddCompletedWorkflowEvent(decisionCompletedEventID int64,
	attributes *workflow.CompleteWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.newCompleteWorkflowExecutionEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddFailWorkflowEvent(decisionCompletedEventID int64,
	attributes *workflow.FailWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.newFailWorkflowExecutionEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimeoutWorkflowEvent() *workflow.HistoryEvent {
	event := b.newTimeoutWorkflowExecutionEvent()

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionTerminatedEvent(
	reason string,
	details []byte,
	identity string,
) *workflow.HistoryEvent {
	event := b.newWorkflowExecutionTerminatedEvent(reason, details, identity)
	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddContinuedAsNewEvent(decisionCompletedEventID int64, newRunID string,
	attributes *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.newWorkflowExecutionContinuedAsNewEvent(decisionCompletedEventID, newRunID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerStartedEvent(decisionCompletedEventID int64,
	request *workflow.StartTimerDecisionAttributes) *workflow.HistoryEvent {

	attributes := &workflow.TimerStartedEventAttributes{}
	attributes.TimerId = common.StringPtr(*request.TimerId)
	attributes.StartToFireTimeoutSeconds = common.Int64Ptr(*request.StartToFireTimeoutSeconds)
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionCompletedEventID)

	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeTimerStarted)
	event.TimerStartedEventAttributes = attributes

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerFiredEvent(
	startedEventID int64,
	timerID string,
) *workflow.HistoryEvent {

	attributes := &workflow.TimerFiredEventAttributes{}
	attributes.TimerId = common.StringPtr(timerID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)

	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeTimerFired)
	event.TimerFiredEventAttributes = attributes

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCancelRequestedEvent(decisionCompletedEventID int64,
	activityID string) *workflow.HistoryEvent {

	attributes := &workflow.ActivityTaskCancelRequestedEventAttributes{}
	attributes.ActivityId = common.StringPtr(activityID)
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionCompletedEventID)

	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeActivityTaskCancelRequested)
	event.ActivityTaskCancelRequestedEventAttributes = attributes

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddRequestCancelActivityTaskFailedEvent(decisionCompletedEventID int64,
	activityID string, cause string) *workflow.HistoryEvent {

	attributes := &workflow.RequestCancelActivityTaskFailedEventAttributes{}
	attributes.ActivityId = common.StringPtr(activityID)
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionCompletedEventID)
	attributes.Cause = common.StringPtr(cause)

	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeRequestCancelActivityTaskFailed)
	event.RequestCancelActivityTaskFailedEventAttributes = attributes

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCanceledEvent(scheduleEventID, startedEventID int64,
	latestCancelRequestedEventID int64, details []byte, identity string) *workflow.HistoryEvent {

	attributes := &workflow.ActivityTaskCanceledEventAttributes{}
	attributes.ScheduledEventId = common.Int64Ptr(scheduleEventID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.LatestCancelRequestedEventId = common.Int64Ptr(latestCancelRequestedEventID)
	attributes.Details = details
	attributes.Identity = common.StringPtr(identity)

	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeActivityTaskCanceled)
	event.ActivityTaskCanceledEventAttributes = attributes

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerCanceledEvent(startedEventID int64,
	decisionTaskCompletedEventID int64, timerID string, identity string) *workflow.HistoryEvent {

	attributes := &workflow.TimerCanceledEventAttributes{}
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.TimerId = common.StringPtr(timerID)
	attributes.Identity = common.StringPtr(identity)

	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeTimerCanceled)
	event.TimerCanceledEventAttributes = attributes

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddCancelTimerFailedEvent(timerID string, decisionTaskCompletedEventID int64,
	cause string, identity string) *workflow.HistoryEvent {

	attributes := &workflow.CancelTimerFailedEventAttributes{}
	attributes.TimerId = common.StringPtr(timerID)
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.Cause = common.StringPtr(cause)
	attributes.Identity = common.StringPtr(identity)

	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeCancelTimerFailed)
	event.CancelTimerFailedEventAttributes = attributes

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionCancelRequestedEvent(cause string,
	request *h.RequestCancelWorkflowExecutionRequest) *workflow.HistoryEvent {
	event := b.newWorkflowExecutionCancelRequestedEvent(cause, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID int64,
	attributes *workflow.CancelWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.newWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	request *workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.newRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	domain, workflowID, runID string, cause workflow.CancelExternalWorkflowExecutionFailedCause) *workflow.HistoryEvent {
	event := b.newRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID,
		domain, workflowID, runID, cause)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddExternalWorkflowExecutionCancelRequested(initiatedEventID int64,
	domain, workflowID, runID string) *workflow.HistoryEvent {
	event := b.newExternalWorkflowExecutionCancelRequestedEvent(initiatedEventID,
		domain, workflowID, runID)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddSignalExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	attributes *workflow.SignalExternalWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.newSignalExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddUpsertWorkflowSearchAttributesEvent(
	decisionTaskCompletedEventID int64,
	attributes *workflow.UpsertWorkflowSearchAttributesDecisionAttributes) *workflow.HistoryEvent {
	event := b.newUpsertWorkflowSearchAttributesEvent(decisionTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	domain, workflowID, runID string, control []byte, cause workflow.SignalExternalWorkflowExecutionFailedCause) *workflow.HistoryEvent {
	event := b.newSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID,
		domain, workflowID, runID, control, cause)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddExternalWorkflowExecutionSignaled(initiatedEventID int64,
	domain, workflowID, runID string, control []byte) *workflow.HistoryEvent {
	event := b.newExternalWorkflowExecutionSignaledEvent(initiatedEventID,
		domain, workflowID, runID, control)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddMarkerRecordedEvent(decisionCompletedEventID int64,
	attributes *workflow.RecordMarkerDecisionAttributes) *workflow.HistoryEvent {
	event := b.newMarkerRecordedEventAttributes(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionSignaledEvent(
	signalName string, input []byte, identity string) *workflow.HistoryEvent {
	event := b.newWorkflowExecutionSignaledEvent(signalName, input, identity)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddStartChildWorkflowExecutionInitiatedEvent(decisionCompletedEventID int64,
	attributes *workflow.StartChildWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.newStartChildWorkflowExecutionInitiatedEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionStartedEvent(
	domain *string,
	execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType,
	initiatedID int64,
	header *workflow.Header,
) *workflow.HistoryEvent {
	event := b.newChildWorkflowExecutionStartedEvent(domain, execution, workflowType, initiatedID, header)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddStartChildWorkflowExecutionFailedEvent(initiatedID int64,
	cause workflow.ChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *workflow.StartChildWorkflowExecutionInitiatedEventAttributes) *workflow.HistoryEvent {
	event := b.newStartChildWorkflowExecutionFailedEvent(initiatedID, cause, initiatedEventAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionCompletedEvent(domain *string, execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType, initiatedID, startedID int64,
	completedAttributes *workflow.WorkflowExecutionCompletedEventAttributes) *workflow.HistoryEvent {
	event := b.newChildWorkflowExecutionCompletedEvent(domain, execution, workflowType, initiatedID, startedID,
		completedAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionFailedEvent(domain *string, execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType, initiatedID, startedID int64,
	failedAttributes *workflow.WorkflowExecutionFailedEventAttributes) *workflow.HistoryEvent {
	event := b.newChildWorkflowExecutionFailedEvent(domain, execution, workflowType, initiatedID, startedID,
		failedAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionCanceledEvent(domain *string, execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType, initiatedID, startedID int64,
	canceledAttributes *workflow.WorkflowExecutionCanceledEventAttributes) *workflow.HistoryEvent {
	event := b.newChildWorkflowExecutionCanceledEvent(domain, execution, workflowType, initiatedID, startedID,
		canceledAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionTerminatedEvent(domain *string, execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType, initiatedID, startedID int64,
	terminatedAttributes *workflow.WorkflowExecutionTerminatedEventAttributes) *workflow.HistoryEvent {
	event := b.newChildWorkflowExecutionTerminatedEvent(domain, execution, workflowType, initiatedID, startedID,
		terminatedAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionTimedOutEvent(domain *string, execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType, initiatedID, startedID int64,
	timedOutAttributes *workflow.WorkflowExecutionTimedOutEventAttributes) *workflow.HistoryEvent {
	event := b.newChildWorkflowExecutionTimedOutEvent(domain, execution, workflowType, initiatedID, startedID,
		timedOutAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) addEventToHistory(event *workflow.HistoryEvent) *workflow.HistoryEvent {
	b.history = append(b.history, event)
	return event
}

func (b *historyBuilder) addTransientEvent(event *workflow.HistoryEvent) *workflow.HistoryEvent {
	b.transientHistory = append(b.transientHistory, event)
	return event
}

func (b *historyBuilder) newWorkflowExecutionStartedEvent(
	startRequest *h.StartWorkflowExecutionRequest, previousExecution *persistence.WorkflowExecutionInfo, firstRunID, originalRunID string) *workflow.HistoryEvent {
	var prevRunID *string
	var resetPoints *workflow.ResetPoints
	if previousExecution != nil {
		prevRunID = common.StringPtr(previousExecution.RunID)
		resetPoints = previousExecution.AutoResetPoints
	}
	request := startRequest.StartRequest
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeWorkflowExecutionStarted)
	attributes := &workflow.WorkflowExecutionStartedEventAttributes{}
	attributes.WorkflowType = request.WorkflowType
	attributes.TaskList = request.TaskList
	attributes.Header = request.Header
	attributes.Input = request.Input
	attributes.ExecutionStartToCloseTimeoutSeconds = common.Int32Ptr(*request.ExecutionStartToCloseTimeoutSeconds)
	attributes.TaskStartToCloseTimeoutSeconds = common.Int32Ptr(*request.TaskStartToCloseTimeoutSeconds)
	attributes.ContinuedExecutionRunId = prevRunID
	attributes.PrevAutoResetPoints = resetPoints
	attributes.Identity = common.StringPtr(common.StringDefault(request.Identity))
	attributes.RetryPolicy = request.RetryPolicy
	attributes.Attempt = common.Int32Ptr(startRequest.GetAttempt())
	attributes.ExpirationTimestamp = startRequest.ExpirationTimestamp
	attributes.CronSchedule = request.CronSchedule
	attributes.LastCompletionResult = startRequest.LastCompletionResult
	attributes.ContinuedFailureReason = startRequest.ContinuedFailureReason
	attributes.ContinuedFailureDetails = startRequest.ContinuedFailureDetails
	attributes.Initiator = startRequest.ContinueAsNewInitiator
	attributes.FirstDecisionTaskBackoffSeconds = startRequest.FirstDecisionTaskBackoffSeconds
	attributes.FirstExecutionRunId = common.StringPtr(firstRunID)
	attributes.OriginalExecutionRunId = common.StringPtr(originalRunID)
	attributes.Memo = request.Memo
	attributes.SearchAttributes = request.SearchAttributes

	parentInfo := startRequest.ParentExecutionInfo
	if parentInfo != nil {
		attributes.ParentWorkflowDomain = parentInfo.Domain
		attributes.ParentWorkflowExecution = parentInfo.Execution
		attributes.ParentInitiatedEventId = parentInfo.InitiatedId
	}
	historyEvent.WorkflowExecutionStartedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskScheduledEvent(taskList string, startToCloseTimeoutSeconds int32,
	attempt int64) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeDecisionTaskScheduled)

	return setDecisionTaskScheduledEventInfo(historyEvent, taskList, startToCloseTimeoutSeconds, attempt)
}

func (b *historyBuilder) newTransientDecisionTaskScheduledEvent(taskList string, startToCloseTimeoutSeconds int32,
	attempt int64, timestamp int64) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEventWithTimestamp(workflow.EventTypeDecisionTaskScheduled, timestamp)

	return setDecisionTaskScheduledEventInfo(historyEvent, taskList, startToCloseTimeoutSeconds, attempt)
}

func (b *historyBuilder) newDecisionTaskStartedEvent(scheduledEventID int64, requestID string,
	identity string) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeDecisionTaskStarted)

	return setDecisionTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func (b *historyBuilder) newTransientDecisionTaskStartedEvent(scheduledEventID int64, requestID string,
	identity string, timestamp int64) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEventWithTimestamp(workflow.EventTypeDecisionTaskStarted, timestamp)

	return setDecisionTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func (b *historyBuilder) newDecisionTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondDecisionTaskCompletedRequest) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeDecisionTaskCompleted)
	attributes := &workflow.DecisionTaskCompletedEventAttributes{}
	attributes.ExecutionContext = request.ExecutionContext
	attributes.ScheduledEventId = common.Int64Ptr(scheduleEventID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.Identity = common.StringPtr(common.StringDefault(request.Identity))
	attributes.BinaryChecksum = request.BinaryChecksum
	historyEvent.DecisionTaskCompletedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskTimedOutEvent(scheduleEventID int64, startedEventID int64, timeoutType workflow.TimeoutType) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeDecisionTaskTimedOut)
	attributes := &workflow.DecisionTaskTimedOutEventAttributes{}
	attributes.ScheduledEventId = common.Int64Ptr(scheduleEventID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.TimeoutType = common.TimeoutTypePtr(timeoutType)
	historyEvent.DecisionTaskTimedOutEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskFailedEvent(attr workflow.DecisionTaskFailedEventAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeDecisionTaskFailed)
	historyEvent.DecisionTaskFailedEventAttributes = &attr
	return historyEvent
}

func (b *historyBuilder) newActivityTaskScheduledEvent(decisionTaskCompletedEventID int64,
	scheduleAttributes *workflow.ScheduleActivityTaskDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeActivityTaskScheduled)
	attributes := &workflow.ActivityTaskScheduledEventAttributes{}
	attributes.ActivityId = common.StringPtr(common.StringDefault(scheduleAttributes.ActivityId))
	attributes.ActivityType = scheduleAttributes.ActivityType
	attributes.TaskList = scheduleAttributes.TaskList
	attributes.Header = scheduleAttributes.Header
	attributes.Input = scheduleAttributes.Input
	attributes.ScheduleToCloseTimeoutSeconds = common.Int32Ptr(common.Int32Default(scheduleAttributes.ScheduleToCloseTimeoutSeconds))
	attributes.ScheduleToStartTimeoutSeconds = common.Int32Ptr(common.Int32Default(scheduleAttributes.ScheduleToStartTimeoutSeconds))
	attributes.StartToCloseTimeoutSeconds = common.Int32Ptr(common.Int32Default(scheduleAttributes.StartToCloseTimeoutSeconds))
	attributes.HeartbeatTimeoutSeconds = common.Int32Ptr(common.Int32Default(scheduleAttributes.HeartbeatTimeoutSeconds))
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.RetryPolicy = scheduleAttributes.RetryPolicy
	historyEvent.ActivityTaskScheduledEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newActivityTaskStartedEvent(scheduledEventID int64, attempt int32, requestID string,
	identity string) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeActivityTaskStarted)
	attributes := &workflow.ActivityTaskStartedEventAttributes{}
	attributes.ScheduledEventId = common.Int64Ptr(scheduledEventID)
	attributes.Attempt = common.Int32Ptr(attempt)
	attributes.Identity = common.StringPtr(identity)
	attributes.RequestId = common.StringPtr(requestID)
	historyEvent.ActivityTaskStartedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newActivityTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondActivityTaskCompletedRequest) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeActivityTaskCompleted)
	attributes := &workflow.ActivityTaskCompletedEventAttributes{}
	attributes.Result = request.Result
	attributes.ScheduledEventId = common.Int64Ptr(scheduleEventID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.Identity = common.StringPtr(common.StringDefault(request.Identity))
	historyEvent.ActivityTaskCompletedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newActivityTaskTimedOutEvent(
	scheduleEventID, startedEventID int64,
	timeoutType workflow.TimeoutType,
	lastHeartBeatDetails []byte,
	lastFailureReason string,
	lastFailureDetail []byte,
) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeActivityTaskTimedOut)
	attributes := &workflow.ActivityTaskTimedOutEventAttributes{}
	attributes.ScheduledEventId = common.Int64Ptr(scheduleEventID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.TimeoutType = common.TimeoutTypePtr(timeoutType)
	attributes.Details = lastHeartBeatDetails
	attributes.LastFailureReason = common.StringPtr(lastFailureReason)
	attributes.LastFailureDetails = lastFailureDetail

	historyEvent.ActivityTaskTimedOutEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newActivityTaskFailedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondActivityTaskFailedRequest) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeActivityTaskFailed)
	attributes := &workflow.ActivityTaskFailedEventAttributes{}
	attributes.Reason = common.StringPtr(common.StringDefault(request.Reason))
	attributes.Details = request.Details
	attributes.ScheduledEventId = common.Int64Ptr(scheduleEventID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.Identity = common.StringPtr(common.StringDefault(request.Identity))
	historyEvent.ActivityTaskFailedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newCompleteWorkflowExecutionEvent(decisionTaskCompletedEventID int64,
	request *workflow.CompleteWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeWorkflowExecutionCompleted)
	attributes := &workflow.WorkflowExecutionCompletedEventAttributes{}
	attributes.Result = request.Result
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	historyEvent.WorkflowExecutionCompletedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newFailWorkflowExecutionEvent(decisionTaskCompletedEventID int64,
	request *workflow.FailWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeWorkflowExecutionFailed)
	attributes := &workflow.WorkflowExecutionFailedEventAttributes{}
	attributes.Reason = common.StringPtr(common.StringDefault(request.Reason))
	attributes.Details = request.Details
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	historyEvent.WorkflowExecutionFailedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newTimeoutWorkflowExecutionEvent() *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeWorkflowExecutionTimedOut)
	attributes := &workflow.WorkflowExecutionTimedOutEventAttributes{}
	attributes.TimeoutType = common.TimeoutTypePtr(workflow.TimeoutTypeStartToClose)
	historyEvent.WorkflowExecutionTimedOutEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionSignaledEvent(
	signalName string, input []byte, identity string) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeWorkflowExecutionSignaled)
	attributes := &workflow.WorkflowExecutionSignaledEventAttributes{}
	attributes.SignalName = common.StringPtr(signalName)
	attributes.Input = input
	attributes.Identity = common.StringPtr(identity)
	historyEvent.WorkflowExecutionSignaledEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionTerminatedEvent(
	reason string, details []byte, identity string) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeWorkflowExecutionTerminated)
	attributes := &workflow.WorkflowExecutionTerminatedEventAttributes{}
	attributes.Reason = common.StringPtr(reason)
	attributes.Details = details
	attributes.Identity = common.StringPtr(identity)
	historyEvent.WorkflowExecutionTerminatedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newMarkerRecordedEventAttributes(decisionTaskCompletedEventID int64,
	request *workflow.RecordMarkerDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeMarkerRecorded)
	attributes := &workflow.MarkerRecordedEventAttributes{}
	attributes.MarkerName = common.StringPtr(common.StringDefault(request.MarkerName))
	attributes.Details = request.Details
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.Header = request.Header
	historyEvent.MarkerRecordedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionCancelRequestedEvent(cause string,
	request *h.RequestCancelWorkflowExecutionRequest) *workflow.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeWorkflowExecutionCancelRequested)
	attributes := &workflow.WorkflowExecutionCancelRequestedEventAttributes{}
	attributes.Cause = common.StringPtr(cause)
	attributes.Identity = common.StringPtr(common.StringDefault(request.CancelRequest.Identity))
	if request.ExternalInitiatedEventId != nil {
		attributes.ExternalInitiatedEventId = common.Int64Ptr(*request.ExternalInitiatedEventId)
	}
	if request.ExternalWorkflowExecution != nil {
		attributes.ExternalWorkflowExecution = request.ExternalWorkflowExecution
	}
	event.WorkflowExecutionCancelRequestedEventAttributes = attributes

	return event
}

func (b *historyBuilder) newWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID int64,
	request *workflow.CancelWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeWorkflowExecutionCanceled)
	attributes := &workflow.WorkflowExecutionCanceledEventAttributes{}
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.Details = request.Details
	event.WorkflowExecutionCanceledEventAttributes = attributes

	return event
}

func (b *historyBuilder) newRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	request *workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeRequestCancelExternalWorkflowExecutionInitiated)
	attributes := &workflow.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.Domain = request.Domain
	attributes.WorkflowExecution = &workflow.WorkflowExecution{
		WorkflowId: request.WorkflowId,
		RunId:      request.RunId,
	}
	attributes.Control = request.Control
	attributes.ChildWorkflowOnly = request.ChildWorkflowOnly
	event.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes = attributes

	return event
}

func (b *historyBuilder) newRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	domain, workflowID, runID string, cause workflow.CancelExternalWorkflowExecutionFailedCause) *workflow.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeRequestCancelExternalWorkflowExecutionFailed)
	attributes := &workflow.RequestCancelExternalWorkflowExecutionFailedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.InitiatedEventId = common.Int64Ptr(initiatedEventID)
	attributes.Domain = common.StringPtr(domain)
	attributes.WorkflowExecution = &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}
	attributes.Cause = common.CancelExternalWorkflowExecutionFailedCausePtr(cause)
	event.RequestCancelExternalWorkflowExecutionFailedEventAttributes = attributes

	return event
}

func (b *historyBuilder) newExternalWorkflowExecutionCancelRequestedEvent(initiatedEventID int64,
	domain, workflowID, runID string) *workflow.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeExternalWorkflowExecutionCancelRequested)
	attributes := &workflow.ExternalWorkflowExecutionCancelRequestedEventAttributes{}
	attributes.InitiatedEventId = common.Int64Ptr(initiatedEventID)
	attributes.Domain = common.StringPtr(domain)
	attributes.WorkflowExecution = &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}
	event.ExternalWorkflowExecutionCancelRequestedEventAttributes = attributes

	return event
}

func (b *historyBuilder) newSignalExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	request *workflow.SignalExternalWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeSignalExternalWorkflowExecutionInitiated)
	attributes := &workflow.SignalExternalWorkflowExecutionInitiatedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.Domain = request.Domain
	attributes.WorkflowExecution = &workflow.WorkflowExecution{
		WorkflowId: request.Execution.WorkflowId,
		RunId:      request.Execution.RunId,
	}
	attributes.SignalName = common.StringPtr(request.GetSignalName())
	attributes.Input = request.Input
	attributes.Control = request.Control
	attributes.ChildWorkflowOnly = request.ChildWorkflowOnly
	event.SignalExternalWorkflowExecutionInitiatedEventAttributes = attributes

	return event
}

func (b *historyBuilder) newUpsertWorkflowSearchAttributesEvent(decisionTaskCompletedEventID int64,
	request *workflow.UpsertWorkflowSearchAttributesDecisionAttributes) *workflow.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeUpsertWorkflowSearchAttributes)
	attributes := &workflow.UpsertWorkflowSearchAttributesEventAttributes{}
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.SearchAttributes = request.GetSearchAttributes()
	event.UpsertWorkflowSearchAttributesEventAttributes = attributes

	return event
}

func (b *historyBuilder) newSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	domain, workflowID, runID string, control []byte, cause workflow.SignalExternalWorkflowExecutionFailedCause) *workflow.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeSignalExternalWorkflowExecutionFailed)
	attributes := &workflow.SignalExternalWorkflowExecutionFailedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.InitiatedEventId = common.Int64Ptr(initiatedEventID)
	attributes.Domain = common.StringPtr(domain)
	attributes.WorkflowExecution = &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}
	attributes.Cause = common.SignalExternalWorkflowExecutionFailedCausePtr(cause)
	attributes.Control = control
	event.SignalExternalWorkflowExecutionFailedEventAttributes = attributes

	return event
}

func (b *historyBuilder) newExternalWorkflowExecutionSignaledEvent(initiatedEventID int64,
	domain, workflowID, runID string, control []byte) *workflow.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeExternalWorkflowExecutionSignaled)
	attributes := &workflow.ExternalWorkflowExecutionSignaledEventAttributes{}
	attributes.InitiatedEventId = common.Int64Ptr(initiatedEventID)
	attributes.Domain = common.StringPtr(domain)
	attributes.WorkflowExecution = &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}
	attributes.Control = control
	event.ExternalWorkflowExecutionSignaledEventAttributes = attributes

	return event
}

func (b *historyBuilder) newWorkflowExecutionContinuedAsNewEvent(decisionTaskCompletedEventID int64,
	newRunID string, request *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeWorkflowExecutionContinuedAsNew)
	attributes := &workflow.WorkflowExecutionContinuedAsNewEventAttributes{}
	attributes.NewExecutionRunId = common.StringPtr(newRunID)
	attributes.WorkflowType = request.WorkflowType
	attributes.TaskList = request.TaskList
	attributes.Header = request.Header
	attributes.Input = request.Input
	attributes.ExecutionStartToCloseTimeoutSeconds = common.Int32Ptr(*request.ExecutionStartToCloseTimeoutSeconds)
	attributes.TaskStartToCloseTimeoutSeconds = common.Int32Ptr(*request.TaskStartToCloseTimeoutSeconds)
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.BackoffStartIntervalInSeconds = common.Int32Ptr(request.GetBackoffStartIntervalInSeconds())
	attributes.Initiator = request.Initiator
	if attributes.Initiator == nil {
		attributes.Initiator = workflow.ContinueAsNewInitiatorDecider.Ptr()
	}
	attributes.FailureReason = request.FailureReason
	attributes.FailureDetails = request.FailureDetails
	attributes.LastCompletionResult = request.LastCompletionResult
	attributes.Memo = request.Memo
	attributes.SearchAttributes = request.SearchAttributes
	historyEvent.WorkflowExecutionContinuedAsNewEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newStartChildWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	startAttributes *workflow.StartChildWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeStartChildWorkflowExecutionInitiated)
	attributes := &workflow.StartChildWorkflowExecutionInitiatedEventAttributes{}
	attributes.Domain = startAttributes.Domain
	attributes.WorkflowId = startAttributes.WorkflowId
	attributes.WorkflowType = startAttributes.WorkflowType
	attributes.TaskList = startAttributes.TaskList
	attributes.Header = startAttributes.Header
	attributes.Input = startAttributes.Input
	attributes.ExecutionStartToCloseTimeoutSeconds = startAttributes.ExecutionStartToCloseTimeoutSeconds
	attributes.TaskStartToCloseTimeoutSeconds = startAttributes.TaskStartToCloseTimeoutSeconds
	attributes.Control = startAttributes.Control
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.WorkflowIdReusePolicy = startAttributes.WorkflowIdReusePolicy
	attributes.RetryPolicy = startAttributes.RetryPolicy
	attributes.CronSchedule = startAttributes.CronSchedule
	attributes.Memo = startAttributes.Memo
	attributes.SearchAttributes = startAttributes.SearchAttributes
	attributes.ParentClosePolicy = common.ParentClosePolicyPtr(startAttributes.GetParentClosePolicy())
	historyEvent.StartChildWorkflowExecutionInitiatedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionStartedEvent(
	domain *string,
	execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType,
	initiatedID int64,
	header *workflow.Header,
) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeChildWorkflowExecutionStarted)
	attributes := &workflow.ChildWorkflowExecutionStartedEventAttributes{}
	attributes.Domain = domain
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = common.Int64Ptr(initiatedID)
	attributes.Header = header
	historyEvent.ChildWorkflowExecutionStartedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newStartChildWorkflowExecutionFailedEvent(initiatedID int64,
	cause workflow.ChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *workflow.StartChildWorkflowExecutionInitiatedEventAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeStartChildWorkflowExecutionFailed)
	attributes := &workflow.StartChildWorkflowExecutionFailedEventAttributes{}
	attributes.Domain = common.StringPtr(*initiatedEventAttributes.Domain)
	attributes.WorkflowId = common.StringPtr(*initiatedEventAttributes.WorkflowId)
	attributes.WorkflowType = initiatedEventAttributes.WorkflowType
	attributes.InitiatedEventId = common.Int64Ptr(initiatedID)
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(*initiatedEventAttributes.DecisionTaskCompletedEventId)
	attributes.Control = initiatedEventAttributes.Control
	attributes.Cause = common.ChildWorkflowExecutionFailedCausePtr(cause)
	historyEvent.StartChildWorkflowExecutionFailedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionCompletedEvent(domain *string, execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType, initiatedID, startedID int64,
	completedAttributes *workflow.WorkflowExecutionCompletedEventAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeChildWorkflowExecutionCompleted)
	attributes := &workflow.ChildWorkflowExecutionCompletedEventAttributes{}
	attributes.Domain = domain
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = common.Int64Ptr(initiatedID)
	attributes.StartedEventId = common.Int64Ptr(startedID)
	attributes.Result = completedAttributes.Result
	historyEvent.ChildWorkflowExecutionCompletedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionFailedEvent(domain *string, execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType, initiatedID, startedID int64,
	failedAttributes *workflow.WorkflowExecutionFailedEventAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeChildWorkflowExecutionFailed)
	attributes := &workflow.ChildWorkflowExecutionFailedEventAttributes{}
	attributes.Domain = domain
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = common.Int64Ptr(initiatedID)
	attributes.StartedEventId = common.Int64Ptr(startedID)
	attributes.Reason = common.StringPtr(common.StringDefault(failedAttributes.Reason))
	attributes.Details = failedAttributes.Details
	historyEvent.ChildWorkflowExecutionFailedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionCanceledEvent(domain *string, execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType, initiatedID, startedID int64,
	canceledAttributes *workflow.WorkflowExecutionCanceledEventAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeChildWorkflowExecutionCanceled)
	attributes := &workflow.ChildWorkflowExecutionCanceledEventAttributes{}
	attributes.Domain = domain
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = common.Int64Ptr(initiatedID)
	attributes.StartedEventId = common.Int64Ptr(startedID)
	attributes.Details = canceledAttributes.Details
	historyEvent.ChildWorkflowExecutionCanceledEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionTerminatedEvent(domain *string, execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType, initiatedID, startedID int64,
	terminatedAttributes *workflow.WorkflowExecutionTerminatedEventAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeChildWorkflowExecutionTerminated)
	attributes := &workflow.ChildWorkflowExecutionTerminatedEventAttributes{}
	attributes.Domain = domain
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = common.Int64Ptr(initiatedID)
	attributes.StartedEventId = common.Int64Ptr(startedID)
	historyEvent.ChildWorkflowExecutionTerminatedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionTimedOutEvent(domain *string, execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType, initiatedID, startedID int64,
	timedOutAttributes *workflow.WorkflowExecutionTimedOutEventAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeChildWorkflowExecutionTimedOut)
	attributes := &workflow.ChildWorkflowExecutionTimedOutEventAttributes{}
	attributes.Domain = domain
	attributes.TimeoutType = timedOutAttributes.TimeoutType
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = common.Int64Ptr(initiatedID)
	attributes.StartedEventId = common.Int64Ptr(startedID)
	historyEvent.ChildWorkflowExecutionTimedOutEventAttributes = attributes

	return historyEvent
}

func newDecisionTaskScheduledEventWithInfo(eventID, timestamp int64, taskList string, startToCloseTimeoutSeconds int32,
	attempt int64) *workflow.HistoryEvent {
	historyEvent := createNewHistoryEvent(eventID, workflow.EventTypeDecisionTaskScheduled, timestamp)

	return setDecisionTaskScheduledEventInfo(historyEvent, taskList, startToCloseTimeoutSeconds, attempt)
}

func newDecisionTaskStartedEventWithInfo(eventID, timestamp int64, scheduledEventID int64, requestID string,
	identity string) *workflow.HistoryEvent {
	historyEvent := createNewHistoryEvent(eventID, workflow.EventTypeDecisionTaskStarted, timestamp)

	return setDecisionTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func createNewHistoryEvent(eventID int64, eventType workflow.EventType, timestamp int64) *workflow.HistoryEvent {
	historyEvent := &workflow.HistoryEvent{}
	historyEvent.EventId = common.Int64Ptr(eventID)
	historyEvent.Timestamp = common.Int64Ptr(timestamp)
	historyEvent.EventType = common.EventTypePtr(eventType)

	return historyEvent
}

func setDecisionTaskScheduledEventInfo(historyEvent *workflow.HistoryEvent, taskList string,
	startToCloseTimeoutSeconds int32, attempt int64) *workflow.HistoryEvent {
	attributes := &workflow.DecisionTaskScheduledEventAttributes{}
	attributes.TaskList = &workflow.TaskList{}
	attributes.TaskList.Name = common.StringPtr(taskList)
	attributes.StartToCloseTimeoutSeconds = common.Int32Ptr(startToCloseTimeoutSeconds)
	attributes.Attempt = common.Int64Ptr(attempt)
	historyEvent.DecisionTaskScheduledEventAttributes = attributes

	return historyEvent
}

func setDecisionTaskStartedEventInfo(historyEvent *workflow.HistoryEvent, scheduledEventID int64, requestID string,
	identity string) *workflow.HistoryEvent {
	attributes := &workflow.DecisionTaskStartedEventAttributes{}
	attributes.ScheduledEventId = common.Int64Ptr(scheduledEventID)
	attributes.Identity = common.StringPtr(identity)
	attributes.RequestId = common.StringPtr(requestID)
	historyEvent.DecisionTaskStartedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) GetHistory() *workflow.History {
	history := workflow.History{Events: b.history}
	return &history
}
