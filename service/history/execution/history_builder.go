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

package execution

import (
	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
)

type (
	// HistoryBuilder builds and stores workflow history events
	HistoryBuilder struct {
		transientHistory []*workflow.HistoryEvent
		history          []*workflow.HistoryEvent
		msBuilder        MutableState
	}
)

// NewHistoryBuilder creates a new history builder
func NewHistoryBuilder(msBuilder MutableState, logger log.Logger) *HistoryBuilder {
	return &HistoryBuilder{
		transientHistory: []*workflow.HistoryEvent{},
		history:          []*workflow.HistoryEvent{},
		msBuilder:        msBuilder,
	}
}

// NewHistoryBuilderFromEvents creates a new history builder based on the given workflow history events
func NewHistoryBuilderFromEvents(history []*workflow.HistoryEvent, logger log.Logger) *HistoryBuilder {
	return &HistoryBuilder{
		history: history,
	}
}

// GetFirstEvent gets the first event in workflow history
// it returns the first transient history event if exists
func (b *HistoryBuilder) GetFirstEvent() *workflow.HistoryEvent {
	// Transient decision events are always written before other events
	if b.transientHistory != nil && len(b.transientHistory) > 0 {
		return b.transientHistory[0]
	}

	if b.history != nil && len(b.history) > 0 {
		return b.history[0]
	}

	return nil
}

// HasTransientEvents returns true if there are transient history events
func (b *HistoryBuilder) HasTransientEvents() bool {
	return b.transientHistory != nil && len(b.transientHistory) > 0
}

// AddWorkflowExecutionStartedEvent adds WorkflowExecutionStarted event to history
// originalRunID is the runID when the WorkflowExecutionStarted event is written
// firstRunID is the very first runID along the chain of ContinueAsNew and Reset
func (b *HistoryBuilder) AddWorkflowExecutionStartedEvent(request *h.StartWorkflowExecutionRequest,
	previousExecution *persistence.WorkflowExecutionInfo, firstRunID, originalRunID string) *workflow.HistoryEvent {
	event := b.newWorkflowExecutionStartedEvent(request, previousExecution, firstRunID, originalRunID)

	return b.addEventToHistory(event)
}

// AddDecisionTaskScheduledEvent adds DecisionTaskScheduled event to history
func (b *HistoryBuilder) AddDecisionTaskScheduledEvent(taskList string,
	startToCloseTimeoutSeconds int32, attempt int64) *workflow.HistoryEvent {
	event := b.newDecisionTaskScheduledEvent(taskList, startToCloseTimeoutSeconds, attempt)

	return b.addEventToHistory(event)
}

// AddTransientDecisionTaskScheduledEvent adds transient DecisionTaskScheduled event
func (b *HistoryBuilder) AddTransientDecisionTaskScheduledEvent(taskList string,
	startToCloseTimeoutSeconds int32, attempt int64, timestamp int64) *workflow.HistoryEvent {
	event := b.newTransientDecisionTaskScheduledEvent(taskList, startToCloseTimeoutSeconds, attempt, timestamp)

	return b.addTransientEvent(event)
}

// AddDecisionTaskStartedEvent adds DecisionTaskStarted event to history
func (b *HistoryBuilder) AddDecisionTaskStartedEvent(scheduleEventID int64, requestID string,
	identity string) *workflow.HistoryEvent {
	event := b.newDecisionTaskStartedEvent(scheduleEventID, requestID, identity)

	return b.addEventToHistory(event)
}

// AddTransientDecisionTaskStartedEvent adds transient DecisionTaskStarted event
func (b *HistoryBuilder) AddTransientDecisionTaskStartedEvent(scheduleEventID int64, requestID string,
	identity string, timestamp int64) *workflow.HistoryEvent {
	event := b.newTransientDecisionTaskStartedEvent(scheduleEventID, requestID, identity, timestamp)

	return b.addTransientEvent(event)
}

// AddDecisionTaskCompletedEvent adds DecisionTaskCompleted event to history
func (b *HistoryBuilder) AddDecisionTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondDecisionTaskCompletedRequest) *workflow.HistoryEvent {
	event := b.newDecisionTaskCompletedEvent(scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

// AddDecisionTaskTimedOutEvent adds DecisionTaskTimedOut event to history
func (b *HistoryBuilder) AddDecisionTaskTimedOutEvent(scheduleEventID int64,
	startedEventID int64, timeoutType workflow.TimeoutType) *workflow.HistoryEvent {
	event := b.newDecisionTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutType)

	return b.addEventToHistory(event)
}

// AddDecisionTaskFailedEvent adds DecisionTaskFailed event to history
func (b *HistoryBuilder) AddDecisionTaskFailedEvent(attr workflow.DecisionTaskFailedEventAttributes) *workflow.HistoryEvent {
	event := b.newDecisionTaskFailedEvent(attr)
	return b.addEventToHistory(event)
}

// AddActivityTaskScheduledEvent adds ActivityTaskScheduled event to history
func (b *HistoryBuilder) AddActivityTaskScheduledEvent(decisionCompletedEventID int64,
	attributes *workflow.ScheduleActivityTaskDecisionAttributes) *workflow.HistoryEvent {
	event := b.newActivityTaskScheduledEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

// AddActivityTaskStartedEvent adds ActivityTaskStarted event to history
func (b *HistoryBuilder) AddActivityTaskStartedEvent(
	scheduleEventID int64,
	attempt int32,
	requestID string,
	identity string,
	lastFailureReason string,
	lastFailureDetails []byte,
) *workflow.HistoryEvent {
	event := b.newActivityTaskStartedEvent(scheduleEventID, attempt, requestID, identity, lastFailureReason,
		lastFailureDetails)

	return b.addEventToHistory(event)
}

// AddActivityTaskCompletedEvent adds ActivityTaskCompleted event to history
func (b *HistoryBuilder) AddActivityTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondActivityTaskCompletedRequest) *workflow.HistoryEvent {
	event := b.newActivityTaskCompletedEvent(scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

// AddActivityTaskFailedEvent adds ActivityTaskFailed event to history
func (b *HistoryBuilder) AddActivityTaskFailedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondActivityTaskFailedRequest) *workflow.HistoryEvent {
	event := b.newActivityTaskFailedEvent(scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

// AddActivityTaskTimedOutEvent adds ActivityTaskTimedOut event to history
func (b *HistoryBuilder) AddActivityTaskTimedOutEvent(
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

// AddCompletedWorkflowEvent adds WorkflowExecutionCompleted event to history
func (b *HistoryBuilder) AddCompletedWorkflowEvent(decisionCompletedEventID int64,
	attributes *workflow.CompleteWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.newCompleteWorkflowExecutionEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

// AddFailWorkflowEvent adds WorkflowExecutionFailed event to history
func (b *HistoryBuilder) AddFailWorkflowEvent(decisionCompletedEventID int64,
	attributes *workflow.FailWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.newFailWorkflowExecutionEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

// AddTimeoutWorkflowEvent adds WorkflowExecutionTimedout event to history
func (b *HistoryBuilder) AddTimeoutWorkflowEvent() *workflow.HistoryEvent {
	event := b.newTimeoutWorkflowExecutionEvent()

	return b.addEventToHistory(event)
}

// AddWorkflowExecutionTerminatedEvent add WorkflowExecutionTerminated event to history
func (b *HistoryBuilder) AddWorkflowExecutionTerminatedEvent(
	reason string,
	details []byte,
	identity string,
) *workflow.HistoryEvent {
	event := b.newWorkflowExecutionTerminatedEvent(reason, details, identity)
	return b.addEventToHistory(event)
}

// AddContinuedAsNewEvent adds WorkflowExecutionContinuedAsNew event to history
func (b *HistoryBuilder) AddContinuedAsNewEvent(decisionCompletedEventID int64, newRunID string,
	attributes *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.newWorkflowExecutionContinuedAsNewEvent(decisionCompletedEventID, newRunID, attributes)

	return b.addEventToHistory(event)
}

// AddTimerStartedEvent adds TimerStart event to history
func (b *HistoryBuilder) AddTimerStartedEvent(decisionCompletedEventID int64,
	request *workflow.StartTimerDecisionAttributes) *workflow.HistoryEvent {

	attributes := &workflow.TimerStartedEventAttributes{}
	attributes.TimerId = common.StringPtr(*request.TimerId)
	attributes.StartToFireTimeoutSeconds = common.Int64Ptr(*request.StartToFireTimeoutSeconds)
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionCompletedEventID)

	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeTimerStarted)
	event.TimerStartedEventAttributes = attributes

	return b.addEventToHistory(event)
}

// AddTimerFiredEvent adds TimerFired event to history
func (b *HistoryBuilder) AddTimerFiredEvent(
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

// AddActivityTaskCancelRequestedEvent add ActivityTaskCancelRequested event to history
func (b *HistoryBuilder) AddActivityTaskCancelRequestedEvent(decisionCompletedEventID int64,
	activityID string) *workflow.HistoryEvent {

	attributes := &workflow.ActivityTaskCancelRequestedEventAttributes{}
	attributes.ActivityId = common.StringPtr(activityID)
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionCompletedEventID)

	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeActivityTaskCancelRequested)
	event.ActivityTaskCancelRequestedEventAttributes = attributes

	return b.addEventToHistory(event)
}

// AddRequestCancelActivityTaskFailedEvent add RequestCancelActivityTaskFailed event to history
func (b *HistoryBuilder) AddRequestCancelActivityTaskFailedEvent(decisionCompletedEventID int64,
	activityID string, cause string) *workflow.HistoryEvent {

	attributes := &workflow.RequestCancelActivityTaskFailedEventAttributes{}
	attributes.ActivityId = common.StringPtr(activityID)
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionCompletedEventID)
	attributes.Cause = common.StringPtr(cause)

	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeRequestCancelActivityTaskFailed)
	event.RequestCancelActivityTaskFailedEventAttributes = attributes

	return b.addEventToHistory(event)
}

// AddActivityTaskCanceledEvent adds ActivityTaskCanceled event to history
func (b *HistoryBuilder) AddActivityTaskCanceledEvent(scheduleEventID, startedEventID int64,
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

// AddTimerCanceledEvent adds TimerCanceled event to history
func (b *HistoryBuilder) AddTimerCanceledEvent(startedEventID int64,
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

// AddCancelTimerFailedEvent adds CancelTimerFailed event to history
func (b *HistoryBuilder) AddCancelTimerFailedEvent(timerID string, decisionTaskCompletedEventID int64,
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

// AddWorkflowExecutionCancelRequestedEvent adds WorkflowExecutionCancelRequested event to history
func (b *HistoryBuilder) AddWorkflowExecutionCancelRequestedEvent(cause string,
	request *h.RequestCancelWorkflowExecutionRequest) *workflow.HistoryEvent {
	event := b.newWorkflowExecutionCancelRequestedEvent(cause, request)

	return b.addEventToHistory(event)
}

// AddWorkflowExecutionCanceledEvent adds WorkflowExecutionCanceled event to history
func (b *HistoryBuilder) AddWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID int64,
	attributes *workflow.CancelWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.newWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

// AddRequestCancelExternalWorkflowExecutionInitiatedEvent adds RequestCancelExternalWorkflowExecutionInitiated event to history
func (b *HistoryBuilder) AddRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	request *workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.newRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID, request)

	return b.addEventToHistory(event)
}

// AddRequestCancelExternalWorkflowExecutionFailedEvent adds RequestCancelExternalWorkflowExecutionFailed event to history
func (b *HistoryBuilder) AddRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	domain, workflowID, runID string, cause workflow.CancelExternalWorkflowExecutionFailedCause) *workflow.HistoryEvent {
	event := b.newRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID,
		domain, workflowID, runID, cause)

	return b.addEventToHistory(event)
}

// AddExternalWorkflowExecutionCancelRequested adds ExternalWorkflowExecutionCancelRequested event to history
func (b *HistoryBuilder) AddExternalWorkflowExecutionCancelRequested(initiatedEventID int64,
	domain, workflowID, runID string) *workflow.HistoryEvent {
	event := b.newExternalWorkflowExecutionCancelRequestedEvent(initiatedEventID,
		domain, workflowID, runID)

	return b.addEventToHistory(event)
}

// AddSignalExternalWorkflowExecutionInitiatedEvent adds SignalExternalWorkflowExecutionInitiated event to history
func (b *HistoryBuilder) AddSignalExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	attributes *workflow.SignalExternalWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.newSignalExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

// AddUpsertWorkflowSearchAttributesEvent adds UpsertWorkflowSearchAttributes event to history
func (b *HistoryBuilder) AddUpsertWorkflowSearchAttributesEvent(
	decisionTaskCompletedEventID int64,
	attributes *workflow.UpsertWorkflowSearchAttributesDecisionAttributes) *workflow.HistoryEvent {
	event := b.newUpsertWorkflowSearchAttributesEvent(decisionTaskCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

// AddSignalExternalWorkflowExecutionFailedEvent adds SignalExternalWorkflowExecutionFailed event to history
func (b *HistoryBuilder) AddSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	domain, workflowID, runID string, control []byte, cause workflow.SignalExternalWorkflowExecutionFailedCause) *workflow.HistoryEvent {
	event := b.newSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID,
		domain, workflowID, runID, control, cause)

	return b.addEventToHistory(event)
}

// AddExternalWorkflowExecutionSignaled adds ExternalWorkflowExecutionSignaled event to history
func (b *HistoryBuilder) AddExternalWorkflowExecutionSignaled(initiatedEventID int64,
	domain, workflowID, runID string, control []byte) *workflow.HistoryEvent {
	event := b.newExternalWorkflowExecutionSignaledEvent(initiatedEventID,
		domain, workflowID, runID, control)

	return b.addEventToHistory(event)
}

// AddMarkerRecordedEvent adds MarkerRecorded event to history
func (b *HistoryBuilder) AddMarkerRecordedEvent(decisionCompletedEventID int64,
	attributes *workflow.RecordMarkerDecisionAttributes) *workflow.HistoryEvent {
	event := b.newMarkerRecordedEventAttributes(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

// AddWorkflowExecutionSignaledEvent adds WorkflowExecutionSignaled event to history
func (b *HistoryBuilder) AddWorkflowExecutionSignaledEvent(
	signalName string, input []byte, identity string) *workflow.HistoryEvent {
	event := b.newWorkflowExecutionSignaledEvent(signalName, input, identity)

	return b.addEventToHistory(event)
}

// AddStartChildWorkflowExecutionInitiatedEvent adds ChildWorkflowExecutionInitiated event to history
func (b *HistoryBuilder) AddStartChildWorkflowExecutionInitiatedEvent(decisionCompletedEventID int64,
	attributes *workflow.StartChildWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.newStartChildWorkflowExecutionInitiatedEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

// AddChildWorkflowExecutionStartedEvent adds ChildWorkflowExecutionStarted event to history
func (b *HistoryBuilder) AddChildWorkflowExecutionStartedEvent(
	domain *string,
	execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType,
	initiatedID int64,
	header *workflow.Header,
) *workflow.HistoryEvent {
	event := b.newChildWorkflowExecutionStartedEvent(domain, execution, workflowType, initiatedID, header)

	return b.addEventToHistory(event)
}

// AddStartChildWorkflowExecutionFailedEvent adds ChildWorkflowExecutionFailed event to history
func (b *HistoryBuilder) AddStartChildWorkflowExecutionFailedEvent(initiatedID int64,
	cause workflow.ChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *workflow.StartChildWorkflowExecutionInitiatedEventAttributes) *workflow.HistoryEvent {
	event := b.newStartChildWorkflowExecutionFailedEvent(initiatedID, cause, initiatedEventAttributes)

	return b.addEventToHistory(event)
}

// AddChildWorkflowExecutionCompletedEvent adds ChildWorkflowExecutionCompleted event to history
func (b *HistoryBuilder) AddChildWorkflowExecutionCompletedEvent(domain *string, execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType, initiatedID, startedID int64,
	completedAttributes *workflow.WorkflowExecutionCompletedEventAttributes) *workflow.HistoryEvent {
	event := b.newChildWorkflowExecutionCompletedEvent(domain, execution, workflowType, initiatedID, startedID,
		completedAttributes)

	return b.addEventToHistory(event)
}

// AddChildWorkflowExecutionFailedEvent adds ChildWorkflowExecutionFailed event to history
func (b *HistoryBuilder) AddChildWorkflowExecutionFailedEvent(domain *string, execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType, initiatedID, startedID int64,
	failedAttributes *workflow.WorkflowExecutionFailedEventAttributes) *workflow.HistoryEvent {
	event := b.newChildWorkflowExecutionFailedEvent(domain, execution, workflowType, initiatedID, startedID,
		failedAttributes)

	return b.addEventToHistory(event)
}

// AddChildWorkflowExecutionCanceledEvent adds ChildWorkflowExecutionCanceled event to history
func (b *HistoryBuilder) AddChildWorkflowExecutionCanceledEvent(domain *string, execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType, initiatedID, startedID int64,
	canceledAttributes *workflow.WorkflowExecutionCanceledEventAttributes) *workflow.HistoryEvent {
	event := b.newChildWorkflowExecutionCanceledEvent(domain, execution, workflowType, initiatedID, startedID,
		canceledAttributes)

	return b.addEventToHistory(event)
}

// AddChildWorkflowExecutionTerminatedEvent adds ChildWorkflowExecutionTerminated event to history
func (b *HistoryBuilder) AddChildWorkflowExecutionTerminatedEvent(domain *string, execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType, initiatedID, startedID int64,
	terminatedAttributes *workflow.WorkflowExecutionTerminatedEventAttributes) *workflow.HistoryEvent {
	event := b.newChildWorkflowExecutionTerminatedEvent(domain, execution, workflowType, initiatedID, startedID,
		terminatedAttributes)

	return b.addEventToHistory(event)
}

// AddChildWorkflowExecutionTimedOutEvent adds ChildWorkflowExecutionTimedOut event to history
func (b *HistoryBuilder) AddChildWorkflowExecutionTimedOutEvent(domain *string, execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType, initiatedID, startedID int64,
	timedOutAttributes *workflow.WorkflowExecutionTimedOutEventAttributes) *workflow.HistoryEvent {
	event := b.newChildWorkflowExecutionTimedOutEvent(domain, execution, workflowType, initiatedID, startedID,
		timedOutAttributes)

	return b.addEventToHistory(event)
}

func (b *HistoryBuilder) addEventToHistory(event *workflow.HistoryEvent) *workflow.HistoryEvent {
	b.history = append(b.history, event)
	return event
}

func (b *HistoryBuilder) addTransientEvent(event *workflow.HistoryEvent) *workflow.HistoryEvent {
	b.transientHistory = append(b.transientHistory, event)
	return event
}

func (b *HistoryBuilder) newWorkflowExecutionStartedEvent(
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

func (b *HistoryBuilder) newDecisionTaskScheduledEvent(taskList string, startToCloseTimeoutSeconds int32,
	attempt int64) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeDecisionTaskScheduled)

	return setDecisionTaskScheduledEventInfo(historyEvent, taskList, startToCloseTimeoutSeconds, attempt)
}

func (b *HistoryBuilder) newTransientDecisionTaskScheduledEvent(taskList string, startToCloseTimeoutSeconds int32,
	attempt int64, timestamp int64) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEventWithTimestamp(workflow.EventTypeDecisionTaskScheduled, timestamp)

	return setDecisionTaskScheduledEventInfo(historyEvent, taskList, startToCloseTimeoutSeconds, attempt)
}

func (b *HistoryBuilder) newDecisionTaskStartedEvent(scheduledEventID int64, requestID string,
	identity string) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeDecisionTaskStarted)

	return setDecisionTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func (b *HistoryBuilder) newTransientDecisionTaskStartedEvent(scheduledEventID int64, requestID string,
	identity string, timestamp int64) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEventWithTimestamp(workflow.EventTypeDecisionTaskStarted, timestamp)

	return setDecisionTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func (b *HistoryBuilder) newDecisionTaskCompletedEvent(scheduleEventID, startedEventID int64,
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

func (b *HistoryBuilder) newDecisionTaskTimedOutEvent(scheduleEventID int64, startedEventID int64, timeoutType workflow.TimeoutType) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeDecisionTaskTimedOut)
	attributes := &workflow.DecisionTaskTimedOutEventAttributes{}
	attributes.ScheduledEventId = common.Int64Ptr(scheduleEventID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.TimeoutType = common.TimeoutTypePtr(timeoutType)
	historyEvent.DecisionTaskTimedOutEventAttributes = attributes

	return historyEvent
}

func (b *HistoryBuilder) newDecisionTaskFailedEvent(attr workflow.DecisionTaskFailedEventAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeDecisionTaskFailed)
	historyEvent.DecisionTaskFailedEventAttributes = &attr
	return historyEvent
}

func (b *HistoryBuilder) newActivityTaskScheduledEvent(decisionTaskCompletedEventID int64,
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

func (b *HistoryBuilder) newActivityTaskStartedEvent(
	scheduledEventID int64,
	attempt int32,
	requestID string,
	identity string,
	lastFailureReason string,
	lastFailureDetails []byte,
) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeActivityTaskStarted)
	attributes := &workflow.ActivityTaskStartedEventAttributes{}
	attributes.ScheduledEventId = common.Int64Ptr(scheduledEventID)
	attributes.Attempt = common.Int32Ptr(attempt)
	attributes.Identity = common.StringPtr(identity)
	attributes.RequestId = common.StringPtr(requestID)
	attributes.LastFailureReason = common.StringPtr(lastFailureReason)
	attributes.LastFailureDetails = lastFailureDetails
	historyEvent.ActivityTaskStartedEventAttributes = attributes

	return historyEvent
}

func (b *HistoryBuilder) newActivityTaskCompletedEvent(scheduleEventID, startedEventID int64,
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

func (b *HistoryBuilder) newActivityTaskTimedOutEvent(
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

func (b *HistoryBuilder) newActivityTaskFailedEvent(scheduleEventID, startedEventID int64,
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

func (b *HistoryBuilder) newCompleteWorkflowExecutionEvent(decisionTaskCompletedEventID int64,
	request *workflow.CompleteWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeWorkflowExecutionCompleted)
	attributes := &workflow.WorkflowExecutionCompletedEventAttributes{}
	attributes.Result = request.Result
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	historyEvent.WorkflowExecutionCompletedEventAttributes = attributes

	return historyEvent
}

func (b *HistoryBuilder) newFailWorkflowExecutionEvent(decisionTaskCompletedEventID int64,
	request *workflow.FailWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeWorkflowExecutionFailed)
	attributes := &workflow.WorkflowExecutionFailedEventAttributes{}
	attributes.Reason = common.StringPtr(common.StringDefault(request.Reason))
	attributes.Details = request.Details
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	historyEvent.WorkflowExecutionFailedEventAttributes = attributes

	return historyEvent
}

func (b *HistoryBuilder) newTimeoutWorkflowExecutionEvent() *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeWorkflowExecutionTimedOut)
	attributes := &workflow.WorkflowExecutionTimedOutEventAttributes{}
	attributes.TimeoutType = common.TimeoutTypePtr(workflow.TimeoutTypeStartToClose)
	historyEvent.WorkflowExecutionTimedOutEventAttributes = attributes

	return historyEvent
}

func (b *HistoryBuilder) newWorkflowExecutionSignaledEvent(
	signalName string, input []byte, identity string) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeWorkflowExecutionSignaled)
	attributes := &workflow.WorkflowExecutionSignaledEventAttributes{}
	attributes.SignalName = common.StringPtr(signalName)
	attributes.Input = input
	attributes.Identity = common.StringPtr(identity)
	historyEvent.WorkflowExecutionSignaledEventAttributes = attributes

	return historyEvent
}

func (b *HistoryBuilder) newWorkflowExecutionTerminatedEvent(
	reason string, details []byte, identity string) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeWorkflowExecutionTerminated)
	attributes := &workflow.WorkflowExecutionTerminatedEventAttributes{}
	attributes.Reason = common.StringPtr(reason)
	attributes.Details = details
	attributes.Identity = common.StringPtr(identity)
	historyEvent.WorkflowExecutionTerminatedEventAttributes = attributes

	return historyEvent
}

func (b *HistoryBuilder) newMarkerRecordedEventAttributes(decisionTaskCompletedEventID int64,
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

func (b *HistoryBuilder) newWorkflowExecutionCancelRequestedEvent(cause string,
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

func (b *HistoryBuilder) newWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID int64,
	request *workflow.CancelWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeWorkflowExecutionCanceled)
	attributes := &workflow.WorkflowExecutionCanceledEventAttributes{}
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.Details = request.Details
	event.WorkflowExecutionCanceledEventAttributes = attributes

	return event
}

func (b *HistoryBuilder) newRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
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

func (b *HistoryBuilder) newRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
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

func (b *HistoryBuilder) newExternalWorkflowExecutionCancelRequestedEvent(initiatedEventID int64,
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

func (b *HistoryBuilder) newSignalExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
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

func (b *HistoryBuilder) newUpsertWorkflowSearchAttributesEvent(decisionTaskCompletedEventID int64,
	request *workflow.UpsertWorkflowSearchAttributesDecisionAttributes) *workflow.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(workflow.EventTypeUpsertWorkflowSearchAttributes)
	attributes := &workflow.UpsertWorkflowSearchAttributesEventAttributes{}
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.SearchAttributes = request.GetSearchAttributes()
	event.UpsertWorkflowSearchAttributesEventAttributes = attributes

	return event
}

func (b *HistoryBuilder) newSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
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

func (b *HistoryBuilder) newExternalWorkflowExecutionSignaledEvent(initiatedEventID int64,
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

func (b *HistoryBuilder) newWorkflowExecutionContinuedAsNewEvent(decisionTaskCompletedEventID int64,
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

func (b *HistoryBuilder) newStartChildWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
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

func (b *HistoryBuilder) newChildWorkflowExecutionStartedEvent(
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

func (b *HistoryBuilder) newStartChildWorkflowExecutionFailedEvent(initiatedID int64,
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

func (b *HistoryBuilder) newChildWorkflowExecutionCompletedEvent(domain *string, execution *workflow.WorkflowExecution,
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

func (b *HistoryBuilder) newChildWorkflowExecutionFailedEvent(domain *string, execution *workflow.WorkflowExecution,
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

func (b *HistoryBuilder) newChildWorkflowExecutionCanceledEvent(domain *string, execution *workflow.WorkflowExecution,
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

func (b *HistoryBuilder) newChildWorkflowExecutionTerminatedEvent(domain *string, execution *workflow.WorkflowExecution,
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

func (b *HistoryBuilder) newChildWorkflowExecutionTimedOutEvent(domain *string, execution *workflow.WorkflowExecution,
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

// GetHistory gets workflow history stored inside history builder
func (b *HistoryBuilder) GetHistory() *workflow.History {
	history := workflow.History{Events: b.history}
	return &history
}

// SetHistory sets workflow history inside history builder
func (b *HistoryBuilder) SetHistory(history *workflow.History) {
	b.history = history.Events
}
