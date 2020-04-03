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
	commonpb "go.temporal.io/temporal-proto/common"
	decisionpb "go.temporal.io/temporal-proto/decision"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	filterpb "go.temporal.io/temporal-proto/filter"
	namespacepb "go.temporal.io/temporal-proto/namespace"
	querypb "go.temporal.io/temporal-proto/query"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
	versionpb "go.temporal.io/temporal-proto/version"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
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

func (b *historyBuilder) AddDecisionTaskScheduledEvent(taskList string,
	startToCloseTimeoutSeconds int32, attempt int64) *historypb.HistoryEvent {
	event := b.newDecisionTaskScheduledEvent(taskList, startToCloseTimeoutSeconds, attempt)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTransientDecisionTaskScheduledEvent(taskList string,
	startToCloseTimeoutSeconds int32, attempt int64, timestamp int64) *historypb.HistoryEvent {
	event := b.newTransientDecisionTaskScheduledEvent(taskList, startToCloseTimeoutSeconds, attempt, timestamp)

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
	startedEventID int64, timeoutType eventpb.TimeoutType) *historypb.HistoryEvent {
	event := b.newDecisionTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutType)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskFailedEvent(attr *decisionpb.DecisionTaskFailedEventAttributes) *historypb.HistoryEvent {
	event := b.newDecisionTaskFailedEvent(attr)
	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskScheduledEvent(decisionCompletedEventID int64,
	attributes *decisionpb.ScheduleActivityTaskDecisionAttributes) *historypb.HistoryEvent {
	event := b.newActivityTaskScheduledEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskStartedEvent(scheduleEventID int64, attempt int32, requestID string,
	identity string) *historypb.HistoryEvent {
	event := b.newActivityTaskStartedEvent(scheduleEventID, attempt, requestID, identity)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondActivityTaskCompletedRequest) *historypb.HistoryEvent {
	event := b.newActivityTaskCompletedEvent(scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskFailedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondActivityTaskFailedRequest) *historypb.HistoryEvent {
	event := b.newActivityTaskFailedEvent(scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskTimedOutEvent(
	scheduleEventID,
	startedEventID int64,
	timeoutType eventpb.TimeoutType,
	lastHeartBeatDetails []byte,
	lastFailureReason string,
	lastFailureDetail []byte,
) *historypb.HistoryEvent {
	event := b.newActivityTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutType, lastHeartBeatDetails,
		lastFailureReason, lastFailureDetail)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddCompletedWorkflowEvent(decisionCompletedEventID int64,
	attributes *decisionpb.CompleteWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	event := b.newCompleteWorkflowExecutionEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddFailWorkflowEvent(decisionCompletedEventID int64,
	attributes *decisionpb.FailWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	event := b.newFailWorkflowExecutionEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimeoutWorkflowEvent() *historypb.HistoryEvent {
	event := b.newTimeoutWorkflowExecutionEvent()

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionTerminatedEvent(
	reason string,
	details []byte,
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

	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeTimerStarted)
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

	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeTimerFired)
	event.Attributes = &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCancelRequestedEvent(decisionCompletedEventID int64,
	activityID string) *historypb.HistoryEvent {

	attributes := &historypb.ActivityTaskCancelRequestedEventAttributes{}
	attributes.ActivityId = activityID
	attributes.DecisionTaskCompletedEventId = decisionCompletedEventID

	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeActivityTaskCancelRequested)
	event.Attributes = &historypb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{ActivityTaskCancelRequestedEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddRequestCancelActivityTaskFailedEvent(decisionCompletedEventID int64,
	activityID string, cause string) *historypb.HistoryEvent {

	attributes := &historypb.RequestCancelActivityTaskFailedEventAttributes{}
	attributes.ActivityId = activityID
	attributes.DecisionTaskCompletedEventId = decisionCompletedEventID
	attributes.Cause = cause

	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeRequestCancelActivityTaskFailed)
	event.Attributes = &historypb.HistoryEvent_RequestCancelActivityTaskFailedEventAttributes{RequestCancelActivityTaskFailedEventAttributes: attributes}

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCanceledEvent(scheduleEventID, startedEventID int64,
	latestCancelRequestedEventID int64, details []byte, identity string) *historypb.HistoryEvent {

	attributes := &historypb.ActivityTaskCanceledEventAttributes{}
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.LatestCancelRequestedEventId = latestCancelRequestedEventID
	attributes.Details = details
	attributes.Identity = identity

	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeActivityTaskCanceled)
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

	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeTimerCanceled)
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

	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeCancelTimerFailed)
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
	namespace, workflowID, runID string, cause eventpb.CancelExternalWorkflowExecutionFailedCause) *historypb.HistoryEvent {
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
	namespace, workflowID, runID string, control []byte, cause eventpb.SignalExternalWorkflowExecutionFailedCause) *historypb.HistoryEvent {
	event := b.newSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID,
		namespace, workflowID, runID, control, cause)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddExternalWorkflowExecutionSignaled(initiatedEventID int64,
	namespace, workflowID, runID string, control []byte) *historypb.HistoryEvent {
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
	signalName string, input []byte, identity string) *historypb.HistoryEvent {
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
	execution *executionpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	header *commonpb.Header,
) *historypb.HistoryEvent {
	event := b.newChildWorkflowExecutionStartedEvent(namespace, execution, workflowType, initiatedID, header)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddStartChildWorkflowExecutionFailedEvent(initiatedID int64,
	cause eventpb.ChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes) *historypb.HistoryEvent {
	event := b.newStartChildWorkflowExecutionFailedEvent(initiatedID, cause, initiatedEventAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionCompletedEvent(namespace string, execution *executionpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	completedAttributes *historypb.WorkflowExecutionCompletedEventAttributes) *historypb.HistoryEvent {
	event := b.newChildWorkflowExecutionCompletedEvent(namespace, execution, workflowType, initiatedID, startedID,
		completedAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionFailedEvent(namespace string, execution *executionpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	failedAttributes *historypb.WorkflowExecutionFailedEventAttributes) *historypb.HistoryEvent {
	event := b.newChildWorkflowExecutionFailedEvent(namespace, execution, workflowType, initiatedID, startedID,
		failedAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionCanceledEvent(namespace string, execution *executionpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	canceledAttributes *historypb.WorkflowExecutionCanceledEventAttributes) *historypb.HistoryEvent {
	event := b.newChildWorkflowExecutionCanceledEvent(namespace, execution, workflowType, initiatedID, startedID,
		canceledAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionTerminatedEvent(namespace string, execution *executionpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	terminatedAttributes *historypb.WorkflowExecutionTerminatedEventAttributes) *historypb.HistoryEvent {
	event := b.newChildWorkflowExecutionTerminatedEvent(namespace, execution, workflowType, initiatedID, startedID,
		terminatedAttributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddChildWorkflowExecutionTimedOutEvent(namespace string, execution *executionpb.WorkflowExecution,
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
	var resetPoints *executionpb.ResetPoints
	if previousExecution != nil {
		prevRunID = previousExecution.RunID
		resetPoints = previousExecution.AutoResetPoints
	}
	request := startRequest.StartRequest
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeWorkflowExecutionStarted)
	attributes := &historypb.WorkflowExecutionStartedEventAttributes{}
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
		attributes.ParentWorkflowNamespace = parentInfo.Namespace
		attributes.ParentWorkflowExecution = parentInfo.Execution
		attributes.ParentInitiatedEventId = parentInfo.InitiatedId
	}
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskScheduledEvent(taskList string, startToCloseTimeoutSeconds int32,
	attempt int64) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeDecisionTaskScheduled)

	return setDecisionTaskScheduledEventInfo(historyEvent, taskList, startToCloseTimeoutSeconds, attempt)
}

func (b *historyBuilder) newTransientDecisionTaskScheduledEvent(taskList string, startToCloseTimeoutSeconds int32,
	attempt int64, timestamp int64) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEventWithTimestamp(eventpb.EventTypeDecisionTaskScheduled, timestamp)

	return setDecisionTaskScheduledEventInfo(historyEvent, taskList, startToCloseTimeoutSeconds, attempt)
}

func (b *historyBuilder) newDecisionTaskStartedEvent(scheduledEventID int64, requestID string,
	identity string) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeDecisionTaskStarted)

	return setDecisionTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func (b *historyBuilder) newTransientDecisionTaskStartedEvent(scheduledEventID int64, requestID string,
	identity string, timestamp int64) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEventWithTimestamp(eventpb.EventTypeDecisionTaskStarted, timestamp)

	return setDecisionTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func (b *historyBuilder) newDecisionTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondDecisionTaskCompletedRequest) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeDecisionTaskCompleted)
	attributes := &decisionpb.DecisionTaskCompletedEventAttributes{}
	attributes.ExecutionContext = request.ExecutionContext
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.Identity = request.Identity
	attributes.BinaryChecksum = request.BinaryChecksum
	historyEvent.Attributes = &historypb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskTimedOutEvent(scheduleEventID int64, startedEventID int64, timeoutType eventpb.TimeoutType) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeDecisionTaskTimedOut)
	attributes := &decisionpb.DecisionTaskTimedOutEventAttributes{}
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.TimeoutType = timeoutType
	historyEvent.Attributes = &historypb.HistoryEvent_DecisionTaskTimedOutEventAttributes{DecisionTaskTimedOutEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskFailedEvent(attr *decisionpb.DecisionTaskFailedEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeDecisionTaskFailed)
	historyEvent.Attributes = &historypb.HistoryEvent_DecisionTaskFailedEventAttributes{DecisionTaskFailedEventAttributes: attr}
	return historyEvent
}

func (b *historyBuilder) newActivityTaskScheduledEvent(decisionTaskCompletedEventID int64,
	scheduleAttributes *decisionpb.ScheduleActivityTaskDecisionAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeActivityTaskScheduled)
	attributes := &historypb.ActivityTaskScheduledEventAttributes{}
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
	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newActivityTaskStartedEvent(scheduledEventID int64, attempt int32, requestID string,
	identity string) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeActivityTaskStarted)
	attributes := &historypb.ActivityTaskStartedEventAttributes{}
	attributes.ScheduledEventId = scheduledEventID
	attributes.Attempt = attempt
	attributes.Identity = identity
	attributes.RequestId = requestID
	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newActivityTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondActivityTaskCompletedRequest) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeActivityTaskCompleted)
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
	timeoutType eventpb.TimeoutType,
	lastHeartBeatDetails []byte,
	lastFailureReason string,
	lastFailureDetail []byte,
) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeActivityTaskTimedOut)
	attributes := &historypb.ActivityTaskTimedOutEventAttributes{}
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.TimeoutType = timeoutType
	attributes.Details = lastHeartBeatDetails
	attributes.LastFailureReason = lastFailureReason
	attributes.LastFailureDetails = lastFailureDetail

	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newActivityTaskFailedEvent(scheduleEventID, startedEventID int64,
	request *workflowservice.RespondActivityTaskFailedRequest) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeActivityTaskFailed)
	attributes := &historypb.ActivityTaskFailedEventAttributes{}
	attributes.Reason = request.Reason
	attributes.Details = request.Details
	attributes.ScheduledEventId = scheduleEventID
	attributes.StartedEventId = startedEventID
	attributes.Identity = request.Identity
	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newCompleteWorkflowExecutionEvent(decisionTaskCompletedEventID int64,
	request *decisionpb.CompleteWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeWorkflowExecutionCompleted)
	attributes := &historypb.WorkflowExecutionCompletedEventAttributes{}
	attributes.Result = request.Result
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newFailWorkflowExecutionEvent(decisionTaskCompletedEventID int64,
	request *decisionpb.FailWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeWorkflowExecutionFailed)
	attributes := &historypb.WorkflowExecutionFailedEventAttributes{}
	attributes.Reason = request.Reason
	attributes.Details = request.Details
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newTimeoutWorkflowExecutionEvent() *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeWorkflowExecutionTimedOut)
	attributes := &historypb.WorkflowExecutionTimedOutEventAttributes{}
	attributes.TimeoutType = eventpb.TimeoutTypeStartToClose
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{WorkflowExecutionTimedOutEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionSignaledEvent(
	signalName string, input []byte, identity string) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeWorkflowExecutionSignaled)
	attributes := &historypb.WorkflowExecutionSignaledEventAttributes{}
	attributes.SignalName = signalName
	attributes.Input = input
	attributes.Identity = identity
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionTerminatedEvent(
	reason string, details []byte, identity string) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeWorkflowExecutionTerminated)
	attributes := &historypb.WorkflowExecutionTerminatedEventAttributes{}
	attributes.Reason = reason
	attributes.Details = details
	attributes.Identity = identity
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newMarkerRecordedEventAttributes(decisionTaskCompletedEventID int64,
	request *decisionpb.RecordMarkerDecisionAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeMarkerRecorded)
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
	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeWorkflowExecutionCancelRequested)
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
	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeWorkflowExecutionCanceled)
	attributes := &historypb.WorkflowExecutionCanceledEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.Details = request.Details
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{WorkflowExecutionCanceledEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	request *decisionpb.RequestCancelExternalWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeRequestCancelExternalWorkflowExecutionInitiated)
	attributes := &historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.Namespace = request.Namespace
	attributes.WorkflowExecution = &executionpb.WorkflowExecution{
		WorkflowId: request.WorkflowId,
		RunId:      request.RunId,
	}
	attributes.Control = request.Control
	attributes.ChildWorkflowOnly = request.ChildWorkflowOnly
	event.Attributes = &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	namespace, workflowID, runID string, cause eventpb.CancelExternalWorkflowExecutionFailedCause) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeRequestCancelExternalWorkflowExecutionFailed)
	attributes := &historypb.RequestCancelExternalWorkflowExecutionFailedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.InitiatedEventId = initiatedEventID
	attributes.Namespace = namespace
	attributes.WorkflowExecution = &executionpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	attributes.Cause = cause
	event.Attributes = &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{RequestCancelExternalWorkflowExecutionFailedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newExternalWorkflowExecutionCancelRequestedEvent(initiatedEventID int64,
	namespace, workflowID, runID string) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeExternalWorkflowExecutionCancelRequested)
	attributes := &historypb.ExternalWorkflowExecutionCancelRequestedEventAttributes{}
	attributes.InitiatedEventId = initiatedEventID
	attributes.Namespace = namespace
	attributes.WorkflowExecution = &executionpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	event.Attributes = &historypb.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{ExternalWorkflowExecutionCancelRequestedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newSignalExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	request *decisionpb.SignalExternalWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeSignalExternalWorkflowExecutionInitiated)
	attributes := &historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.Namespace = request.Namespace
	attributes.WorkflowExecution = &executionpb.WorkflowExecution{
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
	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeUpsertWorkflowSearchAttributes)
	attributes := &historypb.UpsertWorkflowSearchAttributesEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.SearchAttributes = request.GetSearchAttributes()
	event.Attributes = &historypb.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{UpsertWorkflowSearchAttributesEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newSignalExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	namespace, workflowID, runID string, control []byte, cause eventpb.SignalExternalWorkflowExecutionFailedCause) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeSignalExternalWorkflowExecutionFailed)
	attributes := &historypb.SignalExternalWorkflowExecutionFailedEventAttributes{}
	attributes.DecisionTaskCompletedEventId = decisionTaskCompletedEventID
	attributes.InitiatedEventId = initiatedEventID
	attributes.Namespace = namespace
	attributes.WorkflowExecution = &executionpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	attributes.Cause = cause
	attributes.Control = control
	event.Attributes = &historypb.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{SignalExternalWorkflowExecutionFailedEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newExternalWorkflowExecutionSignaledEvent(initiatedEventID int64,
	namespace, workflowID, runID string, control []byte) *historypb.HistoryEvent {
	event := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeExternalWorkflowExecutionSignaled)
	attributes := &historypb.ExternalWorkflowExecutionSignaledEventAttributes{}
	attributes.InitiatedEventId = initiatedEventID
	attributes.Namespace = namespace
	attributes.WorkflowExecution = &executionpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	attributes.Control = control
	event.Attributes = &historypb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{ExternalWorkflowExecutionSignaledEventAttributes: attributes}

	return event
}

func (b *historyBuilder) newWorkflowExecutionContinuedAsNewEvent(decisionTaskCompletedEventID int64,
	newRunID string, request *decisionpb.ContinueAsNewWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeWorkflowExecutionContinuedAsNew)
	attributes := &historypb.WorkflowExecutionContinuedAsNewEventAttributes{}
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
	historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newStartChildWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	startAttributes *decisionpb.StartChildWorkflowExecutionDecisionAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeStartChildWorkflowExecutionInitiated)
	attributes := &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{}
	attributes.Namespace = startAttributes.Namespace
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
	historyEvent.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{StartChildWorkflowExecutionInitiatedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionStartedEvent(
	namespace string,
	execution *executionpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	initiatedID int64,
	header *commonpb.Header,
) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeChildWorkflowExecutionStarted)
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
	cause eventpb.ChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeStartChildWorkflowExecutionFailed)
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

func (b *historyBuilder) newChildWorkflowExecutionCompletedEvent(namespace string, execution *executionpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	completedAttributes *historypb.WorkflowExecutionCompletedEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeChildWorkflowExecutionCompleted)
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

func (b *historyBuilder) newChildWorkflowExecutionFailedEvent(namespace string, execution *executionpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	failedAttributes *historypb.WorkflowExecutionFailedEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeChildWorkflowExecutionFailed)
	attributes := &historypb.ChildWorkflowExecutionFailedEventAttributes{}
	attributes.Namespace = namespace
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.StartedEventId = startedID
	attributes.Reason = failedAttributes.Reason
	attributes.Details = failedAttributes.Details
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{ChildWorkflowExecutionFailedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionCanceledEvent(namespace string, execution *executionpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	canceledAttributes *historypb.WorkflowExecutionCanceledEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeChildWorkflowExecutionCanceled)
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

func (b *historyBuilder) newChildWorkflowExecutionTerminatedEvent(namespace string, execution *executionpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	terminatedAttributes *historypb.WorkflowExecutionTerminatedEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeChildWorkflowExecutionTerminated)
	attributes := &historypb.ChildWorkflowExecutionTerminatedEventAttributes{}
	attributes.Namespace = namespace
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.StartedEventId = startedID
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{ChildWorkflowExecutionTerminatedEventAttributes: attributes}

	return historyEvent
}

func (b *historyBuilder) newChildWorkflowExecutionTimedOutEvent(namespace string, execution *executionpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType, initiatedID, startedID int64,
	timedOutAttributes *historypb.WorkflowExecutionTimedOutEventAttributes) *historypb.HistoryEvent {
	historyEvent := b.msBuilder.CreateNewHistoryEvent(eventpb.EventTypeChildWorkflowExecutionTimedOut)
	attributes := &historypb.ChildWorkflowExecutionTimedOutEventAttributes{}
	attributes.Namespace = namespace
	attributes.TimeoutType = timedOutAttributes.TimeoutType
	attributes.WorkflowExecution = execution
	attributes.WorkflowType = workflowType
	attributes.InitiatedEventId = initiatedID
	attributes.StartedEventId = startedID
	historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{ChildWorkflowExecutionTimedOutEventAttributes: attributes}

	return historyEvent
}

func newDecisionTaskScheduledEventWithInfo(eventID, timestamp int64, taskList string, startToCloseTimeoutSeconds int32,
	attempt int64) *historypb.HistoryEvent {
	historyEvent := createNewHistoryEvent(eventID, eventpb.EventTypeDecisionTaskScheduled, timestamp)

	return setDecisionTaskScheduledEventInfo(historyEvent, taskList, startToCloseTimeoutSeconds, attempt)
}

func newDecisionTaskStartedEventWithInfo(eventID, timestamp int64, scheduledEventID int64, requestID string,
	identity string) *historypb.HistoryEvent {
	historyEvent := createNewHistoryEvent(eventID, eventpb.EventTypeDecisionTaskStarted, timestamp)

	return setDecisionTaskStartedEventInfo(historyEvent, scheduledEventID, requestID, identity)
}

func createNewHistoryEvent(eventID int64, eventType eventpb.EventType, timestamp int64) *historypb.HistoryEvent {
	historyEvent := &historypb.HistoryEvent{}
	historyEvent.EventId = eventID
	historyEvent.Timestamp = timestamp
	historyEvent.EventType = eventType

	return historyEvent
}

func setDecisionTaskScheduledEventInfo(historyEvent *historypb.HistoryEvent, taskList string,
	startToCloseTimeoutSeconds int32, attempt int64) *historypb.HistoryEvent {
	attributes := &decisionpb.DecisionTaskScheduledEventAttributes{}
	attributes.TaskList = &tasklistpb.TaskList{}
	attributes.TaskList.Name = taskList
	attributes.StartToCloseTimeoutSeconds = startToCloseTimeoutSeconds
	attributes.Attempt = attempt
	historyEvent.Attributes = &historypb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: attributes}

	return historyEvent
}

func setDecisionTaskStartedEventInfo(historyEvent *historypb.HistoryEvent, scheduledEventID int64, requestID string,
	identity string) *historypb.HistoryEvent {
	attributes := &decisionpb.DecisionTaskStartedEventAttributes{}
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
