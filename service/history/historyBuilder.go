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
	"github.com/uber-common/bark"
	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/persistence"
)

const (
	firstEventID int64 = 1
	emptyEventID int64 = -23
)

type (
	historyBuilder struct {
		serializer persistence.HistorySerializer
		history    []*workflow.HistoryEvent
		msBuilder  *mutableStateBuilder
		logger     bark.Logger
	}
)

func newHistoryBuilder(msBuilder *mutableStateBuilder, logger bark.Logger) *historyBuilder {
	return &historyBuilder{
		serializer: persistence.NewJSONHistorySerializer(),
		history:    []*workflow.HistoryEvent{},
		msBuilder:  msBuilder,
		logger:     logger.WithField(logging.TagWorkflowComponent, logging.TagValueHistoryBuilderComponent),
	}
}

func (b *historyBuilder) Serialize() (*persistence.SerializedHistoryEventBatch, error) {
	eventBatch := persistence.NewHistoryEventBatch(persistence.GetDefaultHistoryVersion(), b.history)
	history, err := b.serializer.Serialize(eventBatch)
	if err != nil {
		return nil, err
	}
	return history, nil
}

func (b *historyBuilder) AddWorkflowExecutionStartedEvent(
	request *workflow.StartWorkflowExecutionRequest) *workflow.HistoryEvent {
	event := b.newWorkflowExecutionStartedEvent(request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskScheduledEvent(taskList string,
	startToCloseTimeoutSeconds int32) *workflow.HistoryEvent {
	event := b.newDecisionTaskScheduledEvent(taskList, startToCloseTimeoutSeconds)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskStartedEvent(scheduleEventID int64, requestID string,
	request *workflow.PollForDecisionTaskRequest) *workflow.HistoryEvent {
	event := b.newDecisionTaskStartedEvent(scheduleEventID, requestID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondDecisionTaskCompletedRequest) *workflow.HistoryEvent {
	event := b.newDecisionTaskCompletedEvent(scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskTimedOutEvent(scheduleEventID int64,
	startedEventID int64) *workflow.HistoryEvent {
	event := b.newDecisionTaskTimedOutEvent(scheduleEventID, startedEventID)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskFailedEvent(scheduleEventID int64, startedEventID int64,
	cause workflow.DecisionTaskFailedCause, request *workflow.RespondDecisionTaskCompletedRequest) *workflow.HistoryEvent {
	event := b.newDecisionTaskFailedEvent(scheduleEventID, startedEventID, cause, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskScheduledEvent(decisionCompletedEventID int64,
	attributes *workflow.ScheduleActivityTaskDecisionAttributes) *workflow.HistoryEvent {
	event := b.newActivityTaskScheduledEvent(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskStartedEvent(scheduleEventID int64, requestID string,
	request *workflow.PollForActivityTaskRequest) *workflow.HistoryEvent {
	event := b.newActivityTaskStartedEvent(scheduleEventID, requestID, request)

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

func (b *historyBuilder) AddActivityTaskTimedOutEvent(scheduleEventID, startedEventID int64,
	timeoutType workflow.TimeoutType, lastHeartBeatDetails []byte) *workflow.HistoryEvent {
	event := b.newActivityTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutType, lastHeartBeatDetails)

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

func (b *historyBuilder) AddWorkflowExecutionTerminatedEvent(
	request *workflow.TerminateWorkflowExecutionRequest) *workflow.HistoryEvent {
	event := b.newWorkflowExecutionTerminatedEvent(request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddContinuedAsNewEvent(decisionCompletedEventID int64, newRunID string,
	attributes *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.newWorkflowExecutionContinuedAsNewEvent(decisionCompletedEventID, newRunID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerStartedEvent(decisionCompletedEventID int64,
	request *workflow.StartTimerDecisionAttributes) *workflow.HistoryEvent {

	attributes := workflow.NewTimerStartedEventAttributes()
	attributes.TimerId = common.StringPtr(request.GetTimerId())
	attributes.StartToFireTimeoutSeconds = common.Int64Ptr(request.GetStartToFireTimeoutSeconds())
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionCompletedEventID)

	event := b.msBuilder.createNewHistoryEvent(workflow.EventType_TimerStarted)
	event.TimerStartedEventAttributes = attributes

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerFiredEvent(startedEventID int64,
	timerID string) *workflow.HistoryEvent {

	attributes := workflow.NewTimerFiredEventAttributes()
	attributes.TimerId = common.StringPtr(timerID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)

	event := b.msBuilder.createNewHistoryEvent(workflow.EventType_TimerFired)
	event.TimerFiredEventAttributes = attributes

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCancelRequestedEvent(decisionCompletedEventID int64,
	activityID string) *workflow.HistoryEvent {

	attributes := workflow.NewActivityTaskCancelRequestedEventAttributes()
	attributes.ActivityId = common.StringPtr(activityID)
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionCompletedEventID)

	event := b.msBuilder.createNewHistoryEvent(workflow.EventType_ActivityTaskCancelRequested)
	event.ActivityTaskCancelRequestedEventAttributes = attributes

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddRequestCancelActivityTaskFailedEvent(decisionCompletedEventID int64,
	activityID string, cause string) *workflow.HistoryEvent {

	attributes := workflow.NewRequestCancelActivityTaskFailedEventAttributes()
	attributes.ActivityId = common.StringPtr(activityID)
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionCompletedEventID)
	attributes.Cause = common.StringPtr(cause)

	event := b.msBuilder.createNewHistoryEvent(workflow.EventType_RequestCancelActivityTaskFailed)
	event.RequestCancelActivityTaskFailedEventAttributes = attributes

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCanceledEvent(scheduleEventID, startedEventID int64,
	latestCancelRequestedEventID int64, details []byte, identity string) *workflow.HistoryEvent {

	attributes := workflow.NewActivityTaskCanceledEventAttributes()
	attributes.ScheduledEventId = common.Int64Ptr(scheduleEventID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.LatestCancelRequestedEventId = common.Int64Ptr(latestCancelRequestedEventID)
	attributes.Details = details
	attributes.Identity = common.StringPtr(identity)

	event := b.msBuilder.createNewHistoryEvent(workflow.EventType_ActivityTaskCanceled)
	event.ActivityTaskCanceledEventAttributes = attributes

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerCanceledEvent(startedEventID int64,
	decisionTaskCompletedEventID int64, timerID string, identity string) *workflow.HistoryEvent {

	attributes := workflow.NewTimerCanceledEventAttributes()
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.TimerId = common.StringPtr(timerID)
	attributes.Identity = common.StringPtr(identity)

	event := b.msBuilder.createNewHistoryEvent(workflow.EventType_TimerCanceled)
	event.TimerCanceledEventAttributes = attributes

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddCancelTimerFailedEvent(timerID string, decisionTaskCompletedEventID int64,
	cause string, identity string) *workflow.HistoryEvent {

	attributes := workflow.NewCancelTimerFailedEventAttributes()
	attributes.TimerId = common.StringPtr(timerID)
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.Cause = common.StringPtr(cause)
	attributes.Identity = common.StringPtr(identity)

	event := b.msBuilder.createNewHistoryEvent(workflow.EventType_CancelTimerFailed)
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

func (b *historyBuilder) AddMarkerRecordedEvent(decisionCompletedEventID int64,
	attributes *workflow.RecordMarkerDecisionAttributes) *workflow.HistoryEvent {
	event := b.newMarkerRecordedEventAttributes(decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionSignaledEvent(
	request *workflow.SignalWorkflowExecutionRequest) *workflow.HistoryEvent {
	event := b.newWorkflowExecutionSignaledEvent(request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) addEventToHistory(event *workflow.HistoryEvent) *workflow.HistoryEvent {
	b.history = append(b.history, event)
	return event
}

func (b *historyBuilder) newWorkflowExecutionStartedEvent(
	request *workflow.StartWorkflowExecutionRequest) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.createNewHistoryEvent(workflow.EventType_WorkflowExecutionStarted)
	attributes := workflow.NewWorkflowExecutionStartedEventAttributes()
	attributes.WorkflowType = request.GetWorkflowType()
	attributes.TaskList = request.GetTaskList()
	attributes.Input = request.GetInput()
	attributes.ExecutionStartToCloseTimeoutSeconds = common.Int32Ptr(request.GetExecutionStartToCloseTimeoutSeconds())
	attributes.TaskStartToCloseTimeoutSeconds = common.Int32Ptr(request.GetTaskStartToCloseTimeoutSeconds())
	attributes.Identity = common.StringPtr(request.GetIdentity())
	historyEvent.WorkflowExecutionStartedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskScheduledEvent(taskList string,
	startToCloseTimeoutSeconds int32) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.createNewHistoryEvent(workflow.EventType_DecisionTaskScheduled)
	attributes := workflow.NewDecisionTaskScheduledEventAttributes()
	attributes.TaskList = workflow.NewTaskList()
	attributes.TaskList.Name = common.StringPtr(taskList)
	attributes.StartToCloseTimeoutSeconds = common.Int32Ptr(startToCloseTimeoutSeconds)
	historyEvent.DecisionTaskScheduledEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskStartedEvent(scheduledEventID int64, requestID string,
	request *workflow.PollForDecisionTaskRequest) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.createNewHistoryEvent(workflow.EventType_DecisionTaskStarted)
	attributes := workflow.NewDecisionTaskStartedEventAttributes()
	attributes.ScheduledEventId = common.Int64Ptr(scheduledEventID)
	attributes.Identity = common.StringPtr(request.GetIdentity())
	attributes.RequestId = common.StringPtr(requestID)
	historyEvent.DecisionTaskStartedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondDecisionTaskCompletedRequest) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.createNewHistoryEvent(workflow.EventType_DecisionTaskCompleted)
	attributes := workflow.NewDecisionTaskCompletedEventAttributes()
	attributes.ExecutionContext = request.GetExecutionContext()
	attributes.ScheduledEventId = common.Int64Ptr(scheduleEventID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.Identity = common.StringPtr(request.GetIdentity())
	historyEvent.DecisionTaskCompletedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskTimedOutEvent(scheduleEventID int64, startedEventID int64) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.createNewHistoryEvent(workflow.EventType_DecisionTaskTimedOut)
	attributes := workflow.NewDecisionTaskTimedOutEventAttributes()
	attributes.ScheduledEventId = common.Int64Ptr(scheduleEventID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.TimeoutType = workflow.TimeoutTypePtr(workflow.TimeoutType_START_TO_CLOSE)
	historyEvent.DecisionTaskTimedOutEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newDecisionTaskFailedEvent(scheduleEventID int64, startedEventID int64,
	cause workflow.DecisionTaskFailedCause, request *workflow.RespondDecisionTaskCompletedRequest) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.createNewHistoryEvent(workflow.EventType_DecisionTaskFailed)
	attributes := workflow.NewDecisionTaskFailedEventAttributes()
	attributes.ScheduledEventId = common.Int64Ptr(scheduleEventID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.Cause = workflow.DecisionTaskFailedCausePtr(cause)
	attributes.Identity = common.StringPtr(request.GetIdentity())
	historyEvent.DecisionTaskFailedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newActivityTaskScheduledEvent(decisionTaskCompletedEventID int64,
	scheduleAttributes *workflow.ScheduleActivityTaskDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.createNewHistoryEvent(workflow.EventType_ActivityTaskScheduled)
	attributes := workflow.NewActivityTaskScheduledEventAttributes()
	attributes.ActivityId = common.StringPtr(scheduleAttributes.GetActivityId())
	attributes.ActivityType = scheduleAttributes.GetActivityType()
	attributes.TaskList = scheduleAttributes.GetTaskList()
	attributes.Input = scheduleAttributes.GetInput()
	attributes.ScheduleToCloseTimeoutSeconds = common.Int32Ptr(scheduleAttributes.GetScheduleToCloseTimeoutSeconds())
	attributes.ScheduleToStartTimeoutSeconds = common.Int32Ptr(scheduleAttributes.GetScheduleToStartTimeoutSeconds())
	attributes.StartToCloseTimeoutSeconds = common.Int32Ptr(scheduleAttributes.GetStartToCloseTimeoutSeconds())
	attributes.HeartbeatTimeoutSeconds = common.Int32Ptr(scheduleAttributes.GetHeartbeatTimeoutSeconds())
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	historyEvent.ActivityTaskScheduledEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newActivityTaskStartedEvent(scheduledEventID int64, requestID string,
	request *workflow.PollForActivityTaskRequest) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.createNewHistoryEvent(workflow.EventType_ActivityTaskStarted)
	attributes := workflow.NewActivityTaskStartedEventAttributes()
	attributes.ScheduledEventId = common.Int64Ptr(scheduledEventID)
	attributes.Identity = common.StringPtr(request.GetIdentity())
	attributes.RequestId = common.StringPtr(requestID)
	historyEvent.ActivityTaskStartedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newActivityTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondActivityTaskCompletedRequest) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.createNewHistoryEvent(workflow.EventType_ActivityTaskCompleted)
	attributes := workflow.NewActivityTaskCompletedEventAttributes()
	attributes.Result_ = request.GetResult_()
	attributes.ScheduledEventId = common.Int64Ptr(scheduleEventID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.Identity = common.StringPtr(request.GetIdentity())
	historyEvent.ActivityTaskCompletedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newActivityTaskTimedOutEvent(scheduleEventID, startedEventID int64,
	timeoutType workflow.TimeoutType, lastHeartBeatDetails []byte) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.createNewHistoryEvent(workflow.EventType_ActivityTaskTimedOut)
	attributes := workflow.NewActivityTaskTimedOutEventAttributes()
	attributes.ScheduledEventId = common.Int64Ptr(scheduleEventID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.TimeoutType = workflow.TimeoutTypePtr(timeoutType)
	attributes.Details = lastHeartBeatDetails
	historyEvent.ActivityTaskTimedOutEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newActivityTaskFailedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondActivityTaskFailedRequest) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.createNewHistoryEvent(workflow.EventType_ActivityTaskFailed)
	attributes := workflow.NewActivityTaskFailedEventAttributes()
	attributes.Reason = common.StringPtr(request.GetReason())
	attributes.Details = request.GetDetails()
	attributes.ScheduledEventId = common.Int64Ptr(scheduleEventID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.Identity = common.StringPtr(request.GetIdentity())
	historyEvent.ActivityTaskFailedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newCompleteWorkflowExecutionEvent(decisionTaskCompletedEventID int64,
	request *workflow.CompleteWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.createNewHistoryEvent(workflow.EventType_WorkflowExecutionCompleted)
	attributes := workflow.NewWorkflowExecutionCompletedEventAttributes()
	attributes.Result_ = request.GetResult_()
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	historyEvent.WorkflowExecutionCompletedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newFailWorkflowExecutionEvent(decisionTaskCompletedEventID int64,
	request *workflow.FailWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.createNewHistoryEvent(workflow.EventType_WorkflowExecutionFailed)
	attributes := workflow.NewWorkflowExecutionFailedEventAttributes()
	attributes.Reason = common.StringPtr(request.GetReason())
	attributes.Details = request.GetDetails()
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	historyEvent.WorkflowExecutionFailedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionSignaledEvent(
	request *workflow.SignalWorkflowExecutionRequest) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.createNewHistoryEvent(workflow.EventType_WorkflowExecutionSignaled)
	attributes := workflow.NewWorkflowExecutionSignaledEventAttributes()
	attributes.SignalName = common.StringPtr(request.GetSignalName())
	attributes.Input = request.GetInput()
	attributes.Identity = common.StringPtr(request.GetIdentity())
	historyEvent.WorkflowExecutionSignaledEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionTerminatedEvent(
	request *workflow.TerminateWorkflowExecutionRequest) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.createNewHistoryEvent(workflow.EventType_WorkflowExecutionTerminated)
	attributes := workflow.NewWorkflowExecutionTerminatedEventAttributes()
	attributes.Reason = common.StringPtr(request.GetReason())
	attributes.Details = request.GetDetails()
	attributes.Identity = common.StringPtr(request.GetIdentity())
	historyEvent.WorkflowExecutionTerminatedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newMarkerRecordedEventAttributes(decisionTaskCompletedEventID int64,
	request *workflow.RecordMarkerDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.createNewHistoryEvent(workflow.EventType_MarkerRecorded)
	attributes := workflow.NewMarkerRecordedEventAttributes()
	attributes.MarkerName = common.StringPtr(request.GetMarkerName())
	attributes.Details = request.GetDetails()
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	historyEvent.MarkerRecordedEventAttributes = attributes

	return historyEvent
}

func (b *historyBuilder) newWorkflowExecutionCancelRequestedEvent(cause string,
	request *h.RequestCancelWorkflowExecutionRequest) *workflow.HistoryEvent {
	event := b.msBuilder.createNewHistoryEvent(workflow.EventType_WorkflowExecutionCancelRequested)
	attributes := workflow.NewWorkflowExecutionCancelRequestedEventAttributes()
	attributes.Cause = common.StringPtr(cause)
	attributes.Identity = common.StringPtr(request.GetCancelRequest().GetIdentity())
	if request.IsSetExternalInitiatedEventId() {
		attributes.ExternalInitiatedEventId = common.Int64Ptr(request.GetExternalInitiatedEventId())
	}
	if request.IsSetExternalWorkflowExecution() {
		attributes.ExternalWorkflowExecution = request.GetExternalWorkflowExecution()
	}
	event.WorkflowExecutionCancelRequestedEventAttributes = attributes

	return event
}

func (b *historyBuilder) newWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID int64,
	request *workflow.CancelWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.msBuilder.createNewHistoryEvent(workflow.EventType_WorkflowExecutionCanceled)
	attributes := workflow.NewWorkflowExecutionCanceledEventAttributes()
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.Details = request.GetDetails()
	event.WorkflowExecutionCanceledEventAttributes = attributes

	return event
}

func (b *historyBuilder) newRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionTaskCompletedEventID int64,
	request *workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := b.msBuilder.createNewHistoryEvent(workflow.EventType_RequestCancelExternalWorkflowExecutionInitiated)
	attributes := workflow.NewRequestCancelExternalWorkflowExecutionInitiatedEventAttributes()
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.Domain = common.StringPtr(request.GetDomain())
	attributes.WorkflowExecution = &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(request.GetWorkflowId()),
		RunId:      common.StringPtr(request.GetRunId()),
	}
	attributes.Control = request.Control
	event.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes = attributes

	return event
}

func (b *historyBuilder) newRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedEventID int64,
	domain, workflowID, runID string, cause workflow.CancelExternalWorkflowExecutionFailedCause) *workflow.HistoryEvent {
	event := b.msBuilder.createNewHistoryEvent(workflow.EventType_RequestCancelExternalWorkflowExecutionFailed)
	attributes := workflow.NewRequestCancelExternalWorkflowExecutionFailedEventAttributes()
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	attributes.InitiatedEventId = common.Int64Ptr(initiatedEventID)
	attributes.Domain = common.StringPtr(domain)
	attributes.WorkflowExecution = &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}
	attributes.Cause = workflow.CancelExternalWorkflowExecutionFailedCausePtr(cause)
	event.RequestCancelExternalWorkflowExecutionFailedEventAttributes = attributes

	return event
}

func (b *historyBuilder) newExternalWorkflowExecutionCancelRequestedEvent(initiatedEventID int64,
	domain, workflowID, runID string) *workflow.HistoryEvent {
	event := b.msBuilder.createNewHistoryEvent(workflow.EventType_ExternalWorkflowExecutionCancelRequested)
	attributes := workflow.NewExternalWorkflowExecutionCancelRequestedEventAttributes()
	attributes.InitiatedEventId = common.Int64Ptr(initiatedEventID)
	attributes.Domain = common.StringPtr(domain)
	attributes.WorkflowExecution = &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}
	event.ExternalWorkflowExecutionCancelRequestedEventAttributes = attributes

	return event
}

func (b *historyBuilder) newWorkflowExecutionContinuedAsNewEvent(decisionTaskCompletedEventID int64,
	newRunID string, request *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.createNewHistoryEvent(workflow.EventType_WorkflowExecutionContinuedAsNew)
	attributes := workflow.NewWorkflowExecutionContinuedAsNewEventAttributes()
	attributes.NewExecutionRunId_ = common.StringPtr(newRunID)
	attributes.WorkflowType = request.GetWorkflowType()
	attributes.TaskList = request.GetTaskList()
	attributes.Input = request.GetInput()
	attributes.ExecutionStartToCloseTimeoutSeconds = common.Int32Ptr(request.GetExecutionStartToCloseTimeoutSeconds())
	attributes.TaskStartToCloseTimeoutSeconds = common.Int32Ptr(request.GetTaskStartToCloseTimeoutSeconds())
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	historyEvent.WorkflowExecutionContinuedAsNewEventAttributes = attributes

	return historyEvent
}
