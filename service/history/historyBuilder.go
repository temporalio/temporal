package history

import (
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"

	"github.com/uber-common/bark"
)

const (
	firstEventID int64 = 1
	emptyEventID int64 = -23
)

type (
	historyBuilder struct {
		serializer historySerializer
		history    []*workflow.HistoryEvent
		msBuilder  *mutableStateBuilder
		logger     bark.Logger
	}
)

func newHistoryBuilder(msBuilder *mutableStateBuilder, logger bark.Logger) *historyBuilder {
	return &historyBuilder{
		serializer: newJSONHistorySerializer(),
		history:    []*workflow.HistoryEvent{},
		msBuilder:  msBuilder,
		logger:     logger.WithField(tagWorkflowComponent, tagValueHistoryBuilderComponent),
	}
}

func (b *historyBuilder) Serialize() ([]byte, error) {
	history, err := b.serializer.Serialize(b.history)

	return history, err
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

func (b *historyBuilder) AddCompleteWorkflowExecutionFailedEvent(decisionCompletedEventID int64,
	cause workflow.WorkflowCompleteFailedCause) *workflow.HistoryEvent {
	event := b.newCompleteWorkflowExecutionFailedEvent(decisionCompletedEventID, cause)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddWorkflowExecutionTerminatedEvent(
	request *workflow.TerminateWorkflowExecutionRequest) *workflow.HistoryEvent {
	event := b.newWorkflowExecutionTerminatedEvent(request)

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

func (b *historyBuilder) AddMarkerRecordedEvent(decisionCompletedEventID int64,
	attributes *workflow.RecordMarkerDecisionAttributes) *workflow.HistoryEvent {
	event := b.newMarkerRecordedEventAttributes(decisionCompletedEventID, attributes)

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

func (b *historyBuilder) newCompleteWorkflowExecutionFailedEvent(decisionTaskCompletedEventID int64,
	cause workflow.WorkflowCompleteFailedCause) *workflow.HistoryEvent {
	historyEvent := b.msBuilder.createNewHistoryEvent(workflow.EventType_CompleteWorkflowExecutionFailed)
	attributes := workflow.NewCompleteWorkflowExecutionFailedEventAttributes()
	attributes.Cause = workflow.WorkflowCompleteFailedCausePtr(cause)
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	historyEvent.CompleteWorkflowExecutionFailedEventAttributes = attributes

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