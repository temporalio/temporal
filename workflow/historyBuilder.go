package workflow

import (
	"fmt"
	"time"

	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/util"
	"code.uber.internal/devexp/minions/persistence"

	"github.com/uber-common/bark"
)

const (
	firstEventID int64 = 1
	emptyEventID int64 = -23
)

var (
	errInvalidHistory = &workflow.InternalServiceError{Message: "Workflow execution history is invalid"}
)

type (
	historyBuilder struct {
		serializer                       historySerializer
		history                          []*workflow.HistoryEvent
		outstandingActivities            map[int64]int64
		outstandingDecisionTask          map[int64]int64
		outstandingTimerTask             map[int64]string // Timer started event ID -> Timer User ID.
		previousDecisionTaskStartedEvent int64
		nextEventID                      int64
		state                            int
		logger                           bark.Logger
		tBuilder                         *timerBuilder
	}
)

func newHistoryBuilder(tBuilder *timerBuilder, logger bark.Logger) *historyBuilder {
	return &historyBuilder{
		serializer:                       newJSONHistorySerializer(),
		history:                          []*workflow.HistoryEvent{},
		outstandingActivities:            make(map[int64]int64),
		outstandingDecisionTask:          make(map[int64]int64),
		outstandingTimerTask:             make(map[int64]string),
		previousDecisionTaskStartedEvent: emptyEventID,
		nextEventID:                      firstEventID,
		state:                            persistence.WorkflowStateCreated,
		tBuilder:                         tBuilder,
		logger:                           logger.WithField(tagWorkflowComponent, tagValueHistoryBuilderComponent),
	}
}

func (b *historyBuilder) loadExecutionInfo(executionInfo *persistence.WorkflowExecutionInfo) error {
	if executionInfo != nil {
		h, err := b.serializer.Deserialize(executionInfo.History)
		if err != nil {
			return err
		}

		for _, event := range h {
			if b.addEventToHistory(event) == nil {
				return errInvalidHistory
			}

			// Load timer information.
			switch event.GetEventType() {
			case workflow.EventType_TimerStarted:
				startTimerAttr := event.GetTimerStartedEventAttributes()
				expires := util.AddSecondsToBaseTime(event.GetTimestamp(), startTimerAttr.GetStartToFireTimeoutSeconds())
				b.tBuilder.LoadUserTimer(expires, &persistence.UserTimerTask{
					EventID: event.GetEventId(),
				})

			case workflow.EventType_TimerFired:
				fireTimerAttr := event.GetTimerFiredEventAttributes()
				b.tBuilder.UnLoadUserTimer(fireTimerAttr.GetStartedEventId())
			}
		}
	}

	return nil
}

func (b *historyBuilder) GetEvent(eventID int64) *workflow.HistoryEvent {
	return b.history[eventID-firstEventID]
}

func (b *historyBuilder) Serialize() ([]byte, error) {
	history, err := b.serializer.Serialize(b.history)

	return history, err
}

func (b *historyBuilder) isActivityTaskRunning(scheduleID int64) (bool, int64) {
	startedID, ok := b.outstandingActivities[scheduleID]
	return ok, startedID
}

func (b *historyBuilder) isDecisionTaskRunning(scheduleID int64) (bool, int64) {
	startedID, ok := b.outstandingDecisionTask[scheduleID]
	return ok, startedID
}

func (b *historyBuilder) isTimerTaskRunning(startedID int64) (bool, string) {
	timerID, ok := b.outstandingTimerTask[startedID]
	return ok, timerID
}

func (b *historyBuilder) previousDecisionStartedEvent() int64 {
	return b.previousDecisionTaskStartedEvent
}

func (b *historyBuilder) hasPendingDecisionTask() bool {
	return len(b.outstandingDecisionTask) > 0
}

func (b *historyBuilder) hasPendingTasks() bool {
	return len(b.outstandingActivities) > 0
}

func (b *historyBuilder) getWorkflowType() *workflow.WorkflowType {
	if b.history != nil && len(b.history) > 0 {
		event := b.history[0]
		if event.GetEventType() == workflow.EventType_WorkflowExecutionStarted {
			startedEvent := event.WorkflowExecutionStartedEventAttributes
			return startedEvent.GetWorkflowType()
		}
	}

	return nil
}

func (b *historyBuilder) getWorklowState() int {
	return b.state
}

func (b *historyBuilder) getHistory() *workflow.History {
	h := workflow.NewHistory()
	h.Events = b.history

	return h
}

func (b *historyBuilder) AddWorkflowExecutionStartedEvent(
	request *workflow.StartWorkflowExecutionRequest) *workflow.HistoryEvent {
	event := newWorkflowExecutionStartedEvent(b.nextEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskScheduledEvent(taskList string,
	startToCloseTimeoutSeconds int32) *workflow.HistoryEvent {
	event := newDecisionTaskScheduledEvent(b.nextEventID, taskList, startToCloseTimeoutSeconds)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskStartedEvent(scheduleEventID int64,
	request *workflow.PollForDecisionTaskRequest) *workflow.HistoryEvent {
	event := newDecisionTaskStartedEvent(b.nextEventID, scheduleEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondDecisionTaskCompletedRequest) *workflow.HistoryEvent {
	event := newDecisionTaskCompletedEvent(b.nextEventID, scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddDecisionTaskTimedOutEvent(scheduleEventID int64,
	startedEventID int64) *workflow.HistoryEvent {
	event := newDecisionTaskTimedOutEvent(b.nextEventID, scheduleEventID, startedEventID)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskScheduledEvent(decisionCompletedEventID int64,
	attributes *workflow.ScheduleActivityTaskDecisionAttributes) *workflow.HistoryEvent {
	event := newActivityTaskScheduledEvent(b.nextEventID, decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskStartedEvent(scheduleEventID int64,
	request *workflow.PollForActivityTaskRequest) *workflow.HistoryEvent {
	event := newActivityTaskStartedEvent(b.nextEventID, scheduleEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondActivityTaskCompletedRequest) *workflow.HistoryEvent {
	event := newActivityTaskCompletedEvent(b.nextEventID, scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddActivityTaskFailedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondActivityTaskFailedRequest) *workflow.HistoryEvent {
	event := newActivityTaskFailedEvent(b.nextEventID, scheduleEventID, startedEventID, request)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddCompletedWorkflowEvent(decisionCompletedEventID int64,
	attributes *workflow.CompleteWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := newCompleteWorkflowExecutionEvent(b.nextEventID, decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddFailWorkflowEvent(decisionCompletedEventID int64,
	attributes *workflow.FailWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	event := newFailWorkflowExecutionEvent(b.nextEventID, decisionCompletedEventID, attributes)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddCompleteWorkflowExecutionFailedEvent(decisionCompletedEventID int64,
	cause workflow.WorkflowCompleteFailedCause) *workflow.HistoryEvent {
	event := newCompleteWorkflowExecutionFailedEvent(b.nextEventID, decisionCompletedEventID, cause)

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerStartedEvent(decisionCompletedEventID int64,
	request *workflow.StartTimerDecisionAttributes) *workflow.HistoryEvent {

	attributes := workflow.NewTimerStartedEventAttributes()
	attributes.TimerId = common.StringPtr(request.GetTimerId())
	attributes.StartToFireTimeoutSeconds = common.Int64Ptr(request.GetStartToFireTimeoutSeconds())
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionCompletedEventID)

	event := newHistoryEvent(b.nextEventID, workflow.EventType_TimerStarted)
	event.TimerStartedEventAttributes = attributes

	return b.addEventToHistory(event)
}

func (b *historyBuilder) AddTimerFiredEvent(startedEventID int64,
	timerID string, sequenceID int64) (*workflow.HistoryEvent, error) {

	attributes := workflow.NewTimerFiredEventAttributes()
	attributes.TimerId = common.StringPtr(timerID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)

	event := newHistoryEvent(b.nextEventID, workflow.EventType_TimerFired)
	event.TimerFiredEventAttributes = attributes

	return b.addEventToHistory(event), nil
}

func (b *historyBuilder) addEventToHistory(event *workflow.HistoryEvent) *workflow.HistoryEvent {
	//b.logger.Debugf("Adding EventId: %v, Event: %+v", event.GetEventId(), *event)
	eventID := event.GetEventId()
	switch event.GetEventType() {
	case workflow.EventType_WorkflowExecutionStarted:
		if eventID != firstEventID {
			logInvalidHistoryActionEvent(b.logger, tagValueActionWorkflowStarted, eventID, "")
			return nil
		}
	case workflow.EventType_DecisionTaskScheduled:
		outstandingDecisionCount := len(b.outstandingDecisionTask)
		if outstandingDecisionCount > 0 {
			logInvalidHistoryActionEvent(b.logger, tagValueActionDecisionTaskScheduled, eventID, fmt.Sprintf(
				"{DecisionCount: %v}", outstandingDecisionCount))
			return nil
		}
		b.outstandingDecisionTask[eventID] = emptyEventID
	case workflow.EventType_DecisionTaskStarted:
		outstandingDecisionCount := len(b.outstandingDecisionTask)
		scheduleEventID := event.GetDecisionTaskStartedEventAttributes().GetScheduledEventId()
		e, ok := b.outstandingDecisionTask[scheduleEventID]
		if outstandingDecisionCount != 1 || !ok || e != emptyEventID {
			logInvalidHistoryActionEvent(b.logger, tagValueActionDecisionTaskStarted, eventID, fmt.Sprintf(
				"{DecisionCount: %v, ScheduleID: %v, Exist: %v, Value: %v}", outstandingDecisionCount, scheduleEventID, ok, e))
			return nil
		}
		b.outstandingDecisionTask[scheduleEventID] = eventID
		b.state = persistence.WorkflowStateRunning
	case workflow.EventType_DecisionTaskCompleted:
		outstandingDecisionCount := len(b.outstandingDecisionTask)
		scheduleEventID := event.GetDecisionTaskCompletedEventAttributes().GetScheduledEventId()
		startedEventID := event.GetDecisionTaskCompletedEventAttributes().GetStartedEventId()
		e, ok := b.outstandingDecisionTask[scheduleEventID]
		if !ok || e != startedEventID {
			logInvalidHistoryActionEvent(b.logger, tagValueActionDecisionTaskCompleted, eventID, fmt.Sprintf(
				"{DecisionCount: %v, ScheduleID: %v, StartedID: %v, Exist: %v, Value: %v}", outstandingDecisionCount,
				scheduleEventID, startedEventID, ok, e))
			return nil
		}
		b.previousDecisionTaskStartedEvent = startedEventID
		delete(b.outstandingDecisionTask, scheduleEventID)
	case workflow.EventType_DecisionTaskTimedOut:
		outstandingDecisionCount := len(b.outstandingDecisionTask)
		scheduleEventID := event.GetDecisionTaskTimedOutEventAttributes().GetScheduledEventId()
		startedEventID := event.GetDecisionTaskTimedOutEventAttributes().GetStartedEventId()
		e, ok := b.outstandingDecisionTask[scheduleEventID]
		if !ok || e != startedEventID {
			logInvalidHistoryActionEvent(b.logger, tagValueActionDecisionTaskTimedOut, eventID, fmt.Sprintf(
				"{DecisionCount: %v, ScheduleID: %v, StartedID: %v, Exist: %v, e: %v}", outstandingDecisionCount,
				scheduleEventID, startedEventID, ok, e))
			return nil
		}
		delete(b.outstandingDecisionTask, scheduleEventID)
	case workflow.EventType_ActivityTaskScheduled:
		if e, ok := b.outstandingActivities[eventID]; ok {
			logInvalidHistoryActionEvent(b.logger, tagValueActionActivityTaskScheduled, eventID, fmt.Sprintf(
				"{Exist: %v, Value: %v}", ok, e))
			return nil
		}
		b.outstandingActivities[eventID] = emptyEventID
	case workflow.EventType_ActivityTaskStarted:
		scheduleEventID := event.GetActivityTaskStartedEventAttributes().GetScheduledEventId()
		e, ok := b.outstandingActivities[scheduleEventID]
		if !ok || e != emptyEventID {
			logInvalidHistoryActionEvent(b.logger, tagValueActionActivityTaskStarted, eventID, fmt.Sprintf(
				"{ScheduleID: %v, Exist: %v, Value: %v}", scheduleEventID, ok, e))
			return nil
		}
		b.outstandingActivities[scheduleEventID] = eventID
	case workflow.EventType_ActivityTaskCompleted:
		scheduleEventID := event.GetActivityTaskCompletedEventAttributes().GetScheduledEventId()
		startedEventID := event.GetActivityTaskCompletedEventAttributes().GetStartedEventId()
		e, ok := b.outstandingActivities[scheduleEventID]
		if !ok || e != startedEventID {
			logInvalidHistoryActionEvent(b.logger, tagValueActionActivityTaskCompleted, eventID, fmt.Sprintf(
				"{ScheduleID: %v, StartedID: %v, Exist: %v, Value: %v}", scheduleEventID, startedEventID, ok, e))
			return nil
		}
		delete(b.outstandingActivities, scheduleEventID)
	case workflow.EventType_ActivityTaskFailed:
		scheduleEventID := event.GetActivityTaskFailedEventAttributes().GetScheduledEventId()
		startedEventID := event.GetActivityTaskFailedEventAttributes().GetStartedEventId()
		e, ok := b.outstandingActivities[scheduleEventID]
		if !ok || e != startedEventID {
			logInvalidHistoryActionEvent(b.logger, tagValueActionActivityTaskFailed, eventID, fmt.Sprintf(
				"{ScheduleID: %v, StartedID: %v, Exist: %v, Value: %v}", scheduleEventID, startedEventID, ok, e))
			return nil
		}
		delete(b.outstandingActivities, scheduleEventID)
	case workflow.EventType_WorkflowExecutionCompleted:
		if b.hasPendingTasks() || b.hasPendingDecisionTask() {
			logInvalidHistoryActionEvent(b.logger, tagValueActionCompleteWorkflow, eventID, fmt.Sprintf(
				"{OutStandingActivityTasks: %v, OutStandingDecisionTasks: %v}", len(b.outstandingActivities),
				len(b.outstandingDecisionTask)))
		}
		b.state = persistence.WorkflowStateCompleted
	case workflow.EventType_WorkflowExecutionFailed:
		if b.hasPendingTasks() || b.hasPendingDecisionTask() {
			logInvalidHistoryActionEvent(b.logger, tagValueActionFailWorkflow, eventID, fmt.Sprintf(
				"{OutStandingActivityTasks: %v, OutStandingDecisionTasks: %v}", len(b.outstandingActivities),
				len(b.outstandingDecisionTask)))
		}
		b.state = persistence.WorkflowStateCompleted
	case workflow.EventType_CompleteWorkflowExecutionFailed:
	case workflow.EventType_TimerStarted:
		e, ok := b.outstandingTimerTask[eventID]
		if ok {
			logInvalidHistoryActionEvent(b.logger, tagValueActionTimerStarted, eventID, fmt.Sprintf(
				"{Exist: %v, Value: %v}", ok, e))
			return nil
		}
		b.outstandingTimerTask[eventID] = event.GetTimerStartedEventAttributes().GetTimerId()

	case workflow.EventType_TimerFired:
		startedEventID := event.GetTimerFiredEventAttributes().GetStartedEventId()
		e, ok := b.outstandingTimerTask[startedEventID]
		if !ok {
			logInvalidHistoryActionEvent(b.logger, tagValueActionTimerFired, eventID, fmt.Sprintf(
				"{startedEventID: %v, Exist: %v, Value: %v}", startedEventID, ok, e))
			return nil
		}
		delete(b.outstandingTimerTask, startedEventID)

	default:
		logInvalidHistoryActionEvent(b.logger, tagValueActionUnknownEvent, eventID, fmt.Sprintf(
			"{EventType: %v}", event.GetEventType()))
		return nil
	}

	b.nextEventID++
	b.history = append(b.history, event)
	return event
}

func newWorkflowExecutionStartedEvent(eventID int64,
	request *workflow.StartWorkflowExecutionRequest) *workflow.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, workflow.EventType_WorkflowExecutionStarted)
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

func newDecisionTaskScheduledEvent(eventID int64, taskList string,
	startToCloseTimeoutSeconds int32) *workflow.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, workflow.EventType_DecisionTaskScheduled)
	attributes := workflow.NewDecisionTaskScheduledEventAttributes()
	attributes.TaskList = workflow.NewTaskList()
	attributes.TaskList.Name = common.StringPtr(taskList)
	attributes.StartToCloseTimeoutSeconds = common.Int32Ptr(startToCloseTimeoutSeconds)
	historyEvent.DecisionTaskScheduledEventAttributes = attributes

	return historyEvent
}

func newDecisionTaskStartedEvent(eventID, scheduledEventID int64,
	request *workflow.PollForDecisionTaskRequest) *workflow.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, workflow.EventType_DecisionTaskStarted)
	attributes := workflow.NewDecisionTaskStartedEventAttributes()
	attributes.ScheduledEventId = common.Int64Ptr(scheduledEventID)
	attributes.Identity = common.StringPtr(request.GetIdentity())
	historyEvent.DecisionTaskStartedEventAttributes = attributes

	return historyEvent
}

func newDecisionTaskCompletedEvent(eventID, scheduleEventID, startedEventID int64,
	request *workflow.RespondDecisionTaskCompletedRequest) *workflow.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, workflow.EventType_DecisionTaskCompleted)
	attributes := workflow.NewDecisionTaskCompletedEventAttributes()
	attributes.ExecutionContext = request.GetExecutionContext()
	attributes.ScheduledEventId = common.Int64Ptr(scheduleEventID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.Identity = common.StringPtr(request.GetIdentity())
	historyEvent.DecisionTaskCompletedEventAttributes = attributes

	return historyEvent
}

func newDecisionTaskTimedOutEvent(eventID, scheduleEventID int64, startedEventID int64) *workflow.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, workflow.EventType_DecisionTaskTimedOut)
	attributes := workflow.NewDecisionTaskTimedOutEventAttributes()
	attributes.ScheduledEventId = common.Int64Ptr(scheduleEventID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.TimeoutType = workflow.TimeoutTypePtr(workflow.TimeoutType_START_TO_CLOSE)
	historyEvent.DecisionTaskTimedOutEventAttributes = attributes

	return historyEvent
}

func newActivityTaskScheduledEvent(eventID int64, decisionTaskCompletedEventID int64,
	scheduleAttributes *workflow.ScheduleActivityTaskDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, workflow.EventType_ActivityTaskScheduled)
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

func newActivityTaskStartedEvent(eventID, scheduledEventID int64,
	request *workflow.PollForActivityTaskRequest) *workflow.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, workflow.EventType_ActivityTaskStarted)
	attributes := workflow.NewActivityTaskStartedEventAttributes()
	attributes.ScheduledEventId = common.Int64Ptr(scheduledEventID)
	attributes.Identity = common.StringPtr(request.GetIdentity())
	historyEvent.ActivityTaskStartedEventAttributes = attributes

	return historyEvent
}

func newActivityTaskCompletedEvent(eventID, scheduleEventID, startedEventID int64,
	request *workflow.RespondActivityTaskCompletedRequest) *workflow.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, workflow.EventType_ActivityTaskCompleted)
	attributes := workflow.NewActivityTaskCompletedEventAttributes()
	attributes.Result_ = request.GetResult_()
	attributes.ScheduledEventId = common.Int64Ptr(scheduleEventID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.Identity = common.StringPtr(request.GetIdentity())
	historyEvent.ActivityTaskCompletedEventAttributes = attributes

	return historyEvent
}

func newActivityTaskFailedEvent(eventID, scheduleEventID, startedEventID int64,
	request *workflow.RespondActivityTaskFailedRequest) *workflow.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, workflow.EventType_ActivityTaskFailed)
	attributes := workflow.NewActivityTaskFailedEventAttributes()
	attributes.Reason = common.StringPtr(request.GetReason())
	attributes.Details = request.GetDetails()
	attributes.ScheduledEventId = common.Int64Ptr(scheduleEventID)
	attributes.StartedEventId = common.Int64Ptr(startedEventID)
	attributes.Identity = common.StringPtr(request.GetIdentity())
	historyEvent.ActivityTaskFailedEventAttributes = attributes

	return historyEvent
}

func newCompleteWorkflowExecutionEvent(eventID, decisionTaskCompletedEventID int64,
	request *workflow.CompleteWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, workflow.EventType_WorkflowExecutionCompleted)
	attributes := workflow.NewWorkflowExecutionCompletedEventAttributes()
	attributes.Result_ = request.GetResult_()
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	historyEvent.WorkflowExecutionCompletedEventAttributes = attributes

	return historyEvent
}

func newFailWorkflowExecutionEvent(eventID, decisionTaskCompletedEventID int64,
	request *workflow.FailWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, workflow.EventType_WorkflowExecutionFailed)
	attributes := workflow.NewWorkflowExecutionFailedEventAttributes()
	attributes.Reason = common.StringPtr(request.GetReason())
	attributes.Details = request.GetDetails()
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	historyEvent.WorkflowExecutionFailedEventAttributes = attributes

	return historyEvent
}

func newCompleteWorkflowExecutionFailedEvent(eventID, decisionTaskCompletedEventID int64,
	cause workflow.WorkflowCompleteFailedCause) *workflow.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, workflow.EventType_CompleteWorkflowExecutionFailed)
	attributes := workflow.NewCompleteWorkflowExecutionFailedEventAttributes()
	attributes.Cause = workflow.WorkflowCompleteFailedCausePtr(cause)
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	historyEvent.CompleteWorkflowExecutionFailedEventAttributes = attributes

	return historyEvent
}

func newHistoryEvent(eventID int64, eventType workflow.EventType) *workflow.HistoryEvent {
	ts := common.Int64Ptr(time.Now().UnixNano())
	historyEvent := workflow.NewHistoryEvent()
	historyEvent.EventId = common.Int64Ptr(eventID)
	historyEvent.Timestamp = ts
	historyEvent.EventType = eventTypePtr(eventType)

	return historyEvent
}

func eventTypePtr(e workflow.EventType) *workflow.EventType {
	return &e
}
