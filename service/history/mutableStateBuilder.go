package history

import (
	"errors"
	"fmt"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"

	"github.com/uber-common/bark"
)

const (
	emptyUuid = "emptyUuid"
)

type (
	mutableStateBuilder struct {
		pendingActivityInfoIDs          map[int64]*persistence.ActivityInfo // Schedule Event ID -> Activity Info.
		pendingActivityInfoByActivityID map[string]int64                    // Activity ID -> Schedule Event ID of the activity.
		updateActivityInfos             []*persistence.ActivityInfo         // Modified activities from last update.
		deleteActivityInfo              *int64                              // Deleted activities from last update.

		pendingTimerInfoIDs map[string]*persistence.TimerInfo // User Timer ID -> Timer Info.
		updateTimerInfos    []*persistence.TimerInfo          // Modified timers from last update.
		deleteTimerInfos    []string                          // Deleted timers from last update.

		executionInfo   *persistence.WorkflowExecutionInfo // Workflow mutable state info.
		hBuilder        *historyBuilder
		eventSerializer historyEventSerializer
		logger          bark.Logger
	}

	mutableStateSessionUpdates struct {
		newEventsBuilder    *historyBuilder
		updateActivityInfos []*persistence.ActivityInfo
		deleteActivityInfo  *int64
		updateTimerInfos    []*persistence.TimerInfo
		deleteTimerInfos    []string
	}

	// TODO: This should be part of persistence layer
	decisionInfo struct {
		ScheduleID      int64
		StartedID       int64
		RequestID       string
		DecisionTimeout int32
	}
)

func newMutableStateBuilder(logger bark.Logger) *mutableStateBuilder {
	s := &mutableStateBuilder{
		updateActivityInfos:             []*persistence.ActivityInfo{},
		pendingActivityInfoIDs:          make(map[int64]*persistence.ActivityInfo),
		pendingActivityInfoByActivityID: make(map[string]int64),
		pendingTimerInfoIDs:             make(map[string]*persistence.TimerInfo),
		updateTimerInfos:                []*persistence.TimerInfo{},
		deleteTimerInfos:                []string{},
		eventSerializer:                 newJSONHistoryEventSerializer(),
		logger:                          logger,
	}
	s.hBuilder = newHistoryBuilder(s, logger)
	s.executionInfo = &persistence.WorkflowExecutionInfo{
		NextEventID:        firstEventID,
		State:              persistence.WorkflowStateCreated,
		LastProcessedEvent: emptyEventID,
	}

	return s
}

func (e *mutableStateBuilder) Load(state *persistence.WorkflowMutableState) {
	e.pendingActivityInfoIDs = state.ActivitInfos
	e.pendingTimerInfoIDs = state.TimerInfos
	e.executionInfo = state.ExecutionInfo
	for _, ai := range state.ActivitInfos {
		e.pendingActivityInfoByActivityID[ai.ActivityID] = ai.ScheduleID
	}
}

func (e *mutableStateBuilder) CloseUpdateSession() *mutableStateSessionUpdates {
	updates := &mutableStateSessionUpdates{
		newEventsBuilder:    e.hBuilder,
		updateActivityInfos: e.updateActivityInfos,
		deleteActivityInfo:  e.deleteActivityInfo,
		updateTimerInfos:    e.updateTimerInfos,
		deleteTimerInfos:    e.deleteTimerInfos,
	}

	// Clear all updates to prepare for the next session
	e.hBuilder = newHistoryBuilder(e, e.logger)
	e.updateActivityInfos = []*persistence.ActivityInfo{}
	e.updateTimerInfos = []*persistence.TimerInfo{}
	e.deleteTimerInfos = []string{}

	return updates
}

func (e *mutableStateBuilder) createNewHistoryEvent(eventType workflow.EventType) *workflow.HistoryEvent {
	eventID := e.executionInfo.NextEventID
	ts := common.Int64Ptr(time.Now().UnixNano())
	historyEvent := workflow.NewHistoryEvent()
	historyEvent.EventId = common.Int64Ptr(eventID)
	historyEvent.Timestamp = ts
	historyEvent.EventType = workflow.EventTypePtr(eventType)

	e.executionInfo.NextEventID++
	return historyEvent
}

func (e *mutableStateBuilder) getWorkflowType() *workflow.WorkflowType {
	wType := workflow.NewWorkflowType()
	wType.Name = common.StringPtr(e.executionInfo.WorkflowTypeName)

	return wType
}

func (e *mutableStateBuilder) previousDecisionStartedEvent() int64 {
	return e.executionInfo.LastProcessedEvent
}

func (e *mutableStateBuilder) GetActivityScheduledEvent(scheduleEventID int64) (*workflow.HistoryEvent, bool) {
	ai, ok := e.pendingActivityInfoIDs[scheduleEventID]
	if !ok {
		return nil, false
	}

	scheduledEvent, err := e.eventSerializer.Deserialize(ai.ScheduledEvent)
	if err != nil {
		return nil, false
	}

	return scheduledEvent, true
}

func (e *mutableStateBuilder) GetActivityStartedEvent(scheduleEventID int64) (*workflow.HistoryEvent, bool) {
	ai, ok := e.pendingActivityInfoIDs[scheduleEventID]
	if !ok {
		return nil, false
	}

	startedEvent, err := e.eventSerializer.Deserialize(ai.StartedEvent)
	if err != nil {
		return nil, false
	}

	return startedEvent, true
}

// GetActivityInfo gives details about an activity that is currently in progress.
func (e *mutableStateBuilder) GetActivityInfo(scheduleEventID int64) (*persistence.ActivityInfo, bool) {
	ai, ok := e.pendingActivityInfoIDs[scheduleEventID]
	return ai, ok
}

// GetActivityByActivityID gives details about an activity that is currently in progress.
func (e *mutableStateBuilder) GetActivityByActivityID(activityID string) (*persistence.ActivityInfo, bool) {
	eventID, ok := e.pendingActivityInfoByActivityID[activityID]
	if !ok {
		return nil, false
	}

	ai, ok := e.pendingActivityInfoIDs[eventID]
	return ai, ok
}

func (e *mutableStateBuilder) hasPendingTasks() bool {
	return len(e.pendingActivityInfoIDs) > 0 || len(e.pendingTimerInfoIDs) > 0
}

func (e *mutableStateBuilder) updateActivityProgress(ai *persistence.ActivityInfo, details []byte) {
	ai.Details = details
	e.updateActivityInfos = append(e.updateActivityInfos, ai)
}

// DeleteActivity deletes details about an activity.
func (e *mutableStateBuilder) DeleteActivity(scheduleEventID int64) error {
	a, ok := e.pendingActivityInfoIDs[scheduleEventID]
	if !ok {
		errorMsg := fmt.Sprintf("Unable to find activity with schedule event id: %v in mutable state", scheduleEventID)
		logMutableStateInvalidAction(e.logger, errorMsg)
		return errors.New(errorMsg)
	}
	delete(e.pendingActivityInfoIDs, scheduleEventID)

	_, ok = e.pendingActivityInfoByActivityID[a.ActivityID]
	if !ok {
		errorMsg := fmt.Sprintf("Unable to find activity: %v in mutable state", a.ActivityID)
		logMutableStateInvalidAction(e.logger, errorMsg)
		return errors.New(errorMsg)
	}
	delete(e.pendingActivityInfoByActivityID, a.ActivityID)

	e.deleteActivityInfo = common.Int64Ptr(scheduleEventID)
	return nil
}

// GetUserTimer gives details about a user timer.
func (e *mutableStateBuilder) GetUserTimer(timerID string) (bool, *persistence.TimerInfo) {
	a, ok := e.pendingTimerInfoIDs[timerID]
	return ok, a
}

// UpdateUserTimer updates the user timer in progress.
func (e *mutableStateBuilder) UpdateUserTimer(timerID string, ti *persistence.TimerInfo) {
	e.pendingTimerInfoIDs[timerID] = ti
	e.updateTimerInfos = append(e.updateTimerInfos, ti)
}

// DeleteUserTimer deletes an user timer.
func (e *mutableStateBuilder) DeleteUserTimer(timerID string) error {
	_, ok := e.pendingTimerInfoIDs[timerID]
	if !ok {
		errorMsg := fmt.Sprintf("Unable to find pending timer: %v", timerID)
		logMutableStateInvalidAction(e.logger, errorMsg)
		return errors.New(errorMsg)
	}
	delete(e.pendingTimerInfoIDs, timerID)

	e.deleteTimerInfos = append(e.deleteTimerInfos, timerID)
	return nil
}

// GetPendingDecision returns details about the in-progress decision task
func (e *mutableStateBuilder) GetPendingDecision(scheduleEventID int64) (*decisionInfo, bool) {
	di := &decisionInfo{
		ScheduleID:      e.executionInfo.DecisionScheduleID,
		StartedID:       e.executionInfo.DecisionStartedID,
		RequestID:       e.executionInfo.DecisionRequestID,
		DecisionTimeout: e.executionInfo.DecisionTimeout,
	}
	if scheduleEventID == di.ScheduleID {
		return di, true
	}
	return nil, false
}

func (e *mutableStateBuilder) HasPendingDecisionTask() bool {
	return e.executionInfo.DecisionScheduleID != emptyEventID
}

// UpdateDecision updates a decision task.
func (e *mutableStateBuilder) UpdateDecision(di *decisionInfo) {
	e.executionInfo.DecisionScheduleID = di.ScheduleID
	e.executionInfo.DecisionStartedID = di.StartedID
	e.executionInfo.DecisionRequestID = di.RequestID
	e.executionInfo.DecisionTimeout = di.DecisionTimeout
}

// DeleteDecision deletes a decision task.
func (e *mutableStateBuilder) DeleteDecision() {
	emptyDecisionInfo := &decisionInfo{
		ScheduleID:      emptyEventID,
		StartedID:       emptyEventID,
		RequestID:       emptyUuid,
		DecisionTimeout: 0,
	}
	e.UpdateDecision(emptyDecisionInfo)
}

// GetNextEventID returns next event ID
func (e *mutableStateBuilder) GetNextEventID() int64 {
	return e.executionInfo.NextEventID
}

func (e *mutableStateBuilder) isWorkflowExecutionRunning() bool {
	return e.executionInfo.State != persistence.WorkflowStateCompleted
}

func (e *mutableStateBuilder) AddWorkflowExecutionStartedEvent(execution workflow.WorkflowExecution,
	request *workflow.StartWorkflowExecutionRequest) *workflow.HistoryEvent {
	eventID := e.GetNextEventID()
	if eventID != firstEventID {
		logInvalidHistoryActionEvent(e.logger, tagValueActionWorkflowStarted, eventID, "")
		return nil
	}

	e.executionInfo.WorkflowID = execution.GetWorkflowId()
	e.executionInfo.RunID = execution.GetRunId()
	e.executionInfo.TaskList = request.GetTaskList().GetName()
	e.executionInfo.WorkflowTypeName = request.GetWorkflowType().GetName()
	e.executionInfo.DecisionTimeoutValue = request.GetTaskStartToCloseTimeoutSeconds()
	e.executionInfo.State = persistence.WorkflowStateCreated
	e.executionInfo.LastProcessedEvent = emptyEventID
	e.executionInfo.CreateRequestID = request.GetRequestId()
	e.executionInfo.DecisionScheduleID = emptyEventID
	e.executionInfo.DecisionStartedID = emptyEventID
	e.executionInfo.DecisionRequestID = emptyUuid
	e.executionInfo.DecisionTimeout = 0

	return e.hBuilder.AddWorkflowExecutionStartedEvent(request)
}

func (e *mutableStateBuilder) AddDecisionTaskScheduledEvent() (*workflow.HistoryEvent, *decisionInfo) {
	// Tasklist and decision timeout should already be set from workflow execution started event
	taskList := e.executionInfo.TaskList
	startToCloseTimeoutSeconds := e.executionInfo.DecisionTimeoutValue
	if e.HasPendingDecisionTask() {
		logInvalidHistoryActionEvent(e.logger, tagValueActionDecisionTaskScheduled, e.GetNextEventID(), fmt.Sprintf(
			"{Pending Decision ScheduleID: %v}", e.executionInfo.DecisionScheduleID))
		return nil, nil
	}

	newDecisionEvent := e.hBuilder.AddDecisionTaskScheduledEvent(taskList, startToCloseTimeoutSeconds)
	di := &decisionInfo{
		ScheduleID:      newDecisionEvent.GetEventId(),
		StartedID:       emptyEventID,
		RequestID:       emptyUuid,
		DecisionTimeout: newDecisionEvent.GetDecisionTaskScheduledEventAttributes().GetStartToCloseTimeoutSeconds(),
	}
	e.UpdateDecision(di)

	return newDecisionEvent, di
}

func (e *mutableStateBuilder) AddDecisionTaskStartedEvent(scheduleEventID int64, requestID string,
	request *workflow.PollForDecisionTaskRequest) *workflow.HistoryEvent {
	hasPendingDecision := e.HasPendingDecisionTask()
	pendingDecisionTask, ok := e.GetPendingDecision(scheduleEventID)
	if !hasPendingDecision || !ok || pendingDecisionTask.StartedID != emptyEventID {
		logInvalidHistoryActionEvent(e.logger, tagValueActionDecisionTaskStarted, e.GetNextEventID(), fmt.Sprintf(
			"{HasPending: %v, ScheduleID: %v, Exist: %v, Value: %v}", hasPendingDecision, scheduleEventID, ok, e))
		return nil
	}

	event := e.hBuilder.AddDecisionTaskStartedEvent(scheduleEventID, requestID, request)

	// Update mutable decision state
	e.executionInfo.DecisionStartedID = event.GetEventId()
	e.executionInfo.DecisionRequestID = requestID
	e.executionInfo.State = persistence.WorkflowStateRunning

	return event
}

func (e *mutableStateBuilder) AddDecisionTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondDecisionTaskCompletedRequest) *workflow.HistoryEvent {
	hasPendingDecision := e.HasPendingDecisionTask()
	pendingDecisionTask, ok := e.GetPendingDecision(scheduleEventID)
	if !hasPendingDecision || !ok || pendingDecisionTask.StartedID != startedEventID {
		logInvalidHistoryActionEvent(e.logger, tagValueActionDecisionTaskCompleted, e.GetNextEventID(), fmt.Sprintf(
			"{HasPending: %v, ScheduleID: %v, StartedID: %v, Exist: %v}", hasPendingDecision, scheduleEventID,
			startedEventID, ok))
		return nil
	}
	event := e.hBuilder.AddDecisionTaskCompletedEvent(scheduleEventID, startedEventID, request)

	e.executionInfo.LastProcessedEvent = startedEventID
	e.DeleteDecision()
	return event
}

func (e *mutableStateBuilder) AddDecisionTaskTimedOutEvent(scheduleEventID int64,
	startedEventID int64) *workflow.HistoryEvent {
	hasPendingDecision := e.HasPendingDecisionTask()
	pendingDecisionTask, ok := e.GetPendingDecision(scheduleEventID)
	if !hasPendingDecision || !ok || pendingDecisionTask.StartedID != startedEventID {
		logInvalidHistoryActionEvent(e.logger, tagValueActionDecisionTaskTimedOut, e.GetNextEventID(), fmt.Sprintf(
			"{HasPending: %v, ScheduleID: %v, StartedID: %v, Exist: %v}", hasPendingDecision, scheduleEventID,
			startedEventID, ok))
		return nil
	}

	event := e.hBuilder.AddDecisionTaskTimedOutEvent(scheduleEventID, startedEventID)

	e.DeleteDecision()
	return event
}

func (e *mutableStateBuilder) AddActivityTaskScheduledEvent(decisionCompletedEventID int64,
	attributes *workflow.ScheduleActivityTaskDecisionAttributes) (*workflow.HistoryEvent, *persistence.ActivityInfo) {
	if ai, ok := e.GetActivityInfo(e.GetNextEventID()); ok {
		logInvalidHistoryActionEvent(e.logger, tagValueActionActivityTaskScheduled, ai.ScheduleID, fmt.Sprintf(
			"{Exist: %v, Value: %v}", ok, ai.StartedID))
		return nil, nil
	}

	event := e.hBuilder.AddActivityTaskScheduledEvent(decisionCompletedEventID, attributes)

	scheduleEvent, err := e.eventSerializer.Serialize(event)
	if err != nil {
		return nil, nil
	}

	scheduleEventID := event.GetEventId()
	scheduleToStartTimeout := attributes.GetScheduleToStartTimeoutSeconds()
	if scheduleToStartTimeout <= 0 {
		scheduleToStartTimeout = DefaultScheduleToStartActivityTimeoutInSecs
	}
	scheduleToCloseTimeout := attributes.GetScheduleToCloseTimeoutSeconds()
	if scheduleToCloseTimeout <= 0 {
		scheduleToCloseTimeout = DefaultScheduleToCloseActivityTimeoutInSecs
	}
	startToCloseTimeout := attributes.GetStartToCloseTimeoutSeconds()
	if startToCloseTimeout <= 0 {
		startToCloseTimeout = DefaultStartToCloseActivityTimeoutInSecs
	}
	heartbeatTimeout := attributes.GetHeartbeatTimeoutSeconds()

	ai := &persistence.ActivityInfo{
		ScheduleID:             scheduleEventID,
		ScheduledEvent:         scheduleEvent,
		StartedID:              emptyEventID,
		ActivityID:             attributes.GetActivityId(),
		ScheduleToStartTimeout: scheduleToStartTimeout,
		ScheduleToCloseTimeout: scheduleToCloseTimeout,
		StartToCloseTimeout:    startToCloseTimeout,
		HeartbeatTimeout:       heartbeatTimeout,
		CancelRequested:        false,
		CancelRequestID:        emptyEventID,
	}

	e.pendingActivityInfoIDs[scheduleEventID] = ai
	e.pendingActivityInfoByActivityID[ai.ActivityID] = scheduleEventID
	e.updateActivityInfos = append(e.updateActivityInfos, ai)

	return event, ai
}

func (e *mutableStateBuilder) AddActivityTaskStartedEvent(ai *persistence.ActivityInfo, scheduleEventID int64,
	requestID string, request *workflow.PollForActivityTaskRequest) *workflow.HistoryEvent {
	if ai, ok := e.GetActivityInfo(scheduleEventID); !ok || ai.StartedID != emptyEventID {
		logInvalidHistoryActionEvent(e.logger, tagValueActionActivityTaskStarted, e.GetNextEventID(), fmt.Sprintf(
			"{ScheduleID: %v, Exist: %v}", scheduleEventID, ok))
		return nil
	}

	event := e.hBuilder.AddActivityTaskStartedEvent(scheduleEventID, requestID, request)

	ai.StartedID = event.GetEventId()
	ai.RequestID = requestID
	e.updateActivityInfos = append(e.updateActivityInfos, ai)

	return event
}

func (e *mutableStateBuilder) AddActivityTaskCompletedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondActivityTaskCompletedRequest) *workflow.HistoryEvent {
	if ai, ok := e.GetActivityInfo(scheduleEventID); !ok || ai.StartedID != startedEventID {
		logInvalidHistoryActionEvent(e.logger, tagValueActionActivityTaskCompleted, e.GetNextEventID(), fmt.Sprintf(
			"{ScheduleID: %v, StartedID: %v, Exist: %v}", scheduleEventID, startedEventID, ok))
		return nil
	}

	if err := e.DeleteActivity(scheduleEventID); err != nil {
		return nil
	}

	return e.hBuilder.AddActivityTaskCompletedEvent(scheduleEventID, startedEventID, request)
}

func (e *mutableStateBuilder) AddActivityTaskFailedEvent(scheduleEventID, startedEventID int64,
	request *workflow.RespondActivityTaskFailedRequest) *workflow.HistoryEvent {
	if ai, ok := e.GetActivityInfo(scheduleEventID); !ok || ai.StartedID != startedEventID {
		logInvalidHistoryActionEvent(e.logger, tagValueActionActivityTaskFailed, e.GetNextEventID(), fmt.Sprintf(
			"{ScheduleID: %v, StartedID: %v, Exist: %v}", scheduleEventID, startedEventID, ok))
		return nil
	}

	if err := e.DeleteActivity(scheduleEventID); err != nil {
		return nil
	}

	return e.hBuilder.AddActivityTaskFailedEvent(scheduleEventID, startedEventID, request)
}

func (e *mutableStateBuilder) AddActivityTaskTimedOutEvent(scheduleEventID, startedEventID int64,
	timeoutType workflow.TimeoutType, lastHeartBeatDetails []byte) *workflow.HistoryEvent {
	if ai, ok := e.GetActivityInfo(scheduleEventID); !ok || ai.StartedID != startedEventID ||
		((timeoutType == workflow.TimeoutType_START_TO_CLOSE || timeoutType == workflow.TimeoutType_HEARTBEAT) &&
			ai.StartedID == emptyEventID) {
		logInvalidHistoryActionEvent(e.logger, tagValueActionActivityTaskTimedOut, e.GetNextEventID(), fmt.Sprintf(
			"{ScheduleID: %v, StartedID: %v, TimeOutType: %v, Exist: %v}", scheduleEventID, startedEventID,
			timeoutType, ok))
		return nil
	}

	if err := e.DeleteActivity(scheduleEventID); err != nil {
		return nil
	}

	return e.hBuilder.AddActivityTaskTimedOutEvent(scheduleEventID, startedEventID, timeoutType, lastHeartBeatDetails)
}

func (e *mutableStateBuilder) AddActivityTaskCancelRequestedEvent(decisionCompletedEventID int64,
	activityID, identity string) (*workflow.HistoryEvent, *persistence.ActivityInfo, bool) {
	actCancelReqEvent := e.hBuilder.AddActivityTaskCancelRequestedEvent(decisionCompletedEventID, activityID)

	ai, isRunning := e.GetActivityByActivityID(activityID)
	if !isRunning || ai.CancelRequested {
		logInvalidHistoryActionEvent(e.logger, tagValueActionActivityTaskCancelRequest, e.GetNextEventID(), fmt.Sprintf(
			"{isRunning: %v, ActivityID: %v}", isRunning, activityID))
		return nil, nil, false
	}

	// - We have the activity dispatched to worker.
	// - The activity might not be heartbeat'ing, but the activity can still call RecordActivityHeartBeat()
	//   to see cancellation while reporting progress of the activity.
	ai.CancelRequested = true
	ai.CancelRequestID = actCancelReqEvent.GetEventId()
	e.updateActivityInfos = append(e.updateActivityInfos, ai)

	return actCancelReqEvent, ai, isRunning
}

func (e *mutableStateBuilder) AddRequestCancelActivityTaskFailedEvent(decisionCompletedEventID int64,
	activityID string, cause string) *workflow.HistoryEvent {
	return e.hBuilder.AddRequestCancelActivityTaskFailedEvent(decisionCompletedEventID, activityID, cause)
}

func (e *mutableStateBuilder) AddActivityTaskCanceledEvent(scheduleEventID, startedEventID int64,
	latestCancelRequestedEventID int64, details []byte, identity string) *workflow.HistoryEvent {
	ai, ok := e.GetActivityInfo(scheduleEventID)
	if !ok || ai.StartedID != startedEventID {
		logInvalidHistoryActionEvent(e.logger, tagValueActionActivityTaskCanceled, e.GetNextEventID(), fmt.Sprintf(
			"{ScheduleID: %v, StartedID: %v, Exist: %v}", scheduleEventID, startedEventID, ok))
		return nil
	}

	// Verify cancel request as well.
	if !ai.CancelRequested {
		logInvalidHistoryActionEvent(e.logger, tagValueActionActivityTaskCanceled, e.GetNextEventID(), fmt.Sprintf(
			"{No outstanding cancel request. ScheduleID: %v, ActivityID: %v, Exist: %v, Value: %v}",
			scheduleEventID, ai.ActivityID, ok, ai.StartedID))
		return nil
	}

	if err := e.DeleteActivity(scheduleEventID); err != nil {
		return nil
	}

	return e.hBuilder.AddActivityTaskCanceledEvent(scheduleEventID, startedEventID, latestCancelRequestedEventID,
		details, identity)
}

func (e *mutableStateBuilder) AddCompletedWorkflowEvent(decisionCompletedEventID int64,
	attributes *workflow.CompleteWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	if e.hasPendingTasks() || e.HasPendingDecisionTask() {
		logInvalidHistoryActionEvent(e.logger, tagValueActionCompleteWorkflow, e.GetNextEventID(), fmt.Sprintf(
			"{OutStandingActivityTasks: %v, HasPendingDecision: %v}", len(e.pendingActivityInfoIDs),
			e.HasPendingDecisionTask()))
	}

	e.executionInfo.State = persistence.WorkflowStateCompleted
	return e.hBuilder.AddCompletedWorkflowEvent(decisionCompletedEventID, attributes)
}

func (e *mutableStateBuilder) AddFailWorkflowEvent(decisionCompletedEventID int64,
	attributes *workflow.FailWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	if e.hasPendingTasks() || e.HasPendingDecisionTask() {
		logInvalidHistoryActionEvent(e.logger, tagValueActionFailWorkflow, e.GetNextEventID(), fmt.Sprintf(
			"{OutStandingActivityTasks: %v, HasPendingDecision: %v}", len(e.pendingActivityInfoIDs),
			e.HasPendingDecisionTask()))
	}

	e.executionInfo.State = persistence.WorkflowStateCompleted
	return e.hBuilder.AddFailWorkflowEvent(decisionCompletedEventID, attributes)
}

func (e *mutableStateBuilder) AddCompleteWorkflowExecutionFailedEvent(decisionCompletedEventID int64,
	cause workflow.WorkflowCompleteFailedCause) *workflow.HistoryEvent {
	return e.hBuilder.AddCompleteWorkflowExecutionFailedEvent(decisionCompletedEventID, cause)
}

func (e *mutableStateBuilder) AddTimerStartedEvent(decisionCompletedEventID int64,
	request *workflow.StartTimerDecisionAttributes) (*workflow.HistoryEvent, *persistence.TimerInfo) {
	timerID := request.GetTimerId()
	isTimerRunning, ti := e.GetUserTimer(timerID)
	if isTimerRunning {
		logInvalidHistoryActionEvent(e.logger, tagValueActionTimerStarted, e.GetNextEventID(), fmt.Sprintf(
			"{IsTimerRunning: %v, TimerID: %v, StartedID: %v}", isTimerRunning, timerID, ti.StartedID))
		return nil, nil
	}

	event := e.hBuilder.AddTimerStartedEvent(decisionCompletedEventID, request)

	fireTimeout := time.Duration(request.GetStartToFireTimeoutSeconds()) * time.Second
	// TODO: Time skew need to be taken in to account.
	expiryTime := time.Now().Add(fireTimeout)
	ti = &persistence.TimerInfo{
		TimerID:    timerID,
		ExpiryTime: expiryTime,
		StartedID:  event.GetEventId(),
		TaskID:     emptyTimerID,
	}

	e.pendingTimerInfoIDs[timerID] = ti
	e.updateTimerInfos = append(e.updateTimerInfos, ti)

	return event, ti
}

func (e *mutableStateBuilder) AddTimerFiredEvent(startedEventID int64, timerID string) *workflow.HistoryEvent {
	isTimerRunning, _ := e.GetUserTimer(timerID)
	if !isTimerRunning {
		logInvalidHistoryActionEvent(e.logger, tagValueActionTimerFired, e.GetNextEventID(), fmt.Sprintf(
			"{startedEventID: %v, Exist: %v, TimerID: %v}", startedEventID, isTimerRunning, timerID))
		return nil
	}

	// Timer is running.
	err := e.DeleteUserTimer(timerID)
	if err != nil {
		return nil
	}

	return e.hBuilder.AddTimerFiredEvent(startedEventID, timerID)
}

func (e *mutableStateBuilder) AddTimerCanceledEvent(decisionCompletedEventID int64,
	attributes *workflow.CancelTimerDecisionAttributes, identity string) *workflow.HistoryEvent {
	timerID := attributes.GetTimerId()
	isTimerRunning, ti := e.GetUserTimer(timerID)
	if !isTimerRunning {
		logInvalidHistoryActionEvent(e.logger, tagValueActionTimerCanceled, e.GetNextEventID(), fmt.Sprintf(
			"{IsTimerRunning: %v, timerID: %v}", isTimerRunning, timerID))
		return nil
	}

	// Timer is running.
	err := e.DeleteUserTimer(timerID)
	if err != nil {
		return nil
	}
	return e.hBuilder.AddTimerCanceledEvent(ti.StartedID, decisionCompletedEventID, timerID, identity)
}

func (e *mutableStateBuilder) AddCancelTimerFailedEvent(decisionCompletedEventID int64,
	attributes *workflow.CancelTimerDecisionAttributes, identity string) *workflow.HistoryEvent {
	// No Operation: We couldn't cancel it probably TIMER_ID_UNKNOWN
	timerID := attributes.GetTimerId()
	return e.hBuilder.AddCancelTimerFailedEvent(timerID, decisionCompletedEventID,
		timerCancelationMsgTimerIDUnknown, identity)
}

func (e *mutableStateBuilder) AddRecordMarkerEvent(decisionCompletedEventID int64,
	attributes *workflow.RecordMarkerDecisionAttributes) *workflow.HistoryEvent {

	return e.hBuilder.AddMarkerRecordedEvent(decisionCompletedEventID, attributes)
}

func (e *mutableStateBuilder) AddWorkflowExecutionTerminatedEvent(
	request *workflow.TerminateWorkflowExecutionRequest) *workflow.HistoryEvent {
	if e.executionInfo.State == persistence.WorkflowStateCompleted {
		logInvalidHistoryActionEvent(e.logger, tagValueActionWorkflowTerminated, e.GetNextEventID(), fmt.Sprintf(
			"{State: %v}", e.executionInfo.State))
		return nil
	}

	e.executionInfo.State = persistence.WorkflowStateCompleted
	return e.hBuilder.AddWorkflowExecutionTerminatedEvent(request)
}

func (e *mutableStateBuilder) AddWorkflowExecutionSignaled(
	request *workflow.SignalWorkflowExecutionRequest) *workflow.HistoryEvent {
	if e.executionInfo.State == persistence.WorkflowStateCompleted {
		logInvalidHistoryActionEvent(e.logger, tagValueActionWorkflowSignaled, e.GetNextEventID(), fmt.Sprintf(
			"{State: %v}", e.executionInfo.State))
		return nil
	}

	return e.hBuilder.AddWorkflowExecutionSignaledEvent(request)
}
