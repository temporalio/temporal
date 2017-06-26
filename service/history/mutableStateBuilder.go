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
	"errors"
	"fmt"
	"time"

	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/persistence"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
)

const (
	emptyUUID = "emptyUuid"
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

		pendingChildExecutionInfoIDs map[int64]*persistence.ChildExecutionInfo // Initiated Event ID -> Child Execution Info
		updateChildExecutionInfos    []*persistence.ChildExecutionInfo         // Modified ChildExecution Infos since last update
		deleteChildExecutionInfo     *int64                                    // Deleted ChildExecution Info since last update

		pendingRequestCancelInfoIDs map[int64]*persistence.RequestCancelInfo // Initiated Event ID -> RequestCancelInfo
		updateRequestCancelInfos    []*persistence.RequestCancelInfo         // Modified RequestCancel Infos since last update
		deleteRequestCancelInfo     *int64                                   // Deleted RequestCancel Info since last update

		executionInfo   *persistence.WorkflowExecutionInfo // Workflow mutable state info.
		continueAsNew   *persistence.CreateWorkflowExecutionRequest
		hBuilder        *historyBuilder
		eventSerializer historyEventSerializer
		logger          bark.Logger
	}

	mutableStateSessionUpdates struct {
		newEventsBuilder          *historyBuilder
		updateActivityInfos       []*persistence.ActivityInfo
		deleteActivityInfo        *int64
		updateTimerInfos          []*persistence.TimerInfo
		deleteTimerInfos          []string
		updateChildExecutionInfos []*persistence.ChildExecutionInfo
		deleteChildExecutionInfo  *int64
		continueAsNew             *persistence.CreateWorkflowExecutionRequest
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
		updateChildExecutionInfos:       []*persistence.ChildExecutionInfo{},
		pendingChildExecutionInfoIDs:    make(map[int64]*persistence.ChildExecutionInfo),
		updateRequestCancelInfos:        []*persistence.RequestCancelInfo{},
		pendingRequestCancelInfoIDs:     make(map[int64]*persistence.RequestCancelInfo),
		eventSerializer:                 newJSONHistoryEventSerializer(),
		logger:                          logger,
	}
	s.hBuilder = newHistoryBuilder(s, logger)
	s.executionInfo = &persistence.WorkflowExecutionInfo{
		NextEventID:        firstEventID,
		State:              persistence.WorkflowStateCreated,
		CloseStatus:        persistence.WorkflowCloseStatusNone,
		LastProcessedEvent: emptyEventID,
	}

	return s
}

func (e *mutableStateBuilder) Load(state *persistence.WorkflowMutableState) {
	e.pendingActivityInfoIDs = state.ActivitInfos
	e.pendingTimerInfoIDs = state.TimerInfos
	e.pendingChildExecutionInfoIDs = state.ChildExecutionInfos
	e.pendingRequestCancelInfoIDs = state.RequestCancelInfos
	e.executionInfo = state.ExecutionInfo
	for _, ai := range state.ActivitInfos {
		e.pendingActivityInfoByActivityID[ai.ActivityID] = ai.ScheduleID
	}
}

func (e *mutableStateBuilder) CloseUpdateSession() *mutableStateSessionUpdates {
	updates := &mutableStateSessionUpdates{
		newEventsBuilder:          e.hBuilder,
		updateActivityInfos:       e.updateActivityInfos,
		deleteActivityInfo:        e.deleteActivityInfo,
		updateTimerInfos:          e.updateTimerInfos,
		deleteTimerInfos:          e.deleteTimerInfos,
		updateChildExecutionInfos: e.updateChildExecutionInfos,
		deleteChildExecutionInfo:  e.deleteChildExecutionInfo,
		continueAsNew:             e.continueAsNew,
	}

	// Clear all updates to prepare for the next session
	e.hBuilder = newHistoryBuilder(e, e.logger)
	e.updateActivityInfos = []*persistence.ActivityInfo{}
	e.deleteActivityInfo = nil
	e.updateTimerInfos = []*persistence.TimerInfo{}
	e.deleteTimerInfos = []string{}
	e.updateChildExecutionInfos = []*persistence.ChildExecutionInfo{}
	e.deleteChildExecutionInfo = nil
	e.updateRequestCancelInfos = []*persistence.RequestCancelInfo{}
	e.deleteRequestCancelInfo = nil
	e.continueAsNew = nil

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

	return e.getHistoryEvent(ai.ScheduledEvent)
}

func (e *mutableStateBuilder) GetActivityStartedEvent(scheduleEventID int64) (*workflow.HistoryEvent, bool) {
	ai, ok := e.pendingActivityInfoIDs[scheduleEventID]
	if !ok {
		return nil, false
	}

	return e.getHistoryEvent(ai.StartedEvent)
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

// GetChildExecutionInfo gives details about a child execution that is currently in progress.
func (e *mutableStateBuilder) GetChildExecutionInfo(initiatedEventID int64) (*persistence.ChildExecutionInfo, bool) {
	ci, ok := e.pendingChildExecutionInfoIDs[initiatedEventID]
	return ci, ok
}

// GetChildExecutionInitiatedEvent reads out the ChildExecutionInitiatedEvent from mutable state for in-progress child
// executions
func (e *mutableStateBuilder) GetChildExecutionInitiatedEvent(initiatedEventID int64) (*workflow.HistoryEvent, bool) {
	ci, ok := e.pendingChildExecutionInfoIDs[initiatedEventID]
	if !ok {
		return nil, false
	}

	return e.getHistoryEvent(ci.InitiatedEvent)
}

// GetChildExecutionStartedEvent reads out the ChildExecutionStartedEvent from mutable state for in-progress child
// executions
func (e *mutableStateBuilder) GetChildExecutionStartedEvent(initiatedEventID int64) (*workflow.HistoryEvent, bool) {
	ci, ok := e.pendingChildExecutionInfoIDs[initiatedEventID]
	if !ok {
		return nil, false
	}

	return e.getHistoryEvent(ci.StartedEvent)
}

// GetRequestCancelInfo gives details about a request cancellation that is currently in progress.
func (e *mutableStateBuilder) GetRequestCancelInfo(initiatedEventID int64) (*persistence.RequestCancelInfo, bool) {
	ri, ok := e.pendingRequestCancelInfoIDs[initiatedEventID]
	return ri, ok
}

// GetCompletionEvent retrieves the workflow completion event from mutable state
func (e *mutableStateBuilder) GetCompletionEvent() (*workflow.HistoryEvent, bool) {
	serializedEvent := e.executionInfo.CompletionEvent
	if serializedEvent == nil {
		return nil, false
	}

	return e.getHistoryEvent(serializedEvent)
}

// DeletePendingChildExecution deletes details about a ChildExecutionInfo.
func (e *mutableStateBuilder) DeletePendingChildExecution(initiatedEventID int64) error {
	_, ok := e.pendingChildExecutionInfoIDs[initiatedEventID]
	if !ok {
		errorMsg := fmt.Sprintf("Unable to find child execution with initiated event id: %v in mutable state",
			initiatedEventID)
		logging.LogMutableStateInvalidAction(e.logger, errorMsg)
		return errors.New(errorMsg)
	}
	delete(e.pendingChildExecutionInfoIDs, initiatedEventID)

	e.deleteChildExecutionInfo = common.Int64Ptr(initiatedEventID)
	return nil
}

// DeletePendingRequestCancel deletes details about a RequestCancelInfo.
func (e *mutableStateBuilder) DeletePendingRequestCancel(initiatedEventID int64) error {
	_, ok := e.pendingRequestCancelInfoIDs[initiatedEventID]
	if !ok {
		errorMsg := fmt.Sprintf("Unable to find request cancellation with initiated event id: %v in mutable state",
			initiatedEventID)
		logging.LogMutableStateInvalidAction(e.logger, errorMsg)
		return errors.New(errorMsg)
	}
	delete(e.pendingRequestCancelInfoIDs, initiatedEventID)

	e.deleteRequestCancelInfo = common.Int64Ptr(initiatedEventID)
	return nil
}

func (e *mutableStateBuilder) writeCompletionEventToMutableState(completionEvent *workflow.HistoryEvent) error {
	// First check to see if this is a Child Workflow
	if e.hasParentExecution() {
		serializedEvent, err := e.eventSerializer.Serialize(completionEvent)
		if err != nil {
			return err
		}

		// Store the completion result within mutable state so we can communicate the result to parent execution
		// during the processing of DeleteTransferTask
		e.executionInfo.CompletionEvent = serializedEvent
	}

	return nil
}

func (e *mutableStateBuilder) hasPendingTasks() bool {
	return len(e.pendingActivityInfoIDs) > 0 || len(e.pendingTimerInfoIDs) > 0
}

func (e *mutableStateBuilder) hasParentExecution() bool {
	return e.executionInfo.ParentDomainID != "" && e.executionInfo.ParentWorkflowID != ""
}

func (e *mutableStateBuilder) updateActivityProgress(ai *persistence.ActivityInfo,
	request *workflow.RecordActivityTaskHeartbeatRequest) {
	ai.Details = request.GetDetails()
	ai.LastHeartBeatUpdatedTime = time.Now()
	e.updateActivityInfos = append(e.updateActivityInfos, ai)
}

// DeleteActivity deletes details about an activity.
func (e *mutableStateBuilder) DeleteActivity(scheduleEventID int64) error {
	a, ok := e.pendingActivityInfoIDs[scheduleEventID]
	if !ok {
		errorMsg := fmt.Sprintf("Unable to find activity with schedule event id: %v in mutable state", scheduleEventID)
		logging.LogMutableStateInvalidAction(e.logger, errorMsg)
		return errors.New(errorMsg)
	}
	delete(e.pendingActivityInfoIDs, scheduleEventID)

	_, ok = e.pendingActivityInfoByActivityID[a.ActivityID]
	if !ok {
		errorMsg := fmt.Sprintf("Unable to find activity: %v in mutable state", a.ActivityID)
		logging.LogMutableStateInvalidAction(e.logger, errorMsg)
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
		logging.LogMutableStateInvalidAction(e.logger, errorMsg)
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
		RequestID:       emptyUUID,
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

func (e *mutableStateBuilder) isCancelRequested() (bool, string) {
	if e.executionInfo.CancelRequested {
		return e.executionInfo.CancelRequested, e.executionInfo.CancelRequestID
	}

	return false, ""
}

func (e *mutableStateBuilder) getHistoryEvent(serializedEvent []byte) (*workflow.HistoryEvent, bool) {
	event, err := e.eventSerializer.Deserialize(serializedEvent)
	if err != nil {
		return nil, false
	}

	return event, true
}

func (e *mutableStateBuilder) AddWorkflowExecutionStartedEventForContinueAsNew(domainID string,
	execution workflow.WorkflowExecution, previousExecutionState *mutableStateBuilder,
	attributes *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	taskList := previousExecutionState.executionInfo.TaskList
	if attributes.IsSetTaskList() {
		taskList = attributes.GetTaskList().GetName()
	}
	tl := workflow.NewTaskList()
	tl.Name = common.StringPtr(taskList)

	workflowType := previousExecutionState.executionInfo.WorkflowTypeName
	if attributes.IsSetWorkflowType() {
		workflowType = attributes.GetWorkflowType().GetName()
	}
	wType := workflow.NewWorkflowType()
	wType.Name = common.StringPtr(workflowType)

	decisionTimeout := previousExecutionState.executionInfo.DecisionTimeoutValue
	if attributes.IsSetTaskStartToCloseTimeoutSeconds() {
		decisionTimeout = attributes.GetTaskStartToCloseTimeoutSeconds()
	}

	createRequest := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(previousExecutionState.executionInfo.DomainID),
		WorkflowId:                          common.StringPtr(execution.GetWorkflowId()),
		TaskList:                            tl,
		WorkflowType:                        wType,
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(decisionTimeout),
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(attributes.GetExecutionStartToCloseTimeoutSeconds()),
		Input:    attributes.GetInput(),
		Identity: nil,
	}

	return e.AddWorkflowExecutionStartedEvent(domainID, execution, createRequest)
}

func (e *mutableStateBuilder) AddWorkflowExecutionStartedEvent(domainID string, execution workflow.WorkflowExecution,
	request *workflow.StartWorkflowExecutionRequest) *workflow.HistoryEvent {
	eventID := e.GetNextEventID()
	if eventID != firstEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowStarted, eventID, "")
		return nil
	}

	e.executionInfo.DomainID = domainID
	e.executionInfo.WorkflowID = execution.GetWorkflowId()
	e.executionInfo.RunID = execution.GetRunId()
	e.executionInfo.TaskList = request.GetTaskList().GetName()
	e.executionInfo.WorkflowTypeName = request.GetWorkflowType().GetName()
	e.executionInfo.DecisionTimeoutValue = request.GetTaskStartToCloseTimeoutSeconds()

	e.executionInfo.State = persistence.WorkflowStateCreated
	e.executionInfo.CloseStatus = persistence.WorkflowCloseStatusNone
	e.executionInfo.LastProcessedEvent = emptyEventID
	e.executionInfo.CreateRequestID = request.GetRequestId()
	e.executionInfo.DecisionScheduleID = emptyEventID
	e.executionInfo.DecisionStartedID = emptyEventID
	e.executionInfo.DecisionRequestID = emptyUUID
	e.executionInfo.DecisionTimeout = 0

	return e.hBuilder.AddWorkflowExecutionStartedEvent(request)
}

func (e *mutableStateBuilder) AddDecisionTaskScheduledEvent() (*workflow.HistoryEvent, *decisionInfo) {
	// Tasklist and decision timeout should already be set from workflow execution started event
	taskList := e.executionInfo.TaskList
	startToCloseTimeoutSeconds := e.executionInfo.DecisionTimeoutValue
	if e.HasPendingDecisionTask() {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionDecisionTaskScheduled, e.GetNextEventID(), fmt.Sprintf(
			"{Pending Decision ScheduleID: %v}", e.executionInfo.DecisionScheduleID))
		return nil, nil
	}

	newDecisionEvent := e.hBuilder.AddDecisionTaskScheduledEvent(taskList, startToCloseTimeoutSeconds)
	di := &decisionInfo{
		ScheduleID:      newDecisionEvent.GetEventId(),
		StartedID:       emptyEventID,
		RequestID:       emptyUUID,
		DecisionTimeout: startToCloseTimeoutSeconds,
	}
	e.UpdateDecision(di)

	return newDecisionEvent, di
}

func (e *mutableStateBuilder) AddDecisionTaskStartedEvent(scheduleEventID int64, requestID string,
	request *workflow.PollForDecisionTaskRequest) *workflow.HistoryEvent {
	hasPendingDecision := e.HasPendingDecisionTask()
	pendingDecisionTask, ok := e.GetPendingDecision(scheduleEventID)
	if !hasPendingDecision || !ok || pendingDecisionTask.StartedID != emptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionDecisionTaskStarted, e.GetNextEventID(), fmt.Sprintf(
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
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionDecisionTaskCompleted, e.GetNextEventID(), fmt.Sprintf(
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
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionDecisionTaskTimedOut, e.GetNextEventID(), fmt.Sprintf(
			"{HasPending: %v, ScheduleID: %v, StartedID: %v, Exist: %v}", hasPendingDecision, scheduleEventID,
			startedEventID, ok))
		return nil
	}

	event := e.hBuilder.AddDecisionTaskTimedOutEvent(scheduleEventID, startedEventID)

	e.DeleteDecision()
	return event
}

func (e *mutableStateBuilder) AddDecisionTaskFailedEvent(scheduleEventID int64,
	startedEventID int64, cause workflow.DecisionTaskFailedCause,
	request *workflow.RespondDecisionTaskCompletedRequest) *workflow.HistoryEvent {
	hasPendingDecision := e.HasPendingDecisionTask()
	pendingDecisionTask, ok := e.GetPendingDecision(scheduleEventID)
	if !hasPendingDecision || !ok || pendingDecisionTask.StartedID != startedEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionDecisionTaskFailed, e.GetNextEventID(), fmt.Sprintf(
			"{HasPending: %v, ScheduleID: %v, StartedID: %v, Exist: %v}", hasPendingDecision, scheduleEventID,
			startedEventID, ok))
		return nil
	}

	event := e.hBuilder.AddDecisionTaskFailedEvent(scheduleEventID, startedEventID, cause, request)

	e.DeleteDecision()
	return event
}

func (e *mutableStateBuilder) AddActivityTaskScheduledEvent(decisionCompletedEventID int64,
	attributes *workflow.ScheduleActivityTaskDecisionAttributes) (*workflow.HistoryEvent, *persistence.ActivityInfo) {
	if ai, ok := e.GetActivityInfo(e.GetNextEventID()); ok {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionActivityTaskScheduled, ai.ScheduleID, fmt.Sprintf(
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
		ScheduleID:               scheduleEventID,
		ScheduledEvent:           scheduleEvent,
		StartedID:                emptyEventID,
		ActivityID:               attributes.GetActivityId(),
		ScheduleToStartTimeout:   scheduleToStartTimeout,
		ScheduleToCloseTimeout:   scheduleToCloseTimeout,
		StartToCloseTimeout:      startToCloseTimeout,
		HeartbeatTimeout:         heartbeatTimeout,
		CancelRequested:          false,
		CancelRequestID:          emptyEventID,
		LastHeartBeatUpdatedTime: time.Time{},
	}

	e.pendingActivityInfoIDs[scheduleEventID] = ai
	e.pendingActivityInfoByActivityID[ai.ActivityID] = scheduleEventID
	e.updateActivityInfos = append(e.updateActivityInfos, ai)

	return event, ai
}

func (e *mutableStateBuilder) AddActivityTaskStartedEvent(ai *persistence.ActivityInfo, scheduleEventID int64,
	requestID string, request *workflow.PollForActivityTaskRequest) *workflow.HistoryEvent {
	if ai, ok := e.GetActivityInfo(scheduleEventID); !ok || ai.StartedID != emptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionActivityTaskStarted, e.GetNextEventID(), fmt.Sprintf(
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
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionActivityTaskCompleted, e.GetNextEventID(), fmt.Sprintf(
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
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionActivityTaskFailed, e.GetNextEventID(), fmt.Sprintf(
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
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionActivityTaskTimedOut, e.GetNextEventID(), fmt.Sprintf(
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
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionActivityTaskCancelRequest, e.GetNextEventID(), fmt.Sprintf(
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
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionActivityTaskCanceled, e.GetNextEventID(), fmt.Sprintf(
			"{ScheduleID: %v, StartedID: %v, Exist: %v}", scheduleEventID, startedEventID, ok))
		return nil
	}

	// Verify cancel request as well.
	if !ai.CancelRequested {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionActivityTaskCanceled, e.GetNextEventID(), fmt.Sprintf(
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
	if e.executionInfo.State == persistence.WorkflowStateCompleted {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionCompleteWorkflow, e.GetNextEventID(), fmt.Sprintf(
			"{State: %v}", e.executionInfo.State))
	}

	e.executionInfo.State = persistence.WorkflowStateCompleted
	e.executionInfo.CloseStatus = persistence.WorkflowCloseStatusCompleted
	event := e.hBuilder.AddCompletedWorkflowEvent(decisionCompletedEventID, attributes)
	e.writeCompletionEventToMutableState(event)

	return event
}

func (e *mutableStateBuilder) AddFailWorkflowEvent(decisionCompletedEventID int64,
	attributes *workflow.FailWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	if e.executionInfo.State == persistence.WorkflowStateCompleted {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionFailWorkflow, e.GetNextEventID(), fmt.Sprintf(
			"{State: %v}", e.executionInfo.State))
	}

	e.executionInfo.State = persistence.WorkflowStateCompleted
	e.executionInfo.CloseStatus = persistence.WorkflowCloseStatusFailed
	event := e.hBuilder.AddFailWorkflowEvent(decisionCompletedEventID, attributes)
	e.writeCompletionEventToMutableState(event)

	return event
}

func (e *mutableStateBuilder) AddWorkflowExecutionCancelRequestedEvent(cause string,
	request *h.RequestCancelWorkflowExecutionRequest) *workflow.HistoryEvent {
	if e.executionInfo.State == persistence.WorkflowStateCompleted || e.executionInfo.CancelRequested {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionRequestCancelWorkflow, e.GetNextEventID(),
			fmt.Sprintf("{State: %v, CancelRequested: %v, RequestID: %v}", e.executionInfo.State,
				e.executionInfo.CancelRequested, e.executionInfo.CancelRequestID))

		return nil
	}

	e.executionInfo.CancelRequested = true
	if request.GetCancelRequest().IsSetRequestId() {
		e.executionInfo.CancelRequestID = request.GetCancelRequest().GetRequestId()
	}

	return e.hBuilder.AddWorkflowExecutionCancelRequestedEvent(cause, request)
}

func (e *mutableStateBuilder) AddWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID int64,
	attributes *workflow.CancelWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent {
	if e.executionInfo.State == persistence.WorkflowStateCompleted {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowCanceled, e.GetNextEventID(), fmt.Sprintf(
			"{State: %v}", e.executionInfo.State))
	}

	e.executionInfo.State = persistence.WorkflowStateCompleted
	e.executionInfo.CloseStatus = persistence.WorkflowCloseStatusCanceled
	event := e.hBuilder.AddWorkflowExecutionCanceledEvent(decisionTaskCompletedEventID, attributes)
	e.writeCompletionEventToMutableState(event)

	return event
}

func (e *mutableStateBuilder) AddRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID int64,
	cancelRequestID string,
	request *workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent,
	*persistence.RequestCancelInfo) {
	event := e.hBuilder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID, request)
	if event == nil {
		return nil, nil
	}

	initiatedEventID := event.GetEventId()
	ri := &persistence.RequestCancelInfo{
		InitiatedID:     initiatedEventID,
		CancelRequestID: cancelRequestID,
	}

	e.pendingRequestCancelInfoIDs[initiatedEventID] = ri
	e.updateRequestCancelInfos = append(e.updateRequestCancelInfos, ri)

	return event, ri
}

func (e *mutableStateBuilder) AddExternalWorkflowExecutionCancelRequested(initiatedID int64,
	domain, workflowID, runID string) *workflow.HistoryEvent {
	_, ok := e.GetRequestCancelInfo(initiatedID)
	if !ok {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowCancelRequested, e.GetNextEventID(),
			fmt.Sprintf("{InitiatedID: %v, Exist: %v}", initiatedID, ok))
	}

	if e.DeletePendingRequestCancel(initiatedID) == nil {
		return e.hBuilder.AddExternalWorkflowExecutionCancelRequested(initiatedID, domain, workflowID, runID)
	}

	return nil
}

func (e *mutableStateBuilder) AddRequestCancelExternalWorkflowExecutionFailedEvent(
	decisionTaskCompletedEventID, initiatedID int64,
	domain, workflowID, runID string, cause workflow.CancelExternalWorkflowExecutionFailedCause) *workflow.HistoryEvent {
	_, ok := e.GetRequestCancelInfo(initiatedID)
	if !ok {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowCancelFailed, e.GetNextEventID(),
			fmt.Sprintf("{InitiatedID: %v, Exist: %v}", initiatedID, ok))
	}

	if e.DeletePendingRequestCancel(initiatedID) == nil {
		return e.hBuilder.AddRequestCancelExternalWorkflowExecutionFailedEvent(decisionTaskCompletedEventID, initiatedID,
			domain, workflowID, runID, cause)
	}

	return nil
}

func (e *mutableStateBuilder) AddTimerStartedEvent(decisionCompletedEventID int64,
	request *workflow.StartTimerDecisionAttributes) (*workflow.HistoryEvent, *persistence.TimerInfo) {
	timerID := request.GetTimerId()
	isTimerRunning, ti := e.GetUserTimer(timerID)
	if isTimerRunning {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionTimerStarted, e.GetNextEventID(), fmt.Sprintf(
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
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionTimerFired, e.GetNextEventID(), fmt.Sprintf(
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
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionTimerCanceled, e.GetNextEventID(), fmt.Sprintf(
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
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowTerminated, e.GetNextEventID(), fmt.Sprintf(
			"{State: %v}", e.executionInfo.State))
		return nil
	}

	e.executionInfo.State = persistence.WorkflowStateCompleted
	e.executionInfo.CloseStatus = persistence.WorkflowCloseStatusTerminated
	event := e.hBuilder.AddWorkflowExecutionTerminatedEvent(request)
	e.writeCompletionEventToMutableState(event)

	return event
}

func (e *mutableStateBuilder) AddWorkflowExecutionSignaled(
	request *workflow.SignalWorkflowExecutionRequest) *workflow.HistoryEvent {
	if e.executionInfo.State == persistence.WorkflowStateCompleted {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionWorkflowSignaled, e.GetNextEventID(), fmt.Sprintf(
			"{State: %v}", e.executionInfo.State))
		return nil
	}

	return e.hBuilder.AddWorkflowExecutionSignaledEvent(request)
}

func (e *mutableStateBuilder) AddContinueAsNewEvent(decisionCompletedEventID int64, domainID, newRunID string,
	attributes *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, *mutableStateBuilder,
	error) {
	if e.hasPendingTasks() || e.HasPendingDecisionTask() {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionContinueAsNew, e.GetNextEventID(), fmt.Sprintf(
			"{OutStandingActivityTasks: %v, HasPendingDecision: %v}", len(e.pendingActivityInfoIDs),
			e.HasPendingDecisionTask()))
	}

	e.executionInfo.State = persistence.WorkflowStateCompleted
	e.executionInfo.CloseStatus = persistence.WorkflowCloseStatusContinuedAsNew
	newExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(e.executionInfo.WorkflowID),
		RunId:      common.StringPtr(newRunID),
	}

	newStateBuilder := newMutableStateBuilder(e.logger)
	startedEvent := newStateBuilder.AddWorkflowExecutionStartedEventForContinueAsNew(domainID, newExecution, e,
		attributes)
	if startedEvent == nil {
		return nil, nil, &workflow.InternalServiceError{Message: "Failed to add workflow execution started event."}
	}

	_, di := newStateBuilder.AddDecisionTaskScheduledEvent()
	if di == nil {
		return nil, nil, &workflow.InternalServiceError{Message: "Failed to add decision started event."}
	}

	parentDomainID := ""
	var parentExecution *workflow.WorkflowExecution
	initiatedID := emptyEventID
	if e.hasParentExecution() {
		parentDomainID = e.executionInfo.ParentDomainID
		parentExecution = &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(e.executionInfo.ParentWorkflowID),
			RunId:      common.StringPtr(e.executionInfo.ParentRunID),
		}
		initiatedID = e.executionInfo.InitiatedID
	}

	e.continueAsNew = &persistence.CreateWorkflowExecutionRequest{
		RequestID:            uuid.New(),
		DomainID:             domainID,
		Execution:            newExecution,
		ParentDomainID:       parentDomainID,
		ParentExecution:      parentExecution,
		InitiatedID:          initiatedID,
		TaskList:             newStateBuilder.executionInfo.TaskList,
		WorkflowTypeName:     newStateBuilder.executionInfo.WorkflowTypeName,
		DecisionTimeoutValue: newStateBuilder.executionInfo.DecisionTimeoutValue,
		ExecutionContext:     nil,
		NextEventID:          newStateBuilder.GetNextEventID(),
		LastProcessedEvent:   common.EmptyEventID,
		TransferTasks: []persistence.Task{&persistence.DecisionTask{
			DomainID: domainID, TaskList: newStateBuilder.executionInfo.TaskList, ScheduleID: di.ScheduleID,
		}},
		DecisionScheduleID:          di.ScheduleID,
		DecisionStartedID:           di.StartedID,
		DecisionStartToCloseTimeout: di.DecisionTimeout,
		ContinueAsNew:               true,
	}

	return e.hBuilder.AddContinuedAsNewEvent(decisionCompletedEventID, newRunID, attributes), newStateBuilder, nil
}

func (e *mutableStateBuilder) AddStartChildWorkflowExecutionInitiatedEvent(decisionCompletedEventID int64,
	createRequestID string, attributes *workflow.StartChildWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent,
	*persistence.ChildExecutionInfo) {
	event := e.hBuilder.AddStartChildWorkflowExecutionInitiatedEvent(decisionCompletedEventID, attributes)

	initiatedEvent, err := e.eventSerializer.Serialize(event)
	if err != nil {
		return nil, nil
	}

	initiatedEventID := event.GetEventId()
	ci := &persistence.ChildExecutionInfo{
		InitiatedID:     initiatedEventID,
		InitiatedEvent:  initiatedEvent,
		StartedID:       emptyEventID,
		CreateRequestID: createRequestID,
	}

	e.pendingChildExecutionInfoIDs[initiatedEventID] = ci
	e.updateChildExecutionInfos = append(e.updateChildExecutionInfos, ci)

	return event, ci
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionStartedEvent(domain string, execution *workflow.WorkflowExecution,
	workflowType *workflow.WorkflowType, initiatedID int64) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID != emptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionChildExecutionStarted, e.GetNextEventID(), fmt.Sprintf(
			"{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	event := e.hBuilder.AddChildWorkflowExecutionStartedEvent(domain, execution, workflowType, initiatedID)

	startedEvent, err := e.eventSerializer.Serialize(event)
	if err != nil {
		return nil
	}

	ci.StartedID = event.GetEventId()
	ci.StartedEvent = startedEvent
	e.updateChildExecutionInfos = append(e.updateChildExecutionInfos, ci)

	return event
}

func (e *mutableStateBuilder) AddStartChildWorkflowExecutionFailedEvent(initiatedID int64,
	cause workflow.ChildWorkflowExecutionFailedCause,
	initiatedEventAttributes *workflow.StartChildWorkflowExecutionInitiatedEventAttributes) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID != emptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionStartChildExecutionFailed, e.GetNextEventID(), fmt.Sprintf(
			"{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	if err := e.DeletePendingChildExecution(initiatedID); err == nil {
		return e.hBuilder.AddStartChildWorkflowExecutionFailedEvent(initiatedID, cause, initiatedEventAttributes)
	}

	return nil
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionCompletedEvent(initiatedID int64,
	childExecution *workflow.WorkflowExecution,
	attributes *workflow.WorkflowExecutionCompletedEventAttributes) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == emptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionChildExecutionCompleted, e.GetNextEventID(), fmt.Sprintf(
			"{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	startedEvent, _ := e.getHistoryEvent(ci.StartedEvent)

	domain := startedEvent.GetChildWorkflowExecutionStartedEventAttributes().GetDomain()
	workflowType := startedEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowType()

	if err := e.DeletePendingChildExecution(initiatedID); err == nil {
		return e.hBuilder.AddChildWorkflowExecutionCompletedEvent(domain, childExecution, workflowType, ci.InitiatedID,
			ci.StartedID, attributes)
	}

	return nil
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionFailedEvent(initiatedID int64,
	childExecution *workflow.WorkflowExecution,
	attributes *workflow.WorkflowExecutionFailedEventAttributes) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == emptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionChildExecutionFailed, e.GetNextEventID(), fmt.Sprintf(
			"{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	startedEvent, _ := e.getHistoryEvent(ci.StartedEvent)

	domain := startedEvent.GetChildWorkflowExecutionStartedEventAttributes().GetDomain()
	workflowType := startedEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowType()

	if err := e.DeletePendingChildExecution(initiatedID); err == nil {
		return e.hBuilder.AddChildWorkflowExecutionFailedEvent(domain, childExecution, workflowType, ci.InitiatedID,
			ci.StartedID, attributes)
	}

	return nil
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionCanceledEvent(initiatedID int64,
	childExecution *workflow.WorkflowExecution,
	attributes *workflow.WorkflowExecutionCanceledEventAttributes) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == emptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionChildExecutionCanceled, e.GetNextEventID(), fmt.Sprintf(
			"{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	startedEvent, _ := e.getHistoryEvent(ci.StartedEvent)

	domain := startedEvent.GetChildWorkflowExecutionStartedEventAttributes().GetDomain()
	workflowType := startedEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowType()

	if err := e.DeletePendingChildExecution(initiatedID); err == nil {
		return e.hBuilder.AddChildWorkflowExecutionCanceledEvent(domain, childExecution, workflowType, ci.InitiatedID,
			ci.StartedID, attributes)
	}

	return nil
}

func (e *mutableStateBuilder) AddChildWorkflowExecutionTerminatedEvent(initiatedID int64,
	childExecution *workflow.WorkflowExecution,
	attributes *workflow.WorkflowExecutionTerminatedEventAttributes) *workflow.HistoryEvent {
	ci, ok := e.GetChildExecutionInfo(initiatedID)
	if !ok || ci.StartedID == emptyEventID {
		logging.LogInvalidHistoryActionEvent(e.logger, logging.TagValueActionChildExecutionTerminated, e.GetNextEventID(), fmt.Sprintf(
			"{InitiatedID: %v, Exist: %v}", initiatedID, ok))
		return nil
	}

	startedEvent, _ := e.getHistoryEvent(ci.StartedEvent)

	domain := startedEvent.GetChildWorkflowExecutionStartedEventAttributes().GetDomain()
	workflowType := startedEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowType()

	if err := e.DeletePendingChildExecution(initiatedID); err == nil {
		return e.hBuilder.AddChildWorkflowExecutionTerminatedEvent(domain, childExecution, workflowType, ci.InitiatedID,
			ci.StartedID, attributes)
	}

	return nil
}
