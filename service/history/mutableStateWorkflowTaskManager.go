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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination mutableStateWorkflowTaskManager_mock.go

package history

import (
	"fmt"
	"math"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
)

type (
	mutableStateWorkflowTaskManager interface {
		ReplicateWorkflowTaskScheduledEvent(
			version int64,
			scheduleID int64,
			taskQueue string,
			startToCloseTimeoutSeconds int32,
			attempt int64,
			scheduleTimestamp int64,
			originalScheduledTimestamp int64,
		) (*decisionInfo, error)
		ReplicateTransientWorkflowTaskScheduled() (*decisionInfo, error)
		ReplicateWorkflowTaskStartedEvent(
			decision *decisionInfo,
			version int64,
			scheduleID int64,
			startedID int64,
			requestID string,
			timestamp int64,
		) (*decisionInfo, error)
		ReplicateWorkflowTaskCompletedEvent(event *historypb.HistoryEvent) error
		ReplicateWorkflowTaskFailedEvent() error
		ReplicateWorkflowTaskTimedOutEvent(timeoutType enumspb.TimeoutType) error

		AddWorkflowTaskScheduleToStartTimeoutEvent(scheduleEventID int64) (*historypb.HistoryEvent, error)
		AddWorkflowTaskScheduledEventAsHeartbeat(
			bypassTaskGeneration bool,
			originalScheduledTimestamp int64,
		) (*decisionInfo, error)
		AddWorkflowTaskScheduledEvent(bypassTaskGeneration bool) (*decisionInfo, error)
		AddFirstWorkflowTaskScheduled(startEvent *historypb.HistoryEvent) error
		AddWorkflowTaskStartedEvent(
			scheduleEventID int64,
			requestID string,
			request *workflowservice.PollWorkflowTaskQueueRequest,
		) (*historypb.HistoryEvent, *decisionInfo, error)
		AddWorkflowTaskCompletedEvent(
			scheduleEventID int64,
			startedEventID int64,
			request *workflowservice.RespondWorkflowTaskCompletedRequest,
			maxResetPoints int,
		) (*historypb.HistoryEvent, error)
		AddWorkflowTaskFailedEvent(
			scheduleEventID int64,
			startedEventID int64,
			cause enumspb.WorkflowTaskFailedCause,
			failure *failurepb.Failure,
			identity string,
			binChecksum string,
			baseRunID string,
			newRunID string,
			forkEventVersion int64,
		) (*historypb.HistoryEvent, error)
		AddWorkflowTaskTimedOutEvent(scheduleEventID int64, startedEventID int64) (*historypb.HistoryEvent, error)

		FailDecision(incrementAttempt bool)
		DeleteDecision()
		UpdateDecision(decision *decisionInfo)

		HasPendingDecision() bool
		GetPendingDecision() (*decisionInfo, bool)
		HasInFlightDecision() bool
		GetInFlightDecision() (*decisionInfo, bool)
		HasProcessedOrPendingDecision() bool
		GetDecisionInfo(scheduleEventID int64) (*decisionInfo, bool)

		CreateTransientDecisionEvents(decision *decisionInfo, identity string) (*historypb.HistoryEvent, *historypb.HistoryEvent)
	}

	mutableStateWorkflowTaskManagerImpl struct {
		msb *mutableStateBuilder
	}
)

func newMutableStateWorkflowTaskManager(msb *mutableStateBuilder) mutableStateWorkflowTaskManager {
	return &mutableStateWorkflowTaskManagerImpl{
		msb: msb,
	}
}

func (m *mutableStateWorkflowTaskManagerImpl) ReplicateWorkflowTaskScheduledEvent(
	version int64,
	scheduleID int64,
	taskQueue string,
	startToCloseTimeoutSeconds int32,
	attempt int64,
	scheduleTimestamp int64,
	originalScheduledTimestamp int64,
) (*decisionInfo, error) {

	// set workflow state to running, since decision is scheduled
	// NOTE: for zombie workflow, should not change the state
	state, _ := m.msb.GetWorkflowStateStatus()
	if state != enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		if err := m.msb.UpdateWorkflowStateStatus(
			enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		); err != nil {
			return nil, err
		}
	}

	decision := &decisionInfo{
		Version:                    version,
		ScheduleID:                 scheduleID,
		StartedID:                  common.EmptyEventID,
		RequestID:                  emptyUUID,
		DecisionTimeout:            startToCloseTimeoutSeconds,
		TaskQueue:                  taskQueue,
		Attempt:                    attempt,
		ScheduledTimestamp:         scheduleTimestamp,
		StartedTimestamp:           0,
		OriginalScheduledTimestamp: originalScheduledTimestamp,
	}

	m.UpdateDecision(decision)
	return decision, nil
}

func (m *mutableStateWorkflowTaskManagerImpl) ReplicateTransientWorkflowTaskScheduled() (*decisionInfo, error) {
	if m.HasPendingDecision() || m.msb.GetExecutionInfo().DecisionAttempt == 0 {
		return nil, nil
	}

	// the schedule ID for this decision is guaranteed to be wrong
	// since the next event ID is assigned at the very end of when
	// all events are applied for replication.
	// this is OK
	// 1. if a failover happen just after this transient decision,
	// AddWorkflowTaskStartedEvent will handle the correction of schedule ID
	// and set the attempt to 0
	// 2. if no failover happen during the life time of this transient decision
	// then ReplicateWorkflowTaskScheduledEvent will overwrite everything
	// including the decision schedule ID
	decision := &decisionInfo{
		Version:            m.msb.GetCurrentVersion(),
		ScheduleID:         m.msb.GetNextEventID(),
		StartedID:          common.EmptyEventID,
		RequestID:          emptyUUID,
		DecisionTimeout:    m.msb.GetExecutionInfo().WorkflowTaskTimeout,
		TaskQueue:          m.msb.GetExecutionInfo().TaskQueue,
		Attempt:            m.msb.GetExecutionInfo().DecisionAttempt,
		ScheduledTimestamp: m.msb.timeSource.Now().UnixNano(),
		StartedTimestamp:   0,
	}

	m.UpdateDecision(decision)
	return decision, nil
}

func (m *mutableStateWorkflowTaskManagerImpl) ReplicateWorkflowTaskStartedEvent(
	decision *decisionInfo,
	version int64,
	scheduleID int64,
	startedID int64,
	requestID string,
	timestamp int64,
) (*decisionInfo, error) {
	// Replicator calls it with a nil decision info, and it is safe to always lookup the decision in this case as it
	// does not have to deal with transient decision case.
	var ok bool
	if decision == nil {
		decision, ok = m.GetDecisionInfo(scheduleID)
		if !ok {
			return nil, serviceerror.NewInternal(fmt.Sprintf("unable to find decision: %v", scheduleID))
		}
		// setting decision attempt to 0 for workflow task replication
		// this mainly handles transient decision completion
		// for transient decision, active side will write 2 batch in a "transaction"
		// 1. workflow task scheduled & workflow task started
		// 2. workflow task completed & other events
		// since we need to treat each individual event batch as one transaction
		// certain "magic" needs to be done, i.e. setting attempt to 0 so
		// if first batch is replicated, but not the second one, decision can be correctly timed out
		decision.Attempt = 0
	}

	// Update mutable decision state
	decision = &decisionInfo{
		Version:                    version,
		ScheduleID:                 scheduleID,
		StartedID:                  startedID,
		RequestID:                  requestID,
		DecisionTimeout:            decision.DecisionTimeout,
		Attempt:                    decision.Attempt,
		StartedTimestamp:           timestamp,
		ScheduledTimestamp:         decision.ScheduledTimestamp,
		TaskQueue:                  decision.TaskQueue,
		OriginalScheduledTimestamp: decision.OriginalScheduledTimestamp,
	}

	m.UpdateDecision(decision)
	return decision, nil
}

func (m *mutableStateWorkflowTaskManagerImpl) ReplicateWorkflowTaskCompletedEvent(
	event *historypb.HistoryEvent,
) error {
	m.beforeAddWorkflowTaskCompletedEvent()
	return m.afterAddWorkflowTaskCompletedEvent(event, math.MaxInt32)
}

func (m *mutableStateWorkflowTaskManagerImpl) ReplicateWorkflowTaskFailedEvent() error {
	m.FailDecision(true)
	return nil
}

func (m *mutableStateWorkflowTaskManagerImpl) ReplicateWorkflowTaskTimedOutEvent(
	timeoutType enumspb.TimeoutType,
) error {
	incrementAttempt := true
	// Do not increment decision attempt in the case of sticky timeout to prevent creating next decision as transient
	if timeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START {
		incrementAttempt = false
	}
	m.FailDecision(incrementAttempt)
	return nil
}

func (m *mutableStateWorkflowTaskManagerImpl) AddWorkflowTaskScheduleToStartTimeoutEvent(
	scheduleEventID int64,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskTimedOut
	if m.msb.executionInfo.DecisionScheduleID != scheduleEventID || m.msb.executionInfo.DecisionStartedID > 0 {
		m.msb.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.msb.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID),
		)
		return nil, m.msb.createInternalServerError(opTag)
	}

	// Clear stickiness whenever decision fails
	m.msb.ClearStickyness()

	event := m.msb.hBuilder.AddWorkflowTaskTimedOutEvent(scheduleEventID, 0, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START)

	if err := m.ReplicateWorkflowTaskTimedOutEvent(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START); err != nil {
		return nil, err
	}
	return event, nil
}

// originalScheduledTimestamp is to record the first scheduled decision during decision heartbeat.
func (m *mutableStateWorkflowTaskManagerImpl) AddWorkflowTaskScheduledEventAsHeartbeat(
	bypassTaskGeneration bool,
	originalScheduledTimestamp int64,
) (*decisionInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskScheduled
	if m.HasPendingDecision() {
		m.msb.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.msb.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(m.msb.executionInfo.DecisionScheduleID))
		return nil, m.msb.createInternalServerError(opTag)
	}

	// Taskqueue and decision timeout should already be set from workflow execution started event
	taskQueue := m.msb.executionInfo.TaskQueue
	if m.msb.IsStickyTaskQueueEnabled() {
		taskQueue = m.msb.executionInfo.StickyTaskQueue
	} else {
		// It can be because stickyness has expired due to StickyTTL config
		// In that case we need to clear stickyness so that the LastUpdateTimestamp is not corrupted.
		// In other cases, clearing stickyness shouldn't hurt anything.
		// TODO: https://go.temporal.io/server/issues/2357:
		//  if we can use a new field(LastDecisionUpdateTimestamp), then we could get rid of it.
		m.msb.ClearStickyness()
	}
	taskTimeout := m.msb.executionInfo.WorkflowTaskTimeout

	// Flush any buffered events before creating the decision, otherwise it will result in invalid IDs for transient
	// decision and will cause in timeout processing to not work for transient decisions
	if m.msb.HasBufferedEvents() {
		// if creating a decision and in the mean time events are flushed from buffered events
		// than this decision cannot be a transient decision
		m.msb.executionInfo.DecisionAttempt = 0
		if err := m.msb.FlushBufferedEvents(); err != nil {
			return nil, err
		}
	}

	var newDecisionEvent *historypb.HistoryEvent
	scheduleID := m.msb.GetNextEventID() // we will generate the schedule event later for repeatedly failing decisions
	// Avoid creating new history events when decisions are continuously failing
	scheduleTime := m.msb.timeSource.Now().UnixNano()
	if m.msb.executionInfo.DecisionAttempt == 0 {
		newDecisionEvent = m.msb.hBuilder.AddWorkflowTaskScheduledEvent(taskQueue, taskTimeout,
			m.msb.executionInfo.DecisionAttempt)
		scheduleID = newDecisionEvent.GetEventId()
		scheduleTime = newDecisionEvent.GetTimestamp()
	}

	decision, err := m.ReplicateWorkflowTaskScheduledEvent(
		m.msb.GetCurrentVersion(),
		scheduleID,
		taskQueue,
		taskTimeout,
		m.msb.executionInfo.DecisionAttempt,
		scheduleTime,
		originalScheduledTimestamp,
	)
	if err != nil {
		return nil, err
	}

	// TODO merge active & passive task generation
	if !bypassTaskGeneration {
		if err := m.msb.taskGenerator.generateDecisionScheduleTasks(
			m.msb.unixNanoToTime(scheduleTime), // schedule time is now
			scheduleID,
		); err != nil {
			return nil, err
		}
	}

	return decision, nil
}

func (m *mutableStateWorkflowTaskManagerImpl) AddWorkflowTaskScheduledEvent(
	bypassTaskGeneration bool,
) (*decisionInfo, error) {
	return m.AddWorkflowTaskScheduledEventAsHeartbeat(bypassTaskGeneration, m.msb.timeSource.Now().UnixNano())
}

func (m *mutableStateWorkflowTaskManagerImpl) AddFirstWorkflowTaskScheduled(
	startEvent *historypb.HistoryEvent,
) error {
	// handle first decision case, i.e. possible delayed decision
	//
	// below handles the following cases:
	// 1. if not continue as new & if workflow has no parent
	//   -> schedule decision & schedule delayed decision
	// 2. if not continue as new & if workflow has parent
	//   -> this function should not be called during workflow start, but should be called as
	//      part of schedule decision in 2 phase commit
	//
	// if continue as new
	//  1. whether has parent workflow or not
	//   -> schedule decision & schedule delayed decision
	//
	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()
	decisionBackoffDuration := time.Duration(startAttr.GetFirstWorkflowTaskBackoffSeconds()) * time.Second

	var err error
	if decisionBackoffDuration != 0 {
		if err = m.msb.taskGenerator.generateDelayedWorkflowTasks(
			m.msb.unixNanoToTime(startEvent.GetTimestamp()),
			startEvent,
		); err != nil {
			return err
		}
	} else {
		if _, err = m.AddWorkflowTaskScheduledEvent(
			false,
		); err != nil {
			return err
		}
	}

	return nil
}

func (m *mutableStateWorkflowTaskManagerImpl) AddWorkflowTaskStartedEvent(
	scheduleEventID int64,
	requestID string,
	request *workflowservice.PollWorkflowTaskQueueRequest,
) (*historypb.HistoryEvent, *decisionInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskStarted
	decision, ok := m.GetDecisionInfo(scheduleEventID)
	if !ok || decision.StartedID != common.EmptyEventID {
		m.msb.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.msb.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID))
		return nil, nil, m.msb.createInternalServerError(opTag)
	}

	var event *historypb.HistoryEvent
	scheduleID := decision.ScheduleID
	startedID := scheduleID + 1
	taskqueue := request.TaskQueue.GetName()
	startTime := m.msb.timeSource.Now().UnixNano()
	// First check to see if new events came since transient decision was scheduled
	if decision.Attempt > 0 && decision.ScheduleID != m.msb.GetNextEventID() {
		// Also create a new WorkflowTaskScheduledEvent since new events came in when it was scheduled
		scheduleEvent := m.msb.hBuilder.AddWorkflowTaskScheduledEvent(taskqueue, decision.DecisionTimeout, 0)
		scheduleID = scheduleEvent.GetEventId()
		decision.Attempt = 0
	}

	// Avoid creating new history events when decisions are continuously failing
	if decision.Attempt == 0 {
		// Now create WorkflowTaskStartedEvent
		event = m.msb.hBuilder.AddWorkflowTaskStartedEvent(scheduleID, requestID, request.GetIdentity())
		startedID = event.GetEventId()
		startTime = event.GetTimestamp()
	}

	decision, err := m.ReplicateWorkflowTaskStartedEvent(decision, m.msb.GetCurrentVersion(), scheduleID, startedID, requestID, startTime)
	// TODO merge active & passive task generation
	if err := m.msb.taskGenerator.generateDecisionStartTasks(
		m.msb.unixNanoToTime(startTime), // start time is now
		scheduleID,
	); err != nil {
		return nil, nil, err
	}
	return event, decision, err
}

func (m *mutableStateWorkflowTaskManagerImpl) AddWorkflowTaskCompletedEvent(
	scheduleEventID int64,
	startedEventID int64,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
	maxResetPoints int,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskCompleted
	decision, ok := m.GetDecisionInfo(scheduleEventID)
	if !ok || decision.StartedID != startedEventID {
		m.msb.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.msb.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID),
			tag.WorkflowStartedID(startedEventID))

		return nil, m.msb.createInternalServerError(opTag)
	}

	m.beforeAddWorkflowTaskCompletedEvent()
	if decision.Attempt > 0 {
		// Create corresponding WorkflowTaskSchedule and WorkflowTaskStarted events for decisions we have been retrying
		scheduledEvent := m.msb.hBuilder.AddTransientWorkflowTaskScheduledEvent(m.msb.executionInfo.TaskQueue, decision.DecisionTimeout,
			decision.Attempt, decision.ScheduledTimestamp)
		startedEvent := m.msb.hBuilder.AddTransientWorkflowTaskStartedEvent(scheduledEvent.GetEventId(), decision.RequestID,
			request.GetIdentity(), decision.StartedTimestamp)
		startedEventID = startedEvent.GetEventId()
	}
	// Now write the completed event
	event := m.msb.hBuilder.AddWorkflowTaskCompletedEvent(scheduleEventID, startedEventID, request)

	err := m.afterAddWorkflowTaskCompletedEvent(event, maxResetPoints)
	if err != nil {
		return nil, err
	}
	return event, nil
}

func (m *mutableStateWorkflowTaskManagerImpl) AddWorkflowTaskFailedEvent(
	scheduleEventID int64,
	startedEventID int64,
	cause enumspb.WorkflowTaskFailedCause,
	failure *failurepb.Failure,
	identity string,
	binChecksum string,
	baseRunID string,
	newRunID string,
	forkEventVersion int64,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskFailed
	attr := &historypb.WorkflowTaskFailedEventAttributes{
		ScheduledEventId: scheduleEventID,
		StartedEventId:   startedEventID,
		Cause:            cause,
		Failure:          failure,
		Identity:         identity,
		BinaryChecksum:   binChecksum,
		BaseRunId:        baseRunID,
		NewRunId:         newRunID,
		ForkEventVersion: forkEventVersion,
	}

	dt, ok := m.GetDecisionInfo(scheduleEventID)
	if !ok || dt.StartedID != startedEventID {
		m.msb.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.msb.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID),
			tag.WorkflowStartedID(startedEventID))
		return nil, m.msb.createInternalServerError(opTag)
	}

	var event *historypb.HistoryEvent
	// Only emit WorkflowTaskFailedEvent for the very first time
	if dt.Attempt == 0 {
		event = m.msb.hBuilder.AddWorkflowTaskFailedEvent(attr)
	}

	if err := m.ReplicateWorkflowTaskFailedEvent(); err != nil {
		return nil, err
	}

	// always clear decision attempt for reset
	if cause == enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW ||
		cause == enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND {
		m.msb.executionInfo.DecisionAttempt = 0
	}
	return event, nil
}

func (m *mutableStateWorkflowTaskManagerImpl) AddWorkflowTaskTimedOutEvent(
	scheduleEventID int64,
	startedEventID int64,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskTimedOut
	dt, ok := m.GetDecisionInfo(scheduleEventID)
	if !ok || dt.StartedID != startedEventID {
		m.msb.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.msb.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID),
			tag.WorkflowStartedID(startedEventID))
		return nil, m.msb.createInternalServerError(opTag)
	}

	var event *historypb.HistoryEvent
	// Avoid creating new history events when decisions are continuously timing out
	if dt.Attempt == 0 {
		event = m.msb.hBuilder.AddWorkflowTaskTimedOutEvent(scheduleEventID, startedEventID, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	}

	if err := m.ReplicateWorkflowTaskTimedOutEvent(enumspb.TIMEOUT_TYPE_START_TO_CLOSE); err != nil {
		return nil, err
	}
	return event, nil
}

func (m *mutableStateWorkflowTaskManagerImpl) FailDecision(
	incrementAttempt bool,
) {
	// Clear stickiness whenever decision fails
	m.msb.ClearStickyness()

	failDecisionInfo := &decisionInfo{
		Version:                    common.EmptyVersion,
		ScheduleID:                 common.EmptyEventID,
		StartedID:                  common.EmptyEventID,
		RequestID:                  emptyUUID,
		DecisionTimeout:            0,
		StartedTimestamp:           0,
		TaskQueue:                  "",
		OriginalScheduledTimestamp: 0,
	}
	if incrementAttempt {
		failDecisionInfo.Attempt = m.msb.executionInfo.DecisionAttempt + 1
		failDecisionInfo.ScheduledTimestamp = m.msb.timeSource.Now().UnixNano()
	}
	m.UpdateDecision(failDecisionInfo)
}

// DeleteDecision deletes a workflow task.
func (m *mutableStateWorkflowTaskManagerImpl) DeleteDecision() {
	resetDecisionInfo := &decisionInfo{
		Version:            common.EmptyVersion,
		ScheduleID:         common.EmptyEventID,
		StartedID:          common.EmptyEventID,
		RequestID:          emptyUUID,
		DecisionTimeout:    0,
		Attempt:            0,
		StartedTimestamp:   0,
		ScheduledTimestamp: 0,
		TaskQueue:          "",
		// Keep the last original scheduled timestamp, so that AddDecisionAsHeartbeat can continue with it.
		OriginalScheduledTimestamp: m.getDecisionInfo().OriginalScheduledTimestamp,
	}
	m.UpdateDecision(resetDecisionInfo)
}

// UpdateDecision updates a workflow task.
func (m *mutableStateWorkflowTaskManagerImpl) UpdateDecision(
	decision *decisionInfo,
) {
	m.msb.executionInfo.DecisionVersion = decision.Version
	m.msb.executionInfo.DecisionScheduleID = decision.ScheduleID
	m.msb.executionInfo.DecisionStartedID = decision.StartedID
	m.msb.executionInfo.DecisionRequestID = decision.RequestID
	m.msb.executionInfo.DecisionTimeout = decision.DecisionTimeout
	m.msb.executionInfo.DecisionAttempt = decision.Attempt
	m.msb.executionInfo.DecisionStartedTimestamp = decision.StartedTimestamp
	m.msb.executionInfo.DecisionScheduledTimestamp = decision.ScheduledTimestamp
	m.msb.executionInfo.DecisionOriginalScheduledTimestamp = decision.OriginalScheduledTimestamp

	// NOTE: do not update taskqueue in execution info

	m.msb.logger.Debug("Decision Updated",
		tag.WorkflowScheduleID(decision.ScheduleID),
		tag.WorkflowStartedID(decision.StartedID),
		tag.DecisionRequestId(decision.RequestID),
		tag.WorkflowDecisionTimeoutSeconds(decision.DecisionTimeout),
		tag.Attempt(int32(decision.Attempt)),
		tag.TimestampInt(decision.StartedTimestamp))
}

func (m *mutableStateWorkflowTaskManagerImpl) HasPendingDecision() bool {
	return m.msb.executionInfo.DecisionScheduleID != common.EmptyEventID
}

func (m *mutableStateWorkflowTaskManagerImpl) GetPendingDecision() (*decisionInfo, bool) {
	if m.msb.executionInfo.DecisionScheduleID == common.EmptyEventID {
		return nil, false
	}

	decision := m.getDecisionInfo()
	return decision, true
}

func (m *mutableStateWorkflowTaskManagerImpl) HasInFlightDecision() bool {
	return m.msb.executionInfo.DecisionStartedID > 0
}

func (m *mutableStateWorkflowTaskManagerImpl) GetInFlightDecision() (*decisionInfo, bool) {
	if m.msb.executionInfo.DecisionScheduleID == common.EmptyEventID ||
		m.msb.executionInfo.DecisionStartedID == common.EmptyEventID {
		return nil, false
	}

	decision := m.getDecisionInfo()
	return decision, true
}

func (m *mutableStateWorkflowTaskManagerImpl) HasProcessedOrPendingDecision() bool {
	return m.HasPendingDecision() || m.msb.GetPreviousStartedEventID() != common.EmptyEventID
}

// GetDecisionInfo returns details about the in-progress workflow task
func (m *mutableStateWorkflowTaskManagerImpl) GetDecisionInfo(
	scheduleEventID int64,
) (*decisionInfo, bool) {
	decision := m.getDecisionInfo()
	if scheduleEventID == decision.ScheduleID {
		return decision, true
	}
	return nil, false
}

func (m *mutableStateWorkflowTaskManagerImpl) CreateTransientDecisionEvents(
	decision *decisionInfo,
	identity string,
) (*historypb.HistoryEvent, *historypb.HistoryEvent) {
	taskqueue := m.msb.executionInfo.TaskQueue
	scheduledEvent := newWorkflowTaskScheduledEventWithInfo(
		decision.ScheduleID,
		decision.ScheduledTimestamp,
		taskqueue,
		decision.DecisionTimeout,
		decision.Attempt,
	)

	startedEvent := newWorkflowTaskStartedEventWithInfo(
		decision.StartedID,
		decision.StartedTimestamp,
		decision.ScheduleID,
		decision.RequestID,
		identity,
	)

	return scheduledEvent, startedEvent
}

func (m *mutableStateWorkflowTaskManagerImpl) getDecisionInfo() *decisionInfo {
	taskQueue := m.msb.executionInfo.TaskQueue
	if m.msb.IsStickyTaskQueueEnabled() {
		taskQueue = m.msb.executionInfo.StickyTaskQueue
	}
	return &decisionInfo{
		Version:                    m.msb.executionInfo.DecisionVersion,
		ScheduleID:                 m.msb.executionInfo.DecisionScheduleID,
		StartedID:                  m.msb.executionInfo.DecisionStartedID,
		RequestID:                  m.msb.executionInfo.DecisionRequestID,
		DecisionTimeout:            m.msb.executionInfo.DecisionTimeout,
		Attempt:                    m.msb.executionInfo.DecisionAttempt,
		StartedTimestamp:           m.msb.executionInfo.DecisionStartedTimestamp,
		ScheduledTimestamp:         m.msb.executionInfo.DecisionScheduledTimestamp,
		TaskQueue:                  taskQueue,
		OriginalScheduledTimestamp: m.msb.executionInfo.DecisionOriginalScheduledTimestamp,
	}
}

func (m *mutableStateWorkflowTaskManagerImpl) beforeAddWorkflowTaskCompletedEvent() {
	// Make sure to delete decision before adding events.  Otherwise they are buffered rather than getting appended
	m.DeleteDecision()
}

func (m *mutableStateWorkflowTaskManagerImpl) afterAddWorkflowTaskCompletedEvent(
	event *historypb.HistoryEvent,
	maxResetPoints int,
) error {
	m.msb.executionInfo.LastProcessedEvent = event.GetWorkflowTaskCompletedEventAttributes().GetStartedEventId()
	return m.msb.addBinaryCheckSumIfNotExists(event, maxResetPoints)
}
