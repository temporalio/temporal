// The MIT License (MIT)
//
// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package history

import (
	"fmt"
	"math"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/errors"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
)

type (
	mutableStateDecisionTaskManager interface {
		AddInMemoryDecisionTaskScheduled(time.Duration) error
		AddInMemoryDecisionTaskStarted() error
		DeleteInMemoryDecisionTask()
		HasScheduledInMemoryDecisionTask() bool
		HasStartedInMemoryDecisionTask() bool
		HasInMemoryDecisionTask() bool

		ReplicateDecisionTaskScheduledEvent(
			version int64,
			scheduleID int64,
			taskList string,
			startToCloseTimeoutSeconds int32,
			attempt int64,
			scheduleTimestamp int64,
			originalScheduledTimestamp int64,
		) (*decisionInfo, error)
		ReplicateTransientDecisionTaskScheduled() (*decisionInfo, error)
		ReplicateDecisionTaskStartedEvent(
			decision *decisionInfo,
			version int64,
			scheduleID int64,
			startedID int64,
			requestID string,
			timestamp int64,
		) (*decisionInfo, error)
		ReplicateDecisionTaskCompletedEvent(event *workflow.HistoryEvent) error
		ReplicateDecisionTaskFailedEvent() error
		ReplicateDecisionTaskTimedOutEvent(timeoutType workflow.TimeoutType) error

		AddDecisionTaskScheduleToStartTimeoutEvent(scheduleEventID int64) (*workflow.HistoryEvent, error)
		AddDecisionTaskScheduledEventAsHeartbeat(
			bypassTaskGeneration bool,
			originalScheduledTimestamp int64,
		) (*decisionInfo, error)
		AddDecisionTaskScheduledEvent(bypassTaskGeneration bool) (*decisionInfo, error)
		AddFirstDecisionTaskScheduled(startEvent *workflow.HistoryEvent) error
		AddDecisionTaskStartedEvent(
			scheduleEventID int64,
			requestID string,
			request *workflow.PollForDecisionTaskRequest,
		) (*workflow.HistoryEvent, *decisionInfo, error)
		AddDecisionTaskCompletedEvent(
			scheduleEventID int64,
			startedEventID int64,
			request *workflow.RespondDecisionTaskCompletedRequest,
			maxResetPoints int,
		) (*workflow.HistoryEvent, error)
		AddDecisionTaskFailedEvent(
			scheduleEventID int64,
			startedEventID int64,
			cause workflow.DecisionTaskFailedCause,
			details []byte,
			identity string,
			reason string,
			baseRunID string,
			newRunID string,
			forkEventVersion int64,
		) (*workflow.HistoryEvent, error)
		AddDecisionTaskTimedOutEvent(scheduleEventID int64, startedEventID int64) (*workflow.HistoryEvent, error)

		FailDecision(incrementAttempt bool)
		DeleteDecision()
		UpdateDecision(decision *decisionInfo)

		HasPendingDecision() bool
		GetPendingDecision() (*decisionInfo, bool)
		HasInFlightDecision() bool
		GetInFlightDecision() (*decisionInfo, bool)
		HasProcessedOrPendingDecision() bool
		GetDecisionInfo(scheduleEventID int64) (*decisionInfo, bool)

		CreateTransientDecisionEvents(decision *decisionInfo, identity string) (*workflow.HistoryEvent, *workflow.HistoryEvent)
	}

	mutableStateDecisionTaskManagerImpl struct {
		msb             *mutableStateBuilder
		memDecisionTask *memDecisionTask
	}

	memDecisionTaskState int

	// memDecisionTask represents a decisionTask which only ever exists in memory.
	// This decisionTask will never be persisted and does not contain a *decisionInfo.
	// Currently the only use case for memDecisionTask is query, but other potential use cases exist.
	// While memDecisionTask will not be persisted it does impact the logic of decision state machine.
	memDecisionTask struct {
		state  memDecisionTaskState
		expiry time.Time
	}
)

const (
	memDecisionTaskStateNone memDecisionTaskState = iota
	memDecisionTaskStateScheduled
	memDecisionTaskStateStarted
)

func newMutableStateDecisionTaskManager(msb *mutableStateBuilder) mutableStateDecisionTaskManager {
	return &mutableStateDecisionTaskManagerImpl{
		msb:             msb,
		memDecisionTask: &memDecisionTask{},
	}
}

func (m *mutableStateDecisionTaskManagerImpl) ReplicateDecisionTaskScheduledEvent(
	version int64,
	scheduleID int64,
	taskList string,
	startToCloseTimeoutSeconds int32,
	attempt int64,
	scheduleTimestamp int64,
	originalScheduledTimestamp int64,
) (*decisionInfo, error) {
	decision := &decisionInfo{
		Version:                    version,
		ScheduleID:                 scheduleID,
		StartedID:                  common.EmptyEventID,
		RequestID:                  emptyUUID,
		DecisionTimeout:            startToCloseTimeoutSeconds,
		TaskList:                   taskList,
		Attempt:                    attempt,
		ScheduledTimestamp:         scheduleTimestamp,
		StartedTimestamp:           0,
		OriginalScheduledTimestamp: originalScheduledTimestamp,
	}

	m.UpdateDecision(decision)
	return decision, nil
}

func (m *mutableStateDecisionTaskManagerImpl) ReplicateTransientDecisionTaskScheduled() (*decisionInfo, error) {
	if m.HasPendingDecision() || m.msb.GetExecutionInfo().DecisionAttempt == 0 {
		return nil, nil
	}

	// the schedule ID for this decision is guaranteed to be wrong
	// since the next event ID is assigned at the very end of when
	// all events are applied for replication.
	// this is OK
	// 1. if a failover happen just after this transient decision,
	// AddDecisionTaskStartedEvent will handle the correction of schedule ID
	// and set the attempt to 0
	// 2. if no failover happen during the life time of this transient decision
	// then ReplicateDecisionTaskScheduledEvent will overwrite everything
	// including the decision schedule ID
	decision := &decisionInfo{
		Version:            m.msb.GetCurrentVersion(),
		ScheduleID:         m.msb.GetNextEventID(),
		StartedID:          common.EmptyEventID,
		RequestID:          emptyUUID,
		DecisionTimeout:    m.msb.GetExecutionInfo().DecisionTimeoutValue,
		TaskList:           m.msb.GetExecutionInfo().TaskList,
		Attempt:            m.msb.GetExecutionInfo().DecisionAttempt,
		ScheduledTimestamp: m.msb.timeSource.Now().UnixNano(),
		StartedTimestamp:   0,
	}

	m.UpdateDecision(decision)
	return decision, nil
}

func (m *mutableStateDecisionTaskManagerImpl) ReplicateDecisionTaskStartedEvent(
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
			return nil, errors.NewInternalFailureError(fmt.Sprintf("unable to find decision: %v", scheduleID))
		}
		// setting decision attempt to 0 for decision task replication
		// this mainly handles transient decision completion
		// for transient decision, active side will write 2 batch in a "transaction"
		// 1. decision task scheduled & decision task started
		// 2. decision task completed & other events
		// since we need to treat each individual event batch as one transaction
		// certain "magic" needs to be done, i.e. setting attempt to 0 so
		// if first batch is replicated, but not the second one, decision can be correctly timed out
		decision.Attempt = 0
	}

	// set workflow state to running, since decision is scheduled
	if state, _ := m.msb.GetWorkflowStateCloseStatus(); state == persistence.WorkflowStateCreated {
		if err := m.msb.UpdateWorkflowStateCloseStatus(
			persistence.WorkflowStateRunning,
			persistence.WorkflowCloseStatusNone,
		); err != nil {
			return nil, err
		}
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
		TaskList:                   decision.TaskList,
		OriginalScheduledTimestamp: decision.OriginalScheduledTimestamp,
	}

	m.UpdateDecision(decision)
	return decision, nil
}

func (m *mutableStateDecisionTaskManagerImpl) ReplicateDecisionTaskCompletedEvent(
	event *workflow.HistoryEvent,
) error {
	defer m.ensureMemDecisionTaskValid()
	m.beforeAddDecisionTaskCompletedEvent()
	m.afterAddDecisionTaskCompletedEvent(event, math.MaxInt32)
	return nil
}

func (m *mutableStateDecisionTaskManagerImpl) ReplicateDecisionTaskFailedEvent() error {
	m.FailDecision(true)
	return nil
}

func (m *mutableStateDecisionTaskManagerImpl) ReplicateDecisionTaskTimedOutEvent(
	timeoutType workflow.TimeoutType,
) error {
	incrementAttempt := true
	// Do not increment decision attempt in the case of sticky timeout to prevent creating next decision as transient
	if timeoutType == workflow.TimeoutTypeScheduleToStart {
		incrementAttempt = false
	}
	m.FailDecision(incrementAttempt)
	return nil
}

func (m *mutableStateDecisionTaskManagerImpl) AddDecisionTaskScheduleToStartTimeoutEvent(
	scheduleEventID int64,
) (*workflow.HistoryEvent, error) {
	defer m.ensureMemDecisionTaskValid()
	opTag := tag.WorkflowActionDecisionTaskTimedOut
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

	event := m.msb.hBuilder.AddDecisionTaskTimedOutEvent(scheduleEventID, 0, workflow.TimeoutTypeScheduleToStart)

	if err := m.ReplicateDecisionTaskTimedOutEvent(workflow.TimeoutTypeScheduleToStart); err != nil {
		return nil, err
	}
	return event, nil
}

// originalScheduledTimestamp is to record the first scheduled decision during decision heartbeat.
func (m *mutableStateDecisionTaskManagerImpl) AddDecisionTaskScheduledEventAsHeartbeat(
	bypassTaskGeneration bool,
	originalScheduledTimestamp int64,
) (*decisionInfo, error) {
	defer m.ensureMemDecisionTaskValid()
	opTag := tag.WorkflowActionDecisionTaskScheduled
	if m.HasPendingDecision() || m.HasStartedInMemoryDecisionTask() {
		m.msb.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.msb.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(m.msb.executionInfo.DecisionScheduleID))
		return nil, m.msb.createInternalServerError(opTag)
	}

	// set workflow state to running
	// since decision is scheduled
	m.msb.executionInfo.State = persistence.WorkflowStateRunning

	// Tasklist and decision timeout should already be set from workflow execution started event
	taskList := m.msb.executionInfo.TaskList
	if m.msb.IsStickyTaskListEnabled() {
		taskList = m.msb.executionInfo.StickyTaskList
	} else {
		// It can be because stickyness has expired due to StickyTTL config
		// In that case we need to clear stickyness so that the LastUpdateTimestamp is not corrupted.
		// In other cases, clearing stickyness shouldn't hurt anything.
		// TODO: https://github.com/uber/cadence/issues/2357:
		//  if we can use a new field(LastDecisionUpdateTimestamp), then we could get rid of it.
		m.msb.ClearStickyness()
	}
	startToCloseTimeoutSeconds := m.msb.executionInfo.DecisionTimeoutValue

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

	var newDecisionEvent *workflow.HistoryEvent
	scheduleID := m.msb.GetNextEventID() // we will generate the schedule event later for repeatedly failing decisions
	// Avoid creating new history events when decisions are continuously failing
	scheduleTime := m.msb.timeSource.Now().UnixNano()
	if m.msb.executionInfo.DecisionAttempt == 0 {
		newDecisionEvent = m.msb.hBuilder.AddDecisionTaskScheduledEvent(taskList, startToCloseTimeoutSeconds,
			m.msb.executionInfo.DecisionAttempt)
		scheduleID = newDecisionEvent.GetEventId()
		scheduleTime = newDecisionEvent.GetTimestamp()
	}

	decision, err := m.ReplicateDecisionTaskScheduledEvent(
		m.msb.GetCurrentVersion(),
		scheduleID,
		taskList,
		startToCloseTimeoutSeconds,
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

func (m *mutableStateDecisionTaskManagerImpl) AddDecisionTaskScheduledEvent(
	bypassTaskGeneration bool,
) (*decisionInfo, error) {
	return m.AddDecisionTaskScheduledEventAsHeartbeat(bypassTaskGeneration, m.msb.timeSource.Now().UnixNano())
}

func (m *mutableStateDecisionTaskManagerImpl) AddFirstDecisionTaskScheduled(
	startEvent *workflow.HistoryEvent,
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
	startAttr := startEvent.WorkflowExecutionStartedEventAttributes
	decisionBackoffDuration := time.Duration(startAttr.GetFirstDecisionTaskBackoffSeconds()) * time.Second

	var err error
	if decisionBackoffDuration != 0 {
		if err = m.msb.taskGenerator.generateDelayedDecisionTasks(
			m.msb.unixNanoToTime(startEvent.GetTimestamp()),
			startEvent,
		); err != nil {
			return err
		}
	} else {
		if _, err = m.AddDecisionTaskScheduledEvent(
			false,
		); err != nil {
			return err
		}
	}

	return nil
}

func (m *mutableStateDecisionTaskManagerImpl) AddDecisionTaskStartedEvent(
	scheduleEventID int64,
	requestID string,
	request *workflow.PollForDecisionTaskRequest,
) (*workflow.HistoryEvent, *decisionInfo, error) {
	defer m.ensureMemDecisionTaskValid()
	opTag := tag.WorkflowActionDecisionTaskStarted
	decision, ok := m.GetDecisionInfo(scheduleEventID)
	if !ok || decision.StartedID != common.EmptyEventID || m.HasStartedInMemoryDecisionTask() {
		m.msb.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.msb.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID))
		return nil, nil, m.msb.createInternalServerError(opTag)
	}

	var event *workflow.HistoryEvent
	scheduleID := decision.ScheduleID
	startedID := scheduleID + 1
	tasklist := request.TaskList.GetName()
	startTime := m.msb.timeSource.Now().UnixNano()
	// First check to see if new events came since transient decision was scheduled
	if decision.Attempt > 0 && decision.ScheduleID != m.msb.GetNextEventID() {
		// Also create a new DecisionTaskScheduledEvent since new events came in when it was scheduled
		scheduleEvent := m.msb.hBuilder.AddDecisionTaskScheduledEvent(tasklist, decision.DecisionTimeout, 0)
		scheduleID = scheduleEvent.GetEventId()
		decision.Attempt = 0
	}

	// Avoid creating new history events when decisions are continuously failing
	if decision.Attempt == 0 {
		// Now create DecisionTaskStartedEvent
		event = m.msb.hBuilder.AddDecisionTaskStartedEvent(scheduleID, requestID, request.GetIdentity())
		startedID = event.GetEventId()
		startTime = event.GetTimestamp()
	}

	decision, err := m.ReplicateDecisionTaskStartedEvent(decision, m.msb.GetCurrentVersion(), scheduleID, startedID, requestID, startTime)
	// TODO merge active & passive task generation
	if err := m.msb.taskGenerator.generateDecisionStartTasks(
		m.msb.unixNanoToTime(startTime), // start time is now
		scheduleID,
	); err != nil {
		return nil, nil, err
	}
	return event, decision, err
}

func (m *mutableStateDecisionTaskManagerImpl) AddDecisionTaskCompletedEvent(
	scheduleEventID int64,
	startedEventID int64,
	request *workflow.RespondDecisionTaskCompletedRequest,
	maxResetPoints int,
) (*workflow.HistoryEvent, error) {
	defer m.ensureMemDecisionTaskValid()
	opTag := tag.WorkflowActionDecisionTaskCompleted
	decision, ok := m.GetDecisionInfo(scheduleEventID)
	if !ok || decision.StartedID != startedEventID {
		m.msb.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.msb.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID),
			tag.WorkflowStartedID(startedEventID))

		return nil, m.msb.createInternalServerError(opTag)
	}

	m.beforeAddDecisionTaskCompletedEvent()
	if decision.Attempt > 0 {
		// Create corresponding DecisionTaskSchedule and DecisionTaskStarted events for decisions we have been retrying
		scheduledEvent := m.msb.hBuilder.AddTransientDecisionTaskScheduledEvent(m.msb.executionInfo.TaskList, decision.DecisionTimeout,
			decision.Attempt, decision.ScheduledTimestamp)
		startedEvent := m.msb.hBuilder.AddTransientDecisionTaskStartedEvent(scheduledEvent.GetEventId(), decision.RequestID,
			request.GetIdentity(), decision.StartedTimestamp)
		startedEventID = startedEvent.GetEventId()
	}
	// Now write the completed event
	event := m.msb.hBuilder.AddDecisionTaskCompletedEvent(scheduleEventID, startedEventID, request)

	m.afterAddDecisionTaskCompletedEvent(event, maxResetPoints)
	return event, nil
}

func (m *mutableStateDecisionTaskManagerImpl) AddDecisionTaskFailedEvent(
	scheduleEventID int64,
	startedEventID int64,
	cause workflow.DecisionTaskFailedCause,
	details []byte,
	identity string,
	reason string,
	baseRunID string,
	newRunID string,
	forkEventVersion int64,
) (*workflow.HistoryEvent, error) {
	defer m.ensureMemDecisionTaskValid()
	opTag := tag.WorkflowActionDecisionTaskFailed
	attr := workflow.DecisionTaskFailedEventAttributes{
		ScheduledEventId: common.Int64Ptr(scheduleEventID),
		StartedEventId:   common.Int64Ptr(startedEventID),
		Cause:            common.DecisionTaskFailedCausePtr(cause),
		Details:          details,
		Identity:         common.StringPtr(identity),
		Reason:           common.StringPtr(reason),
		BaseRunId:        common.StringPtr(baseRunID),
		NewRunId:         common.StringPtr(newRunID),
		ForkEventVersion: common.Int64Ptr(forkEventVersion),
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

	var event *workflow.HistoryEvent
	// Only emit DecisionTaskFailedEvent for the very first time
	if dt.Attempt == 0 {
		event = m.msb.hBuilder.AddDecisionTaskFailedEvent(attr)
	}

	if err := m.ReplicateDecisionTaskFailedEvent(); err != nil {
		return nil, err
	}

	// always clear decision attempt for reset
	if cause == workflow.DecisionTaskFailedCauseResetWorkflow ||
		cause == workflow.DecisionTaskFailedCauseFailoverCloseDecision {
		m.msb.executionInfo.DecisionAttempt = 0
	}
	return event, nil
}

func (m *mutableStateDecisionTaskManagerImpl) AddDecisionTaskTimedOutEvent(
	scheduleEventID int64,
	startedEventID int64,
) (*workflow.HistoryEvent, error) {
	defer m.ensureMemDecisionTaskValid()
	opTag := tag.WorkflowActionDecisionTaskTimedOut
	dt, ok := m.GetDecisionInfo(scheduleEventID)
	if !ok || dt.StartedID != startedEventID {
		m.msb.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.msb.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID),
			tag.WorkflowStartedID(startedEventID))
		return nil, m.msb.createInternalServerError(opTag)
	}

	var event *workflow.HistoryEvent
	// Avoid creating new history events when decisions are continuously timing out
	if dt.Attempt == 0 {
		event = m.msb.hBuilder.AddDecisionTaskTimedOutEvent(scheduleEventID, startedEventID, workflow.TimeoutTypeStartToClose)
	}

	if err := m.ReplicateDecisionTaskTimedOutEvent(workflow.TimeoutTypeStartToClose); err != nil {
		return nil, err
	}
	return event, nil
}

func (m *mutableStateDecisionTaskManagerImpl) AddInMemoryDecisionTaskScheduled(ttl time.Duration) error {
	defer m.ensureMemDecisionTaskValid()
	opTag := tag.WorkflowActionInMemoryDecisionTaskScheduled
	if m.HasPendingDecision() || m.HasInMemoryDecisionTask() {
		m.msb.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag, tag.ErrorTypeInvalidMemDecisionTaskAction)
		return m.msb.createInternalServerError(opTag)
	}
	m.memDecisionTask.state = memDecisionTaskStateScheduled
	m.memDecisionTask.expiry = m.msb.timeSource.Now().Add(ttl)
	return nil
}

func (m *mutableStateDecisionTaskManagerImpl) AddInMemoryDecisionTaskStarted() error {
	defer m.ensureMemDecisionTaskValid()
	opTag := tag.WorkflowActionInMemoryDecisionTaskStarted
	if m.HasPendingDecision() || !m.HasScheduledInMemoryDecisionTask() {
		m.msb.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag, tag.ErrorTypeInvalidMemDecisionTaskAction)
		return m.msb.createInternalServerError(opTag)
	}
	m.memDecisionTask.state = memDecisionTaskStateStarted
	return nil
}

func (m *mutableStateDecisionTaskManagerImpl) DeleteInMemoryDecisionTask() {
	m.memDecisionTask.state = memDecisionTaskStateNone
	m.memDecisionTask.expiry = time.Time{}
}

func (m *mutableStateDecisionTaskManagerImpl) HasScheduledInMemoryDecisionTask() bool {
	return m.memDecisionTask.state == memDecisionTaskStateScheduled && m.memDecisionTask.expiry.After(m.msb.timeSource.Now())
}

func (m *mutableStateDecisionTaskManagerImpl) HasStartedInMemoryDecisionTask() bool {
	return m.memDecisionTask.state == memDecisionTaskStateStarted && m.memDecisionTask.expiry.After(m.msb.timeSource.Now())
}

func (m *mutableStateDecisionTaskManagerImpl) HasInMemoryDecisionTask() bool {
	return m.memDecisionTask.state != memDecisionTaskStateNone && m.memDecisionTask.expiry.After(m.msb.timeSource.Now())
}

func (m *mutableStateDecisionTaskManagerImpl) FailDecision(
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
		TaskList:                   "",
		OriginalScheduledTimestamp: 0,
	}
	if incrementAttempt {
		failDecisionInfo.Attempt = m.msb.executionInfo.DecisionAttempt + 1
		failDecisionInfo.ScheduledTimestamp = m.msb.timeSource.Now().UnixNano()
	}
	m.UpdateDecision(failDecisionInfo)
}

// DeleteDecision deletes a decision task.
func (m *mutableStateDecisionTaskManagerImpl) DeleteDecision() {
	resetDecisionInfo := &decisionInfo{
		Version:            common.EmptyVersion,
		ScheduleID:         common.EmptyEventID,
		StartedID:          common.EmptyEventID,
		RequestID:          emptyUUID,
		DecisionTimeout:    0,
		Attempt:            0,
		StartedTimestamp:   0,
		ScheduledTimestamp: 0,
		TaskList:           "",
		// Keep the last original scheduled timestamp, so that AddDecisionAsHeartbeat can continue with it.
		OriginalScheduledTimestamp: m.getDecisionInfo().OriginalScheduledTimestamp,
	}
	m.UpdateDecision(resetDecisionInfo)
}

// UpdateDecision updates a decision task.
func (m *mutableStateDecisionTaskManagerImpl) UpdateDecision(
	decision *decisionInfo,
) {
	defer m.ensureMemDecisionTaskValid()
	m.msb.executionInfo.DecisionVersion = decision.Version
	m.msb.executionInfo.DecisionScheduleID = decision.ScheduleID
	m.msb.executionInfo.DecisionStartedID = decision.StartedID
	m.msb.executionInfo.DecisionRequestID = decision.RequestID
	m.msb.executionInfo.DecisionTimeout = decision.DecisionTimeout
	m.msb.executionInfo.DecisionAttempt = decision.Attempt
	m.msb.executionInfo.DecisionStartedTimestamp = decision.StartedTimestamp
	m.msb.executionInfo.DecisionScheduledTimestamp = decision.ScheduledTimestamp
	m.msb.executionInfo.DecisionOriginalScheduledTimestamp = decision.OriginalScheduledTimestamp

	// NOTE: do not update tasklist in execution info

	m.msb.logger.Debug(fmt.Sprintf(
		"Decision Updated: {Schedule: %v, Started: %v, ID: %v, Timeout: %v, Attempt: %v, Timestamp: %v}",
		decision.ScheduleID,
		decision.StartedID,
		decision.RequestID,
		decision.DecisionTimeout,
		decision.Attempt,
		decision.StartedTimestamp,
	))
}

func (m *mutableStateDecisionTaskManagerImpl) HasPendingDecision() bool {
	return m.msb.executionInfo.DecisionScheduleID != common.EmptyEventID
}

func (m *mutableStateDecisionTaskManagerImpl) GetPendingDecision() (*decisionInfo, bool) {
	if m.msb.executionInfo.DecisionScheduleID == common.EmptyEventID {
		return nil, false
	}

	decision := m.getDecisionInfo()
	return decision, true
}

func (m *mutableStateDecisionTaskManagerImpl) HasInFlightDecision() bool {
	return m.msb.executionInfo.DecisionStartedID > 0
}

func (m *mutableStateDecisionTaskManagerImpl) GetInFlightDecision() (*decisionInfo, bool) {
	if m.msb.executionInfo.DecisionScheduleID == common.EmptyEventID ||
		m.msb.executionInfo.DecisionStartedID == common.EmptyEventID {
		return nil, false
	}

	decision := m.getDecisionInfo()
	return decision, true
}

func (m *mutableStateDecisionTaskManagerImpl) HasProcessedOrPendingDecision() bool {
	return m.HasPendingDecision() || m.msb.GetPreviousStartedEventID() != common.EmptyEventID
}

// GetDecisionInfo returns details about the in-progress decision task
func (m *mutableStateDecisionTaskManagerImpl) GetDecisionInfo(
	scheduleEventID int64,
) (*decisionInfo, bool) {
	decision := m.getDecisionInfo()
	if scheduleEventID == decision.ScheduleID {
		return decision, true
	}
	return nil, false
}

func (m *mutableStateDecisionTaskManagerImpl) CreateTransientDecisionEvents(
	decision *decisionInfo,
	identity string,
) (*workflow.HistoryEvent, *workflow.HistoryEvent) {
	tasklist := m.msb.executionInfo.TaskList
	scheduledEvent := newDecisionTaskScheduledEventWithInfo(
		decision.ScheduleID,
		decision.ScheduledTimestamp,
		tasklist,
		decision.DecisionTimeout,
		decision.Attempt,
	)

	startedEvent := newDecisionTaskStartedEventWithInfo(
		decision.StartedID,
		decision.StartedTimestamp,
		decision.ScheduleID,
		decision.RequestID,
		identity,
	)

	return scheduledEvent, startedEvent
}

func (m *mutableStateDecisionTaskManagerImpl) getDecisionInfo() *decisionInfo {
	taskList := m.msb.executionInfo.TaskList
	if m.msb.IsStickyTaskListEnabled() {
		taskList = m.msb.executionInfo.StickyTaskList
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
		TaskList:                   taskList,
		OriginalScheduledTimestamp: m.msb.executionInfo.DecisionOriginalScheduledTimestamp,
	}
}

func (m *mutableStateDecisionTaskManagerImpl) beforeAddDecisionTaskCompletedEvent() {
	// Make sure to delete decision before adding events.  Otherwise they are buffered rather than getting appended
	m.DeleteDecision()
}

func (m *mutableStateDecisionTaskManagerImpl) afterAddDecisionTaskCompletedEvent(
	event *workflow.HistoryEvent,
	maxResetPoints int,
) {
	m.msb.executionInfo.LastProcessedEvent = event.GetDecisionTaskCompletedEventAttributes().GetStartedEventId()
	m.msb.addBinaryCheckSumIfNotExists(event, maxResetPoints)
}

func (m *mutableStateDecisionTaskManagerImpl) ensureMemDecisionTaskValid() {
	// it is invalid to ever ever have both memDecisionTask and realDecisionTask
	// if this state arises it either indicates a bug or it indicates a scheduled memDecisionTask
	// is being converted to a real decisionTask in either case the correct thing to do is delete the memDecisionTask
	if m.HasInMemoryDecisionTask() && m.HasPendingDecision() {
		m.DeleteInMemoryDecisionTask()
	}
}
