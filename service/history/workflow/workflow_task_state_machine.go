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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination workflow_task_state_machine_mock.go

package workflow

import (
	"fmt"
	"math"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	workflowTaskStateMachine struct {
		ms *MutableStateImpl
	}
)

func newWorkflowTaskStateMachine(
	ms *MutableStateImpl,
) *workflowTaskStateMachine {
	return &workflowTaskStateMachine{
		ms: ms,
	}
}

func (m *workflowTaskStateMachine) ReplicateWorkflowTaskScheduledEvent(version int64, scheduleID int64, taskQueue *taskqueuepb.TaskQueue, startToCloseTimeoutSeconds int32, attempt int32, scheduleTimestamp *time.Time, originalScheduledTimestamp *time.Time) (*WorkflowTaskInfo, error) {

	// set workflow state to running, since workflow task is scheduled
	// NOTE: for zombie workflow, should not change the state
	state, _ := m.ms.GetWorkflowStateStatus()
	if state != enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		if err := m.ms.UpdateWorkflowStateStatus(
			enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		); err != nil {
			return nil, err
		}
	}

	workflowTask := &WorkflowTaskInfo{
		Version:               version,
		ScheduleID:            scheduleID,
		StartedID:             common.EmptyEventID,
		RequestID:             emptyUUID,
		WorkflowTaskTimeout:   timestamp.DurationFromSeconds(int64(startToCloseTimeoutSeconds)),
		TaskQueue:             taskQueue,
		Attempt:               attempt,
		ScheduledTime:         scheduleTimestamp,
		StartedTime:           nil,
		OriginalScheduledTime: originalScheduledTimestamp,
	}

	m.UpdateWorkflowTask(workflowTask)
	return workflowTask, nil
}

func (m *workflowTaskStateMachine) ReplicateTransientWorkflowTaskScheduled() (*WorkflowTaskInfo, error) {
	if m.HasPendingWorkflowTask() || m.ms.GetExecutionInfo().WorkflowTaskAttempt == 1 {
		return nil, nil
	}

	// the schedule ID for this workflow task is guaranteed to be wrong
	// since the next event ID is assigned at the very end of when
	// all events are applied for replication.
	// this is OK
	// 1. if a failover happen just after this transient workflow task,
	// AddWorkflowTaskStartedEvent will handle the correction of schedule ID
	// and set the attempt to 1
	// 2. if no failover happen during the life time of this transient workflow task
	// then ReplicateWorkflowTaskScheduledEvent will overwrite everything
	// including the workflow task schedule ID
	workflowTask := &WorkflowTaskInfo{
		Version:             m.ms.GetCurrentVersion(),
		ScheduleID:          m.ms.GetNextEventID(),
		StartedID:           common.EmptyEventID,
		RequestID:           emptyUUID,
		WorkflowTaskTimeout: m.ms.GetExecutionInfo().DefaultWorkflowTaskTimeout,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: m.ms.GetExecutionInfo().TaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Attempt:             m.ms.GetExecutionInfo().WorkflowTaskAttempt,
		ScheduledTime:       timestamp.TimePtr(m.ms.timeSource.Now()),
		StartedTime:         timestamp.UnixOrZeroTimePtr(0),
	}

	m.UpdateWorkflowTask(workflowTask)
	return workflowTask, nil
}

func (m *workflowTaskStateMachine) ReplicateWorkflowTaskStartedEvent(
	workflowTask *WorkflowTaskInfo,
	version int64,
	scheduleID int64,
	startedID int64,
	requestID string,
	timestamp time.Time,
) (*WorkflowTaskInfo, error) {
	// Replicator calls it with a nil workflow task info, and it is safe to always lookup the workflow task in this case as it
	// does not have to deal with transient workflow task case.
	var ok bool
	if workflowTask == nil {
		workflowTask, ok = m.GetWorkflowTaskInfo(scheduleID)
		if !ok {
			return nil, serviceerror.NewInternal(fmt.Sprintf("unable to find workflow task: %v", scheduleID))
		}
		// setting workflow task attempt to 1 for workflow task replication
		// this mainly handles transient workflow task completion
		// for transient workflow task, active side will write 2 batch in a "transaction"
		// 1. workflow task scheduled & workflow task started
		// 2. workflow task completed & other events
		// since we need to treat each individual event batch as one transaction
		// certain "magic" needs to be done, i.e. setting attempt to 1 so
		// if first batch is replicated, but not the second one, workflow task can be correctly timed out
		workflowTask.Attempt = 1
	}

	// Update mutable workflow task state
	workflowTask = &WorkflowTaskInfo{
		Version:               version,
		ScheduleID:            scheduleID,
		StartedID:             startedID,
		RequestID:             requestID,
		WorkflowTaskTimeout:   workflowTask.WorkflowTaskTimeout,
		Attempt:               workflowTask.Attempt,
		StartedTime:           &timestamp,
		ScheduledTime:         workflowTask.ScheduledTime,
		TaskQueue:             workflowTask.TaskQueue,
		OriginalScheduledTime: workflowTask.OriginalScheduledTime,
	}

	m.UpdateWorkflowTask(workflowTask)
	return workflowTask, nil
}

func (m *workflowTaskStateMachine) ReplicateWorkflowTaskCompletedEvent(
	event *historypb.HistoryEvent,
) error {
	m.beforeAddWorkflowTaskCompletedEvent()
	return m.afterAddWorkflowTaskCompletedEvent(event, math.MaxInt32)
}

func (m *workflowTaskStateMachine) ReplicateWorkflowTaskFailedEvent() error {
	m.FailWorkflowTask(true)
	return nil
}

func (m *workflowTaskStateMachine) ReplicateWorkflowTaskTimedOutEvent(
	timeoutType enumspb.TimeoutType,
) error {
	incrementAttempt := true
	// Do not increment workflow task attempt in the case of sticky timeout to prevent creating next workflow task as transient
	if timeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START {
		incrementAttempt = false
	}
	m.FailWorkflowTask(incrementAttempt)
	return nil
}

func (m *workflowTaskStateMachine) AddWorkflowTaskScheduleToStartTimeoutEvent(
	scheduleEventID int64,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskTimedOut
	if m.ms.executionInfo.WorkflowTaskScheduleId != scheduleEventID || m.ms.executionInfo.WorkflowTaskStartedId > 0 {
		m.ms.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID),
		)
		return nil, m.ms.createInternalServerError(opTag)
	}

	// clear stickiness whenever workflow task fails
	m.ms.ClearStickyness()

	event := m.ms.hBuilder.AddWorkflowTaskTimedOutEvent(
		scheduleEventID,
		common.EmptyEventID,
		enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
	)
	if err := m.ReplicateWorkflowTaskTimedOutEvent(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START); err != nil {
		return nil, err
	}
	return event, nil
}

// AddWorkflowTaskScheduledEventAsHeartbeat is to record the first scheduled workflow task during workflow task heartbeat.
func (m *workflowTaskStateMachine) AddWorkflowTaskScheduledEventAsHeartbeat(
	bypassTaskGeneration bool,
	originalScheduledTimestamp *time.Time,
) (*WorkflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskScheduled
	if m.HasPendingWorkflowTask() {
		m.ms.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(m.ms.executionInfo.WorkflowTaskScheduleId))
		return nil, m.ms.createInternalServerError(opTag)
	}

	// Task queue and workflow task timeout should already be set from workflow execution started event
	taskQueue := &taskqueuepb.TaskQueue{}
	if m.ms.IsStickyTaskQueueEnabled() {
		taskQueue.Name = m.ms.executionInfo.StickyTaskQueue
		taskQueue.Kind = enumspb.TASK_QUEUE_KIND_STICKY
	} else {
		// It can be because stickyness has expired due to StickyTTL config
		// In that case we need to clear stickyness so that the LastUpdatedTimestamp is not corrupted.
		// In other cases, clearing stickyness shouldn't hurt anything.
		// TODO: https://github.com/temporalio/temporal/issues/2357:
		//  if we can use a new field(LastWorkflowTaskUpdateTimestamp), then we could get rid of it.
		m.ms.ClearStickyness()
		taskQueue.Name = m.ms.executionInfo.TaskQueue
		taskQueue.Kind = enumspb.TASK_QUEUE_KIND_NORMAL
	}
	taskTimeout := timestamp.DurationValue(m.ms.executionInfo.DefaultWorkflowTaskTimeout)

	// Flush any buffered events before creating the workflow task, otherwise it will result in invalid IDs for transient
	// workflow task and will cause in timeout processing to not work for transient workflow tasks
	if m.ms.HasBufferedEvents() {
		// if creating a workflow task and in the mean time events are flushed from buffered events
		// than this workflow taks cannot be a transient workflow task.
		m.ms.executionInfo.WorkflowTaskAttempt = 1
		m.ms.updatePendingEventIDs(m.ms.hBuilder.FlushBufferToCurrentBatch())
	} else if m.ms.executionInfo.WorkflowTaskAttempt > 1 {
		lastWriteVersion, err := m.ms.GetLastWriteVersion()
		if err != nil {
			return nil, err
		}

		if m.ms.GetCurrentVersion() != lastWriteVersion {
			// during transient workflow task cannot allow version changes
			// mark the attempt to be 1 to NOT use transient workflow task
			m.ms.executionInfo.WorkflowTaskAttempt = 1
		}
	}

	var newWorkflowTaskEvent *historypb.HistoryEvent
	scheduleID := m.ms.GetNextEventID() // we will generate the schedule event later for repeatedly failing workflow tasks
	// Avoid creating new history events when workflow tasks are continuously failing
	scheduleTime := m.ms.timeSource.Now().UTC()
	if m.ms.executionInfo.WorkflowTaskAttempt == 1 {
		newWorkflowTaskEvent = m.ms.hBuilder.AddWorkflowTaskScheduledEvent(
			taskQueue,
			int32(taskTimeout.Seconds()),
			m.ms.executionInfo.WorkflowTaskAttempt,
			m.ms.timeSource.Now(),
		)
		scheduleID = newWorkflowTaskEvent.GetEventId()
		scheduleTime = timestamp.TimeValue(newWorkflowTaskEvent.GetEventTime())
	}

	workflowTask, err := m.ReplicateWorkflowTaskScheduledEvent(
		m.ms.GetCurrentVersion(),
		scheduleID,
		taskQueue,
		int32(taskTimeout.Seconds()),
		m.ms.executionInfo.WorkflowTaskAttempt,
		&scheduleTime,
		originalScheduledTimestamp,
	)
	if err != nil {
		return nil, err
	}

	// TODO merge active & passive task generation
	if !bypassTaskGeneration {
		if err := m.ms.taskGenerator.GenerateScheduleWorkflowTaskTasks(
			scheduleTime, // schedule time is now
			scheduleID,
		); err != nil {
			return nil, err
		}
	}

	return workflowTask, nil
}

func (m *workflowTaskStateMachine) AddWorkflowTaskScheduledEvent(
	bypassTaskGeneration bool,
) (*WorkflowTaskInfo, error) {
	return m.AddWorkflowTaskScheduledEventAsHeartbeat(bypassTaskGeneration, timestamp.TimePtr(m.ms.timeSource.Now()))
}

func (m *workflowTaskStateMachine) AddFirstWorkflowTaskScheduled(
	startEvent *historypb.HistoryEvent,
) error {
	// handle first workflow task case, i.e. possible delayed workflow task
	//
	// below handles the following cases:
	// 1. if not continue as new & if workflow has no parent
	//   -> schedule workflow task & schedule delayed workflow task
	// 2. if not continue as new & if workflow has parent
	//   -> this function should not be called during workflow start, but should be called as
	//      part of schedule workflow task in 2 phase commit
	//
	// if continue as new
	//  1. whether has parent workflow or not
	//   -> schedule workflow task & schedule delayed workflow task
	//
	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()
	workflowTaskBackoffDuration := timestamp.DurationValue(startAttr.GetFirstWorkflowTaskBackoff())

	var err error
	if workflowTaskBackoffDuration != 0 {
		if err = m.ms.taskGenerator.GenerateDelayedWorkflowTasks(
			timestamp.TimeValue(startEvent.GetEventTime()),
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

func (m *workflowTaskStateMachine) AddWorkflowTaskStartedEvent(
	scheduleEventID int64,
	requestID string,
	taskQueue *taskqueuepb.TaskQueue,
	identity string,
) (*historypb.HistoryEvent, *WorkflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskStarted
	workflowTask, ok := m.GetWorkflowTaskInfo(scheduleEventID)
	if !ok || workflowTask.StartedID != common.EmptyEventID {
		m.ms.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID))
		return nil, nil, m.ms.createInternalServerError(opTag)
	}

	var event *historypb.HistoryEvent
	scheduleID := workflowTask.ScheduleID
	startedID := scheduleID + 1
	startTime := m.ms.timeSource.Now()
	// First check to see if new events came since transient workflowTask was scheduled
	if workflowTask.Attempt > 1 && (workflowTask.ScheduleID != m.ms.GetNextEventID() || workflowTask.Version != m.ms.GetCurrentVersion()) {
		// Also create a new WorkflowTaskScheduledEvent since new events came in when it was scheduled
		scheduleEvent := m.ms.hBuilder.AddWorkflowTaskScheduledEvent(
			taskQueue,
			int32(workflowTask.WorkflowTaskTimeout.Seconds()),
			1,
			m.ms.timeSource.Now(),
		)
		scheduleID = scheduleEvent.GetEventId()
		workflowTask.Attempt = 1
	}

	// Avoid creating new history events when workflow tasks are continuously failing
	if workflowTask.Attempt == 1 {
		// Now create WorkflowTaskStartedEvent
		event = m.ms.hBuilder.AddWorkflowTaskStartedEvent(
			scheduleID,
			requestID,
			identity,
			m.ms.timeSource.Now(),
		)
		m.ms.hBuilder.FlushAndCreateNewBatch()
		startedID = event.GetEventId()
		startTime = timestamp.TimeValue(event.GetEventTime())
	}

	workflowTask, err := m.ReplicateWorkflowTaskStartedEvent(workflowTask, m.ms.GetCurrentVersion(), scheduleID, startedID, requestID, startTime)
	// TODO merge active & passive task generation
	if err := m.ms.taskGenerator.GenerateStartWorkflowTaskTasks(
		startTime, // start time is now
		scheduleID,
	); err != nil {
		return nil, nil, err
	}
	return event, workflowTask, err
}

func (m *workflowTaskStateMachine) AddWorkflowTaskCompletedEvent(
	scheduleEventID int64,
	startedEventID int64,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
	maxResetPoints int,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskCompleted
	workflowTask, ok := m.GetWorkflowTaskInfo(scheduleEventID)
	if !ok || workflowTask.StartedID != startedEventID {
		m.ms.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID),
			tag.WorkflowStartedID(startedEventID))

		return nil, m.ms.createInternalServerError(opTag)
	}

	m.beforeAddWorkflowTaskCompletedEvent()
	if workflowTask.Attempt > 1 {
		// Create corresponding WorkflowTaskSchedule and WorkflowTaskStarted events for workflow tasks we have been retrying
		taskQueue := &taskqueuepb.TaskQueue{
			Name: m.ms.executionInfo.TaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}
		scheduledEvent := m.ms.hBuilder.AddWorkflowTaskScheduledEvent(
			taskQueue,
			int32(workflowTask.WorkflowTaskTimeout.Seconds()),
			workflowTask.Attempt,
			timestamp.TimeValue(workflowTask.ScheduledTime).UTC(),
		)
		startedEvent := m.ms.hBuilder.AddWorkflowTaskStartedEvent(
			scheduledEvent.GetEventId(),
			workflowTask.RequestID,
			request.GetIdentity(),
			timestamp.TimeValue(workflowTask.StartedTime),
		)
		m.ms.hBuilder.FlushAndCreateNewBatch()
		startedEventID = startedEvent.GetEventId()
	}
	// Now write the completed event
	event := m.ms.hBuilder.AddWorkflowTaskCompletedEvent(
		scheduleEventID,
		startedEventID,
		request.Identity,
		request.BinaryChecksum,
	)

	err := m.afterAddWorkflowTaskCompletedEvent(event, maxResetPoints)
	if err != nil {
		return nil, err
	}
	return event, nil
}

func (m *workflowTaskStateMachine) AddWorkflowTaskFailedEvent(
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

	dt, ok := m.GetWorkflowTaskInfo(scheduleEventID)
	if !ok || dt.StartedID != startedEventID {
		m.ms.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID),
			tag.WorkflowStartedID(startedEventID))
		return nil, m.ms.createInternalServerError(opTag)
	}

	var event *historypb.HistoryEvent
	// Only emit WorkflowTaskFailedEvent for the very first time
	if dt.Attempt == 1 {
		event = m.ms.hBuilder.AddWorkflowTaskFailedEvent(
			attr.ScheduledEventId,
			attr.StartedEventId,
			attr.Cause,
			attr.Failure,
			attr.Identity,
			attr.BaseRunId,
			attr.NewRunId,
			attr.ForkEventVersion,
			attr.BinaryChecksum,
		)
	}

	if err := m.ReplicateWorkflowTaskFailedEvent(); err != nil {
		return nil, err
	}

	// always clear workflow task attempt for reset
	if cause == enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW ||
		cause == enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND {
		m.ms.executionInfo.WorkflowTaskAttempt = 1
	}
	return event, nil
}

func (m *workflowTaskStateMachine) AddWorkflowTaskTimedOutEvent(
	scheduleEventID int64,
	startedEventID int64,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskTimedOut
	dt, ok := m.GetWorkflowTaskInfo(scheduleEventID)
	if !ok || dt.StartedID != startedEventID {
		m.ms.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID),
			tag.WorkflowStartedID(startedEventID))
		return nil, m.ms.createInternalServerError(opTag)
	}

	var event *historypb.HistoryEvent
	// Avoid creating new history events when workflow tasks are continuously timing out
	if dt.Attempt == 1 {
		event = m.ms.hBuilder.AddWorkflowTaskTimedOutEvent(
			scheduleEventID,
			startedEventID,
			enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		)
	}

	if err := m.ReplicateWorkflowTaskTimedOutEvent(enumspb.TIMEOUT_TYPE_START_TO_CLOSE); err != nil {
		return nil, err
	}
	return event, nil
}

func (m *workflowTaskStateMachine) FailWorkflowTask(
	incrementAttempt bool,
) {
	// clear stickiness whenever workflow task fails
	m.ms.ClearStickyness()

	failWorkflowTaskInfo := &WorkflowTaskInfo{
		Version:               common.EmptyVersion,
		ScheduleID:            common.EmptyEventID,
		StartedID:             common.EmptyEventID,
		RequestID:             emptyUUID,
		WorkflowTaskTimeout:   timestamp.DurationFromSeconds(0),
		StartedTime:           timestamp.UnixOrZeroTimePtr(0),
		TaskQueue:             nil,
		OriginalScheduledTime: timestamp.UnixOrZeroTimePtr(0),
		Attempt:               1,
	}
	if incrementAttempt {
		failWorkflowTaskInfo.Attempt = m.ms.executionInfo.WorkflowTaskAttempt + 1
		failWorkflowTaskInfo.ScheduledTime = timestamp.TimePtr(m.ms.timeSource.Now().UTC())
	}
	m.UpdateWorkflowTask(failWorkflowTaskInfo)
}

// DeleteWorkflowTask deletes a workflow task.
func (m *workflowTaskStateMachine) DeleteWorkflowTask() {
	resetWorkflowTaskInfo := &WorkflowTaskInfo{
		Version:             common.EmptyVersion,
		ScheduleID:          common.EmptyEventID,
		StartedID:           common.EmptyEventID,
		RequestID:           emptyUUID,
		WorkflowTaskTimeout: timestamp.DurationFromSeconds(0),
		Attempt:             1,
		StartedTime:         timestamp.UnixOrZeroTimePtr(0),
		ScheduledTime:       timestamp.UnixOrZeroTimePtr(0),

		TaskQueue: nil,
		// Keep the last original scheduled Timestamp, so that AddWorkflowTaskScheduledEventAsHeartbeat can continue with it.
		OriginalScheduledTime: m.getWorkflowTaskInfo().OriginalScheduledTime,
	}
	m.UpdateWorkflowTask(resetWorkflowTaskInfo)
}

// UpdateWorkflowTask updates a workflow task.
func (m *workflowTaskStateMachine) UpdateWorkflowTask(
	workflowTask *WorkflowTaskInfo,
) {
	m.ms.executionInfo.WorkflowTaskVersion = workflowTask.Version
	m.ms.executionInfo.WorkflowTaskScheduleId = workflowTask.ScheduleID
	m.ms.executionInfo.WorkflowTaskStartedId = workflowTask.StartedID
	m.ms.executionInfo.WorkflowTaskRequestId = workflowTask.RequestID
	m.ms.executionInfo.WorkflowTaskTimeout = workflowTask.WorkflowTaskTimeout
	m.ms.executionInfo.WorkflowTaskAttempt = workflowTask.Attempt
	m.ms.executionInfo.WorkflowTaskStartedTime = workflowTask.StartedTime
	m.ms.executionInfo.WorkflowTaskScheduledTime = workflowTask.ScheduledTime
	m.ms.executionInfo.WorkflowTaskOriginalScheduledTime = workflowTask.OriginalScheduledTime

	// NOTE: do not update taskqueue in execution info

	m.ms.logger.Debug("Workflow task updated",
		tag.WorkflowScheduleID(workflowTask.ScheduleID),
		tag.WorkflowStartedID(workflowTask.StartedID),
		tag.WorkflowTaskRequestId(workflowTask.RequestID),
		tag.WorkflowTaskTimeout(workflowTask.WorkflowTaskTimeout),
		tag.Attempt(workflowTask.Attempt),
		tag.WorkflowStartedTimestamp(workflowTask.StartedTime))
}

func (m *workflowTaskStateMachine) HasPendingWorkflowTask() bool {
	return m.ms.executionInfo.WorkflowTaskScheduleId != common.EmptyEventID
}

func (m *workflowTaskStateMachine) GetPendingWorkflowTask() (*WorkflowTaskInfo, bool) {
	if m.ms.executionInfo.WorkflowTaskScheduleId == common.EmptyEventID {
		return nil, false
	}

	workflowTask := m.getWorkflowTaskInfo()
	return workflowTask, true
}

func (m *workflowTaskStateMachine) HasInFlightWorkflowTask() bool {
	return m.ms.executionInfo.WorkflowTaskStartedId > 0
}

func (m *workflowTaskStateMachine) GetInFlightWorkflowTask() (*WorkflowTaskInfo, bool) {
	if m.ms.executionInfo.WorkflowTaskScheduleId == common.EmptyEventID ||
		m.ms.executionInfo.WorkflowTaskStartedId == common.EmptyEventID {
		return nil, false
	}

	workflowTask := m.getWorkflowTaskInfo()
	return workflowTask, true
}

func (m *workflowTaskStateMachine) HasProcessedOrPendingWorkflowTask() bool {
	return m.HasPendingWorkflowTask() || m.ms.GetPreviousStartedEventID() != common.EmptyEventID
}

// GetWorkflowTaskInfo returns details about the in-progress workflow task
func (m *workflowTaskStateMachine) GetWorkflowTaskInfo(
	scheduleEventID int64,
) (*WorkflowTaskInfo, bool) {
	workflowTask := m.getWorkflowTaskInfo()
	if scheduleEventID == workflowTask.ScheduleID {
		return workflowTask, true
	}
	return nil, false
}

func (m *workflowTaskStateMachine) CreateTransientWorkflowTaskEvents(
	workflowTask *WorkflowTaskInfo,
	identity string,
) (*historypb.HistoryEvent, *historypb.HistoryEvent) {
	taskQueue := &taskqueuepb.TaskQueue{
		Name: m.ms.executionInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	scheduledEvent := &historypb.HistoryEvent{
		EventId:   workflowTask.ScheduleID,
		EventTime: workflowTask.ScheduledTime,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Version:   m.ms.currentVersion,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
			WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
				TaskQueue:           taskQueue,
				StartToCloseTimeout: workflowTask.WorkflowTaskTimeout,
				Attempt:             workflowTask.Attempt,
			},
		},
	}

	startEvent := &historypb.HistoryEvent{
		EventId:   workflowTask.StartedID,
		EventTime: workflowTask.StartedTime,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Version:   m.ms.currentVersion,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
			WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
				ScheduledEventId: workflowTask.ScheduleID,
				Identity:         identity,
				RequestId:        workflowTask.RequestID,
			},
		},
	}

	return scheduledEvent, startEvent
}

func (m *workflowTaskStateMachine) getWorkflowTaskInfo() *WorkflowTaskInfo {
	taskQueue := &taskqueuepb.TaskQueue{}
	if m.ms.IsStickyTaskQueueEnabled() {
		taskQueue.Name = m.ms.executionInfo.StickyTaskQueue
		taskQueue.Kind = enumspb.TASK_QUEUE_KIND_STICKY
	} else {
		taskQueue.Name = m.ms.executionInfo.TaskQueue
		taskQueue.Kind = enumspb.TASK_QUEUE_KIND_NORMAL
	}

	return &WorkflowTaskInfo{
		Version:               m.ms.executionInfo.WorkflowTaskVersion,
		ScheduleID:            m.ms.executionInfo.WorkflowTaskScheduleId,
		StartedID:             m.ms.executionInfo.WorkflowTaskStartedId,
		RequestID:             m.ms.executionInfo.WorkflowTaskRequestId,
		WorkflowTaskTimeout:   m.ms.executionInfo.WorkflowTaskTimeout,
		Attempt:               m.ms.executionInfo.WorkflowTaskAttempt,
		StartedTime:           m.ms.executionInfo.WorkflowTaskStartedTime,
		ScheduledTime:         m.ms.executionInfo.WorkflowTaskScheduledTime,
		TaskQueue:             taskQueue,
		OriginalScheduledTime: m.ms.executionInfo.WorkflowTaskOriginalScheduledTime,
	}
}

func (m *workflowTaskStateMachine) beforeAddWorkflowTaskCompletedEvent() {
	// Make sure to delete workflow task before adding events. Otherwise they are buffered rather than getting appended.
	m.DeleteWorkflowTask()
}

func (m *workflowTaskStateMachine) afterAddWorkflowTaskCompletedEvent(
	event *historypb.HistoryEvent,
	maxResetPoints int,
) error {
	m.ms.executionInfo.LastProcessedEvent = event.GetWorkflowTaskCompletedEventAttributes().GetStartedEventId()
	return m.ms.addBinaryCheckSumIfNotExists(event, maxResetPoints)
}
