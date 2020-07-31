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
	mutableStateWorkflowTaskManager interface {
		ReplicateWorkflowTaskScheduledEvent(version int64, scheduleID int64, taskQueue *taskqueuepb.TaskQueue, startToCloseTimeoutSeconds int32, attempt int32, scheduleTimestamp int64, originalScheduledTimestamp int64) (*workflowTaskInfo, error)
		ReplicateTransientWorkflowTaskScheduled() (*workflowTaskInfo, error)
		ReplicateWorkflowTaskStartedEvent(
			workflowTask *workflowTaskInfo,
			version int64,
			scheduleID int64,
			startedID int64,
			requestID string,
			timestamp int64,
		) (*workflowTaskInfo, error)
		ReplicateWorkflowTaskCompletedEvent(event *historypb.HistoryEvent) error
		ReplicateWorkflowTaskFailedEvent() error
		ReplicateWorkflowTaskTimedOutEvent(timeoutType enumspb.TimeoutType) error

		AddWorkflowTaskScheduleToStartTimeoutEvent(scheduleEventID int64) (*historypb.HistoryEvent, error)
		AddWorkflowTaskScheduledEventAsHeartbeat(
			bypassTaskGeneration bool,
			originalScheduledTimestamp int64,
		) (*workflowTaskInfo, error)
		AddWorkflowTaskScheduledEvent(bypassTaskGeneration bool) (*workflowTaskInfo, error)
		AddFirstWorkflowTaskScheduled(startEvent *historypb.HistoryEvent) error
		AddWorkflowTaskStartedEvent(
			scheduleEventID int64,
			requestID string,
			request *workflowservice.PollWorkflowTaskQueueRequest,
		) (*historypb.HistoryEvent, *workflowTaskInfo, error)
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

		FailWorkflowTask(incrementAttempt bool)
		DeleteWorkflowTask()
		UpdateWorkflowTask(workflowTask *workflowTaskInfo)

		HasPendingWorkflowTask() bool
		GetPendingWorkflowTask() (*workflowTaskInfo, bool)
		HasInFlightWorkflowTask() bool
		GetInFlightWorkflowTask() (*workflowTaskInfo, bool)
		HasProcessedOrPendingWorkflowTask() bool
		GetWorkflowTaskInfo(scheduleEventID int64) (*workflowTaskInfo, bool)

		CreateTransientWorkflowTaskEvents(workflowTask *workflowTaskInfo, identity string) (*historypb.HistoryEvent, *historypb.HistoryEvent)
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

func (m *mutableStateWorkflowTaskManagerImpl) ReplicateWorkflowTaskScheduledEvent(version int64, scheduleID int64, taskQueue *taskqueuepb.TaskQueue, startToCloseTimeoutSeconds int32, attempt int32, scheduleTimestamp int64, originalScheduledTimestamp int64) (*workflowTaskInfo, error) {

	// set workflow state to running, since workflow task is scheduled
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

	workflowTask := &workflowTaskInfo{
		Version:                    version,
		ScheduleID:                 scheduleID,
		StartedID:                  common.EmptyEventID,
		RequestID:                  emptyUUID,
		WorkflowTaskTimeout:        int64(startToCloseTimeoutSeconds),
		TaskQueue:                  taskQueue,
		Attempt:                    attempt,
		ScheduledTimestamp:         scheduleTimestamp,
		StartedTimestamp:           0,
		OriginalScheduledTimestamp: originalScheduledTimestamp,
	}

	m.UpdateWorkflowTask(workflowTask)
	return workflowTask, nil
}

func (m *mutableStateWorkflowTaskManagerImpl) ReplicateTransientWorkflowTaskScheduled() (*workflowTaskInfo, error) {
	if m.HasPendingWorkflowTask() || m.msb.GetExecutionInfo().WorkflowTaskAttempt == 1 {
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
	workflowTask := &workflowTaskInfo{
		Version:             m.msb.GetCurrentVersion(),
		ScheduleID:          m.msb.GetNextEventID(),
		StartedID:           common.EmptyEventID,
		RequestID:           emptyUUID,
		WorkflowTaskTimeout: m.msb.GetExecutionInfo().DefaultWorkflowTaskTimeout,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: m.msb.GetExecutionInfo().TaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Attempt:             m.msb.GetExecutionInfo().WorkflowTaskAttempt,
		ScheduledTimestamp:  m.msb.timeSource.Now().UnixNano(),
		StartedTimestamp:    0,
	}

	m.UpdateWorkflowTask(workflowTask)
	return workflowTask, nil
}

func (m *mutableStateWorkflowTaskManagerImpl) ReplicateWorkflowTaskStartedEvent(
	workflowTask *workflowTaskInfo,
	version int64,
	scheduleID int64,
	startedID int64,
	requestID string,
	timestamp int64,
) (*workflowTaskInfo, error) {
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
	workflowTask = &workflowTaskInfo{
		Version:                    version,
		ScheduleID:                 scheduleID,
		StartedID:                  startedID,
		RequestID:                  requestID,
		WorkflowTaskTimeout:        workflowTask.WorkflowTaskTimeout,
		Attempt:                    workflowTask.Attempt,
		StartedTimestamp:           timestamp,
		ScheduledTimestamp:         workflowTask.ScheduledTimestamp,
		TaskQueue:                  workflowTask.TaskQueue,
		OriginalScheduledTimestamp: workflowTask.OriginalScheduledTimestamp,
	}

	m.UpdateWorkflowTask(workflowTask)
	return workflowTask, nil
}

func (m *mutableStateWorkflowTaskManagerImpl) ReplicateWorkflowTaskCompletedEvent(
	event *historypb.HistoryEvent,
) error {
	m.beforeAddWorkflowTaskCompletedEvent()
	return m.afterAddWorkflowTaskCompletedEvent(event, math.MaxInt32)
}

func (m *mutableStateWorkflowTaskManagerImpl) ReplicateWorkflowTaskFailedEvent() error {
	m.FailWorkflowTask(true)
	return nil
}

func (m *mutableStateWorkflowTaskManagerImpl) ReplicateWorkflowTaskTimedOutEvent(
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

func (m *mutableStateWorkflowTaskManagerImpl) AddWorkflowTaskScheduleToStartTimeoutEvent(
	scheduleEventID int64,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskTimedOut
	if m.msb.executionInfo.WorkflowTaskScheduleID != scheduleEventID || m.msb.executionInfo.WorkflowTaskStartedID > 0 {
		m.msb.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.msb.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID),
		)
		return nil, m.msb.createInternalServerError(opTag)
	}

	// Clear stickiness whenever workflow task fails
	m.msb.ClearStickyness()

	event := m.msb.hBuilder.AddWorkflowTaskTimedOutEvent(scheduleEventID, 0, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START)

	if err := m.ReplicateWorkflowTaskTimedOutEvent(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START); err != nil {
		return nil, err
	}
	return event, nil
}

// originalScheduledTimestamp is to record the first scheduled workflow task during workflow task heartbeat.
func (m *mutableStateWorkflowTaskManagerImpl) AddWorkflowTaskScheduledEventAsHeartbeat(
	bypassTaskGeneration bool,
	originalScheduledTimestamp int64,
) (*workflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskScheduled
	if m.HasPendingWorkflowTask() {
		m.msb.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.msb.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(m.msb.executionInfo.WorkflowTaskScheduleID))
		return nil, m.msb.createInternalServerError(opTag)
	}

	// Task queue and workflow task timeout should already be set from workflow execution started event
	taskQueue := &taskqueuepb.TaskQueue{}
	if m.msb.IsStickyTaskQueueEnabled() {
		taskQueue.Name = m.msb.executionInfo.StickyTaskQueue
		taskQueue.Kind = enumspb.TASK_QUEUE_KIND_STICKY
	} else {
		// It can be because stickyness has expired due to StickyTTL config
		// In that case we need to clear stickyness so that the LastUpdateTimestamp is not corrupted.
		// In other cases, clearing stickyness shouldn't hurt anything.
		// TODO: https://github.com/temporalio/temporal/issues/2357:
		//  if we can use a new field(LastWorkflowTaskUpdateTimestamp), then we could get rid of it.
		m.msb.ClearStickyness()
		taskQueue.Name = m.msb.executionInfo.TaskQueue
		taskQueue.Kind = enumspb.TASK_QUEUE_KIND_NORMAL
	}
	taskTimeout := int32(m.msb.executionInfo.DefaultWorkflowTaskTimeout)

	// Flush any buffered events before creating the workflow task, otherwise it will result in invalid IDs for transient
	// workflow task and will cause in timeout processing to not work for transient workflow tasks
	if m.msb.HasBufferedEvents() {
		// if creating a workflow task and in the mean time events are flushed from buffered events
		// than this workflow taks cannot be a transient workflow task.
		m.msb.executionInfo.WorkflowTaskAttempt = 1
		if err := m.msb.FlushBufferedEvents(); err != nil {
			return nil, err
		}
	}

	var newWorkflowTaskEvent *historypb.HistoryEvent
	scheduleID := m.msb.GetNextEventID() // we will generate the schedule event later for repeatedly failing workflow tasks
	// Avoid creating new history events when workflow tasks are continuously failing
	scheduleTime := m.msb.timeSource.Now()
	if m.msb.executionInfo.WorkflowTaskAttempt == 1 {
		newWorkflowTaskEvent = m.msb.hBuilder.AddWorkflowTaskScheduledEvent(taskQueue, taskTimeout,
			m.msb.executionInfo.WorkflowTaskAttempt)
		scheduleID = newWorkflowTaskEvent.GetEventId()
		scheduleTime = timestamp.TimeValue(newWorkflowTaskEvent.GetEventTime())
	}

	workflowTask, err := m.ReplicateWorkflowTaskScheduledEvent(
		m.msb.GetCurrentVersion(),
		scheduleID,
		taskQueue,
		taskTimeout,
		m.msb.executionInfo.WorkflowTaskAttempt,
		scheduleTime.UnixNano(),
		originalScheduledTimestamp,
	)
	if err != nil {
		return nil, err
	}

	// TODO merge active & passive task generation
	if !bypassTaskGeneration {
		if err := m.msb.taskGenerator.generateScheduleWorkflowTaskTasks(
			scheduleTime, // schedule time is now
			scheduleID,
		); err != nil {
			return nil, err
		}
	}

	return workflowTask, nil
}

func (m *mutableStateWorkflowTaskManagerImpl) AddWorkflowTaskScheduledEvent(
	bypassTaskGeneration bool,
) (*workflowTaskInfo, error) {
	return m.AddWorkflowTaskScheduledEventAsHeartbeat(bypassTaskGeneration, m.msb.timeSource.Now().UnixNano())
}

func (m *mutableStateWorkflowTaskManagerImpl) AddFirstWorkflowTaskScheduled(
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
		if err = m.msb.taskGenerator.generateDelayedWorkflowTasks(
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

func (m *mutableStateWorkflowTaskManagerImpl) AddWorkflowTaskStartedEvent(
	scheduleEventID int64,
	requestID string,
	request *workflowservice.PollWorkflowTaskQueueRequest,
) (*historypb.HistoryEvent, *workflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskStarted
	workflowTask, ok := m.GetWorkflowTaskInfo(scheduleEventID)
	if !ok || workflowTask.StartedID != common.EmptyEventID {
		m.msb.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.msb.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID))
		return nil, nil, m.msb.createInternalServerError(opTag)
	}

	var event *historypb.HistoryEvent
	scheduleID := workflowTask.ScheduleID
	startedID := scheduleID + 1
	startTime := m.msb.timeSource.Now()
	// First check to see if new events came since transient workflowTask was scheduled
	if workflowTask.Attempt > 1 && workflowTask.ScheduleID != m.msb.GetNextEventID() {
		// Also create a new WorkflowTaskScheduledEvent since new events came in when it was scheduled
		scheduleEvent := m.msb.hBuilder.AddWorkflowTaskScheduledEvent(request.GetTaskQueue(), int32(workflowTask.WorkflowTaskTimeout), 0)
		scheduleID = scheduleEvent.GetEventId()
		workflowTask.Attempt = 1
	}

	// Avoid creating new history events when workflow tasks are continuously failing
	if workflowTask.Attempt == 1 {
		// Now create WorkflowTaskStartedEvent
		event = m.msb.hBuilder.AddWorkflowTaskStartedEvent(scheduleID, requestID, request.GetIdentity())
		startedID = event.GetEventId()
		startTime = timestamp.TimeValue(event.GetEventTime())
	}

	workflowTask, err := m.ReplicateWorkflowTaskStartedEvent(workflowTask, m.msb.GetCurrentVersion(), scheduleID, startedID, requestID, startTime.UnixNano())
	// TODO merge active & passive task generation
	if err := m.msb.taskGenerator.generateStartWorkflowTaskTasks(
		startTime, // start time is now
		scheduleID,
	); err != nil {
		return nil, nil, err
	}
	return event, workflowTask, err
}

func (m *mutableStateWorkflowTaskManagerImpl) AddWorkflowTaskCompletedEvent(
	scheduleEventID int64,
	startedEventID int64,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
	maxResetPoints int,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskCompleted
	workflowTask, ok := m.GetWorkflowTaskInfo(scheduleEventID)
	if !ok || workflowTask.StartedID != startedEventID {
		m.msb.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.msb.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID),
			tag.WorkflowStartedID(startedEventID))

		return nil, m.msb.createInternalServerError(opTag)
	}

	m.beforeAddWorkflowTaskCompletedEvent()
	if workflowTask.Attempt > 1 {
		// Create corresponding WorkflowTaskSchedule and WorkflowTaskStarted events for workflow tasks we have been retrying
		taskQueue := &taskqueuepb.TaskQueue{
			Name: m.msb.executionInfo.TaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}
		scheduledEvent := m.msb.hBuilder.AddTransientWorkflowTaskScheduledEvent(taskQueue, int32(workflowTask.WorkflowTaskTimeout),
			workflowTask.Attempt, timestamp.UnixOrZeroTime(workflowTask.ScheduledTimestamp))
		startedEvent := m.msb.hBuilder.AddTransientWorkflowTaskStartedEvent(scheduledEvent.GetEventId(), workflowTask.RequestID,
			request.GetIdentity(), timestamp.UnixOrZeroTime(workflowTask.StartedTimestamp))
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

	dt, ok := m.GetWorkflowTaskInfo(scheduleEventID)
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
	if dt.Attempt == 1 {
		event = m.msb.hBuilder.AddWorkflowTaskFailedEvent(attr)
	}

	if err := m.ReplicateWorkflowTaskFailedEvent(); err != nil {
		return nil, err
	}

	// always clear workflow task attempt for reset
	if cause == enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW ||
		cause == enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND {
		m.msb.executionInfo.WorkflowTaskAttempt = 1
	}
	return event, nil
}

func (m *mutableStateWorkflowTaskManagerImpl) AddWorkflowTaskTimedOutEvent(
	scheduleEventID int64,
	startedEventID int64,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskTimedOut
	dt, ok := m.GetWorkflowTaskInfo(scheduleEventID)
	if !ok || dt.StartedID != startedEventID {
		m.msb.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.msb.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduleID(scheduleEventID),
			tag.WorkflowStartedID(startedEventID))
		return nil, m.msb.createInternalServerError(opTag)
	}

	var event *historypb.HistoryEvent
	// Avoid creating new history events when workflow tasks are continuously timing out
	if dt.Attempt == 1 {
		event = m.msb.hBuilder.AddWorkflowTaskTimedOutEvent(scheduleEventID, startedEventID, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	}

	if err := m.ReplicateWorkflowTaskTimedOutEvent(enumspb.TIMEOUT_TYPE_START_TO_CLOSE); err != nil {
		return nil, err
	}
	return event, nil
}

func (m *mutableStateWorkflowTaskManagerImpl) FailWorkflowTask(
	incrementAttempt bool,
) {
	// Clear stickiness whenever workflow task fails
	m.msb.ClearStickyness()

	failWorkflowTaskInfo := &workflowTaskInfo{
		Version:                    common.EmptyVersion,
		ScheduleID:                 common.EmptyEventID,
		StartedID:                  common.EmptyEventID,
		RequestID:                  emptyUUID,
		WorkflowTaskTimeout:        0,
		StartedTimestamp:           0,
		TaskQueue:                  nil,
		OriginalScheduledTimestamp: 0,
		Attempt:                    1,
	}
	if incrementAttempt {
		failWorkflowTaskInfo.Attempt = m.msb.executionInfo.WorkflowTaskAttempt + 1
		failWorkflowTaskInfo.ScheduledTimestamp = m.msb.timeSource.Now().UnixNano()
	}
	m.UpdateWorkflowTask(failWorkflowTaskInfo)
}

// DeleteWorkflowTask deletes a workflow task.
func (m *mutableStateWorkflowTaskManagerImpl) DeleteWorkflowTask() {
	resetWorkflowTaskInfo := &workflowTaskInfo{
		Version:             common.EmptyVersion,
		ScheduleID:          common.EmptyEventID,
		StartedID:           common.EmptyEventID,
		RequestID:           emptyUUID,
		WorkflowTaskTimeout: 0,
		Attempt:             1,
		StartedTimestamp:    0,
		ScheduledTimestamp:  0,

		TaskQueue: nil,
		// Keep the last original scheduled timestamp, so that AddWorkflowTaskScheduledEventAsHeartbeat can continue with it.
		OriginalScheduledTimestamp: m.getWorkflowTaskInfo().OriginalScheduledTimestamp,
	}
	m.UpdateWorkflowTask(resetWorkflowTaskInfo)
}

// UpdateWorkflowTask updates a workflow task.
func (m *mutableStateWorkflowTaskManagerImpl) UpdateWorkflowTask(
	workflowTask *workflowTaskInfo,
) {
	m.msb.executionInfo.WorkflowTaskVersion = workflowTask.Version
	m.msb.executionInfo.WorkflowTaskScheduleID = workflowTask.ScheduleID
	m.msb.executionInfo.WorkflowTaskStartedID = workflowTask.StartedID
	m.msb.executionInfo.WorkflowTaskRequestID = workflowTask.RequestID
	m.msb.executionInfo.WorkflowTaskTimeout = workflowTask.WorkflowTaskTimeout
	m.msb.executionInfo.WorkflowTaskAttempt = workflowTask.Attempt
	m.msb.executionInfo.WorkflowTaskStartedTimestamp = workflowTask.StartedTimestamp
	m.msb.executionInfo.WorkflowTaskScheduledTimestamp = workflowTask.ScheduledTimestamp
	m.msb.executionInfo.WorkflowTaskOriginalScheduledTimestamp = workflowTask.OriginalScheduledTimestamp

	// NOTE: do not update taskqueue in execution info

	m.msb.logger.Debug("Workflow task updated",
		tag.WorkflowScheduleID(workflowTask.ScheduleID),
		tag.WorkflowStartedID(workflowTask.StartedID),
		tag.WorkflowTaskRequestId(workflowTask.RequestID),
		tag.WorkflowTaskTimeoutSeconds(workflowTask.WorkflowTaskTimeout),
		tag.Attempt(workflowTask.Attempt),
		tag.TimestampInt(workflowTask.StartedTimestamp))
}

func (m *mutableStateWorkflowTaskManagerImpl) HasPendingWorkflowTask() bool {
	return m.msb.executionInfo.WorkflowTaskScheduleID != common.EmptyEventID
}

func (m *mutableStateWorkflowTaskManagerImpl) GetPendingWorkflowTask() (*workflowTaskInfo, bool) {
	if m.msb.executionInfo.WorkflowTaskScheduleID == common.EmptyEventID {
		return nil, false
	}

	workflowTask := m.getWorkflowTaskInfo()
	return workflowTask, true
}

func (m *mutableStateWorkflowTaskManagerImpl) HasInFlightWorkflowTask() bool {
	return m.msb.executionInfo.WorkflowTaskStartedID > 0
}

func (m *mutableStateWorkflowTaskManagerImpl) GetInFlightWorkflowTask() (*workflowTaskInfo, bool) {
	if m.msb.executionInfo.WorkflowTaskScheduleID == common.EmptyEventID ||
		m.msb.executionInfo.WorkflowTaskStartedID == common.EmptyEventID {
		return nil, false
	}

	workflowTask := m.getWorkflowTaskInfo()
	return workflowTask, true
}

func (m *mutableStateWorkflowTaskManagerImpl) HasProcessedOrPendingWorkflowTask() bool {
	return m.HasPendingWorkflowTask() || m.msb.GetPreviousStartedEventID() != common.EmptyEventID
}

// GetWorkflowTaskInfo returns details about the in-progress workflow task
func (m *mutableStateWorkflowTaskManagerImpl) GetWorkflowTaskInfo(
	scheduleEventID int64,
) (*workflowTaskInfo, bool) {
	workflowTask := m.getWorkflowTaskInfo()
	if scheduleEventID == workflowTask.ScheduleID {
		return workflowTask, true
	}
	return nil, false
}

func (m *mutableStateWorkflowTaskManagerImpl) CreateTransientWorkflowTaskEvents(
	workflowTask *workflowTaskInfo,
	identity string,
) (*historypb.HistoryEvent, *historypb.HistoryEvent) {
	taskqueue := &taskqueuepb.TaskQueue{
		Name: m.msb.executionInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	scheduledEvent := newWorkflowTaskScheduledEventWithInfo(
		workflowTask.ScheduleID,
		workflowTask.ScheduledTimestamp,
		taskqueue,
		int32(workflowTask.WorkflowTaskTimeout),
		workflowTask.Attempt,
	)

	startedEvent := newWorkflowTaskStartedEventWithInfo(
		workflowTask.StartedID,
		workflowTask.StartedTimestamp,
		workflowTask.ScheduleID,
		workflowTask.RequestID,
		identity,
	)

	return scheduledEvent, startedEvent
}

func (m *mutableStateWorkflowTaskManagerImpl) getWorkflowTaskInfo() *workflowTaskInfo {
	taskQueue := &taskqueuepb.TaskQueue{}
	if m.msb.IsStickyTaskQueueEnabled() {
		taskQueue.Name = m.msb.executionInfo.StickyTaskQueue
		taskQueue.Kind = enumspb.TASK_QUEUE_KIND_STICKY
	} else {
		taskQueue.Name = m.msb.executionInfo.TaskQueue
		taskQueue.Kind = enumspb.TASK_QUEUE_KIND_NORMAL
	}

	return &workflowTaskInfo{
		Version:                    m.msb.executionInfo.WorkflowTaskVersion,
		ScheduleID:                 m.msb.executionInfo.WorkflowTaskScheduleID,
		StartedID:                  m.msb.executionInfo.WorkflowTaskStartedID,
		RequestID:                  m.msb.executionInfo.WorkflowTaskRequestID,
		WorkflowTaskTimeout:        m.msb.executionInfo.WorkflowTaskTimeout,
		Attempt:                    m.msb.executionInfo.WorkflowTaskAttempt,
		StartedTimestamp:           m.msb.executionInfo.WorkflowTaskStartedTimestamp,
		ScheduledTimestamp:         m.msb.executionInfo.WorkflowTaskScheduledTimestamp,
		TaskQueue:                  taskQueue,
		OriginalScheduledTimestamp: m.msb.executionInfo.WorkflowTaskOriginalScheduledTimestamp,
	}
}

func (m *mutableStateWorkflowTaskManagerImpl) beforeAddWorkflowTaskCompletedEvent() {
	// Make sure to delete workflow task before adding events. Otherwise they are buffered rather than getting appended.
	m.DeleteWorkflowTask()
}

func (m *mutableStateWorkflowTaskManagerImpl) afterAddWorkflowTaskCompletedEvent(
	event *historypb.HistoryEvent,
	maxResetPoints int,
) error {
	m.msb.executionInfo.LastProcessedEvent = event.GetWorkflowTaskCompletedEventAttributes().GetStartedEventId()
	return m.msb.addBinaryCheckSumIfNotExists(event, maxResetPoints)
}
