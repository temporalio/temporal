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

	"github.com/gogo/protobuf/types"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	workflowTaskStateMachine struct {
		ms *MutableStateImpl
	}
)

const (
	workflowTaskRetryBackoffMinAttempts = 3
	workflowTaskRetryInitialInterval    = 5 * time.Second
)

func newWorkflowTaskStateMachine(
	ms *MutableStateImpl,
) *workflowTaskStateMachine {
	return &workflowTaskStateMachine{
		ms: ms,
	}
}

func (m *workflowTaskStateMachine) ReplicateWorkflowTaskScheduledEvent(
	version int64,
	scheduledEventID int64,
	taskQueue *taskqueuepb.TaskQueue,
	startToCloseTimeout *time.Duration,
	attempt int32,
	scheduledTime *time.Time,
	originalScheduledTimestamp *time.Time,
	workflowTaskType enumsspb.WorkflowTaskType,
) (*WorkflowTaskInfo, error) {

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
		ScheduledEventID:      scheduledEventID,
		StartedEventID:        common.EmptyEventID,
		RequestID:             emptyUUID,
		WorkflowTaskTimeout:   startToCloseTimeout,
		TaskQueue:             taskQueue,
		Attempt:               attempt,
		ScheduledTime:         scheduledTime,
		StartedTime:           nil,
		OriginalScheduledTime: originalScheduledTimestamp,
		Type:                  workflowTaskType,
		SuggestContinueAsNew:  false, // reset, will be recomputed on workflow task started
		HistorySizeBytes:      0,     // reset, will be recomputed on workflow task started
	}

	m.UpdateWorkflowTask(workflowTask)
	return workflowTask, nil
}

func (m *workflowTaskStateMachine) ReplicateTransientWorkflowTaskScheduled() (*WorkflowTaskInfo, error) {
	// When workflow task fails/timeout it gets removed from mutable state, but attempt count is incremented.
	// If attempt count was incremented and now is greater than 1, then next workflow task should be created as transient.
	// This method will do it only if there is no other workflow task and next workflow task should be transient.
	if m.HasPendingWorkflowTask() || !m.ms.IsTransientWorkflowTask() {
		return nil, nil
	}

	// Because this method is called from ApplyEvents, where nextEventID is updated at the very end of the func,
	// ScheduledEventID for this workflow task is guaranteed to be wrong.
	// But this is OK. Whatever ScheduledEventID value is set here it will be stored in mutable state
	// and returned to the caller. It must be the same value because it will be used to look up
	// workflow task from mutable state.
	// There are 2 possible scenarios:
	//   1. If a failover happen just after this transient workflow task is scheduled with wrong ScheduledEventID,
	//   then AddWorkflowTaskStartedEvent will handle the correction of ScheduledEventID,
	//   and set the attempt to 1 (convert transient workflow task to normal workflow task).
	//   2. If no failover happen during the lifetime of this transient workflow task
	//   then ReplicateWorkflowTaskScheduledEvent will overwrite everything
	//   including the workflow task ScheduledEventID.
	//
	// Regarding workflow task timeout calculation:
	//   1. Attempt will be set to 1, so we still use default workflow task timeout.
	//   2. ReplicateWorkflowTaskScheduledEvent will overwrite everything including WorkflowTaskTimeout.
	workflowTask := &WorkflowTaskInfo{
		Version:             m.ms.GetCurrentVersion(),
		ScheduledEventID:    m.ms.GetNextEventID(),
		StartedEventID:      common.EmptyEventID,
		RequestID:           emptyUUID,
		WorkflowTaskTimeout: m.ms.GetExecutionInfo().DefaultWorkflowTaskTimeout,
		// Task queue is always normal (not sticky) because transient workflow task is created only for
		// failed/timed out workflow task and fail/timeout clears sticky task queue.
		TaskQueue:            m.ms.CurrentTaskQueue(),
		Attempt:              m.ms.GetExecutionInfo().WorkflowTaskAttempt,
		ScheduledTime:        timestamp.TimePtr(m.ms.timeSource.Now()),
		StartedTime:          timestamp.UnixOrZeroTimePtr(0),
		Type:                 enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
		SuggestContinueAsNew: false, // reset, will be recomputed on workflow task started
		HistorySizeBytes:     0,     // reset, will be recomputed on workflow task started
	}

	m.UpdateWorkflowTask(workflowTask)
	return workflowTask, nil
}

func (m *workflowTaskStateMachine) ReplicateWorkflowTaskStartedEvent(
	workflowTask *WorkflowTaskInfo,
	version int64,
	scheduledEventID int64,
	startedEventID int64,
	requestID string,
	timestamp time.Time,
	suggestContinueAsNew bool,
	historySizeBytes int64,
) (*WorkflowTaskInfo, error) {
	// When this function is called from ApplyEvents, workflowTask is nil.
	// It is safe to look up the workflow task as it does not have to deal with transient workflow task case.
	if workflowTask == nil {
		workflowTask = m.GetWorkflowTaskByID(scheduledEventID)
		if workflowTask == nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("unable to find workflow task: %v", scheduledEventID))
		}
		// Transient workflow task events are not replicated but attempt count in mutable state
		// can be updated from previous workflow task failed/timeout event.
		// During replication, "active" side will send 2 batches:
		//   1. WorkflowTaskScheduledEvent and WorkflowTaskStartedEvent
		//   2. WorkflowTaskCompletedEvents & other events
		// Each batch is like a transaction but there is no guarantee that all batches will be delivered before failover is happened.
		// Therefore, we need to support case when 1st batch is replicated but 2nd is not.
		// If previous workflow task was failed/timed out, mutable state will have attempt count > 1.
		// Because attempt count controls whether workflow task is transient or not,
		// "standby" side would consider this workflow task as transient, while it is actually not.
		// To prevent this attempt count is reset to 1, workflow task will be treated as normal workflow task,
		// and will be timed out or completed correctly.
		workflowTask.Attempt = 1
	}

	workflowTask = &WorkflowTaskInfo{
		Version:               version,
		ScheduledEventID:      scheduledEventID,
		StartedEventID:        startedEventID,
		RequestID:             requestID,
		WorkflowTaskTimeout:   workflowTask.WorkflowTaskTimeout,
		Attempt:               workflowTask.Attempt,
		StartedTime:           &timestamp,
		ScheduledTime:         workflowTask.ScheduledTime,
		TaskQueue:             workflowTask.TaskQueue,
		OriginalScheduledTime: workflowTask.OriginalScheduledTime,
		Type:                  workflowTask.Type,
		SuggestContinueAsNew:  suggestContinueAsNew,
		HistorySizeBytes:      historySizeBytes,
	}

	m.UpdateWorkflowTask(workflowTask)
	return workflowTask, nil
}

func (m *workflowTaskStateMachine) ReplicateWorkflowTaskCompletedEvent(
	event *historypb.HistoryEvent,
) error {
	m.beforeAddWorkflowTaskCompletedEvent()
	return m.afterAddWorkflowTaskCompletedEvent(event, WorkflowTaskCompletionLimits{math.MaxInt32, math.MaxInt32})
}

func (m *workflowTaskStateMachine) ReplicateWorkflowTaskFailedEvent() error {
	m.failWorkflowTask(true)
	return nil
}

func (m *workflowTaskStateMachine) ReplicateWorkflowTaskTimedOutEvent(
	timeoutType enumspb.TimeoutType,
) error {
	incrementAttempt := true
	// Do not increment workflow task attempt in the case of sticky timeout to prevent creating next workflow task as transient.
	if timeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START {
		incrementAttempt = false
	}
	m.failWorkflowTask(incrementAttempt)
	return nil
}

func (m *workflowTaskStateMachine) AddWorkflowTaskScheduleToStartTimeoutEvent(
	workflowTask *WorkflowTaskInfo,
) (*historypb.HistoryEvent, error) {
	opTag := tag.WorkflowActionWorkflowTaskTimedOut
	if m.ms.executionInfo.WorkflowTaskScheduledEventId != workflowTask.ScheduledEventID || m.ms.executionInfo.WorkflowTaskStartedEventId > 0 {
		m.ms.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduledEventID(workflowTask.ScheduledEventID),
		)
		return nil, m.ms.createInternalServerError(opTag)
	}

	if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		m.ms.RemoveSpeculativeWorkflowTaskTimeoutTask()

		// Create corresponding WorkflowTaskScheduled event for speculative WT.
		scheduledEvent := m.ms.hBuilder.AddWorkflowTaskScheduledEvent(
			m.ms.CurrentTaskQueue(),
			workflowTask.WorkflowTaskTimeout,
			workflowTask.Attempt,
			timestamp.TimeValue(workflowTask.ScheduledTime).UTC(),
		)
		workflowTask.ScheduledEventID = scheduledEvent.GetEventId()
	}

	event := m.ms.hBuilder.AddWorkflowTaskTimedOutEvent(
		workflowTask.ScheduledEventID,
		common.EmptyEventID,
		enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
	)
	if err := m.ReplicateWorkflowTaskTimedOutEvent(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START); err != nil {
		return nil, err
	}
	return event, nil
}

// AddWorkflowTaskScheduledEventAsHeartbeat is to record the first scheduled workflow task during workflow task heartbeat.
// If bypassTaskGeneration is specified, a transfer task will not be generated.
func (m *workflowTaskStateMachine) AddWorkflowTaskScheduledEventAsHeartbeat(
	bypassTaskGeneration bool,
	originalScheduledTimestamp *time.Time,
	workflowTaskType enumsspb.WorkflowTaskType,
) (*WorkflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskScheduled
	if m.HasPendingWorkflowTask() {
		m.ms.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduledEventID(m.ms.executionInfo.WorkflowTaskScheduledEventId))
		return nil, m.ms.createInternalServerError(opTag)
	}

	// Create real WorkflowTaskScheduledEvent when workflow task:
	//  - is not transient (is not continuously failing)
	//  and
	//  - is not speculative.
	createWorkflowTaskScheduledEvent := !m.ms.IsTransientWorkflowTask() && workflowTaskType != enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE

	// If while scheduling a workflow task and new events has come, then this workflow task cannot be a transient/speculative.
	// Flush any buffered events before creating the workflow task, otherwise it will result in invalid IDs for
	// transient/speculative workflow task and will cause in timeout processing to not work for transient workflow tasks.
	if m.ms.HasBufferedEvents() {
		m.ms.executionInfo.WorkflowTaskAttempt = 1
		workflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
		createWorkflowTaskScheduledEvent = true
		m.ms.updatePendingEventIDs(m.ms.hBuilder.FlushBufferToCurrentBatch())
	}
	if m.ms.IsTransientWorkflowTask() {
		lastWriteVersion, err := m.ms.GetLastWriteVersion()
		if err != nil {
			return nil, err
		}

		// If failover happened during transient workflow task,
		// then reset the attempt to 1, and not use transient workflow task.
		if m.ms.GetCurrentVersion() != lastWriteVersion {
			m.ms.executionInfo.WorkflowTaskAttempt = 1
			workflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
			createWorkflowTaskScheduledEvent = true
		}
	}

	scheduleTime := m.ms.timeSource.Now().UTC()
	attempt := m.ms.executionInfo.WorkflowTaskAttempt
	// TaskQueue should already be set from workflow execution started event.
	taskQueue := m.ms.CurrentTaskQueue()
	// DefaultWorkflowTaskTimeout should already be set from workflow execution started event.
	startToCloseTimeout := m.getStartToCloseTimeout(m.ms.executionInfo.DefaultWorkflowTaskTimeout, attempt)

	var scheduledEvent *historypb.HistoryEvent
	var scheduledEventID int64

	if createWorkflowTaskScheduledEvent {
		scheduledEvent = m.ms.hBuilder.AddWorkflowTaskScheduledEvent(
			taskQueue,
			startToCloseTimeout,
			attempt,
			scheduleTime,
		)
		scheduledEventID = scheduledEvent.GetEventId()
	} else {
		// WorkflowTaskScheduledEvent will be created later.
		scheduledEventID = m.ms.GetNextEventID()
	}

	workflowTask, err := m.ReplicateWorkflowTaskScheduledEvent(
		m.ms.GetCurrentVersion(),
		scheduledEventID,
		taskQueue,
		startToCloseTimeout,
		attempt,
		&scheduleTime,
		originalScheduledTimestamp,
		workflowTaskType,
	)
	if err != nil {
		return nil, err
	}

	// TODO merge active & passive task generation
	// Always bypass task generation for speculative workflow task.
	if !bypassTaskGeneration {
		generateTimeoutTaskOnly := false
		if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
			generateTimeoutTaskOnly = true
		}

		if err := m.ms.taskGenerator.GenerateScheduleWorkflowTaskTasks(
			scheduledEventID,
			generateTimeoutTaskOnly,
		); err != nil {
			return nil, err
		}
	}

	return workflowTask, nil
}

// AddWorkflowTaskScheduledEvent adds a WorkflowTaskScheduled event to the mutable state and generates a transfer task
// unless bypassTaskGeneration is specified.
func (m *workflowTaskStateMachine) AddWorkflowTaskScheduledEvent(
	bypassTaskGeneration bool,
	workflowTaskType enumsspb.WorkflowTaskType,
) (*WorkflowTaskInfo, error) {
	return m.AddWorkflowTaskScheduledEventAsHeartbeat(bypassTaskGeneration, timestamp.TimePtr(m.ms.timeSource.Now()), workflowTaskType)
}

// AddFirstWorkflowTaskScheduled adds the first workflow task scheduled event unless it should be delayed as indicated
// by the startEvent's FirstWorkflowTaskBackoff.
// If bypassTaskGeneration is specified, a transfer task will not be created.
// Returns the workflow task's scheduled event ID if a task was scheduled, 0 otherwise.
func (m *workflowTaskStateMachine) AddFirstWorkflowTaskScheduled(
	startEvent *historypb.HistoryEvent,
	bypassTaskGeneration bool,
) (int64, error) {
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

	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()
	workflowTaskBackoffDuration := timestamp.DurationValue(startAttr.GetFirstWorkflowTaskBackoff())

	if workflowTaskBackoffDuration != 0 {
		err := m.ms.taskGenerator.GenerateDelayedWorkflowTasks(
			startEvent,
		)
		return 0, err
	} else {
		info, err := m.AddWorkflowTaskScheduledEvent(
			bypassTaskGeneration,
			enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
		)
		if err != nil {
			return 0, err
		}
		return info.ScheduledEventID, nil
	}
}

func (m *workflowTaskStateMachine) AddWorkflowTaskStartedEvent(
	scheduledEventID int64,
	requestID string,
	taskQueue *taskqueuepb.TaskQueue,
	identity string,
) (*historypb.HistoryEvent, *WorkflowTaskInfo, error) {
	opTag := tag.WorkflowActionWorkflowTaskStarted
	workflowTask := m.GetWorkflowTaskByID(scheduledEventID)
	if workflowTask == nil || workflowTask.StartedEventID != common.EmptyEventID {
		m.ms.logger.Warn(mutableStateInvalidHistoryActionMsg, opTag,
			tag.WorkflowEventID(m.ms.GetNextEventID()),
			tag.ErrorTypeInvalidHistoryAction,
			tag.WorkflowScheduledEventID(scheduledEventID))
		return nil, nil, m.ms.createInternalServerError(opTag)
	}

	m.ms.RemoveSpeculativeWorkflowTaskTimeoutTask()

	scheduledEventID = workflowTask.ScheduledEventID
	startedEventID := scheduledEventID + 1
	startTime := m.ms.timeSource.Now()

	// The history size computed here might not include this workflow task scheduled or started
	// events. That's okay, it doesn't have to be 100% accurate. It just has to be kept
	// consistent between the started event in history and the event that was sent to the SDK
	// that resulted in the successful completion.
	suggestContinueAsNew, historySizeBytes := m.getHistorySizeInfo()

	workflowTaskScheduledEventCreated := !m.ms.IsTransientWorkflowTask() && workflowTask.Type != enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE

	// If new events came since transient/speculative WT was scheduled or failover happened during lifetime of transient/speculative WT,
	// transient/speculative WT needs to be converted to normal WT, i.e. WorkflowTaskScheduledEvent needs to be created now.
	if !workflowTaskScheduledEventCreated &&
		(workflowTask.ScheduledEventID != m.ms.GetNextEventID() || workflowTask.Version != m.ms.GetCurrentVersion()) {

		workflowTask.Attempt = 1
		workflowTask.Type = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
		workflowTaskScheduledEventCreated = true
		scheduledEvent := m.ms.hBuilder.AddWorkflowTaskScheduledEvent(
			taskQueue,
			workflowTask.WorkflowTaskTimeout,
			workflowTask.Attempt,
			startTime,
		)
		scheduledEventID = scheduledEvent.GetEventId()
	}

	// Create WorkflowTaskStartedEvent only if WorkflowTaskScheduledEvent was created.
	// (it wasn't created for transient/speculative WT).
	var startedEvent *historypb.HistoryEvent
	if workflowTaskScheduledEventCreated {
		startedEvent = m.ms.hBuilder.AddWorkflowTaskStartedEvent(
			scheduledEventID,
			requestID,
			identity,
			startTime,
			suggestContinueAsNew,
			historySizeBytes,
		)
		m.ms.hBuilder.FlushAndCreateNewBatch()
		startedEventID = startedEvent.GetEventId()
	}

	workflowTask, err := m.ReplicateWorkflowTaskStartedEvent(
		workflowTask, m.ms.GetCurrentVersion(), scheduledEventID, startedEventID, requestID, startTime,
		suggestContinueAsNew, historySizeBytes,
	)

	m.emitWorkflowTaskAttemptStats(workflowTask.Attempt)

	// TODO merge active & passive task generation
	if err := m.ms.taskGenerator.GenerateStartWorkflowTaskTasks(
		scheduledEventID,
	); err != nil {
		return nil, nil, err
	}

	return startedEvent, workflowTask, err
}
func (m *workflowTaskStateMachine) skipWorkflowTaskCompletedEvent(workflowTaskType enumsspb.WorkflowTaskType, request *workflowservice.RespondWorkflowTaskCompletedRequest) bool {
	if workflowTaskType != enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		// Only Speculative WT can skip WorkflowTaskCompletedEvent.
		return false
	}
	if len(request.GetCommands()) != 0 {
		// If worker returned commands, they will be converted to events, which must follow by WorkflowTaskCompletedEvent.
		return false
	}
	if len(request.GetMessages()) == 0 {
		// If both commands & messages are empty, then this is heartbeat response.
		// New WT will be created as Normal and WorkflowTaskCompletedEvent for this WT is also must be written.
		// In the future, if we decide not to write heartbeat WT to the history, this check should be removed,
		// and extra logic should be added to create next WT as Speculative. Currently, new heartbeat WT is always created as Normal.
		return false
	}

	onlyUpdateRejectionMessages := true
	for _, message := range request.Messages {
		if !types.Is(message.GetBody(), (*updatepb.Rejection)(nil)) {
			onlyUpdateRejectionMessages = false
			break
		}
	}
	return onlyUpdateRejectionMessages
}
func (m *workflowTaskStateMachine) AddWorkflowTaskCompletedEvent(
	workflowTask *WorkflowTaskInfo,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
	limits WorkflowTaskCompletionLimits,
) (*historypb.HistoryEvent, error) {

	m.ms.RemoveSpeculativeWorkflowTaskTimeoutTask()

	// Capture if WorkflowTaskScheduled and WorkflowTaskStarted events were created
	// before calling m.beforeAddWorkflowTaskCompletedEvent() because it will delete workflow task info from mutable state.
	workflowTaskScheduledStartedEventsCreated := !m.ms.IsTransientWorkflowTask() && workflowTask.Type != enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE
	m.beforeAddWorkflowTaskCompletedEvent()

	if m.skipWorkflowTaskCompletedEvent(workflowTask.Type, request) {
		return nil, nil
	}

	if !workflowTaskScheduledStartedEventsCreated {
		// Create corresponding WorkflowTaskScheduled and WorkflowTaskStarted events for transient/speculative workflow tasks.
		scheduledEvent := m.ms.hBuilder.AddWorkflowTaskScheduledEvent(
			m.ms.CurrentTaskQueue(),
			workflowTask.WorkflowTaskTimeout,
			workflowTask.Attempt,
			timestamp.TimeValue(workflowTask.ScheduledTime).UTC(),
		)
		workflowTask.ScheduledEventID = scheduledEvent.GetEventId()
		startedEvent := m.ms.hBuilder.AddWorkflowTaskStartedEvent(
			workflowTask.ScheduledEventID,
			workflowTask.RequestID,
			request.GetIdentity(),
			timestamp.TimeValue(workflowTask.StartedTime),
			workflowTask.SuggestContinueAsNew,
			workflowTask.HistorySizeBytes,
		)
		m.ms.hBuilder.FlushAndCreateNewBatch()
		workflowTask.StartedEventID = startedEvent.GetEventId()
	}

	// Now write the completed event
	event := m.ms.hBuilder.AddWorkflowTaskCompletedEvent(
		workflowTask.ScheduledEventID,
		workflowTask.StartedEventID,
		request.Identity,
		request.BinaryChecksum,
		request.WorkerVersionStamp,
		request.SdkMetadata,
		request.MeteringMetadata,
	)

	err := m.afterAddWorkflowTaskCompletedEvent(event, limits)
	if err != nil {
		return nil, err
	}
	return event, nil
}

func (m *workflowTaskStateMachine) AddWorkflowTaskFailedEvent(
	workflowTask *WorkflowTaskInfo,
	cause enumspb.WorkflowTaskFailedCause,
	failure *failurepb.Failure,
	identity string,
	binaryChecksum string,
	baseRunID string,
	newRunID string,
	forkEventVersion int64,
) (*historypb.HistoryEvent, error) {

	// IMPORTANT: returned event can be nil under some circumstances. Specifically, if WT is transient.

	if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		m.ms.RemoveSpeculativeWorkflowTaskTimeoutTask()

		// Create corresponding WorkflowTaskScheduled and WorkflowTaskStarted events for speculative WT.
		scheduledEvent := m.ms.hBuilder.AddWorkflowTaskScheduledEvent(
			m.ms.CurrentTaskQueue(),
			workflowTask.WorkflowTaskTimeout,
			workflowTask.Attempt,
			timestamp.TimeValue(workflowTask.ScheduledTime).UTC(),
		)
		workflowTask.ScheduledEventID = scheduledEvent.GetEventId()
		startedEvent := m.ms.hBuilder.AddWorkflowTaskStartedEvent(
			workflowTask.ScheduledEventID,
			workflowTask.RequestID,
			identity,
			timestamp.TimeValue(workflowTask.StartedTime),
			workflowTask.SuggestContinueAsNew,
			workflowTask.HistorySizeBytes,
		)
		m.ms.hBuilder.FlushAndCreateNewBatch()
		workflowTask.StartedEventID = startedEvent.GetEventId()
	}

	var event *historypb.HistoryEvent
	// Only emit WorkflowTaskFailedEvent if workflow task is not transient.
	if !m.ms.IsTransientWorkflowTask() {
		event = m.ms.hBuilder.AddWorkflowTaskFailedEvent(
			workflowTask.ScheduledEventID,
			workflowTask.StartedEventID,
			cause,
			failure,
			identity,
			baseRunID,
			newRunID,
			forkEventVersion,
			binaryChecksum,
		)
	}

	if err := m.ReplicateWorkflowTaskFailedEvent(); err != nil {
		return nil, err
	}

	switch cause {
	case enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND:
		// always clear workflow task attempt for reset and failover close command
		m.ms.executionInfo.WorkflowTaskAttempt = 1
	case enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND:
		// workflow attempted to close but failed due to unhandled buffer events
		m.ms.workflowCloseAttempted = true
	}

	// Attempt counter was incremented directly in mutable state. Current WT attempt counter needs to be updated.
	workflowTask.Attempt = m.ms.GetExecutionInfo().GetWorkflowTaskAttempt()

	return event, nil
}

func (m *workflowTaskStateMachine) AddWorkflowTaskTimedOutEvent(
	workflowTask *WorkflowTaskInfo,
) (*historypb.HistoryEvent, error) {

	if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		m.ms.RemoveSpeculativeWorkflowTaskTimeoutTask()

		// Create corresponding WorkflowTaskScheduled and WorkflowTaskStarted events for speculative WT.
		scheduledEvent := m.ms.hBuilder.AddWorkflowTaskScheduledEvent(
			m.ms.CurrentTaskQueue(),
			workflowTask.WorkflowTaskTimeout,
			workflowTask.Attempt,
			timestamp.TimeValue(workflowTask.ScheduledTime).UTC(),
		)
		workflowTask.ScheduledEventID = scheduledEvent.GetEventId()
		startedEvent := m.ms.hBuilder.AddWorkflowTaskStartedEvent(
			workflowTask.ScheduledEventID,
			workflowTask.RequestID,
			"",
			timestamp.TimeValue(workflowTask.StartedTime),
			workflowTask.SuggestContinueAsNew,
			workflowTask.HistorySizeBytes,
		)
		m.ms.hBuilder.FlushAndCreateNewBatch()
		workflowTask.StartedEventID = startedEvent.GetEventId()
	}

	var event *historypb.HistoryEvent
	// Avoid creating WorkflowTaskTimedOut history event when workflow task is transient.
	if !m.ms.IsTransientWorkflowTask() {
		event = m.ms.hBuilder.AddWorkflowTaskTimedOutEvent(
			workflowTask.ScheduledEventID,
			workflowTask.StartedEventID,
			enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		)
	}

	if err := m.ReplicateWorkflowTaskTimedOutEvent(enumspb.TIMEOUT_TYPE_START_TO_CLOSE); err != nil {
		return nil, err
	}
	return event, nil
}

func (m *workflowTaskStateMachine) failWorkflowTask(
	incrementAttempt bool,
) {
	// Increment attempts only if workflow task is failing on non-sticky task queue.
	// If it was sticky task queue, clear sticky task queue first and try again before creating transient workflow task.
	if m.ms.IsStickyTaskQueueSet() {
		incrementAttempt = false
		m.ms.ClearStickyTaskQueue()
	}

	failWorkflowTaskInfo := &WorkflowTaskInfo{
		Version:               common.EmptyVersion,
		ScheduledEventID:      common.EmptyEventID,
		StartedEventID:        common.EmptyEventID,
		RequestID:             emptyUUID,
		WorkflowTaskTimeout:   timestamp.DurationFromSeconds(0),
		StartedTime:           timestamp.UnixOrZeroTimePtr(0),
		TaskQueue:             nil,
		OriginalScheduledTime: timestamp.UnixOrZeroTimePtr(0),
		Attempt:               1,
		Type:                  enumsspb.WORKFLOW_TASK_TYPE_UNSPECIFIED,
		SuggestContinueAsNew:  false,
		HistorySizeBytes:      0,
	}
	if incrementAttempt {
		failWorkflowTaskInfo.Attempt = m.ms.executionInfo.WorkflowTaskAttempt + 1
		failWorkflowTaskInfo.ScheduledTime = timestamp.TimePtr(m.ms.timeSource.Now().UTC())
	}
	m.UpdateWorkflowTask(failWorkflowTaskInfo)
}

// deleteWorkflowTask deletes a workflow task.
func (m *workflowTaskStateMachine) deleteWorkflowTask() {
	resetWorkflowTaskInfo := &WorkflowTaskInfo{
		Version:             common.EmptyVersion,
		ScheduledEventID:    common.EmptyEventID,
		StartedEventID:      common.EmptyEventID,
		RequestID:           emptyUUID,
		WorkflowTaskTimeout: timestamp.DurationFromSeconds(0),
		Attempt:             1,
		StartedTime:         timestamp.UnixOrZeroTimePtr(0),
		ScheduledTime:       timestamp.UnixOrZeroTimePtr(0),

		TaskQueue: nil,
		// Keep the last original scheduled Timestamp, so that AddWorkflowTaskScheduledEventAsHeartbeat can continue with it.
		OriginalScheduledTime: m.getWorkflowTaskInfo().OriginalScheduledTime,
		Type:                  enumsspb.WORKFLOW_TASK_TYPE_UNSPECIFIED,
		SuggestContinueAsNew:  false,
		HistorySizeBytes:      0,
	}
	m.UpdateWorkflowTask(resetWorkflowTaskInfo)
}

// UpdateWorkflowTask updates a workflow task.
func (m *workflowTaskStateMachine) UpdateWorkflowTask(
	workflowTask *WorkflowTaskInfo,
) {
	if m.HasStartedWorkflowTask() && workflowTask.StartedEventID == common.EmptyEventID {
		// reset the flag whenever started workflow task closes, there could be three cases:
		// 1. workflow task completed:
		//    a. workflow task contains close workflow command, the fact that workflow task
		//       completes successfully means workflow will also close and the value of the
		//       flag doesn't matter.
		//    b. workflow task doesn't contain close workflow command, then by definition,
		//       workflow is not trying to close, so unset the flag.
		// 2. workflow task timedout: we don't know if workflow is trying to close or not,
		//    reset the flag to be safe. It's possible that workflow task is trying to signal
		//    itself within a local activity when this flag is set, which may result in timeout.
		//    reset the flag will allow the workflow to proceed.
		// 3. workflow failed: always reset the flag here. If failure is due to unhandled command,
		//    AddWorkflowTaskFailedEvent will set the flag.
		m.ms.workflowCloseAttempted = false
	}

	m.ms.executionInfo.WorkflowTaskVersion = workflowTask.Version
	m.ms.executionInfo.WorkflowTaskScheduledEventId = workflowTask.ScheduledEventID
	m.ms.executionInfo.WorkflowTaskStartedEventId = workflowTask.StartedEventID
	m.ms.executionInfo.WorkflowTaskRequestId = workflowTask.RequestID
	m.ms.executionInfo.WorkflowTaskTimeout = workflowTask.WorkflowTaskTimeout
	m.ms.executionInfo.WorkflowTaskAttempt = workflowTask.Attempt
	m.ms.executionInfo.WorkflowTaskStartedTime = workflowTask.StartedTime
	m.ms.executionInfo.WorkflowTaskScheduledTime = workflowTask.ScheduledTime
	m.ms.executionInfo.WorkflowTaskOriginalScheduledTime = workflowTask.OriginalScheduledTime
	m.ms.executionInfo.WorkflowTaskType = workflowTask.Type
	m.ms.executionInfo.WorkflowTaskSuggestContinueAsNew = workflowTask.SuggestContinueAsNew
	m.ms.executionInfo.WorkflowTaskHistorySizeBytes = workflowTask.HistorySizeBytes

	// NOTE: do not update task queue in execution info

	m.ms.logger.Debug("Workflow task updated",
		tag.WorkflowScheduledEventID(workflowTask.ScheduledEventID),
		tag.WorkflowStartedEventID(workflowTask.StartedEventID),
		tag.WorkflowTaskRequestId(workflowTask.RequestID),
		tag.WorkflowTaskTimeout(workflowTask.WorkflowTaskTimeout),
		tag.Attempt(workflowTask.Attempt),
		tag.WorkflowStartedTimestamp(workflowTask.StartedTime),
		tag.WorkflowTaskType(workflowTask.Type.String()))
}

func (m *workflowTaskStateMachine) HasPendingWorkflowTask() bool {
	return m.ms.executionInfo.WorkflowTaskScheduledEventId != common.EmptyEventID
}

func (m *workflowTaskStateMachine) GetPendingWorkflowTask() *WorkflowTaskInfo {
	if !m.HasPendingWorkflowTask() {
		return nil
	}

	workflowTask := m.getWorkflowTaskInfo()
	return workflowTask
}

func (m *workflowTaskStateMachine) HasStartedWorkflowTask() bool {
	return m.ms.executionInfo.WorkflowTaskScheduledEventId != common.EmptyEventID &&
		m.ms.executionInfo.WorkflowTaskStartedEventId != common.EmptyEventID
}

func (m *workflowTaskStateMachine) GetStartedWorkflowTask() *WorkflowTaskInfo {
	if !m.HasStartedWorkflowTask() {
		return nil
	}

	workflowTask := m.getWorkflowTaskInfo()
	return workflowTask
}

func (m *workflowTaskStateMachine) HadOrHasWorkflowTask() bool {
	return m.HasPendingWorkflowTask() || m.ms.GetLastWorkflowTaskStartedEventID() != common.EmptyEventID
}

// GetWorkflowTaskByID returns details about the current workflow task by scheduled event ID.
func (m *workflowTaskStateMachine) GetWorkflowTaskByID(scheduledEventID int64) *WorkflowTaskInfo {
	workflowTask := m.getWorkflowTaskInfo()
	if scheduledEventID == workflowTask.ScheduledEventID {
		return workflowTask
	}

	return nil
}

func (m *workflowTaskStateMachine) GetTransientWorkflowTaskInfo(
	workflowTask *WorkflowTaskInfo,
	identity string,
) *historyspb.TransientWorkflowTaskInfo {

	// Create scheduled and started events which are not written to the history yet.
	scheduledEvent := &historypb.HistoryEvent{
		EventId:   workflowTask.ScheduledEventID,
		EventTime: workflowTask.ScheduledTime,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Version:   m.ms.currentVersion,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
			WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
				TaskQueue:           m.ms.CurrentTaskQueue(),
				StartToCloseTimeout: workflowTask.WorkflowTaskTimeout,
				Attempt:             workflowTask.Attempt,
			},
		},
	}

	startedEvent := &historypb.HistoryEvent{
		EventId:   workflowTask.StartedEventID,
		EventTime: workflowTask.StartedTime,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Version:   m.ms.currentVersion,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
			WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
				ScheduledEventId:     workflowTask.ScheduledEventID,
				Identity:             identity,
				RequestId:            workflowTask.RequestID,
				SuggestContinueAsNew: workflowTask.SuggestContinueAsNew,
				HistorySizeBytes:     workflowTask.HistorySizeBytes,
			},
		},
	}

	return &historyspb.TransientWorkflowTaskInfo{
		HistorySuffix: []*historypb.HistoryEvent{scheduledEvent, startedEvent},
	}
}

func (m *workflowTaskStateMachine) getWorkflowTaskInfo() *WorkflowTaskInfo {
	return &WorkflowTaskInfo{
		Version:               m.ms.executionInfo.WorkflowTaskVersion,
		ScheduledEventID:      m.ms.executionInfo.WorkflowTaskScheduledEventId,
		StartedEventID:        m.ms.executionInfo.WorkflowTaskStartedEventId,
		RequestID:             m.ms.executionInfo.WorkflowTaskRequestId,
		WorkflowTaskTimeout:   m.ms.executionInfo.WorkflowTaskTimeout,
		Attempt:               m.ms.executionInfo.WorkflowTaskAttempt,
		StartedTime:           m.ms.executionInfo.WorkflowTaskStartedTime,
		ScheduledTime:         m.ms.executionInfo.WorkflowTaskScheduledTime,
		TaskQueue:             m.ms.CurrentTaskQueue(),
		OriginalScheduledTime: m.ms.executionInfo.WorkflowTaskOriginalScheduledTime,
		Type:                  m.ms.executionInfo.WorkflowTaskType,
		SuggestContinueAsNew:  m.ms.executionInfo.WorkflowTaskSuggestContinueAsNew,
		HistorySizeBytes:      m.ms.executionInfo.WorkflowTaskHistorySizeBytes,
	}
}

func (m *workflowTaskStateMachine) beforeAddWorkflowTaskCompletedEvent() {
	// Make sure to delete workflow task before adding events. Otherwise they are buffered rather than getting appended.
	m.deleteWorkflowTask()
}

func (m *workflowTaskStateMachine) afterAddWorkflowTaskCompletedEvent(
	event *historypb.HistoryEvent,
	limits WorkflowTaskCompletionLimits,
) error {
	attrs := event.GetWorkflowTaskCompletedEventAttributes()
	m.ms.executionInfo.LastWorkflowTaskStartedEventId = attrs.GetStartedEventId()
	m.ms.executionInfo.WorkerVersionStamp = attrs.GetWorkerVersion()
	if err := m.ms.trackBuildIdFromCompletion(attrs.GetWorkerVersion(), event.GetEventId(), limits); err != nil {
		return err
	}
	return m.ms.addBinaryCheckSumIfNotExists(event, limits.MaxResetPoints)
}

func (m *workflowTaskStateMachine) emitWorkflowTaskAttemptStats(
	attempt int32,
) {
	namespaceName := m.ms.GetNamespaceEntry().Name().String()
	m.ms.metricsHandler.Histogram(metrics.WorkflowTaskAttempt.GetMetricName(), metrics.WorkflowTaskAttempt.GetMetricUnit()).
		Record(int64(attempt), metrics.NamespaceTag(namespaceName))
	if attempt >= int32(m.ms.shard.GetConfig().WorkflowTaskCriticalAttempts()) {
		m.ms.shard.GetThrottledLogger().Warn("Critical attempts processing workflow task",
			tag.WorkflowNamespace(namespaceName),
			tag.WorkflowID(m.ms.GetExecutionInfo().WorkflowId),
			tag.WorkflowRunID(m.ms.GetExecutionState().RunId),
			tag.Attempt(attempt),
		)
	}
}

func (m *workflowTaskStateMachine) getStartToCloseTimeout(
	defaultTimeout *time.Duration,
	attempt int32,
) *time.Duration {
	// This util function is only for calculating active workflow task timeout.
	// Transient workflow task in passive cluster won't call this function and
	// always use default timeout as it will either be completely overwritten by
	// a replicated workflow schedule event from active cluster, or if used, it's
	// attempt will be reset to 1.
	// Check ReplicateTransientWorkflowTaskScheduled for details.

	if defaultTimeout == nil {
		defaultTimeout = timestamp.DurationPtr(0)
	}

	if attempt <= workflowTaskRetryBackoffMinAttempts {
		return defaultTimeout
	}

	policy := backoff.NewExponentialRetryPolicy(workflowTaskRetryInitialInterval).
		WithMaximumInterval(m.ms.shard.GetConfig().WorkflowTaskRetryMaxInterval()).
		WithExpirationInterval(backoff.NoInterval)
	startToCloseTimeout := *defaultTimeout + policy.ComputeNextDelay(0, int(attempt)-workflowTaskRetryBackoffMinAttempts)
	return &startToCloseTimeout
}

func (m *workflowTaskStateMachine) getHistorySizeInfo() (bool, int64) {
	stats := m.ms.GetExecutionInfo().ExecutionStats
	if stats == nil {
		return false, 0
	}
	// This only includes events that have actually been written to persistence, so it won't
	// include the workflow task started event that we're currently writing. That's okay, it
	// doesn't have to be 100% accurate.
	historySize := stats.HistorySize
	// This is called right before AddWorkflowTaskStartedEvent, so at this point, nextEventID
	// is the ID of the workflow task started event.
	historyCount := m.ms.GetNextEventID()
	config := m.ms.shard.GetConfig()
	namespaceName := m.ms.GetNamespaceEntry().Name().String()
	sizeLimit := int64(config.HistorySizeSuggestContinueAsNew(namespaceName))
	countLimit := int64(config.HistoryCountSuggestContinueAsNew(namespaceName))
	suggestContinueAsNew := historySize >= sizeLimit || historyCount >= countLimit
	return suggestContinueAsNew, historySize
}

func (m *workflowTaskStateMachine) convertSpeculativeWorkflowTaskToNormal() error {
	if m.ms.executionInfo.WorkflowTaskType != enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		return nil
	}

	// Workflow task can't be persisted as Speculative, because when it is completed,
	// it gets deleted from memory only but not from the database.
	// If execution info in mutable state has speculative workflow task, then
	// convert it to normal workflow task before persisting.
	m.ms.RemoveSpeculativeWorkflowTaskTimeoutTask()

	if !m.ms.IsWorkflowExecutionRunning() {
		// Workflow execution can be terminated. New events can't be added after workflow is finished.
		return nil
	}

	m.ms.executionInfo.WorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL

	wt := m.getWorkflowTaskInfo()

	scheduledEvent := m.ms.hBuilder.AddWorkflowTaskScheduledEvent(
		wt.TaskQueue,
		wt.WorkflowTaskTimeout,
		wt.Attempt,
		timestamp.TimeValue(wt.ScheduledTime),
	)

	if scheduledEvent.EventId != wt.ScheduledEventID {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, scheduled event Id: %d for normal workflow task doesn't match the one from speculative workflow task: %d", scheduledEvent.EventId, wt.ScheduledEventID))
	}

	if wt.StartedEventID != common.EmptyEventID {
		// If WT is has started then started event is written to the history and
		// timeout timer task (for START_TO_CLOSE timeout) is created.

		_ = m.ms.hBuilder.AddWorkflowTaskStartedEvent(
			scheduledEvent.EventId,
			wt.RequestID,
			"", // identity is not stored in the mutable state.
			timestamp.TimeValue(wt.StartedTime),
			wt.SuggestContinueAsNew,
			wt.HistorySizeBytes,
		)
		m.ms.hBuilder.FlushAndCreateNewBatch()

		if err := m.ms.taskGenerator.GenerateStartWorkflowTaskTasks(
			scheduledEvent.EventId,
		); err != nil {
			return err
		}
	} else {
		if err := m.ms.taskGenerator.GenerateScheduleWorkflowTaskTasks(
			scheduledEvent.EventId,
			true, // Only generate SCHEDULE_TO_START timeout timer task, but not a transfer task which push WT to matching because WT was already pushed to matching.
		); err != nil {
			return err
		}
	}

	return nil
}
