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

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
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

func (m *workflowTaskStateMachine) ApplyWorkflowTaskScheduledEvent(
	version int64,
	scheduledEventID int64,
	taskQueue *taskqueuepb.TaskQueue,
	startToCloseTimeout *durationpb.Duration,
	attempt int32,
	scheduledTime *timestamppb.Timestamp,
	originalScheduledTimestamp *timestamppb.Timestamp,
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
		WorkflowTaskTimeout:   startToCloseTimeout.AsDuration(),
		TaskQueue:             taskQueue,
		Attempt:               attempt,
		ScheduledTime:         scheduledTime.AsTime(),
		StartedTime:           time.Time{},
		OriginalScheduledTime: originalScheduledTimestamp.AsTime(),
		Type:                  workflowTaskType,
		SuggestContinueAsNew:  false, // reset, will be recomputed on workflow task started
		HistorySizeBytes:      0,     // reset, will be recomputed on workflow task started
	}

	m.retainWorkflowTaskBuildIdInfo(workflowTask)
	m.UpdateWorkflowTask(workflowTask)
	return workflowTask, nil
}

// if this is a transient WFT (attempt > 1), we make sure to keep the following from the previous attempt:
//   - BuildId of the previous attempt to be able to compare it with next attempt and renew tasks if it changes
//   - BuildIdRedirectCounter so add the right BuildIdRedirectCounter to the WFT started event that will be
//     created at WFT completion time
func (m *workflowTaskStateMachine) retainWorkflowTaskBuildIdInfo(workflowTask *WorkflowTaskInfo) {
	if workflowTask.Attempt > 1 {
		workflowTask.BuildId = m.ms.executionInfo.WorkflowTaskBuildId
		workflowTask.BuildIdRedirectCounter = m.ms.executionInfo.BuildIdRedirectCounter
	}
}

func (m *workflowTaskStateMachine) ApplyTransientWorkflowTaskScheduled() (*WorkflowTaskInfo, error) {
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
	//   then ApplyWorkflowTaskScheduledEvent will overwrite everything
	//   including the workflow task ScheduledEventID.
	//
	// Regarding workflow task timeout calculation:
	//   1. Attempt will be set to 1, so we still use default workflow task timeout.
	//   2. ApplyWorkflowTaskScheduledEvent will overwrite everything including WorkflowTaskTimeout.
	workflowTask := &WorkflowTaskInfo{
		Version:             m.ms.GetCurrentVersion(),
		ScheduledEventID:    m.ms.GetNextEventID(),
		StartedEventID:      common.EmptyEventID,
		RequestID:           emptyUUID,
		WorkflowTaskTimeout: m.ms.GetExecutionInfo().DefaultWorkflowTaskTimeout.AsDuration(),
		// Task queue is always normal (not sticky) because transient workflow task is created only for
		// failed/timed out workflow task and fail/timeout clears sticky task queue.
		TaskQueue:            m.ms.CurrentTaskQueue(),
		Attempt:              m.ms.GetExecutionInfo().WorkflowTaskAttempt,
		ScheduledTime:        timestamppb.New(m.ms.timeSource.Now()).AsTime(),
		StartedTime:          time.Unix(0, 0).UTC(),
		Type:                 enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
		SuggestContinueAsNew: false, // reset, will be recomputed on workflow task started
		HistorySizeBytes:     0,     // reset, will be recomputed on workflow task started
	}

	m.retainWorkflowTaskBuildIdInfo(workflowTask)
	m.UpdateWorkflowTask(workflowTask)
	return workflowTask, nil
}

func (m *workflowTaskStateMachine) ApplyWorkflowTaskStartedEvent(
	workflowTask *WorkflowTaskInfo,
	version int64,
	scheduledEventID int64,
	startedEventID int64,
	requestID string,
	startedTime time.Time,
	suggestContinueAsNew bool,
	historySizeBytes int64,
	versioningStamp *commonpb.WorkerVersionStamp,
	redirectCounter int64,
) (*WorkflowTaskInfo, error) {
	// When this function is called from ApplyEvents, workflowTask is nil.
	// It is safe to look up the workflow task as it does not have to deal with transient workflow task case.
	if workflowTask == nil {
		workflowTask = m.GetWorkflowTaskByID(scheduledEventID)
		if workflowTask == nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("unable to find workflow task: %v", scheduledEventID))
		}
		// Transient workflow task events are not applied but attempt count in mutable state
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
		Version:                version,
		ScheduledEventID:       scheduledEventID,
		StartedEventID:         startedEventID,
		RequestID:              requestID,
		WorkflowTaskTimeout:    workflowTask.WorkflowTaskTimeout,
		Attempt:                workflowTask.Attempt,
		StartedTime:            startedTime,
		ScheduledTime:          workflowTask.ScheduledTime,
		TaskQueue:              workflowTask.TaskQueue,
		OriginalScheduledTime:  workflowTask.OriginalScheduledTime,
		Type:                   workflowTask.Type,
		SuggestContinueAsNew:   suggestContinueAsNew,
		HistorySizeBytes:       historySizeBytes,
		BuildIdRedirectCounter: redirectCounter,
	}

	if buildId := worker_versioning.BuildIdIfUsingVersioning(versioningStamp); buildId != "" {
		if redirectCounter == 0 {
			// this is the initial build ID, it should normally be persisted after scheduling the wf task,
			// but setting it here again in case it failed to be persisted before.
			err := m.ms.UpdateBuildIdAssignment(buildId)
			if err != nil {
				return nil, err
			}
		} else {
			// apply redirect if applicable
			err := m.ms.ApplyBuildIdRedirect(scheduledEventID, buildId, redirectCounter)
			if err != nil {
				return nil, err
			}
		}
		workflowTask.BuildId = buildId
		workflowTask.BuildIdRedirectCounter = m.ms.GetExecutionInfo().GetBuildIdRedirectCounter()
	}

	m.UpdateWorkflowTask(workflowTask)
	return workflowTask, nil
}

func (m *workflowTaskStateMachine) ApplyWorkflowTaskCompletedEvent(
	event *historypb.HistoryEvent,
) error {
	m.beforeAddWorkflowTaskCompletedEvent()
	return m.afterAddWorkflowTaskCompletedEvent(event, WorkflowTaskCompletionLimits{math.MaxInt32, math.MaxInt32})
}

func (m *workflowTaskStateMachine) ApplyWorkflowTaskFailedEvent() error {
	m.failWorkflowTask(true)
	return nil
}

func (m *workflowTaskStateMachine) ApplyWorkflowTaskTimedOutEvent(
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
			durationpb.New(workflowTask.WorkflowTaskTimeout),
			workflowTask.Attempt,
			workflowTask.ScheduledTime.UTC(),
		)
		workflowTask.ScheduledEventID = scheduledEvent.GetEventId()
	}

	event := m.ms.hBuilder.AddWorkflowTaskTimedOutEvent(
		workflowTask.ScheduledEventID,
		common.EmptyEventID,
		enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
	)
	if err := m.ApplyWorkflowTaskTimedOutEvent(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START); err != nil {
		return nil, err
	}
	return event, nil
}

// AddWorkflowTaskScheduledEventAsHeartbeat records the first scheduled workflow task during workflow task heartbeat.
// If bypassTaskGeneration is specified, a transfer task will not be generated.
func (m *workflowTaskStateMachine) AddWorkflowTaskScheduledEventAsHeartbeat(
	bypassTaskGeneration bool,
	originalScheduledTimestamp *timestamppb.Timestamp,
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

	workflowTask, err := m.ApplyWorkflowTaskScheduledEvent(
		m.ms.GetCurrentVersion(),
		scheduledEventID,
		taskQueue,
		startToCloseTimeout,
		attempt,
		timestamppb.New(scheduleTime),
		originalScheduledTimestamp,
		workflowTaskType,
	)
	if err != nil {
		return nil, err
	}

	// TODO merge active & passive task generation
	if !bypassTaskGeneration {
		if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
			err = m.ms.taskGenerator.GenerateScheduleSpeculativeWorkflowTaskTasks(workflowTask)
		} else {
			err = m.ms.taskGenerator.GenerateScheduleWorkflowTaskTasks(scheduledEventID)
		}
		if err != nil {
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
	return m.AddWorkflowTaskScheduledEventAsHeartbeat(bypassTaskGeneration, timestamppb.New(m.ms.timeSource.Now()), workflowTaskType)
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
	versioningStamp *commonpb.WorkerVersionStamp,
	redirectInfo *taskqueuespb.BuildIdRedirectInfo,
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

	workflowTask, scheduledEventCreatedForRedirect, redirectCounter, err := m.processBuildIdRedirectInfo(versioningStamp, workflowTask, redirectInfo)
	if err != nil {
		return nil, nil, err
	}

	workflowTaskScheduledEventCreated := scheduledEventCreatedForRedirect ||
		(!m.ms.IsTransientWorkflowTask() && workflowTask.Type != enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE)

	// If new events came since transient/speculative WT was scheduled or failover happened during lifetime of transient/speculative WT,
	// transient/speculative WT needs to be converted to normal WT, i.e. WorkflowTaskScheduledEvent needs to be created now.
	if !workflowTaskScheduledEventCreated &&
		(workflowTask.ScheduledEventID != m.ms.GetNextEventID() || workflowTask.Version != m.ms.GetCurrentVersion()) {

		workflowTask.Attempt = 1
		workflowTask.Type = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
		workflowTaskScheduledEventCreated = true
		scheduledEvent := m.ms.hBuilder.AddWorkflowTaskScheduledEvent(
			// taskQueue may come directly from RecordWorkflowTaskStarted from matching, which will
			// contain a specific partition name. We only want to record the base name here.
			cleanTaskQueue(taskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW),
			durationpb.New(workflowTask.WorkflowTaskTimeout),
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
			versioningStamp,
			redirectCounter,
		)
		m.ms.hBuilder.FlushAndCreateNewBatch()
		startedEventID = startedEvent.GetEventId()
	}

	workflowTask, err = m.ApplyWorkflowTaskStartedEvent(
		workflowTask,
		m.ms.GetCurrentVersion(),
		scheduledEventID,
		startedEventID,
		requestID,
		startTime,
		suggestContinueAsNew,
		historySizeBytes,
		versioningStamp,
		redirectCounter,
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

// processBuildIdRedirectInfo validated possible build ID redirect based on the versioningStamp and redirectInfo.
// If a valid redirect is being applied to a transient WFT, the transient WFT is converted to normal and a
// scheduled event is created. In this case, the returned workflow info will be different from the given one and
// `converted` will be true.
// Also returns redirect counter that shall be used in the WFT started event.
func (m *workflowTaskStateMachine) processBuildIdRedirectInfo(
	versioningStamp *commonpb.WorkerVersionStamp,
	workflowTask *WorkflowTaskInfo,
	redirectInfo *taskqueuespb.BuildIdRedirectInfo,
) (newWorkflowTask *WorkflowTaskInfo, converted bool, redirectCounter int64, err error) {
	if !versioningStamp.GetUseVersioning() || versioningStamp.GetBuildId() == "" {
		return workflowTask, false, 0, nil
	}
	buildId := versioningStamp.GetBuildId()

	redirectCounter, err = m.ms.validateBuildIdRedirectInfo(versioningStamp, redirectInfo)
	if err != nil {
		return nil, false, redirectCounter, err
	}

	if m.ms.IsTransientWorkflowTask() && m.ms.GetExecutionInfo().GetWorkflowTaskBuildId() != buildId {
		// we're retrying a workflow task and this attempt is on a different build ID, converting the transient wf task
		// to a normal wf task by creating a scheduled event for it and setting its attempt to 1.
		scheduledEvent := m.ms.hBuilder.AddWorkflowTaskScheduledEvent(
			m.ms.CurrentTaskQueue(),
			durationpb.New(workflowTask.WorkflowTaskTimeout),
			// Preserving the number of previous attempts. This value shows the number of attempts made on the last
			// build ID + 1 (because it's being reset to 1 for the next build ID. See bellow.)
			workflowTask.Attempt,
			workflowTask.ScheduledTime,
		)
		newWorkflowTask = m.getWorkflowTaskInfo()
		newWorkflowTask.ScheduledEventID = scheduledEvent.GetEventId()
		// Using 1 as the attempt in MS. it's needed so that the new WFT is not considered transient.
		// TODO: maybe add a separate field in MS for flagging transient WFT instead of relying on attempt so that we
		// can put total attempt count (across all build IDs) here?
		newWorkflowTask.Attempt = 1
		m.UpdateWorkflowTask(newWorkflowTask)
		return newWorkflowTask, true, redirectCounter, nil
	}
	return workflowTask, false, redirectCounter, nil
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

	if request.GetForceCreateNewWorkflowTask() {
		// If ForceCreateNewWorkflowTask is set to true, then this is a heartbeat response.
		// New WT will be created as Normal and WorkflowTaskCompletedEvent for this WT is also must be written.
		// In the future, if we decide not to write heartbeat of speculative WT to the history, this check should be removed,
		// and extra logic should be added to create next WT as Speculative. Currently, new heartbeat WT is always created as Normal.
		return false
	}

	for _, message := range request.Messages {
		if !message.GetBody().MessageIs((*updatepb.Rejection)(nil)) {
			return false
		}
	}

	// Speculative WT can be dropped when response contains only rejection messages.
	// Empty messages list is equivalent to only rejection messages because server will reject all sent updates (if any).
	return true
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
			durationpb.New(workflowTask.WorkflowTaskTimeout),
			workflowTask.Attempt,
			workflowTask.ScheduledTime.UTC(),
		)

		workflowTask.ScheduledEventID = scheduledEvent.GetEventId()
		startedEvent := m.ms.hBuilder.AddWorkflowTaskStartedEvent(
			workflowTask.ScheduledEventID,
			workflowTask.RequestID,
			request.GetIdentity(),
			workflowTask.StartedTime,
			workflowTask.SuggestContinueAsNew,
			workflowTask.HistorySizeBytes,
			request.WorkerVersionStamp,
			workflowTask.BuildIdRedirectCounter,
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
	versioningStamp *commonpb.WorkerVersionStamp,
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
			durationpb.New(workflowTask.WorkflowTaskTimeout),
			workflowTask.Attempt,
			workflowTask.ScheduledTime.UTC(),
		)
		workflowTask.ScheduledEventID = scheduledEvent.GetEventId()
		startedEvent := m.ms.hBuilder.AddWorkflowTaskStartedEvent(
			workflowTask.ScheduledEventID,
			workflowTask.RequestID,
			identity,
			workflowTask.StartedTime,
			workflowTask.SuggestContinueAsNew,
			workflowTask.HistorySizeBytes,
			versioningStamp,
			workflowTask.BuildIdRedirectCounter,
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

	if err := m.ApplyWorkflowTaskFailedEvent(); err != nil {
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
			durationpb.New(workflowTask.WorkflowTaskTimeout),
			workflowTask.Attempt,
			workflowTask.ScheduledTime.UTC(),
		)
		workflowTask.ScheduledEventID = scheduledEvent.GetEventId()
		startedEvent := m.ms.hBuilder.AddWorkflowTaskStartedEvent(
			workflowTask.ScheduledEventID,
			workflowTask.RequestID,
			"",
			workflowTask.StartedTime,
			workflowTask.SuggestContinueAsNew,
			workflowTask.HistorySizeBytes,
			nil,
			workflowTask.BuildIdRedirectCounter,
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

	if err := m.ApplyWorkflowTaskTimedOutEvent(enumspb.TIMEOUT_TYPE_START_TO_CLOSE); err != nil {
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
		WorkflowTaskTimeout:   time.Duration(0),
		StartedTime:           time.Unix(0, 0).UTC(),
		TaskQueue:             nil,
		OriginalScheduledTime: time.Unix(0, 0).UTC(),
		Attempt:               1,
		Type:                  enumsspb.WORKFLOW_TASK_TYPE_UNSPECIFIED,
		SuggestContinueAsNew:  false,
		HistorySizeBytes:      0,
		// need to retain Build ID of failed WF task to compare it with the build ID of next attempt
		BuildId: m.ms.executionInfo.WorkflowTaskBuildId,
	}
	if incrementAttempt {
		failWorkflowTaskInfo.Attempt = m.ms.executionInfo.WorkflowTaskAttempt + 1
		failWorkflowTaskInfo.ScheduledTime = m.ms.timeSource.Now().UTC()
	}
	m.retainWorkflowTaskBuildIdInfo(failWorkflowTaskInfo)
	m.UpdateWorkflowTask(failWorkflowTaskInfo)
}

// deleteWorkflowTask deletes a workflow task.
func (m *workflowTaskStateMachine) deleteWorkflowTask() {
	resetWorkflowTaskInfo := &WorkflowTaskInfo{
		Version:             common.EmptyVersion,
		ScheduledEventID:    common.EmptyEventID,
		StartedEventID:      common.EmptyEventID,
		RequestID:           emptyUUID,
		WorkflowTaskTimeout: time.Duration(0),
		Attempt:             1,
		StartedTime:         time.Unix(0, 0).UTC(),
		ScheduledTime:       time.Unix(0, 0).UTC(),

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
	m.ms.executionInfo.WorkflowTaskTimeout = durationpb.New(workflowTask.WorkflowTaskTimeout)
	m.ms.executionInfo.WorkflowTaskAttempt = workflowTask.Attempt
	if !workflowTask.StartedTime.IsZero() {
		m.ms.executionInfo.WorkflowTaskStartedTime = timestamppb.New(workflowTask.StartedTime)

	}
	if !workflowTask.ScheduledTime.IsZero() {
		m.ms.executionInfo.WorkflowTaskScheduledTime = timestamppb.New(workflowTask.ScheduledTime)
	}
	m.ms.executionInfo.WorkflowTaskOriginalScheduledTime = timestamppb.New(workflowTask.OriginalScheduledTime)
	m.ms.executionInfo.WorkflowTaskType = workflowTask.Type
	m.ms.executionInfo.WorkflowTaskSuggestContinueAsNew = workflowTask.SuggestContinueAsNew
	m.ms.executionInfo.WorkflowTaskHistorySizeBytes = workflowTask.HistorySizeBytes

	m.ms.executionInfo.WorkflowTaskBuildId = workflowTask.BuildId
	m.ms.executionInfo.WorkflowTaskBuildIdRedirectCounter = workflowTask.BuildIdRedirectCounter

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
	return m.HasPendingWorkflowTask() || m.ms.HasCompletedAnyWorkflowTask()
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
		EventTime: timestamppb.New(workflowTask.ScheduledTime),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Version:   m.ms.currentVersion,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
			WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
				TaskQueue:           m.ms.CurrentTaskQueue(),
				StartToCloseTimeout: durationpb.New(workflowTask.WorkflowTaskTimeout),
				Attempt:             workflowTask.Attempt,
			},
		},
	}

	var versioningStamp *commonpb.WorkerVersionStamp
	if workflowTask.BuildId != "" {
		// fill out the stamp value of the transient WFT based on MS data
		versioningStamp = &commonpb.WorkerVersionStamp{UseVersioning: true, BuildId: workflowTask.BuildId}
	}

	startedEvent := &historypb.HistoryEvent{
		EventId:   workflowTask.StartedEventID,
		EventTime: timestamppb.New(workflowTask.StartedTime),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Version:   m.ms.currentVersion,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
			WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
				ScheduledEventId:     workflowTask.ScheduledEventID,
				Identity:             identity,
				RequestId:            workflowTask.RequestID,
				SuggestContinueAsNew: workflowTask.SuggestContinueAsNew,
				HistorySizeBytes:     workflowTask.HistorySizeBytes,
				WorkerVersion:        versioningStamp,
			},
		},
	}

	return &historyspb.TransientWorkflowTaskInfo{
		HistorySuffix: []*historypb.HistoryEvent{scheduledEvent, startedEvent},
	}
}

func (m *workflowTaskStateMachine) getWorkflowTaskInfo() *WorkflowTaskInfo {
	return &WorkflowTaskInfo{
		Version:                m.ms.executionInfo.WorkflowTaskVersion,
		ScheduledEventID:       m.ms.executionInfo.WorkflowTaskScheduledEventId,
		StartedEventID:         m.ms.executionInfo.WorkflowTaskStartedEventId,
		RequestID:              m.ms.executionInfo.WorkflowTaskRequestId,
		WorkflowTaskTimeout:    m.ms.executionInfo.WorkflowTaskTimeout.AsDuration(),
		Attempt:                m.ms.executionInfo.WorkflowTaskAttempt,
		StartedTime:            m.ms.executionInfo.WorkflowTaskStartedTime.AsTime(),
		ScheduledTime:          m.ms.executionInfo.WorkflowTaskScheduledTime.AsTime(),
		TaskQueue:              m.ms.CurrentTaskQueue(),
		OriginalScheduledTime:  m.ms.executionInfo.WorkflowTaskOriginalScheduledTime.AsTime(),
		Type:                   m.ms.executionInfo.WorkflowTaskType,
		SuggestContinueAsNew:   m.ms.executionInfo.WorkflowTaskSuggestContinueAsNew,
		HistorySizeBytes:       m.ms.executionInfo.WorkflowTaskHistorySizeBytes,
		BuildId:                m.ms.executionInfo.WorkflowTaskBuildId,
		BuildIdRedirectCounter: m.ms.executionInfo.WorkflowTaskBuildIdRedirectCounter,
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
	m.ms.executionInfo.MostRecentWorkerVersionStamp = attrs.GetWorkerVersion()
	addedResetPoint := m.ms.addResetPointFromCompletion(
		attrs.GetBinaryChecksum(),
		attrs.GetWorkerVersion().GetBuildId(),
		event.GetEventId(),
		limits.MaxResetPoints,
	)
	// For versioned workflows the search attributes should be already up-to-date based on the task started events.
	// This is still useful for unversioned workers.
	if err := m.ms.updateBuildIdsSearchAttribute(attrs.GetWorkerVersion(), limits.MaxSearchAttributeValueSize); err != nil {
		return err
	}
	if addedResetPoint && len(attrs.GetBinaryChecksum()) > 0 {
		if err := m.ms.updateBinaryChecksumSearchAttribute(); err != nil {
			return err
		}
	}
	return nil
}

func (m *workflowTaskStateMachine) emitWorkflowTaskAttemptStats(
	attempt int32,
) {
	namespaceName := m.ms.GetNamespaceEntry().Name().String()
	metrics.WorkflowTaskAttempt.With(m.ms.metricsHandler).
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
	defaultTimeout *durationpb.Duration,
	attempt int32,
) *durationpb.Duration {
	// This util function is only for calculating active workflow task timeout.
	// Transient workflow task in passive cluster won't call this function and
	// always use default timeout as it will either be completely overwritten by
	// a replicated workflow schedule event from active cluster, or if used, it's
	// attempt will be reset to 1.
	// Check ApplyTransientWorkflowTaskScheduled for details.

	if defaultTimeout == nil {
		defaultTimeout = durationpb.New(0)
	}

	if attempt <= workflowTaskRetryBackoffMinAttempts {
		return defaultTimeout
	}

	policy := backoff.NewExponentialRetryPolicy(workflowTaskRetryInitialInterval).
		WithMaximumInterval(m.ms.shard.GetConfig().WorkflowTaskRetryMaxInterval()).
		WithExpirationInterval(backoff.NoInterval)
	startToCloseTimeout := defaultTimeout.AsDuration() + policy.ComputeNextDelay(0, int(attempt)-workflowTaskRetryBackoffMinAttempts)
	return durationpb.New(startToCloseTimeout)
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
		durationpb.New(wt.WorkflowTaskTimeout),
		wt.Attempt,
		wt.ScheduledTime,
	)

	if scheduledEvent.EventId != wt.ScheduledEventID {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, scheduled event Id: %d for normal workflow task doesn't match the one from speculative workflow task: %d", scheduledEvent.EventId, wt.ScheduledEventID))
	}

	if wt.StartedEventID != common.EmptyEventID {
		// If WT is started then started event is written to the history and
		// timeout timer task (for START_TO_CLOSE timeout) is created.

		_ = m.ms.hBuilder.AddWorkflowTaskStartedEvent(
			scheduledEvent.EventId,
			wt.RequestID,
			"",
			wt.StartedTime,
			wt.SuggestContinueAsNew,
			wt.HistorySizeBytes,
			nil,
			wt.BuildIdRedirectCounter,
		)
		m.ms.hBuilder.FlushAndCreateNewBatch()

		if err := m.ms.taskGenerator.GenerateStartWorkflowTaskTasks(
			scheduledEvent.EventId,
		); err != nil {
			return err
		}
	} else {
		if err := m.ms.taskGenerator.GenerateScheduleSpeculativeWorkflowTaskTasks(
			wt,
		); err != nil {
			return err
		}
	}

	return nil
}

func cleanTaskQueue(proto *taskqueuepb.TaskQueue, taskType enumspb.TaskQueueType) *taskqueuepb.TaskQueue {
	if proto == nil {
		return proto
	}
	partition, err := tqid.PartitionFromProto(proto, "", taskType)
	if err != nil {
		return proto
	}

	cleanTq := common.CloneProto(proto)
	cleanTq.Name = partition.TaskQueue().Name()
	return cleanTq
}
