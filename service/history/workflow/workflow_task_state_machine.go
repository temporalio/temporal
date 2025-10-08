//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination workflow_task_state_machine_mock.go

package workflow

import (
	"cmp"
	"math"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
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
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow/update"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	workflowTaskStateMachine struct {
		ms             *MutableStateImpl
		metricsHandler metrics.Handler
	}
)

const (
	workflowTaskRetryBackoffMinAttempts = 3
	workflowTaskRetryInitialInterval    = 5 * time.Second
	maxWorkflowTaskTimeoutToDelete      = 120 * time.Second
)

func newWorkflowTaskStateMachine(
	ms *MutableStateImpl,
	metricsHandler metrics.Handler,
) *workflowTaskStateMachine {
	return &workflowTaskStateMachine{
		ms:             ms,
		metricsHandler: metricsHandler,
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
) (*historyi.WorkflowTaskInfo, error) {

	// set workflow state to running, since workflow task is scheduled
	// NOTE: for zombie workflow, should not change the state
	state, _ := m.ms.GetWorkflowStateStatus()
	if state != enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		if _, err := m.ms.UpdateWorkflowStateStatus(
			enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		); err != nil {
			return nil, err
		}
	}

	workflowTask := &historyi.WorkflowTaskInfo{
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
func (m *workflowTaskStateMachine) retainWorkflowTaskBuildIdInfo(workflowTask *historyi.WorkflowTaskInfo) {
	if workflowTask.Attempt > 1 {
		workflowTask.BuildId = m.ms.executionInfo.WorkflowTaskBuildId
		workflowTask.BuildIdRedirectCounter = m.ms.executionInfo.BuildIdRedirectCounter
	}
}

func (m *workflowTaskStateMachine) ApplyTransientWorkflowTaskScheduled() (*historyi.WorkflowTaskInfo, error) {
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
	workflowTask := &historyi.WorkflowTaskInfo{
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
	workflowTask *historyi.WorkflowTaskInfo,
	version int64,
	scheduledEventID int64,
	startedEventID int64,
	requestID string,
	startedTime time.Time,
	suggestContinueAsNew bool,
	historySizeBytes int64,
	versioningStamp *commonpb.WorkerVersionStamp,
	redirectCounter int64,
) (*historyi.WorkflowTaskInfo, error) {
	// When this function is called from ApplyEvents, workflowTask is nil.
	// It is safe to look up the workflow task as it does not have to deal with transient workflow task case.
	if workflowTask == nil {
		workflowTask = m.GetWorkflowTaskByID(scheduledEventID)
		if workflowTask == nil {
			return nil, serviceerror.NewInternalf("unable to find workflow task: %v", scheduledEventID)
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

	workflowTask = &historyi.WorkflowTaskInfo{
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
	return m.afterAddWorkflowTaskCompletedEvent(
		event,
		historyi.WorkflowTaskCompletionLimits{MaxResetPoints: math.MaxInt, MaxSearchAttributeValueSize: math.MaxInt},
	)
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
	workflowTask *historyi.WorkflowTaskInfo,
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
) (*historyi.WorkflowTaskInfo, error) {
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
		// TODO: ideally this should be the version of the last started workflow task.
		// but we are using the last event version here instead since there's no other
		// events when there's a started workflow task.
		lastEventVersion, err := m.ms.GetLastEventVersion()
		if err != nil {
			return nil, err
		}

		// If failover happened during transient workflow task,
		// then reset the attempt to 1, and not use transient workflow task.
		if m.ms.GetCurrentVersion() != lastEventVersion {
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
) (*historyi.WorkflowTaskInfo, error) {
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
	skipVersioningCheck bool,
	updateReg update.Registry,
) (*historypb.HistoryEvent, *historyi.WorkflowTaskInfo, error) {
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
	if updateReg != nil {
		suggestContinueAsNew = cmp.Or(suggestContinueAsNew, updateReg.SuggestContinueAsNew())
	}

	workflowTask, scheduledEventCreatedForRedirect, redirectCounter, err := m.processBuildIdRedirectInfo(versioningStamp, workflowTask, taskQueue, redirectInfo, skipVersioningCheck)
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
	if err != nil {
		return nil, nil, err
	}

	m.emitWorkflowTaskAttemptStats(workflowTask.Attempt)

	// TODO merge active & passive task generation
	if err = m.ms.taskGenerator.GenerateStartWorkflowTaskTasks(
		scheduledEventID,
	); err != nil {
		return nil, nil, err
	}

	return startedEvent, workflowTask, nil
}

// processBuildIdRedirectInfo validated possible build ID redirect based on the versioningStamp and redirectInfo.
// If a valid redirect is being applied to a transient WFT, the transient WFT is converted to normal and a
// scheduled event is created. In this case, the returned workflow info will be different from the given one and
// `converted` will be true.
// Also returns redirect counter that shall be used in the WFT started event.
func (m *workflowTaskStateMachine) processBuildIdRedirectInfo(
	versioningStamp *commonpb.WorkerVersionStamp,
	workflowTask *historyi.WorkflowTaskInfo,
	taskQueue *taskqueuepb.TaskQueue,
	redirectInfo *taskqueuespb.BuildIdRedirectInfo,
	skipVersioningCheck bool,
) (newWorkflowTask *historyi.WorkflowTaskInfo, converted bool, redirectCounter int64, err error) {
	buildId := worker_versioning.BuildIdIfUsingVersioning(versioningStamp)
	if buildId == "" && (m.ms.GetAssignedBuildId() == "" || // unversioned workflow
		skipVersioningCheck || // resetter may add WFT started events without stamps, it sets skipVersioningCheck=true
		(taskQueue.GetKind() == enumspb.TASK_QUEUE_KIND_STICKY && m.ms.executionInfo.GetStickyTaskQueue() == taskQueue.GetName())) {
		// build ID is expected to be empty for sticky queues until old versioning is removed [cleanup-old-wv]
		return workflowTask, false, 0, nil
	}

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
		metrics.SpeculativeWorkflowTaskCommits.With(m.metricsHandler).Record(1,
			metrics.ReasonTag("worker_returned_commands"))
		return false
	}

	if request.GetForceCreateNewWorkflowTask() {
		// If ForceCreateNewWorkflowTask is set to true, then this is a heartbeat response.
		// New WFT will be created as Normal and WorkflowTaskCompletedEvent for this WFT is also must be written.
		// In the future, if we decide not to write heartbeat of speculative WFT to the history, this check should be removed,
		// and extra logic should be added to create next WFT as Speculative. Currently, new heartbeat WFT is always created as Normal.
		metrics.SpeculativeWorkflowTaskCommits.With(m.metricsHandler).Record(1,
			metrics.ReasonTag("force_create_task"))
		return false
	}

	// Speculative WFT that has only Update rejection messages should be discarded (this function returns `true`).
	// If speculative WFT also shipped events to the worker and was discarded, then
	// next WFT will ship these events again. Unfortunately, old SDKs don't support receiving same events more than once.
	// If SDK supports this, it will set DiscardSpeculativeWorkflowTaskWithEvents to `true`
	// and server can discard speculative WFT even if it had events.

	// Otherwise, server needs to determinate if there were events on this speculative WFT,
	// i.e. last event in the history is WFTCompleted event.
	// It is guaranteed that WFTStarted event is followed by WFTCompleted event and history tail might look like:
	//   previous WFTStarted
	//   previous WFTCompleted
	//   --> NextEventID points here because it doesn't move for speculative WFT.
	// In this case, the difference between NextEventID and LastCompletedWorkflowTaskStartedEventId is 2.
	// If there are other events after WFTCompleted event, then the difference is > 2 and speculative WFT can't be discarded.
	if !request.GetCapabilities().GetDiscardSpeculativeWorkflowTaskWithEvents() &&
		m.ms.GetNextEventID() > m.ms.GetLastCompletedWorkflowTaskStartedEventId()+2 {
		metrics.SpeculativeWorkflowTaskCommits.With(m.metricsHandler).Record(1,
			metrics.ReasonTag("interleaved_events"))
		return false
	}

	// Even if worker supports receiving same events more than once,
	// server still writes speculative WFT if it had too many events.
	// This is to prevent shipping a big set of events to the worker over and over again,
	// in case if Updates are constantly rejected.
	if request.GetCapabilities().GetDiscardSpeculativeWorkflowTaskWithEvents() &&
		m.ms.GetNextEventID() > m.ms.GetLastCompletedWorkflowTaskStartedEventId()+2+int64(m.ms.config.DiscardSpeculativeWorkflowTaskMaximumEventsCount()) {
		metrics.SpeculativeWorkflowTaskCommits.With(m.metricsHandler).Record(1,
			metrics.ReasonTag("too_many_interleaved_events"))
		return false
	}

	for _, message := range request.Messages {
		if !message.GetBody().MessageIs((*updatepb.Rejection)(nil)) {
			metrics.SpeculativeWorkflowTaskCommits.With(m.metricsHandler).Record(1,
				metrics.ReasonTag("update_accepted"))
			return false
		}
	}

	// Speculative WFT can be discarded when response contains only rejection messages.
	// Empty messages list is equivalent to only rejection messages because server will reject all sent updates (if any).

	// TODO: We should perform a shard ownership check here to prevent the case where the entire speculative workflow task
	// is done on a stale mutable state and the fact that mutable state is stale caused workflow update requests to be rejected.
	// NOTE: The AssertShardOwnership persistence API is not implemented in the repo.

	metrics.SpeculativeWorkflowTaskRollbacks.With(m.metricsHandler).Record(1)
	return true
}

func (m *workflowTaskStateMachine) AddWorkflowTaskCompletedEvent(
	workflowTask *historyi.WorkflowTaskInfo,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
	limits historyi.WorkflowTaskCompletionLimits,
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

	deploymentName := request.GetDeploymentOptions().GetDeploymentName()
	if deploymentName == "" {
		//nolint:staticcheck // SA1019 deprecated Deployment will clean up later
		deploymentName = request.GetDeployment().GetSeriesName()
	}

	vb := request.VersioningBehavior
	if request.DeploymentOptions != nil && request.DeploymentOptions.GetWorkerVersioningMode() != enumspb.WORKER_VERSIONING_MODE_VERSIONED {
		// SDK has a bug that reports behavior if user has specified a default behavior without enabling versioning.
		// Until that is fixed, we should adjust this value so the workflow works correctly.
		vb = enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED
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
		deploymentName,
		//nolint:staticcheck // SA1019 deprecated Deployment will clean up later
		worker_versioning.DeploymentOrVersion(request.Deployment, worker_versioning.DeploymentVersionFromOptions(request.DeploymentOptions)),
		vb,
	)

	err := m.afterAddWorkflowTaskCompletedEvent(event, limits)
	if err != nil {
		return nil, err
	}

	metrics.WorkflowTasksCompleted.With(m.metricsHandler).Record(1,
		metrics.NamespaceTag(m.ms.GetNamespaceEntry().Name().String()),
		metrics.VersioningBehaviorTag(vb),
		metrics.FirstAttemptTag(workflowTask.Attempt),
	)

	return event, nil
}

func (m *workflowTaskStateMachine) AddWorkflowTaskFailedEvent(
	workflowTask *historyi.WorkflowTaskInfo,
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
	workflowTask *historyi.WorkflowTaskInfo,
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

func (m *workflowTaskStateMachine) recordTimeoutTasksForDeletion(workflowTask *historyi.WorkflowTaskInfo) {
	// Record persisted workflow task timeout tasks for deletion after successful persistence update.
	if task := workflowTask.ScheduleToStartTimeoutTask; task != nil {
		key := task.GetKey()
		if key.FireTime.Sub(workflowTask.ScheduledTime) < maxWorkflowTaskTimeoutToDelete {
			m.ms.BestEffortDeleteTasks[tasks.CategoryTimer] = append(m.ms.BestEffortDeleteTasks[tasks.CategoryTimer], key)
		}
	}
	if task := workflowTask.StartToCloseTimeoutTask; task != nil {
		key := task.GetKey()
		if key.FireTime.Sub(workflowTask.StartedTime) < maxWorkflowTaskTimeoutToDelete {
			m.ms.BestEffortDeleteTasks[tasks.CategoryTimer] = append(m.ms.BestEffortDeleteTasks[tasks.CategoryTimer], key)
		}
	}
}

func (m *workflowTaskStateMachine) failWorkflowTask(
	incrementAttempt bool,
) {
	// Get current workflow task info before clearing it, to capture timeout tasks for deletion
	currentWorkflowTask := m.getWorkflowTaskInfo()
	m.recordTimeoutTasksForDeletion(currentWorkflowTask)

	// Increment attempts only if workflow task is failing on non-sticky task queue.
	// If it was sticky task queue, clear sticky task queue first and try again before creating transient workflow task.
	if m.ms.IsStickyTaskQueueSet() {
		incrementAttempt = false
		m.ms.ClearStickyTaskQueue()
	}

	failWorkflowTaskInfo := &historyi.WorkflowTaskInfo{
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
	// Get current workflow task info before deleting it, to capture timeout tasks for deletion
	currentWorkflowTask := m.getWorkflowTaskInfo()
	m.recordTimeoutTasksForDeletion(currentWorkflowTask)

	// Clear in-memory timeout tasks
	m.ms.SetWorkflowTaskScheduleToStartTimeoutTask(nil)
	m.ms.SetWorkflowTaskStartToCloseTimeoutTask(nil)

	resetWorkflowTaskInfo := &historyi.WorkflowTaskInfo{
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
		OriginalScheduledTime: currentWorkflowTask.OriginalScheduledTime,
		Type:                  enumsspb.WORKFLOW_TASK_TYPE_UNSPECIFIED,
		SuggestContinueAsNew:  false,
		HistorySizeBytes:      0,
	}
	m.UpdateWorkflowTask(resetWorkflowTaskInfo)
}

// UpdateWorkflowTask updates a workflow task.
func (m *workflowTaskStateMachine) UpdateWorkflowTask(
	workflowTask *historyi.WorkflowTaskInfo,
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

	m.ms.workflowTaskUpdated = true

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

func (m *workflowTaskStateMachine) GetPendingWorkflowTask() *historyi.WorkflowTaskInfo {
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

func (m *workflowTaskStateMachine) GetStartedWorkflowTask() *historyi.WorkflowTaskInfo {
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
func (m *workflowTaskStateMachine) GetWorkflowTaskByID(scheduledEventID int64) *historyi.WorkflowTaskInfo {
	workflowTask := m.getWorkflowTaskInfo()
	if scheduledEventID == workflowTask.ScheduledEventID {
		return workflowTask
	}

	return nil
}

func (m *workflowTaskStateMachine) GetTransientWorkflowTaskInfo(
	workflowTask *historyi.WorkflowTaskInfo,
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

func (m *workflowTaskStateMachine) getWorkflowTaskInfo() *historyi.WorkflowTaskInfo {
	wft := &historyi.WorkflowTaskInfo{
		Version:                    m.ms.executionInfo.WorkflowTaskVersion,
		ScheduledEventID:           m.ms.executionInfo.WorkflowTaskScheduledEventId,
		StartedEventID:             m.ms.executionInfo.WorkflowTaskStartedEventId,
		RequestID:                  m.ms.executionInfo.WorkflowTaskRequestId,
		WorkflowTaskTimeout:        m.ms.executionInfo.WorkflowTaskTimeout.AsDuration(),
		Attempt:                    m.ms.executionInfo.WorkflowTaskAttempt,
		StartedTime:                m.ms.executionInfo.WorkflowTaskStartedTime.AsTime(),
		ScheduledTime:              m.ms.executionInfo.WorkflowTaskScheduledTime.AsTime(),
		TaskQueue:                  m.ms.CurrentTaskQueue(),
		OriginalScheduledTime:      m.ms.executionInfo.WorkflowTaskOriginalScheduledTime.AsTime(),
		Type:                       m.ms.executionInfo.WorkflowTaskType,
		SuggestContinueAsNew:       m.ms.executionInfo.WorkflowTaskSuggestContinueAsNew,
		HistorySizeBytes:           m.ms.executionInfo.WorkflowTaskHistorySizeBytes,
		BuildId:                    m.ms.executionInfo.WorkflowTaskBuildId,
		BuildIdRedirectCounter:     m.ms.executionInfo.WorkflowTaskBuildIdRedirectCounter,
		ScheduleToStartTimeoutTask: m.ms.GetWorkflowTaskScheduleToStartTimeoutTask(),
		StartToCloseTimeoutTask:    m.ms.GetWorkflowTaskStartToCloseTimeoutTask(),
	}

	return wft
}

func (m *workflowTaskStateMachine) beforeAddWorkflowTaskCompletedEvent() {
	// Make sure to delete workflow task before adding events. Otherwise they are buffered rather than getting appended.
	m.deleteWorkflowTask()
}

func (m *workflowTaskStateMachine) afterAddWorkflowTaskCompletedEvent(
	event *historypb.HistoryEvent,
	limits historyi.WorkflowTaskCompletionLimits,
) error {
	attrs := event.GetWorkflowTaskCompletedEventAttributes()
	m.ms.executionInfo.LastCompletedWorkflowTaskStartedEventId = attrs.GetStartedEventId()
	m.ms.executionInfo.MostRecentWorkerVersionStamp = attrs.GetWorkerVersion()
	m.ms.executionInfo.WorkerDeploymentName = attrs.GetWorkerDeploymentName()

	//nolint:staticcheck // SA1019 deprecated Deployment will clean up later
	wftDeployment := attrs.GetDeployment()
	if v := attrs.GetWorkerDeploymentVersion(); v != "" { //nolint:staticcheck // SA1019: worker versioning v0.31
		dv, _ := worker_versioning.WorkerDeploymentVersionFromStringV31(v)
		wftDeployment = worker_versioning.DeploymentFromDeploymentVersion(dv)
	}
	if v := attrs.GetDeploymentVersion(); v != nil {
		wftDeployment = worker_versioning.DeploymentFromExternalDeploymentVersion(v)
	}
	wftBehavior := attrs.GetVersioningBehavior()
	versioningInfo := m.ms.GetExecutionInfo().GetVersioningInfo()
	transition := m.ms.GetDeploymentTransition()

	var completedTransition bool
	if transition != nil {
		// It's possible that the completed WFT is not yet from the current transition because when
		// the transition started, the current wft was already started. In this case, we allow the
		// started wft to run and when completed, we create another wft immediately.
		if transition.GetDeployment().Equal(wftDeployment) {
			versioningInfo.DeploymentTransition = nil //nolint:staticcheck // SA1019: worker versioning v0.30
			versioningInfo.VersionTransition = nil
			transition = nil
			completedTransition = true
		}
	}

	// Deployment and behavior before applying the data came from the completed wft.
	wfDeploymentBefore := m.ms.GetEffectiveDeployment()
	wfBehaviorBefore := m.ms.GetEffectiveVersioningBehavior()

	// Change deployment and behavior based on completed wft.
	if wftBehavior == enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED {
		if versioningInfo != nil {
			versioningInfo.Behavior = wftBehavior
			// Deployment Version is not set for unversioned workers.
			versioningInfo.DeploymentVersion = nil
			//nolint:staticcheck // SA1019 deprecated Version will clean up later
			versioningInfo.Version = ""
			//nolint:staticcheck // SA1019 deprecated Deployment will clean up later
			versioningInfo.Deployment = nil
		}
	} else {
		if versioningInfo == nil {
			versioningInfo = &workflowpb.WorkflowExecutionVersioningInfo{}
			m.ms.GetExecutionInfo().VersioningInfo = versioningInfo
		}
		versioningInfo.Behavior = wftBehavior
		// Only populating the new field.
		//nolint:staticcheck // SA1019 deprecated Deployment will clean up later
		versioningInfo.Deployment = nil
		//nolint:staticcheck // SA1019 deprecated Version will clean up later [cleanup-wv-3.1]
		versioningInfo.Version = worker_versioning.WorkerDeploymentVersionToStringV31(worker_versioning.DeploymentVersionFromDeployment(wftDeployment))
		versioningInfo.DeploymentVersion = worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(wftDeployment)
	}

	// Deployment and behavior after applying the data came from the completed wft.
	wfDeploymentAfter := m.ms.GetEffectiveDeployment()
	wfBehaviorAfter := m.ms.GetEffectiveVersioningBehavior()
	// We reschedule activities if a transition was completed because during the transition
	// ATs might have been dropped. Note that it is possible that transition completes and still
	// `wfDeploymentBefore == wfDeploymentAfter`. Example: wf was on deployment1, started
	// transition to deployment2, before completing the transition it changed the transition to
	// deployment1 (maybe user rolled back current deployment), now the transition completes.
	if completedTransition ||
		// It is possible that this WFT is changing workflow's deployment even if there was no
		// ongoing transition in the MS. That is possible when the wft is speculative. We still
		// want to reschedule the activities so they are queued with the up-to-date directive.
		!wfDeploymentBefore.Equal(wfDeploymentAfter) ||
		// If effective behavior changes we also want to reschedule the pending activities, so
		// they go to the right matching queues.
		wfBehaviorBefore != wfBehaviorAfter {
		if err := m.ms.reschedulePendingActivities(); err != nil {
			return err
		}
	}

	//nolint:staticcheck // SA1019: worker versioning v2
	buildId := attrs.GetWorkerVersion().GetBuildId()
	if wftDeployment != nil {
		buildId = wftDeployment.GetBuildId()
	}
	addedResetPoint := m.ms.addResetPointFromCompletion(
		attrs.GetBinaryChecksum(),
		buildId,
		event.GetEventId(),
		limits.MaxResetPoints,
	)

	// For v3 versioned workflows (ms.GetEffectiveVersioningBehavior() != UNSPECIFIED), this will update the reachability
	// search attribute based on the execution_info.deployment and/or override deployment if one exists. We must update the
	// search attribute here because the reachability deployment may have just been changed by CompleteDeploymentTransition.
	// This is also useful for unversioned workers.
	// For v1 and v2 versioned workflows the search attributes should be already up-to-date based on the task started events.
	if err := m.ms.updateBuildIdsAndDeploymentSearchAttributes(attrs.GetWorkerVersion(), limits.MaxSearchAttributeValueSize); err != nil {
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
	startToCloseTimeout := defaultTimeout.AsDuration() + policy.ComputeNextDelay(0, int(attempt)-workflowTaskRetryBackoffMinAttempts, nil)
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

	if !m.ms.workflowTaskUpdated {
		// Whenever we close transaction, speculative workflow task will be converted to normal.
		// This means when there's a speculative workflow task, we haven't closed the transaction for the
		// speculative workflow task yet after it's created.
		// Upon creation of the speculative workflow task, the workflowTaskUpdated flag should be set.
		// The flag is only unset when closing the transaction, which we know haven't happened yet.
		// So the workflowTaskUpdated flag should always be set here.
		m.ms.logger.Warn("Speculative workflow task didn't set workflowTaskUpdated flag, likely due to a bug")
		m.ms.workflowTaskUpdated = true
	}

	m.ms.executionInfo.WorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
	metrics.SpeculativeWorkflowTaskCommits.With(m.metricsHandler).Record(1,
		metrics.ReasonTag("close_transaction"))

	wt := m.getWorkflowTaskInfo()

	scheduledEvent := m.ms.hBuilder.AddWorkflowTaskScheduledEvent(
		wt.TaskQueue,
		durationpb.New(wt.WorkflowTaskTimeout),
		wt.Attempt,
		wt.ScheduledTime,
	)

	if scheduledEvent.EventId != wt.ScheduledEventID {
		return serviceerror.NewInternalf("it could be a bug, scheduled event Id: %d for normal workflow task doesn't match the one from speculative workflow task: %d", scheduledEvent.EventId, wt.ScheduledEventID)
	}

	if wtAlreadyStarted := wt.StartedEventID != common.EmptyEventID; wtAlreadyStarted {
		// If WT was already started then started event is written to the history and
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
