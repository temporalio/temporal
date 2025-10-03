//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination task_generator_mock.go

package workflow

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

type (
	TaskGenerator interface {
		GenerateWorkflowStartTasks(
			startEvent *historypb.HistoryEvent,
		) (executionTimeoutTimerTaskStatus int32, err error)
		GenerateWorkflowCloseTasks(
			closedTime time.Time,
			deleteAfterClose bool,
		) error
		// GenerateDeleteHistoryEventTask adds a tasks.DeleteHistoryEventTask to the mutable state.
		// This task is used to delete the history events of the workflow execution after the retention period expires.
		GenerateDeleteHistoryEventTask(closeTime time.Time) error
		GenerateDeleteExecutionTask() (*tasks.DeleteExecutionTask, error)
		GenerateRecordWorkflowStartedTasks(
			startEvent *historypb.HistoryEvent,
		) error
		GenerateDelayedWorkflowTasks(
			startEvent *historypb.HistoryEvent,
		) error
		GenerateScheduleWorkflowTaskTasks(
			workflowTaskScheduledEventID int64,
		) error
		GenerateScheduleSpeculativeWorkflowTaskTasks(
			workflowTask *historyi.WorkflowTaskInfo,
		) error
		GenerateStartWorkflowTaskTasks(
			workflowTaskScheduledEventID int64,
		) error
		GenerateActivityTasks(
			activityScheduledEventID int64,
		) error
		GenerateActivityRetryTasks(activityInfo *persistencespb.ActivityInfo) error
		GenerateChildWorkflowTasks(
			childInitiatedEventId int64,
		) error
		GenerateRequestCancelExternalTasks(
			event *historypb.HistoryEvent,
		) error
		GenerateSignalExternalTasks(
			event *historypb.HistoryEvent,
		) error
		GenerateUpsertVisibilityTask() error
		GenerateWorkflowResetTasks() error

		// these 2 APIs should only be called when mutable state transaction is being closed
		GenerateActivityTimerTasks() error
		GenerateUserTimerTasks() error

		// replication tasks
		GenerateHistoryReplicationTasks(
			eventBatches [][]*historypb.HistoryEvent,
		) ([]tasks.Task, error)
		GenerateMigrationTasks(targetClusters []string) ([]tasks.Task, int64, error)

		// Generate tasks for any updated state machines on mutable state.
		// Looks up machine definition in the provided registry.
		// Must be called **after** updating transition history for the current transition
		GenerateDirtySubStateMachineTasks(stateMachineRegistry *hsm.Registry) error
	}

	TaskGeneratorImpl struct {
		namespaceRegistry namespace.Registry
		mutableState      historyi.MutableState
		config            *configs.Config
		archivalMetadata  archiver.ArchivalMetadata
	}
)

const defaultWorkflowRetention = 1 * 24 * time.Hour

var _ TaskGenerator = (*TaskGeneratorImpl)(nil)

func NewTaskGenerator(
	namespaceRegistry namespace.Registry,
	mutableState historyi.MutableState,
	config *configs.Config,
	archivalMetadata archiver.ArchivalMetadata,
) *TaskGeneratorImpl {
	return &TaskGeneratorImpl{
		namespaceRegistry: namespaceRegistry,
		mutableState:      mutableState,
		config:            config,
		archivalMetadata:  archivalMetadata,
	}
}

func (r *TaskGeneratorImpl) GenerateWorkflowStartTasks(
	startEvent *historypb.HistoryEvent,
) (int32, error) {

	executionInfo := r.mutableState.GetExecutionInfo()
	executionTimeoutTimerTaskStatus := executionInfo.WorkflowExecutionTimerTaskStatus
	if !r.mutableState.IsWorkflowExecutionRunning() {
		return executionTimeoutTimerTaskStatus, nil
	}

	workflowExecutionTimeoutTimerEnabled := r.config.EnableWorkflowExecutionTimeoutTimer()
	if !workflowExecutionTimeoutTimerEnabled {
		// when the feature is disabled, reset this field so that it won't be carried over to the next run
		// and new runs can always have the run timeout timer always generated.
		executionTimeoutTimerTaskStatus = TimerTaskStatusNone
	}

	// The WorkflowExecutionTimeoutTask is more expensive than other tasks as it's
	// not for a certain run, but for a workflowID. When processing that task, the
	// logic needs to load the current run, which require two persistence GetCurrentExecution
	// calls for closed workflows (and workflow is likely to be already closed). Always
	// generating the WorkflowExecutionTimeoutTask means lots of extra load to persistence.
	//
	// So here we don't generate the WorkflowExecutionTimeoutTask on the first run. If the
	// workflow has a second run, the it's likely to have more runs, and only then the extra load
	// from WorkflowExecutionTimeoutTask is justified.
	//
	// Also note the run timeout, if not specified, defaults to execution timeout, so we won't run
	// into the situation where execution timeout is set but no timeout timer task is generated.

	isFirstRun := executionInfo.FirstExecutionRunId == r.mutableState.GetExecutionState().RunId
	workflowExecutionExpirationTime := timestamp.TimeValue(
		executionInfo.WorkflowExecutionExpirationTime,
	)
	if workflowExecutionTimeoutTimerEnabled &&
		!isFirstRun &&
		!workflowExecutionExpirationTime.IsZero() &&
		executionInfo.WorkflowExecutionTimerTaskStatus == TimerTaskStatusNone {
		r.mutableState.AddTasks(&tasks.WorkflowExecutionTimeoutTask{
			// TaskID is set by shard
			NamespaceID:         executionInfo.NamespaceId,
			WorkflowID:          executionInfo.WorkflowId,
			FirstRunID:          executionInfo.FirstExecutionRunId,
			VisibilityTimestamp: workflowExecutionExpirationTime,
		})
		executionTimeoutTimerTaskStatus = TimerTaskStatusCreated
	}

	workflowRunExpirationTime := timestamp.TimeValue(
		executionInfo.WorkflowRunExpirationTime,
	)
	if workflowRunExpirationTime.IsZero() {
		return executionTimeoutTimerTaskStatus, nil
	}
	if executionTimeoutTimerTaskStatus == TimerTaskStatusNone ||
		workflowRunExpirationTime.Before(workflowExecutionExpirationTime) {
		r.mutableState.AddTasks(&tasks.WorkflowRunTimeoutTask{
			// TaskID is set by shard
			WorkflowKey:         r.mutableState.GetWorkflowKey(),
			VisibilityTimestamp: workflowRunExpirationTime,
			Version:             startEvent.GetVersion(),
		})
	}

	return executionTimeoutTimerTaskStatus, nil
}

func (r *TaskGeneratorImpl) GenerateWorkflowCloseTasks(
	closedTime time.Time,
	deleteAfterClose bool,
) error {
	closeVersion, err := r.mutableState.GetCloseVersion()
	if err != nil {
		return err
	}

	closeExecutionTask := &tasks.CloseExecutionTask{
		// TaskID, Visiblitytimestamp is set by shard
		WorkflowKey:      r.mutableState.GetWorkflowKey(),
		Version:          closeVersion,
		DeleteAfterClose: deleteAfterClose,
	}
	closeTasks := []tasks.Task{
		closeExecutionTask,
	}

	// To avoid race condition between visibility close and delete tasks, visibility close task is not created here.
	// Also, there is no reason to schedule history retention task if workflow executions in about to be deleted.
	// This will also save one call to visibility storage and one timer task creation.
	if !deleteAfterClose {
		// In most cases, the value of "now" is the closeEvent time.
		// however this is not true for task refresh, where now is
		// the refresh time, not the close time.
		// Also can't always use close time as "now" when calling the method
		// as it will be used as visibilityTimestamp for immediate task and
		// for emitting task_latency_queue/load metric. If close time is used
		// as now, upon refresh the latency metric may see a huge value.
		// TODO: remove all "now" parameters from task generator interface,
		// visibility timestamp for scheduled task should be calculated from event
		// or execution info in mutable state. For immediate task, visibility timestamp
		// should always be when the task is generated so that task_latency_queue/load
		// truly measures only task processing/loading latency.
		closeTasks = append(closeTasks,
			&tasks.CloseExecutionVisibilityTask{
				// TaskID, VisibilityTimestamp is set by shard
				WorkflowKey: r.mutableState.GetWorkflowKey(),
				Version:     closeVersion,
			},
		)
		if r.archivalEnabled() {
			retention, err := r.getRetention()
			if err != nil {
				return err
			}
			// We schedule the archival task for a random time in the near future to avoid sending a surge of tasks
			// to the archival system at the same time

			delay := backoff.FullJitter(r.config.ArchivalProcessorArchiveDelay())
			if delay > retention {
				delay = retention
			}
			// archiveTime is the time when the archival queue recognizes the ArchiveExecutionTask as ready-to-process
			archiveTime := closedTime.Add(delay)

			task := &tasks.ArchiveExecutionTask{
				// TaskID is set by the shard
				WorkflowKey:         r.mutableState.GetWorkflowKey(),
				VisibilityTimestamp: archiveTime,
				Version:             closeVersion,
			}
			closeTasks = append(closeTasks, task)
		} else if err := r.GenerateDeleteHistoryEventTask(closedTime); err != nil {
			return err
		}
	}

	r.mutableState.AddTasks(closeTasks...)

	return nil
}

// getRetention returns the retention period for this task generator's workflow execution.
// The retention period represents how long the workflow data should exist in primary storage after the workflow closes.
// If the workflow namespace is not found, the default retention period is returned.
// This method returns an error when the GetNamespaceByID call fails with anything other than
// serviceerror.NamespaceNotFound.
func (r *TaskGeneratorImpl) getRetention() (time.Duration, error) {
	retention := defaultWorkflowRetention
	executionInfo := r.mutableState.GetExecutionInfo()
	namespaceEntry, err := r.namespaceRegistry.GetNamespaceByID(namespace.ID(executionInfo.NamespaceId))
	switch err.(type) {
	case nil:
		retention = namespaceEntry.Retention()
	case *serviceerror.NamespaceNotFound:
		// namespace is not accessible, use default value above
	default:
		return 0, err
	}
	return retention, nil
}

func (r *TaskGeneratorImpl) GenerateDirtySubStateMachineTasks(
	stateMachineRegistry *hsm.Registry,
) error {
	tree := r.mutableState.HSM()
	opLog, err := tree.OpLog()
	if err != nil {
		return err
	}

	for _, op := range opLog {
		switch transitionOp := op.(type) {
		case hsm.DeleteOperation:
			deleteStateMachineTimersByPath(r.mutableState.GetExecutionInfo(), transitionOp.Path())
		case hsm.TransitionOperation:
			node, err := tree.Child(transitionOp.Path())
			if err != nil {
				return err
			}
			for _, task := range transitionOp.Output.Tasks {
				// since this method is called after transition history is updated for the current transition,
				// we can safely call generateSubStateMachineTask which sets MutableStateVersionedTransition
				// to the last versioned transition in StateMachineRef
				if err := generateSubStateMachineTask(
					r.mutableState,
					stateMachineRegistry,
					node,
					transitionOp.Path(),
					transitionOp.Output.TransitionCount,
					task,
				); err != nil {
					return err
				}
			}
		}
	}

	AddNextStateMachineTimerTask(r.mutableState)

	return nil
}

// GenerateDeleteHistoryEventTask adds a task to delete all history events for a workflow execution.
// This method only adds the task to the mutable state object in memory; it does not write the task to the database.
// You must call shard.Context#AddTasks to notify the history engine of this task.
func (r *TaskGeneratorImpl) GenerateDeleteHistoryEventTask(closeTime time.Time) error {
	retention, err := r.getRetention()
	if err != nil {
		return err
	}
	closeVersion, err := r.mutableState.GetCloseVersion()
	if err != nil {
		return err
	}

	branchToken, err := r.mutableState.GetCurrentBranchToken()
	if err != nil {
		return err
	}

	retentionJitterDuration := backoff.FullJitter(r.config.RetentionTimerJitterDuration())
	deleteTime := closeTime.Add(retention).Add(retentionJitterDuration)
	r.mutableState.AddTasks(&tasks.DeleteHistoryEventTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		VisibilityTimestamp: deleteTime,
		Version:             closeVersion,
		BranchToken:         branchToken,
	})
	return nil
}

func (r *TaskGeneratorImpl) GenerateDeleteExecutionTask() (*tasks.DeleteExecutionTask, error) {
	return &tasks.DeleteExecutionTask{
		// TaskID, VisibilityTimestamp is set by shard
		WorkflowKey: r.mutableState.GetWorkflowKey(),
	}, nil
}

func (r *TaskGeneratorImpl) GenerateDelayedWorkflowTasks(
	startEvent *historypb.HistoryEvent,
) error {

	startVersion := startEvent.GetVersion()
	// start time may not be "now" if method called by refresher
	startTime := timestamp.TimeValue(startEvent.GetEventTime())
	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()

	workflowTaskBackoffDuration := timestamp.DurationValue(startAttr.GetFirstWorkflowTaskBackoff())
	executionTimestamp := startTime.Add(workflowTaskBackoffDuration)

	var workflowBackoffType enumsspb.WorkflowBackoffType
	switch startAttr.GetInitiator() {
	case enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY:
		workflowBackoffType = enumsspb.WORKFLOW_BACKOFF_TYPE_RETRY
	case enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE, enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW:
		workflowBackoffType = enumsspb.WORKFLOW_BACKOFF_TYPE_CRON
	default:
		workflowBackoffType = enumsspb.WORKFLOW_BACKOFF_TYPE_DELAY_START
	}

	r.mutableState.AddTasks(&tasks.WorkflowBackoffTimerTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		VisibilityTimestamp: executionTimestamp,
		WorkflowBackoffType: workflowBackoffType,
		Version:             startVersion,
	})

	return nil
}

func (r *TaskGeneratorImpl) GenerateRecordWorkflowStartedTasks(
	startEvent *historypb.HistoryEvent,
) error {

	startVersion := startEvent.GetVersion()

	r.mutableState.AddTasks(&tasks.StartExecutionVisibilityTask{
		// TaskID, VisibilityTimestamp is set by shard
		WorkflowKey: r.mutableState.GetWorkflowKey(),
		Version:     startVersion,
	})
	return nil
}

func (r *TaskGeneratorImpl) GenerateScheduleWorkflowTaskTasks(
	workflowTaskScheduledEventID int64,
) error {

	workflowTask := r.mutableState.GetWorkflowTaskByID(
		workflowTaskScheduledEventID,
	)
	if workflowTask == nil {
		return serviceerror.NewInternalf("it could be a bug, cannot get pending workflow task: %v", workflowTaskScheduledEventID)
	}
	if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		return serviceerror.NewInternalf("it could be a bug, GenerateScheduleSpeculativeWorkflowTaskTasks must be called for speculative workflow task: %v", workflowTaskScheduledEventID)
	}

	if r.mutableState.IsStickyTaskQueueSet() {
		scheduleToStartTimeout := timestamp.DurationValue(r.mutableState.GetExecutionInfo().StickyScheduleToStartTimeout)
		wttt := &tasks.WorkflowTaskTimeoutTask{
			// TaskID is set by shard
			WorkflowKey:         r.mutableState.GetWorkflowKey(),
			VisibilityTimestamp: workflowTask.ScheduledTime.Add(scheduleToStartTimeout),
			TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
			EventID:             workflowTask.ScheduledEventID,
			ScheduleAttempt:     workflowTask.Attempt,
			Version:             workflowTask.Version,
		}
		r.mutableState.AddTasks(wttt)
		r.mutableState.SetWorkflowTaskScheduleToStartTimeoutTask(wttt)
	}

	r.mutableState.AddTasks(&tasks.WorkflowTask{
		// TaskID, VisibilityTimestamp is set by shard
		WorkflowKey: r.mutableState.GetWorkflowKey(),
		// Store current task queue to the transfer task.
		// If current task queue becomes sticky in between when this transfer task is created and processed,
		// it can't be used at process time, because timeout timer was not created for it,
		// because it used to be non-sticky when this transfer task was created here.
		// In short, task queue that was "current" when transfer task was created must be used when task is processed.
		TaskQueue:        workflowTask.TaskQueue.GetName(),
		ScheduledEventID: workflowTask.ScheduledEventID,
		Version:          workflowTask.Version,
	})

	return nil
}

// GenerateScheduleSpeculativeWorkflowTaskTasks is different from GenerateScheduleWorkflowTaskTasks (above):
//  1. Always create ScheduleToStart timeout timer task (even for normal task queue).
//  2. Don't create transfer task to push WT to matching.
func (r *TaskGeneratorImpl) GenerateScheduleSpeculativeWorkflowTaskTasks(
	workflowTask *historyi.WorkflowTaskInfo,
) error {

	var scheduleToStartTimeout time.Duration
	if r.mutableState.IsStickyTaskQueueSet() {
		scheduleToStartTimeout = timestamp.DurationValue(r.mutableState.GetExecutionInfo().StickyScheduleToStartTimeout)
	} else {
		// Speculative WT has ScheduleToStart timeout even on normal task queue.
		// Normally WT should be added to matching right after being created
		// (i.e. from UpdateWorkflowExecution API handler), but if this "add" operation failed,
		// there is no good way to handle the error.
		// In this case WT will be timed out (as if it was on sticky task queue),
		// and new normal WT will be created.
		// Note: this timer will also fire if workflow received an update,
		// but there is no workers available. Speculative WT will time out, and normal WT will be created.
		scheduleToStartTimeout = tasks.SpeculativeWorkflowTaskScheduleToStartTimeout
	}

	isSpeculative := workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE
	wttt := &tasks.WorkflowTaskTimeoutTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		VisibilityTimestamp: workflowTask.ScheduledTime.Add(scheduleToStartTimeout),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             workflowTask.ScheduledEventID,
		ScheduleAttempt:     workflowTask.Attempt,
		Version:             workflowTask.Version,
		InMemory:            isSpeculative,
	}

	if isSpeculative {
		// If WT is still speculative, create task in in-memory task queue.
		return r.mutableState.SetSpeculativeWorkflowTaskTimeoutTask(wttt)
	}

	// This function can be called for speculative WT which just was converted to normal
	// (it will be of type Normal). In this case persisted timer task needs to be created.
	r.mutableState.AddTasks(wttt)
	r.mutableState.SetWorkflowTaskScheduleToStartTimeoutTask(wttt)
	return nil

	// Note: no transfer task is created for speculative WT or speculative WT
	// which became normal. API handler (i.e. UpdateWorkflowExecution) should
	// add WT to matching directly. If WT wasn't added it will be timed out by timers above.
}

func (r *TaskGeneratorImpl) GenerateStartWorkflowTaskTasks(
	workflowTaskScheduledEventID int64,
) error {
	workflowTask := r.mutableState.GetWorkflowTaskByID(
		workflowTaskScheduledEventID,
	)
	if workflowTask == nil {
		return serviceerror.NewInternalf("it could be a bug, cannot get pending workflow task: %v", workflowTaskScheduledEventID)
	}

	isSpeculative := workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE
	wttt := &tasks.WorkflowTaskTimeoutTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		VisibilityTimestamp: workflowTask.StartedTime.Add(workflowTask.WorkflowTaskTimeout),
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		EventID:             workflowTask.ScheduledEventID,
		ScheduleAttempt:     workflowTask.Attempt,
		Version:             workflowTask.Version,
		InMemory:            isSpeculative,
	}

	if isSpeculative {
		// If WT is speculative, create task in in-memory task queue.
		return r.mutableState.SetSpeculativeWorkflowTaskTimeoutTask(wttt)
	}
	r.mutableState.AddTasks(wttt)
	r.mutableState.SetWorkflowTaskStartToCloseTimeoutTask(wttt)

	return nil
}

func (r *TaskGeneratorImpl) GenerateActivityTasks(
	activityScheduledEventID int64,
) error {
	activityInfo, ok := r.mutableState.GetActivityInfo(activityScheduledEventID)
	if !ok {
		return serviceerror.NewInternalf("it could be a bug, cannot get pending activity: %v", activityScheduledEventID)
	}

	r.mutableState.AddTasks(&tasks.ActivityTask{
		// TaskID, VisibilityTimestamp is set by shard
		WorkflowKey:      r.mutableState.GetWorkflowKey(),
		TaskQueue:        activityInfo.TaskQueue,
		ScheduledEventID: activityInfo.ScheduledEventId,
		Version:          activityInfo.Version,
	})

	return nil
}

func (r *TaskGeneratorImpl) GenerateActivityRetryTasks(activityInfo *persistencespb.ActivityInfo) error {
	r.mutableState.AddTasks(&tasks.ActivityRetryTimerTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		Version:             activityInfo.GetVersion(),
		VisibilityTimestamp: activityInfo.GetScheduledTime().AsTime(),
		EventID:             activityInfo.GetScheduledEventId(),
		Attempt:             activityInfo.GetAttempt(),
		Stamp:               activityInfo.Stamp,
	})
	return nil
}

func (r *TaskGeneratorImpl) GenerateChildWorkflowTasks(
	childInitiatedEventId int64,
) error {

	childWorkflowInfo, ok := r.mutableState.GetChildExecutionInfo(childInitiatedEventId)
	if !ok {
		return serviceerror.NewInternalf("it could be a bug, cannot get pending child workflow: %v", childInitiatedEventId)
	}

	targetNamespaceID, err := r.getTargetNamespaceID(
		namespace.Name(childWorkflowInfo.GetNamespace()),
		namespace.ID(childWorkflowInfo.GetNamespaceId()),
	)
	if err != nil {
		return err
	}

	r.mutableState.AddTasks(&tasks.StartChildExecutionTask{
		// TaskID, VisibilityTimestamp is set by shard
		WorkflowKey:       r.mutableState.GetWorkflowKey(),
		TargetNamespaceID: targetNamespaceID.String(),
		TargetWorkflowID:  childWorkflowInfo.StartedWorkflowId,
		InitiatedEventID:  childWorkflowInfo.InitiatedEventId,
		Version:           childWorkflowInfo.Version,
	})

	return nil
}

// TODO: Take in scheduledEventID instead of event once
// TargetNamespaceID, TargetWorkflowID, TargetRunID & TargetChildWorkflowOnly
// are removed from CancelExecutionTask.
func (r *TaskGeneratorImpl) GenerateRequestCancelExternalTasks(
	event *historypb.HistoryEvent,
) error {

	attr := event.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes()
	scheduledEventID := event.GetEventId()
	version := event.GetVersion()
	targetWorkflowID := attr.GetWorkflowExecution().GetWorkflowId()
	targetRunID := attr.GetWorkflowExecution().GetRunId()
	targetChildOnly := attr.GetChildWorkflowOnly()

	_, ok := r.mutableState.GetRequestCancelInfo(scheduledEventID)
	if !ok {
		return serviceerror.NewInternalf("it could be a bug, cannot get pending request cancel external workflow: %v", scheduledEventID)
	}

	targetNamespaceID, err := r.getTargetNamespaceID(namespace.Name(attr.GetNamespace()), namespace.ID(attr.GetNamespaceId()))
	if err != nil {
		return err
	}

	r.mutableState.AddTasks(&tasks.CancelExecutionTask{
		// TaskID, VisibilityTimestamp is set by shard
		WorkflowKey:             r.mutableState.GetWorkflowKey(),
		TargetNamespaceID:       targetNamespaceID.String(),
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildOnly,
		InitiatedEventID:        scheduledEventID,
		Version:                 version,
	})

	return nil
}

// TODO: Take in scheduledEventID instead of event once
// TargetNamespaceID, TargetWorkflowID, TargetRunID & TargetChildWorkflowOnly
// are removed from SignalExecutionTask.
func (r *TaskGeneratorImpl) GenerateSignalExternalTasks(
	event *historypb.HistoryEvent,
) error {

	attr := event.GetSignalExternalWorkflowExecutionInitiatedEventAttributes()
	scheduledEventID := event.GetEventId()
	version := event.GetVersion()
	targetWorkflowID := attr.GetWorkflowExecution().GetWorkflowId()
	targetRunID := attr.GetWorkflowExecution().GetRunId()
	targetChildOnly := attr.GetChildWorkflowOnly()

	_, ok := r.mutableState.GetSignalInfo(scheduledEventID)
	if !ok {
		return serviceerror.NewInternalf("it could be a bug, cannot get pending signal external workflow: %v", scheduledEventID)
	}

	targetNamespaceID, err := r.getTargetNamespaceID(namespace.Name(attr.GetNamespace()), namespace.ID(attr.GetNamespaceId()))
	if err != nil {
		return err
	}

	r.mutableState.AddTasks(&tasks.SignalExecutionTask{
		// TaskID, VisibilityTimestamp is set by shard
		WorkflowKey:             r.mutableState.GetWorkflowKey(),
		TargetNamespaceID:       targetNamespaceID.String(),
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildOnly,
		InitiatedEventID:        scheduledEventID,
		Version:                 version,
	})

	return nil
}

func (r *TaskGeneratorImpl) GenerateUpsertVisibilityTask() error {
	r.mutableState.AddTasks(&tasks.UpsertExecutionVisibilityTask{
		// TaskID, VisibilityTimestamp is set by shard
		WorkflowKey: r.mutableState.GetWorkflowKey(),
	})
	return nil
}

func (r *TaskGeneratorImpl) GenerateWorkflowResetTasks() error {

	currentVersion := r.mutableState.GetCurrentVersion()

	r.mutableState.AddTasks(&tasks.ResetWorkflowTask{
		// TaskID, VisibilityTimestamp is set by shard
		WorkflowKey: r.mutableState.GetWorkflowKey(),
		Version:     currentVersion,
	})

	return nil
}

func (r *TaskGeneratorImpl) GenerateActivityTimerTasks() error {
	_, err := r.getTimerSequence().CreateNextActivityTimer()
	return err
}

func (r *TaskGeneratorImpl) GenerateUserTimerTasks() error {
	_, err := r.getTimerSequence().CreateNextUserTimer()
	return err
}

func (r *TaskGeneratorImpl) GenerateHistoryReplicationTasks(
	eventBatches [][]*historypb.HistoryEvent,
) ([]tasks.Task, error) {
	if len(eventBatches) == 0 {
		return nil, nil
	}
	for _, events := range eventBatches {
		if len(events) == 0 {
			return nil, serviceerror.NewInternal("TaskGeneratorImpl encountered empty event batch")
		}
	}

	firstBatch := eventBatches[0]
	firstEvent := firstBatch[0]
	lastBatch := eventBatches[len(eventBatches)-1]
	lastEvent := lastBatch[len(lastBatch)-1]
	version := firstEvent.GetVersion()
	for _, events := range eventBatches {
		if events[0].GetVersion() != version || events[len(events)-1].GetVersion() != version {
			return nil, serviceerror.NewInternal("TaskGeneratorImpl encountered contradicting versions")
		}
	}

	return []tasks.Task{
		&tasks.HistoryReplicationTask{
			// TaskID, VisibilityTimestamp is set by shard
			WorkflowKey:  r.mutableState.GetWorkflowKey(),
			FirstEventID: firstEvent.GetEventId(),
			NextEventID:  lastEvent.GetEventId() + 1,
			Version:      version,
		},
	}, nil
}

func (r *TaskGeneratorImpl) GenerateMigrationTasks(targetClusters []string) ([]tasks.Task, int64, error) {
	executionInfo := r.mutableState.GetExecutionInfo()
	versionHistory, err := versionhistory.GetCurrentVersionHistory(executionInfo.GetVersionHistories())
	if err != nil {
		return nil, 0, err
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(versionHistory)
	if err != nil {
		return nil, 0, err
	}
	now := time.Now().UTC()
	workflowKey := r.mutableState.GetWorkflowKey()

	if r.mutableState.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		syncWorkflowStateTask := []tasks.Task{&tasks.SyncWorkflowStateTask{
			// TaskID, VisibilityTimestamp is set by shard
			WorkflowKey:    workflowKey,
			Version:        lastItem.GetVersion(),
			Priority:       enumsspb.TASK_PRIORITY_LOW,
			TargetClusters: targetClusters,
		}}
		if r.mutableState.IsTransitionHistoryEnabled() &&
			// even though current cluster may enabled state transition, but transition history can be cleared
			// by processing a replication task from a cluster that has state transition disabled
			len(executionInfo.TransitionHistory) > 0 {

			transitionHistory := executionInfo.TransitionHistory
			return []tasks.Task{&tasks.SyncVersionedTransitionTask{
				WorkflowKey:         workflowKey,
				Priority:            enumsspb.TASK_PRIORITY_LOW,
				VersionedTransition: transitionhistory.LastVersionedTransition(transitionHistory),
				FirstEventID:        executionInfo.LastFirstEventId,
				FirstEventVersion:   lastItem.Version,
				NextEventID:         lastItem.GetEventId() + 1,
				TaskEquivalents:     syncWorkflowStateTask,
				TargetClusters:      targetClusters,
			}}, 1, nil
		}
		return syncWorkflowStateTask, 1, nil
	}

	replicationTasks := make([]tasks.Task, 0, len(r.mutableState.GetPendingActivityInfos())+1)
	replicationTasks = append(replicationTasks, &tasks.HistoryReplicationTask{
		// TaskID, VisibilityTimestamp is set by shard
		WorkflowKey:    workflowKey,
		FirstEventID:   executionInfo.LastFirstEventId,
		NextEventID:    lastItem.GetEventId() + 1,
		Version:        lastItem.GetVersion(),
		TargetClusters: targetClusters,
	})
	activityIDs := make(map[int64]struct{}, len(r.mutableState.GetPendingActivityInfos()))
	for activityID := range r.mutableState.GetPendingActivityInfos() {
		activityIDs[activityID] = struct{}{}
	}
	replicationTasks = append(replicationTasks, convertSyncActivityInfos(
		now,
		workflowKey,
		r.mutableState.GetPendingActivityInfos(),
		activityIDs,
		targetClusters,
	)...)
	if r.config.EnableNexus() {
		replicationTasks = append(replicationTasks, &tasks.SyncHSMTask{
			WorkflowKey: workflowKey,
			// TaskID and VisibilityTimestamp are set by shard
			TargetClusters: targetClusters,
		})
	}

	if r.mutableState.IsTransitionHistoryEnabled() &&
		// even though current cluster may enabled state transition, but transition history can be cleared
		// by processing a replication task from a cluster that has state transition disabled
		len(executionInfo.TransitionHistory) > 0 {

		transitionHistory := executionInfo.TransitionHistory
		return []tasks.Task{&tasks.SyncVersionedTransitionTask{
			WorkflowKey:         workflowKey,
			Priority:            enumsspb.TASK_PRIORITY_LOW,
			VersionedTransition: transitionhistory.LastVersionedTransition(transitionHistory),
			FirstEventID:        executionInfo.LastFirstEventId,
			FirstEventVersion:   lastItem.GetVersion(),
			NextEventID:         lastItem.GetEventId() + 1,
			TaskEquivalents:     replicationTasks,
			TargetClusters:      targetClusters,
		}}, 1, nil
	}
	return replicationTasks, executionInfo.StateTransitionCount, nil
}

func (r *TaskGeneratorImpl) getTimerSequence() TimerSequence {
	return NewTimerSequence(r.mutableState)
}

func (r *TaskGeneratorImpl) getTargetNamespaceID(
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
) (namespace.ID, error) {
	if !targetNamespaceID.IsEmpty() {
		return targetNamespaceID, nil
	}

	// TODO: Remove targetNamespace after NamespaceId is back filled.
	// Backward compatibility: old events/mutable state doesn't have targetNamespaceID.
	if !targetNamespace.IsEmpty() {
		targetNamespaceEntry, err := r.namespaceRegistry.GetNamespace(targetNamespace)
		if err != nil {
			return "", err
		}
		return targetNamespaceEntry.ID(), nil
	}

	return namespace.ID(r.mutableState.GetExecutionInfo().NamespaceId), nil
}

// archivalEnabled returns true if archival is enabled for either history or visibility.
// For both history and visibility, we check that archival is enabled for both the cluster and the namespace.
func (r *TaskGeneratorImpl) archivalEnabled() bool {
	namespaceEntry := r.mutableState.GetNamespaceEntry()
	return r.archivalMetadata.GetHistoryConfig().ClusterConfiguredForArchival() &&
		namespaceEntry.HistoryArchivalState().State == enumspb.ARCHIVAL_STATE_ENABLED ||
		r.archivalMetadata.GetVisibilityConfig().ClusterConfiguredForArchival() &&
			namespaceEntry.VisibilityArchivalState().State == enumspb.ARCHIVAL_STATE_ENABLED
}

func generateSubStateMachineTask(
	mutableState historyi.MutableState,
	stateMachineRegistry *hsm.Registry,
	node *hsm.Node,
	subStateMachinePath []hsm.Key,
	transitionCount int64,
	task hsm.Task,
) error {
	ser, ok := stateMachineRegistry.TaskSerializer(task.Type())
	if !ok {
		return serviceerror.NewInternalf("no task serializer for %v", task.Type())
	}
	data, err := ser.Serialize(task)
	if err != nil {
		return err
	}
	ppath := make([]*persistencespb.StateMachineKey, len(subStateMachinePath))
	for i, k := range subStateMachinePath {
		ppath[i] = &persistencespb.StateMachineKey{
			Type: k.Type,
			Id:   k.ID,
		}
	}
	machineLastUpdateVersionedTransition := node.InternalRepr().GetLastUpdateVersionedTransition()

	currentVersionedTransition := mutableState.CurrentVersionedTransition()
	ref := &persistencespb.StateMachineRef{
		Path:                                 ppath,
		MutableStateVersionedTransition:      currentVersionedTransition,
		MachineInitialVersionedTransition:    node.InternalRepr().GetInitialVersionedTransition(),
		MachineLastUpdateVersionedTransition: machineLastUpdateVersionedTransition,
		MachineTransitionCount:               transitionCount,
	}

	// Task is invalid at generation time.
	// This may happen during replication when multiple event batches are applied in a single transaction.
	if err := task.Validate(ref, node); err != nil {
		return nil
	}

	taskInfo := &persistencespb.StateMachineTaskInfo{
		Ref:  ref,
		Type: task.Type(),
		Data: data,
	}
	// NOTE: at the moment deadline is mutually exclusive with destination.
	// This will change when we add the outbound timer queue.
	if task.Deadline() != hsm.Immediate {
		if task.Destination() != "" {
			// TODO: support outbound timer tasks.
			return fmt.Errorf("task cannot have both a deadline and destination due to missing outbound timer queue implementation")
		}
		TrackStateMachineTimer(mutableState, task.Deadline(), taskInfo)
	} else if task.Destination() != "" {
		mutableState.AddTasks(&tasks.StateMachineOutboundTask{
			StateMachineTask: tasks.StateMachineTask{
				WorkflowKey: mutableState.GetWorkflowKey(),
				Info:        taskInfo,
			},
			Destination: task.Destination(),
		})
	} else {
		// TODO: support "transfer" tasks - immediate without destination.
		return fmt.Errorf("task has no deadline or destination")
	}

	return nil
}

func deleteStateMachineTimersByPath(execInfo *persistencespb.WorkflowExecutionInfo, path []hsm.Key) {
	trimmedTimers := make([]*persistencespb.StateMachineTimerGroup, 0, len(execInfo.StateMachineTimers))

	for _, group := range execInfo.StateMachineTimers {
		trimmedInfos := make([]*persistencespb.StateMachineTaskInfo, 0, len(group.Infos))
		for _, info := range group.GetInfos() {
			if !isPathAffectedByDelete(path, info.GetRef().GetPath()) {
				trimmedInfos = append(trimmedInfos, info)
			}
		}
		if len(trimmedInfos) > 0 {
			trimmedTimers = append(trimmedTimers, &persistencespb.StateMachineTimerGroup{
				Infos:     trimmedInfos,
				Deadline:  group.Deadline,
				Scheduled: group.Scheduled,
			})
		}
	}
	execInfo.StateMachineTimers = trimmedTimers
}

func isPathAffectedByDelete(deletePath []hsm.Key, timerPath []*persistencespb.StateMachineKey) bool {
	if len(deletePath) > len(timerPath) {
		return false
	}
	for i := range deletePath {
		if deletePath[i].Type != timerPath[i].GetType() || deletePath[i].ID != timerPath[i].GetId() {
			return false
		}
	}
	return true
}
