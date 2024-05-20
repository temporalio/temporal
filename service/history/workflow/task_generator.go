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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination task_generator_mock.go

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
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/hsm"
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
			workflowTask *WorkflowTaskInfo,
		) error
		GenerateStartWorkflowTaskTasks(
			workflowTaskScheduledEventID int64,
		) error
		GenerateActivityTasks(
			activityScheduledEventID int64,
		) error
		GenerateActivityRetryTasks(eventID int64, visibilityTimestamp time.Time, nextAttempt int32) error
		GenerateChildWorkflowTasks(
			event *historypb.HistoryEvent,
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
			events []*historypb.HistoryEvent,
		) error
		GenerateMigrationTasks() ([]tasks.Task, int64, error)

		// Generate tasks for any updated state machines on mutable state.
		// Looks up machine definition in the provided registry.
		GenerateDirtySubStateMachineTasks(stateMachineRegistry *hsm.Registry) error
	}

	TaskGeneratorImpl struct {
		namespaceRegistry namespace.Registry
		mutableState      MutableState
		config            *configs.Config
		archivalMetadata  archiver.ArchivalMetadata
	}
)

const defaultWorkflowRetention = 1 * 24 * time.Hour

var _ TaskGenerator = (*TaskGeneratorImpl)(nil)

func NewTaskGenerator(
	namespaceRegistry namespace.Registry,
	mutableState MutableState,
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
	currentVersion := r.mutableState.GetCurrentVersion()

	closeExecutionTask := &tasks.CloseExecutionTask{
		// TaskID, Visiblitytimestamp is set by shard
		WorkflowKey:      r.mutableState.GetWorkflowKey(),
		Version:          currentVersion,
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
				Version:     currentVersion,
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
				Version:             currentVersion,
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
	// Early return here to avoid accessing the transition history. It may be disabled via dynamic config.
	outputs := tree.Outputs()
	if len(outputs) == 0 {
		return nil
	}
	transitionHistory := r.mutableState.GetExecutionInfo().TransitionHistory
	versionedTransition := transitionHistory[len(transitionHistory)-1]
	for _, pao := range outputs {
		node, err := tree.Child(pao.Path)
		if err != nil {
			return err
		}
		for _, output := range pao.Outputs {
			for _, task := range output.Tasks {
				ser, ok := stateMachineRegistry.TaskSerializer(task.Type().ID)
				if !ok {
					return serviceerror.NewInternal(fmt.Sprintf("no task serializer for %v", task.Type()))
				}
				data, err := ser.Serialize(task)
				if err != nil {
					return err
				}
				ppath := make([]*persistencespb.StateMachineKey, len(pao.Path))
				for i, k := range pao.Path {
					ppath[i] = &persistencespb.StateMachineKey{
						Type: k.Type,
						Id:   k.ID,
					}
				}
				// Only set transition count if a task is non-concurrent.
				transitionCount := int64(0)
				if !task.Concurrent() {
					transitionCount = node.TransitionCount()
				}
				smt := tasks.StateMachineTask{
					WorkflowKey: r.mutableState.GetWorkflowKey(),
					Info: &persistencespb.StateMachineTaskInfo{
						Ref: &persistencespb.StateMachineRef{
							Path:                                 ppath,
							MutableStateNamespaceFailoverVersion: versionedTransition.NamespaceFailoverVersion,
							MutableStateTransitionCount:          versionedTransition.MaxTransitionCount,
							MachineTransitionCount:               transitionCount,
						},
						Type: task.Type().ID,
						Data: data,
					},
				}
				switch kind := task.Kind().(type) {
				case hsm.TaskKindOutbound:
					r.mutableState.AddTasks(&tasks.StateMachineOutboundTask{
						StateMachineTask: smt,
						Destination:      kind.Destination,
					})
				case hsm.TaskKindTimer:
					smt.VisibilityTimestamp = kind.Deadline
					r.mutableState.AddTasks(&tasks.StateMachineTimerTask{
						StateMachineTask: smt,
					})
				}
			}
		}
	}
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
	currentVersion := r.mutableState.GetCurrentVersion()
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
		Version:             currentVersion,
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
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending workflow task: %v", workflowTaskScheduledEventID))
	}
	if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, GenerateScheduleSpeculativeWorkflowTaskTasks must be called for speculative workflow task: %v", workflowTaskScheduledEventID))
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
	workflowTask *WorkflowTaskInfo,
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

	wttt := &tasks.WorkflowTaskTimeoutTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		VisibilityTimestamp: workflowTask.ScheduledTime.Add(scheduleToStartTimeout),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             workflowTask.ScheduledEventID,
		ScheduleAttempt:     workflowTask.Attempt,
		Version:             workflowTask.Version,
	}

	if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		// It WT is still speculative, create task in in-memory task queue.
		return r.mutableState.SetSpeculativeWorkflowTaskTimeoutTask(wttt)
	}

	// This function can be called for speculative WT which just was converted to normal
	// (it will be of type Normal). In this case persisted timer task needs to be created.
	r.mutableState.AddTasks(wttt)
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
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending workflow task: %v", workflowTaskScheduledEventID))
	}

	wttt := &tasks.WorkflowTaskTimeoutTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		VisibilityTimestamp: workflowTask.StartedTime.Add(workflowTask.WorkflowTaskTimeout),
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		EventID:             workflowTask.ScheduledEventID,
		ScheduleAttempt:     workflowTask.Attempt,
		Version:             workflowTask.Version,
	}
	if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		return r.mutableState.SetSpeculativeWorkflowTaskTimeoutTask(wttt)
	}

	r.mutableState.AddTasks(wttt)

	return nil
}

func (r *TaskGeneratorImpl) GenerateActivityTasks(
	activityScheduledEventID int64,
) error {
	activityInfo, ok := r.mutableState.GetActivityInfo(activityScheduledEventID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending activity: %v", activityScheduledEventID))
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

func (r *TaskGeneratorImpl) GenerateActivityRetryTasks(eventID int64, visibilityTimestamp time.Time, nextAttempt int32) error {
	r.mutableState.AddTasks(&tasks.ActivityRetryTimerTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		Version:             r.mutableState.GetCurrentVersion(),
		VisibilityTimestamp: visibilityTimestamp,
		EventID:             eventID,
		Attempt:             nextAttempt,
	})
	return nil
}

func (r *TaskGeneratorImpl) GenerateChildWorkflowTasks(
	event *historypb.HistoryEvent,
) error {

	attr := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
	childWorkflowScheduledEventID := event.GetEventId()

	childWorkflowInfo, ok := r.mutableState.GetChildExecutionInfo(childWorkflowScheduledEventID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending child workflow: %v", childWorkflowScheduledEventID))
	}

	targetNamespaceID, err := r.getTargetNamespaceID(namespace.Name(attr.GetNamespace()), namespace.ID(attr.GetNamespaceId()))
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
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending request cancel external workflow: %v", scheduledEventID))
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
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending signal external workflow: %v", scheduledEventID))
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
	events []*historypb.HistoryEvent,
) error {
	if len(events) == 0 {
		return nil
	}

	firstEvent := events[0]
	lastEvent := events[len(events)-1]
	if firstEvent.GetVersion() != lastEvent.GetVersion() {
		return serviceerror.NewInternal("TaskGeneratorImpl encountered contradicting versions")
	}
	version := firstEvent.GetVersion()

	r.mutableState.AddTasks(&tasks.HistoryReplicationTask{
		// TaskID, VisibilityTimestamp is set by shard
		WorkflowKey:  r.mutableState.GetWorkflowKey(),
		FirstEventID: firstEvent.GetEventId(),
		NextEventID:  lastEvent.GetEventId() + 1,
		Version:      version,
	})
	return nil
}

func (r *TaskGeneratorImpl) GenerateMigrationTasks() ([]tasks.Task, int64, error) {
	executionInfo := r.mutableState.GetExecutionInfo()
	versionHistory, err := versionhistory.GetCurrentVersionHistory(executionInfo.GetVersionHistories())
	if err != nil {
		return nil, 0, err
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(versionHistory)
	if err != nil {
		return nil, 0, err
	}

	workflowKey := r.mutableState.GetWorkflowKey()

	if r.mutableState.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		return []tasks.Task{&tasks.SyncWorkflowStateTask{
			// TaskID, VisibilityTimestamp is set by shard
			WorkflowKey: workflowKey,
			Version:     lastItem.GetVersion(),
			Priority:    enumsspb.TASK_PRIORITY_LOW,
		}}, 1, nil
	}

	now := time.Now().UTC()
	replicationTasks := make([]tasks.Task, 0, len(r.mutableState.GetPendingActivityInfos())+1)
	replicationTasks = append(replicationTasks, &tasks.HistoryReplicationTask{
		// TaskID, VisibilityTimestamp is set by shard
		WorkflowKey:  workflowKey,
		FirstEventID: executionInfo.LastFirstEventId,
		NextEventID:  lastItem.GetEventId() + 1,
		Version:      lastItem.GetVersion(),
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
	)...)
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

	// TODO (alex): Remove targetNamespace after NamespaceId is back filled. Backward compatibility: old events doesn't have targetNamespaceID.
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
