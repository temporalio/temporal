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
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tasks"
)

type (
	TaskGenerator interface {
		GenerateWorkflowStartTasks(
			startEvent *historypb.HistoryEvent,
		) error
		GenerateWorkflowCloseTasks(
			// TODO: remove closeEvent parameter
			// when deprecating the backward compatible logic
			// for getting close time from close event.
			closeEvent *historypb.HistoryEvent,
			deleteAfterClose bool,
		) error
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
		GenerateStartWorkflowTaskTasks(
			workflowTaskScheduledEventID int64,
		) error
		GenerateActivityTasks(
			event *historypb.HistoryEvent,
		) error
		GenerateActivityRetryTasks(
			activityScheduledEventID int64,
		) error
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
			branchToken []byte,
			events []*historypb.HistoryEvent,
		) error
		GenerateMigrationTasks() (tasks.Task, error)
	}

	TaskGeneratorImpl struct {
		namespaceRegistry namespace.Registry
		mutableState      MutableState
		config            *configs.Config
	}
)

const defaultWorkflowRetention = 1 * 24 * time.Hour

var _ TaskGenerator = (*TaskGeneratorImpl)(nil)

func NewTaskGenerator(
	namespaceRegistry namespace.Registry,
	mutableState MutableState,
	config *configs.Config,
) *TaskGeneratorImpl {
	return &TaskGeneratorImpl{
		namespaceRegistry: namespaceRegistry,
		mutableState:      mutableState,
		config:            config,
	}
}

func (r *TaskGeneratorImpl) GenerateWorkflowStartTasks(
	startEvent *historypb.HistoryEvent,
) error {

	startVersion := startEvent.GetVersion()
	workflowRunExpirationTime := timestamp.TimeValue(r.mutableState.GetExecutionInfo().WorkflowRunExpirationTime)
	if workflowRunExpirationTime.IsZero() {
		// this mean infinite timeout
		return nil
	}

	r.mutableState.AddTasks(&tasks.WorkflowTimeoutTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		VisibilityTimestamp: workflowRunExpirationTime,
		Version:             startVersion,
	})

	return nil
}

func (r *TaskGeneratorImpl) GenerateWorkflowCloseTasks(
	closeEvent *historypb.HistoryEvent,
	deleteAfterClose bool,
) error {

	currentVersion := r.mutableState.GetCurrentVersion()
	executionInfo := r.mutableState.GetExecutionInfo()

	retention := defaultWorkflowRetention
	namespaceEntry, err := r.namespaceRegistry.GetNamespaceByID(namespace.ID(executionInfo.NamespaceId))
	switch err.(type) {
	case nil:
		retention = namespaceEntry.Retention()
	case *serviceerror.NamespaceNotFound:
		// namespace is not accessible, use default value above
	default:
		return err
	}
	branchToken, err := r.mutableState.GetCurrentBranchToken()
	if err != nil {
		return err
	}

	closeTasks := []tasks.Task{
		&tasks.CloseExecutionTask{
			// TaskID, Visiblitytimestamp is set by shard
			WorkflowKey:      r.mutableState.GetWorkflowKey(),
			Version:          currentVersion,
			DeleteAfterClose: deleteAfterClose,
		},
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
		if r.config.DurableArchivalEnabled() {
			closeTasks = append(closeTasks, &tasks.ArchiveExecutionTask{
				// TaskID and VisibilityTimestamp are set by the shard
				WorkflowKey: r.mutableState.GetWorkflowKey(),
				Version:     currentVersion,
			})
		} else {
			closeTime := timestamp.TimeValue(closeEvent.GetEventTime())
			retentionJitterDuration := backoff.JitDuration(r.config.RetentionTimerJitterDuration(), 1) / 2
			closeTasks = append(closeTasks, &tasks.DeleteHistoryEventTask{
				// TaskID is set by shard
				WorkflowKey:         r.mutableState.GetWorkflowKey(),
				VisibilityTimestamp: closeTime.Add(retention).Add(retentionJitterDuration),
				Version:             currentVersion,
				BranchToken:         branchToken,
			})
		}
	}

	r.mutableState.AddTasks(closeTasks...)
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
		return serviceerror.NewInternal(fmt.Sprintf("unknown initiator: %v", startAttr.GetInitiator()))
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

	workflowTask, ok := r.mutableState.GetWorkflowTaskInfo(
		workflowTaskScheduledEventID,
	)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending workflow task: %v", workflowTaskScheduledEventID))
	}

	r.mutableState.AddTasks(&tasks.WorkflowTask{
		// TaskID, VisibilityTimestamp is set by shard
		WorkflowKey:      r.mutableState.GetWorkflowKey(),
		TaskQueue:        workflowTask.TaskQueue.GetName(),
		ScheduledEventID: workflowTask.ScheduledEventID,
		Version:          workflowTask.Version,
	})

	if r.mutableState.IsStickyTaskQueueEnabled() {
		scheduledTime := timestamp.TimeValue(workflowTask.ScheduledTime)
		scheduleToStartTimeout := timestamp.DurationValue(r.mutableState.GetExecutionInfo().StickyScheduleToStartTimeout)

		r.mutableState.AddTasks(&tasks.WorkflowTaskTimeoutTask{
			// TaskID is set by shard
			WorkflowKey:         r.mutableState.GetWorkflowKey(),
			VisibilityTimestamp: scheduledTime.Add(scheduleToStartTimeout),
			TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
			EventID:             workflowTask.ScheduledEventID,
			ScheduleAttempt:     workflowTask.Attempt,
			Version:             workflowTask.Version,
		})
	}

	return nil
}

func (r *TaskGeneratorImpl) GenerateStartWorkflowTaskTasks(
	workflowTaskScheduledEventID int64,
) error {

	workflowTask, ok := r.mutableState.GetWorkflowTaskInfo(
		workflowTaskScheduledEventID,
	)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending workflowTaskInfo: %v", workflowTaskScheduledEventID))
	}

	startedTime := timestamp.TimeValue(workflowTask.StartedTime)
	workflowTaskTimeout := timestamp.DurationValue(workflowTask.WorkflowTaskTimeout)

	r.mutableState.AddTasks(&tasks.WorkflowTaskTimeoutTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		VisibilityTimestamp: startedTime.Add(workflowTaskTimeout),
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		EventID:             workflowTask.ScheduledEventID,
		ScheduleAttempt:     workflowTask.Attempt,
		Version:             workflowTask.Version,
	})

	return nil
}

func (r *TaskGeneratorImpl) GenerateActivityTasks(
	event *historypb.HistoryEvent,
) error {

	activityScheduledEventID := event.GetEventId()
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

func (r *TaskGeneratorImpl) GenerateActivityRetryTasks(
	activityScheduledEventID int64,
) error {

	ai, ok := r.mutableState.GetActivityInfo(activityScheduledEventID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending activity: %v", activityScheduledEventID))
	}

	r.mutableState.AddTasks(&tasks.ActivityRetryTimerTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		Version:             ai.Version,
		VisibilityTimestamp: *ai.ScheduledTime,
		EventID:             ai.ScheduledEventId,
		Attempt:             ai.Attempt,
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
	currentVersion := r.mutableState.GetCurrentVersion()

	r.mutableState.AddTasks(&tasks.UpsertExecutionVisibilityTask{
		// TaskID, VisibilityTimestamp is set by shard
		WorkflowKey: r.mutableState.GetWorkflowKey(),
		Version:     currentVersion, // task processing does not check this version
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
	branchToken []byte,
	events []*historypb.HistoryEvent,
) error {
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

func (r *TaskGeneratorImpl) GenerateMigrationTasks() (tasks.Task, error) {
	executionInfo := r.mutableState.GetExecutionInfo()
	versionHistory, err := versionhistory.GetCurrentVersionHistory(executionInfo.GetVersionHistories())
	if err != nil {
		return nil, err
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(versionHistory)
	if err != nil {
		return nil, err
	}

	if r.mutableState.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		return &tasks.SyncWorkflowStateTask{
			// TaskID, VisibilityTimestamp is set by shard
			WorkflowKey: r.mutableState.GetWorkflowKey(),
			Version:     lastItem.GetVersion(),
		}, nil
	} else {
		return &tasks.HistoryReplicationTask{
			// TaskID, VisibilityTimestamp is set by shard
			WorkflowKey:  r.mutableState.GetWorkflowKey(),
			FirstEventID: executionInfo.LastFirstEventId,
			NextEventID:  lastItem.GetEventId() + 1,
			Version:      lastItem.GetVersion(),
		}, nil
	}
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
