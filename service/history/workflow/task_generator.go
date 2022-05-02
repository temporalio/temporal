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
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/tasks"
)

type (
	TaskGenerator interface {
		GenerateWorkflowStartTasks(
			now time.Time,
			startEvent *historypb.HistoryEvent,
		) error
		GenerateWorkflowCloseTasks(
			now time.Time,
		) error
		GenerateDeleteExecutionTask(
			now time.Time,
		) (*tasks.DeleteExecutionTask, error)
		GenerateRecordWorkflowStartedTasks(
			now time.Time,
			startEvent *historypb.HistoryEvent,
		) error
		GenerateDelayedWorkflowTasks(
			now time.Time,
			startEvent *historypb.HistoryEvent,
		) error
		GenerateScheduleWorkflowTaskTasks(
			now time.Time,
			workflowTaskScheduleID int64,
		) error
		GenerateStartWorkflowTaskTasks(
			now time.Time,
			workflowTaskScheduleID int64,
		) error
		GenerateActivityTasks(
			now time.Time,
			event *historypb.HistoryEvent,
		) error
		GenerateActivityRetryTasks(
			activityScheduleID int64,
		) error
		GenerateChildWorkflowTasks(
			now time.Time,
			event *historypb.HistoryEvent,
		) error
		GenerateRequestCancelExternalTasks(
			now time.Time,
			event *historypb.HistoryEvent,
		) error
		GenerateSignalExternalTasks(
			now time.Time,
			event *historypb.HistoryEvent,
		) error
		GenerateWorkflowSearchAttrTasks(
			now time.Time,
		) error
		GenerateWorkflowResetTasks(
			now time.Time,
		) error

		// these 2 APIs should only be called when mutable state transaction is being closed

		GenerateActivityTimerTasks(
			now time.Time,
		) error
		GenerateUserTimerTasks(
			now time.Time,
		) error

		// replication tasks

		GenerateHistoryReplicationTasks(
			now time.Time,
			branchToken []byte,
			events []*historypb.HistoryEvent,
		) error
		GenerateLastHistoryReplicationTasks(
			now time.Time,
		) (tasks.Task, error)
	}

	TaskGeneratorImpl struct {
		namespaceRegistry namespace.Registry
		mutableState      MutableState
	}
)

const defaultWorkflowRetention = 1 * 24 * time.Hour

var _ TaskGenerator = (*TaskGeneratorImpl)(nil)

func NewTaskGenerator(
	namespaceRegistry namespace.Registry,
	mutableState MutableState,
) *TaskGeneratorImpl {
	return &TaskGeneratorImpl{
		namespaceRegistry: namespaceRegistry,
		mutableState:      mutableState,
	}
}

func (r *TaskGeneratorImpl) GenerateWorkflowStartTasks(
	_ time.Time,
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
	now time.Time,
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
	r.mutableState.AddTasks(
		&tasks.CloseExecutionTask{
			// TaskID is set by shard
			WorkflowKey:         r.mutableState.GetWorkflowKey(),
			VisibilityTimestamp: now,
			Version:             currentVersion,
		},
		&tasks.CloseExecutionVisibilityTask{
			// TaskID is set by shard
			WorkflowKey:         r.mutableState.GetWorkflowKey(),
			VisibilityTimestamp: now,
			Version:             currentVersion,
		},
		&tasks.DeleteHistoryEventTask{
			// TaskID is set by shard
			WorkflowKey:         r.mutableState.GetWorkflowKey(),
			VisibilityTimestamp: now.Add(retention),
			Version:             currentVersion,
			BranchToken:         branchToken,
		},
	)

	return nil
}

func (r *TaskGeneratorImpl) GenerateDeleteExecutionTask(
	now time.Time,
) (*tasks.DeleteExecutionTask, error) {

	currentVersion := r.mutableState.GetCurrentVersion()

	return &tasks.DeleteExecutionTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		VisibilityTimestamp: now,
		Version:             currentVersion,
	}, nil
}

func (r *TaskGeneratorImpl) GenerateDelayedWorkflowTasks(
	now time.Time,
	startEvent *historypb.HistoryEvent,
) error {

	startVersion := startEvent.GetVersion()

	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()
	workflowTaskBackoffDuration := timestamp.DurationValue(startAttr.GetFirstWorkflowTaskBackoff())
	executionTimestamp := now.Add(workflowTaskBackoffDuration)

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
	now time.Time,
	startEvent *historypb.HistoryEvent,
) error {

	startVersion := startEvent.GetVersion()

	r.mutableState.AddTasks(&tasks.StartExecutionVisibilityTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		VisibilityTimestamp: now,
		Version:             startVersion,
	})
	return nil
}

func (r *TaskGeneratorImpl) GenerateScheduleWorkflowTaskTasks(
	now time.Time,
	workflowTaskScheduleID int64,
) error {

	workflowTask, ok := r.mutableState.GetWorkflowTaskInfo(
		workflowTaskScheduleID,
	)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending workflow task: %v", workflowTaskScheduleID))
	}

	r.mutableState.AddTasks(&tasks.WorkflowTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		VisibilityTimestamp: now,
		TaskQueue:           workflowTask.TaskQueue.GetName(),
		ScheduleID:          workflowTask.ScheduleID,
		Version:             workflowTask.Version,
	})

	if r.mutableState.IsStickyTaskQueueEnabled() {
		scheduledTime := timestamp.TimeValue(workflowTask.ScheduledTime)
		scheduleToStartTimeout := timestamp.DurationValue(r.mutableState.GetExecutionInfo().StickyScheduleToStartTimeout)

		r.mutableState.AddTasks(&tasks.WorkflowTaskTimeoutTask{
			// TaskID is set by shard
			WorkflowKey:         r.mutableState.GetWorkflowKey(),
			VisibilityTimestamp: scheduledTime.Add(scheduleToStartTimeout),
			TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
			EventID:             workflowTask.ScheduleID,
			ScheduleAttempt:     workflowTask.Attempt,
			Version:             workflowTask.Version,
		})
	}

	return nil
}

func (r *TaskGeneratorImpl) GenerateStartWorkflowTaskTasks(
	_ time.Time,
	workflowTaskScheduleID int64,
) error {

	workflowTask, ok := r.mutableState.GetWorkflowTaskInfo(
		workflowTaskScheduleID,
	)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending workflowTaskInfo: %v", workflowTaskScheduleID))
	}

	startedTime := timestamp.TimeValue(workflowTask.StartedTime)
	workflowTaskTimeout := timestamp.DurationValue(workflowTask.WorkflowTaskTimeout)

	r.mutableState.AddTasks(&tasks.WorkflowTaskTimeoutTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		VisibilityTimestamp: startedTime.Add(workflowTaskTimeout),
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		EventID:             workflowTask.ScheduleID,
		ScheduleAttempt:     workflowTask.Attempt,
		Version:             workflowTask.Version,
	})

	return nil
}

func (r *TaskGeneratorImpl) GenerateActivityTasks(
	now time.Time,
	event *historypb.HistoryEvent,
) error {

	activityScheduleID := event.GetEventId()
	activityInfo, ok := r.mutableState.GetActivityInfo(activityScheduleID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending activity: %v", activityScheduleID))
	}

	r.mutableState.AddTasks(&tasks.ActivityTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		VisibilityTimestamp: now,
		TaskQueue:           activityInfo.TaskQueue,
		ScheduleID:          activityInfo.ScheduleId,
		Version:             activityInfo.Version,
	})

	return nil
}

func (r *TaskGeneratorImpl) GenerateActivityRetryTasks(
	activityScheduleID int64,
) error {

	ai, ok := r.mutableState.GetActivityInfo(activityScheduleID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending activity: %v", activityScheduleID))
	}

	r.mutableState.AddTasks(&tasks.ActivityRetryTimerTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		Version:             ai.Version,
		VisibilityTimestamp: *ai.ScheduledTime,
		EventID:             ai.ScheduleId,
		Attempt:             ai.Attempt,
	})
	return nil
}

func (r *TaskGeneratorImpl) GenerateChildWorkflowTasks(
	now time.Time,
	event *historypb.HistoryEvent,
) error {

	attr := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
	childWorkflowScheduleID := event.GetEventId()
	childWorkflowTargetNamespace := namespace.Name(attr.GetNamespace())

	childWorkflowInfo, ok := r.mutableState.GetChildExecutionInfo(childWorkflowScheduleID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending child workflow: %v", childWorkflowScheduleID))
	}

	targetNamespaceID, err := r.getTargetNamespaceID(childWorkflowTargetNamespace)
	if err != nil {
		return err
	}

	r.mutableState.AddTasks(&tasks.StartChildExecutionTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		VisibilityTimestamp: now,
		TargetNamespaceID:   targetNamespaceID.String(),
		TargetWorkflowID:    childWorkflowInfo.StartedWorkflowId,
		InitiatedID:         childWorkflowInfo.InitiatedId,
		Version:             childWorkflowInfo.Version,
	})

	return nil
}

func (r *TaskGeneratorImpl) GenerateRequestCancelExternalTasks(
	now time.Time,
	event *historypb.HistoryEvent,
) error {

	attr := event.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes()
	scheduleID := event.GetEventId()
	version := event.GetVersion()
	targetNamespace := namespace.Name(attr.GetNamespace())
	targetWorkflowID := attr.GetWorkflowExecution().GetWorkflowId()
	targetRunID := attr.GetWorkflowExecution().GetRunId()
	targetChildOnly := attr.GetChildWorkflowOnly()

	_, ok := r.mutableState.GetRequestCancelInfo(scheduleID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending request cancel external workflow: %v", scheduleID))
	}

	targetNamespaceID, err := r.getTargetNamespaceID(targetNamespace)
	if err != nil {
		return err
	}

	r.mutableState.AddTasks(&tasks.CancelExecutionTask{
		// TaskID is set by shard
		WorkflowKey:             r.mutableState.GetWorkflowKey(),
		VisibilityTimestamp:     now,
		TargetNamespaceID:       targetNamespaceID.String(),
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildOnly,
		InitiatedID:             scheduleID,
		Version:                 version,
	})

	return nil
}

func (r *TaskGeneratorImpl) GenerateSignalExternalTasks(
	now time.Time,
	event *historypb.HistoryEvent,
) error {

	attr := event.GetSignalExternalWorkflowExecutionInitiatedEventAttributes()
	scheduleID := event.GetEventId()
	version := event.GetVersion()
	targetNamespace := namespace.Name(attr.GetNamespace())
	targetWorkflowID := attr.GetWorkflowExecution().GetWorkflowId()
	targetRunID := attr.GetWorkflowExecution().GetRunId()
	targetChildOnly := attr.GetChildWorkflowOnly()

	_, ok := r.mutableState.GetSignalInfo(scheduleID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending signal external workflow: %v", scheduleID))
	}

	targetNamespaceID, err := r.getTargetNamespaceID(targetNamespace)
	if err != nil {
		return err
	}

	r.mutableState.AddTasks(&tasks.SignalExecutionTask{
		// TaskID is set by shard
		WorkflowKey:             r.mutableState.GetWorkflowKey(),
		VisibilityTimestamp:     now,
		TargetNamespaceID:       targetNamespaceID.String(),
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildOnly,
		InitiatedID:             scheduleID,
		Version:                 version,
	})

	return nil
}

func (r *TaskGeneratorImpl) GenerateWorkflowSearchAttrTasks(
	now time.Time,
) error {

	currentVersion := r.mutableState.GetCurrentVersion()

	r.mutableState.AddTasks(&tasks.UpsertExecutionVisibilityTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		VisibilityTimestamp: now,
		Version:             currentVersion, // task processing does not check this version
	})
	return nil
}

func (r *TaskGeneratorImpl) GenerateWorkflowResetTasks(
	now time.Time,
) error {

	currentVersion := r.mutableState.GetCurrentVersion()

	r.mutableState.AddTasks(&tasks.ResetWorkflowTask{
		// TaskID is set by shard
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		VisibilityTimestamp: now,
		Version:             currentVersion,
	})

	return nil
}

func (r *TaskGeneratorImpl) GenerateActivityTimerTasks(
	now time.Time,
) error {

	_, err := r.getTimerSequence(now).CreateNextActivityTimer()
	return err
}

func (r *TaskGeneratorImpl) GenerateUserTimerTasks(
	now time.Time,
) error {

	_, err := r.getTimerSequence(now).CreateNextUserTimer()
	return err
}

func (r *TaskGeneratorImpl) GenerateHistoryReplicationTasks(
	now time.Time,
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
		// TaskID is set by shard
		VisibilityTimestamp: now,
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		FirstEventID:        firstEvent.GetEventId(),
		NextEventID:         lastEvent.GetEventId() + 1,
		Version:             version,
		BranchToken:         branchToken,
		NewRunBranchToken:   nil,
	})
	return nil
}

func (r *TaskGeneratorImpl) GenerateLastHistoryReplicationTasks(
	now time.Time,
) (tasks.Task, error) {
	executionInfo := r.mutableState.GetExecutionInfo()
	versionHistory, err := versionhistory.GetCurrentVersionHistory(executionInfo.GetVersionHistories())
	if err != nil {
		return nil, err
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(versionHistory)
	if err != nil {
		return nil, err
	}

	return &tasks.HistoryReplicationTask{
		// TaskID is set by shard
		VisibilityTimestamp: now,
		WorkflowKey:         r.mutableState.GetWorkflowKey(),
		FirstEventID:        executionInfo.LastFirstEventId,
		NextEventID:         lastItem.GetEventId() + 1,
		Version:             lastItem.GetVersion(),
		BranchToken:         versionHistory.BranchToken,
		NewRunBranchToken:   nil,
	}, nil
}

func (r *TaskGeneratorImpl) getTimerSequence(now time.Time) TimerSequence {
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)
	return NewTimerSequence(timeSource, r.mutableState)
}

func (r *TaskGeneratorImpl) getTargetNamespaceID(
	targetNamespace namespace.Name,
) (namespace.ID, error) {

	targetNamespaceID := namespace.ID(r.mutableState.GetExecutionInfo().NamespaceId)
	if targetNamespace != "" {
		targetNamespaceEntry, err := r.namespaceRegistry.GetNamespace(targetNamespace)
		if err != nil {
			return "", err
		}
		targetNamespaceID = targetNamespaceEntry.ID()
	}

	return targetNamespaceID, nil
}
