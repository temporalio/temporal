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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination task_refresher_mock.go

package workflow

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
)

type (
	TaskRefresher interface {
		RefreshTasks(now time.Time, mutableState MutableState) error
	}

	TaskRefresherImpl struct {
		config            *configs.Config
		namespaceRegistry namespace.Registry
		eventsCache       events.Cache
		logger            log.Logger
	}
)

func NewTaskRefresher(
	config *configs.Config,
	namespaceRegistry namespace.Registry,
	eventsCache events.Cache,
	logger log.Logger,
) *TaskRefresherImpl {

	return &TaskRefresherImpl{
		config:            config,
		namespaceRegistry: namespaceRegistry,
		eventsCache:       eventsCache,
		logger:            logger,
	}
}

func (r *TaskRefresherImpl) RefreshTasks(
	now time.Time,
	mutableState MutableState,
) error {

	taskGenerator := NewTaskGenerator(
		r.namespaceRegistry,
		r.logger,
		mutableState,
	)

	if err := r.refreshTasksForWorkflowStart(
		now,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForWorkflowClose(
		now,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForRecordWorkflowStarted(
		now,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshWorkflowTaskTasks(
		now,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForActivity(
		now,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForTimer(
		now,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForChildWorkflow(
		now,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForRequestCancelExternalWorkflow(
		now,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForSignalExternalWorkflow(
		now,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if r.config.AdvancedVisibilityWritingMode() != visibility.AdvancedVisibilityWritingModeOff {
		if err := r.refreshTasksForWorkflowSearchAttr(
			now,
			mutableState,
			taskGenerator,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *TaskRefresherImpl) refreshTasksForWorkflowStart(
	now time.Time,
	mutableState MutableState,
	taskGenerator TaskGenerator,
) error {

	startEvent, err := mutableState.GetStartEvent()
	if err != nil {
		return err
	}

	if err := taskGenerator.GenerateWorkflowStartTasks(
		now,
		startEvent,
	); err != nil {
		return err
	}

	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()
	if !mutableState.HasProcessedOrPendingWorkflowTask() && timestamp.DurationValue(startAttr.GetFirstWorkflowTaskBackoff()) > 0 {
		if err := taskGenerator.GenerateDelayedWorkflowTasks(
			now,
			startEvent,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *TaskRefresherImpl) refreshTasksForWorkflowClose(
	now time.Time,
	mutableState MutableState,
	taskGenerator TaskGenerator,
) error {

	executionState := mutableState.GetExecutionState()

	if executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return taskGenerator.GenerateWorkflowCloseTasks(
			now,
		)
	}

	return nil
}

func (r *TaskRefresherImpl) refreshTasksForRecordWorkflowStarted(
	now time.Time,
	mutableState MutableState,
	taskGenerator TaskGenerator,
) error {

	startEvent, err := mutableState.GetStartEvent()
	if err != nil {
		return err
	}

	executionState := mutableState.GetExecutionState()

	if executionState.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return taskGenerator.GenerateRecordWorkflowStartedTasks(
			now,
			startEvent,
		)
	}

	return nil
}

func (r *TaskRefresherImpl) refreshWorkflowTaskTasks(
	now time.Time,
	mutableState MutableState,
	taskGenerator TaskGenerator,
) error {

	if !mutableState.HasPendingWorkflowTask() {
		// no workflow task at all
		return nil
	}

	workflowTask, ok := mutableState.GetPendingWorkflowTask()
	if !ok {
		return serviceerror.NewInternal("it could be a bug, cannot get pending workflow task")
	}

	// workflowTask already started
	if workflowTask.StartedID != common.EmptyEventID {
		return taskGenerator.GenerateStartWorkflowTaskTasks(
			now,
			workflowTask.ScheduleID,
		)
	}

	// workflowTask only scheduled
	return taskGenerator.GenerateScheduleWorkflowTaskTasks(
		now,
		workflowTask.ScheduleID,
	)
}

func (r *TaskRefresherImpl) refreshTasksForActivity(
	now time.Time,
	mutableState MutableState,
	taskGenerator TaskGenerator,
) error {

	executionInfo := mutableState.GetExecutionInfo()
	executionState := mutableState.GetExecutionState()
	pendingActivityInfos := mutableState.GetPendingActivityInfos()

	currentBranchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return err
	}

Loop:
	for _, activityInfo := range pendingActivityInfos {
		// clear all activity timer task mask for later activity timer task re-generation
		activityInfo.TimerTaskStatus = TimerTaskStatusNone

		// need to update activity timer task mask for which task is generated
		if err := mutableState.UpdateActivity(
			activityInfo,
		); err != nil {
			return err
		}

		if activityInfo.StartedId != common.EmptyEventID {
			continue Loop
		}

		scheduleEvent, err := r.eventsCache.GetEvent(
			events.EventKey{
				NamespaceID: namespace.ID(executionInfo.NamespaceId),
				WorkflowID:  executionInfo.WorkflowId,
				RunID:       executionState.RunId,
				EventID:     activityInfo.ScheduleId,
				Version:     activityInfo.Version,
			},
			activityInfo.ScheduledEventBatchId,
			currentBranchToken,
		)
		if err != nil {
			return err
		}

		if err := taskGenerator.GenerateActivityTransferTasks(
			now,
			scheduleEvent,
		); err != nil {
			return err
		}
	}

	if _, err := NewTimerSequence(
		r.getTimeSource(now),
		mutableState,
	).CreateNextActivityTimer(); err != nil {
		return err
	}

	return nil
}

func (r *TaskRefresherImpl) refreshTasksForTimer(
	now time.Time,
	mutableState MutableState,
	_ TaskGenerator,
) error {

	pendingTimerInfos := mutableState.GetPendingTimerInfos()

	for _, timerInfo := range pendingTimerInfos {
		// clear all timer task mask for later timer task re-generation
		timerInfo.TaskStatus = TimerTaskStatusNone

		// need to update user timer task mask for which task is generated
		if err := mutableState.UpdateUserTimer(
			timerInfo,
		); err != nil {
			return err
		}
	}

	if _, err := NewTimerSequence(
		r.getTimeSource(now),
		mutableState,
	).CreateNextUserTimer(); err != nil {
		return err
	}

	return nil
}

func (r *TaskRefresherImpl) refreshTasksForChildWorkflow(
	now time.Time,
	mutableState MutableState,
	taskGenerator TaskGenerator,
) error {

	executionInfo := mutableState.GetExecutionInfo()
	executionState := mutableState.GetExecutionState()
	pendingChildWorkflowInfos := mutableState.GetPendingChildExecutionInfos()

	currentBranchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return err
	}

Loop:
	for _, childWorkflowInfo := range pendingChildWorkflowInfos {
		if childWorkflowInfo.StartedId != common.EmptyEventID {
			continue Loop
		}

		scheduleEvent, err := r.eventsCache.GetEvent(
			events.EventKey{
				NamespaceID: namespace.ID(executionInfo.NamespaceId),
				WorkflowID:  executionInfo.WorkflowId,
				RunID:       executionState.RunId,
				EventID:     childWorkflowInfo.InitiatedId,
				Version:     childWorkflowInfo.Version,
			},
			childWorkflowInfo.InitiatedEventBatchId,
			currentBranchToken,
		)
		if err != nil {
			return err
		}

		if err := taskGenerator.GenerateChildWorkflowTasks(
			now,
			scheduleEvent,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *TaskRefresherImpl) refreshTasksForRequestCancelExternalWorkflow(
	now time.Time,
	mutableState MutableState,
	taskGenerator TaskGenerator,
) error {

	executionInfo := mutableState.GetExecutionInfo()
	executionState := mutableState.GetExecutionState()
	pendingRequestCancelInfos := mutableState.GetPendingRequestCancelExternalInfos()

	currentBranchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return err
	}

	for _, requestCancelInfo := range pendingRequestCancelInfos {
		initiateEvent, err := r.eventsCache.GetEvent(
			events.EventKey{
				NamespaceID: namespace.ID(executionInfo.NamespaceId),
				WorkflowID:  executionInfo.WorkflowId,
				RunID:       executionState.RunId,
				EventID:     requestCancelInfo.GetInitiatedId(),
				Version:     requestCancelInfo.GetVersion(),
			},
			requestCancelInfo.GetInitiatedEventBatchId(),
			currentBranchToken,
		)
		if err != nil {
			return err
		}

		if err := taskGenerator.GenerateRequestCancelExternalTasks(
			now,
			initiateEvent,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *TaskRefresherImpl) refreshTasksForSignalExternalWorkflow(
	now time.Time,
	mutableState MutableState,
	taskGenerator TaskGenerator,
) error {

	executionInfo := mutableState.GetExecutionInfo()
	executionState := mutableState.GetExecutionState()
	pendingSignalInfos := mutableState.GetPendingSignalExternalInfos()

	currentBranchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return err
	}

	for _, signalInfo := range pendingSignalInfos {
		initiateEvent, err := r.eventsCache.GetEvent(
			events.EventKey{
				NamespaceID: namespace.ID(executionInfo.NamespaceId),
				WorkflowID:  executionInfo.WorkflowId,
				RunID:       executionState.RunId,
				EventID:     signalInfo.GetInitiatedId(),
				Version:     signalInfo.GetVersion(),
			},
			signalInfo.GetInitiatedEventBatchId(),
			currentBranchToken,
		)
		if err != nil {
			return err
		}

		if err := taskGenerator.GenerateSignalExternalTasks(
			now,
			initiateEvent,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *TaskRefresherImpl) refreshTasksForWorkflowSearchAttr(
	now time.Time,
	_ MutableState,
	taskGenerator TaskGenerator,
) error {

	return taskGenerator.GenerateWorkflowSearchAttrTasks(
		now,
	)
}

func (r *TaskRefresherImpl) getTimeSource(
	now time.Time,
) clock.TimeSource {

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)
	return timeSource
}
