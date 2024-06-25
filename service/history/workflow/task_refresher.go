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
	"context"
	"errors"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
)

type (
	TaskRefresher interface {
		RefreshTasks(ctx context.Context, mutableState MutableState) error
	}

	TaskRefresherImpl struct {
		shard  shard.Context
		logger log.Logger
	}
)

func NewTaskRefresher(
	shard shard.Context,
	logger log.Logger,
) *TaskRefresherImpl {

	return &TaskRefresherImpl{
		shard:  shard,
		logger: logger,
	}
}

func (r *TaskRefresherImpl) RefreshTasks(
	ctx context.Context,
	mutableState MutableState,
) error {
	// Invalidate all tasks generated for this mutable state before the refresh.
	mutableState.GetExecutionInfo().TaskGenerationShardClockTimestamp = r.shard.CurrentVectorClock().GetClock()

	taskGenerator := taskGeneratorProvider.NewTaskGenerator(
		r.shard,
		mutableState,
	)

	if err := RefreshTasksForWorkflowStart(
		ctx,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForWorkflowClose(
		ctx,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForRecordWorkflowStarted(
		ctx,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshWorkflowTaskTasks(
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForActivity(
		ctx,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForTimer(
		mutableState,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForChildWorkflow(
		ctx,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForRequestCancelExternalWorkflow(
		ctx,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForSignalExternalWorkflow(
		ctx,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForWorkflowSearchAttr(
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	return r.refreshTasksForSubStateMachines(
		mutableState,
	)
}

func RefreshTasksForWorkflowStart(
	ctx context.Context,
	mutableState MutableState,
	taskGenerator TaskGenerator,
) error {

	executionState := mutableState.GetExecutionState()
	if executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil
	}

	startEvent, err := mutableState.GetStartEvent(ctx)
	if err != nil {
		return err
	}

	// first clear execution timeout timer task status
	executionInfo := mutableState.GetExecutionInfo()
	executionInfo.WorkflowExecutionTimerTaskStatus = TimerTaskStatusNone

	executionInfo.WorkflowExecutionTimerTaskStatus, err = taskGenerator.GenerateWorkflowStartTasks(
		startEvent,
	)
	if err != nil {
		return err
	}

	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()
	if !mutableState.HadOrHasWorkflowTask() && timestamp.DurationValue(startAttr.GetFirstWorkflowTaskBackoff()) > 0 {
		if err := taskGenerator.GenerateDelayedWorkflowTasks(
			startEvent,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *TaskRefresherImpl) refreshTasksForWorkflowClose(
	ctx context.Context,
	mutableState MutableState,
	taskGenerator TaskGenerator,
) error {

	executionState := mutableState.GetExecutionState()
	if executionState.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil
	}
	closeEventTime, err := mutableState.GetWorkflowCloseTime(ctx)
	if err != nil {
		return err
	}

	return taskGenerator.GenerateWorkflowCloseTasks(
		closeEventTime,
		false,
	)
}

func (r *TaskRefresherImpl) refreshTasksForRecordWorkflowStarted(
	ctx context.Context,
	mutableState MutableState,
	taskGenerator TaskGenerator,
) error {

	executionState := mutableState.GetExecutionState()
	if executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil
	}

	startEvent, err := mutableState.GetStartEvent(ctx)
	if err != nil {
		return err
	}

	return taskGenerator.GenerateRecordWorkflowStartedTasks(
		startEvent,
	)
}

func (r *TaskRefresherImpl) refreshWorkflowTaskTasks(
	mutableState MutableState,
	taskGenerator TaskGenerator,
) error {

	executionState := mutableState.GetExecutionState()
	if executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil
	}

	if !mutableState.HasPendingWorkflowTask() {
		// no workflow task at all
		return nil
	}

	workflowTask := mutableState.GetPendingWorkflowTask()
	if workflowTask == nil {
		return serviceerror.NewInternal("it could be a bug, cannot get pending workflow task")
	}

	if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		// Do not generate tasks because speculative WT doesn't have any timer or transfer tasks associated with it.
		return nil
	}

	// workflowTask already started
	if workflowTask.StartedEventID != common.EmptyEventID {
		return taskGenerator.GenerateStartWorkflowTaskTasks(
			workflowTask.ScheduledEventID,
		)
	}

	// workflowTask only scheduled
	return taskGenerator.GenerateScheduleWorkflowTaskTasks(
		workflowTask.ScheduledEventID,
	)
}

func (r *TaskRefresherImpl) refreshTasksForActivity(
	ctx context.Context,
	mutableState MutableState,
	taskGenerator TaskGenerator,
) error {

	executionState := mutableState.GetExecutionState()
	if executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil
	}

	pendingActivityInfos := mutableState.GetPendingActivityInfos()

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

		if activityInfo.StartedEventId != common.EmptyEventID {
			continue Loop
		}

		scheduleEvent, err := mutableState.GetActivityScheduledEvent(ctx, activityInfo.ScheduledEventId)
		if err != nil {
			return err
		}

		if err := taskGenerator.GenerateActivityTasks(
			scheduleEvent.GetEventId(),
		); err != nil {
			return err
		}
	}

	if _, err := NewTimerSequence(
		mutableState,
	).CreateNextActivityTimer(); err != nil {
		return err
	}

	return nil
}

func (r *TaskRefresherImpl) refreshTasksForTimer(
	mutableState MutableState,
) error {

	executionState := mutableState.GetExecutionState()
	if executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil
	}

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
		mutableState,
	).CreateNextUserTimer(); err != nil {
		return err
	}

	return nil
}

func (r *TaskRefresherImpl) refreshTasksForChildWorkflow(
	ctx context.Context,
	mutableState MutableState,
	taskGenerator TaskGenerator,
) error {

	pendingChildWorkflowInfos := mutableState.GetPendingChildExecutionInfos()

Loop:
	for _, childWorkflowInfo := range pendingChildWorkflowInfos {
		if childWorkflowInfo.StartedEventId != common.EmptyEventID {
			continue Loop
		}
		scheduleEvent, err := mutableState.GetChildExecutionInitiatedEvent(ctx, childWorkflowInfo.InitiatedEventId)
		if err != nil {
			return err
		}

		if err := taskGenerator.GenerateChildWorkflowTasks(
			scheduleEvent,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *TaskRefresherImpl) refreshTasksForRequestCancelExternalWorkflow(
	ctx context.Context,
	mutableState MutableState,
	taskGenerator TaskGenerator,
) error {

	executionState := mutableState.GetExecutionState()
	if executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil
	}

	pendingRequestCancelInfos := mutableState.GetPendingRequestCancelExternalInfos()

	for _, requestCancelInfo := range pendingRequestCancelInfos {
		initiateEvent, err := mutableState.GetRequesteCancelExternalInitiatedEvent(ctx, requestCancelInfo.GetInitiatedEventId())
		if err != nil {
			return err
		}

		if err := taskGenerator.GenerateRequestCancelExternalTasks(
			initiateEvent,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *TaskRefresherImpl) refreshTasksForSignalExternalWorkflow(
	ctx context.Context,
	mutableState MutableState,
	taskGenerator TaskGenerator,
) error {

	executionState := mutableState.GetExecutionState()
	if executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil
	}

	pendingSignalInfos := mutableState.GetPendingSignalExternalInfos()

	for _, signalInfo := range pendingSignalInfos {

		initiateEvent, err := mutableState.GetSignalExternalInitiatedEvent(ctx, signalInfo.GetInitiatedEventId())
		if err != nil {
			return err
		}

		if err := taskGenerator.GenerateSignalExternalTasks(
			initiateEvent,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *TaskRefresherImpl) refreshTasksForWorkflowSearchAttr(
	mutableState MutableState,
	taskGenerator TaskGenerator,
) error {
	executionState := mutableState.GetExecutionState()
	if executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil
	}

	return taskGenerator.GenerateUpsertVisibilityTask()
}

func (r *TaskRefresherImpl) refreshTasksForSubStateMachines(
	mutableState MutableState,
) error {

	// NOTE: Not all callers of TaskRefresher goes through the closeTransaction process.
	// If we were to regenerate tasks here by doing a state machine transition and return
	// a TransitionOutput, then, we need to
	//   1. Call taskGenerator.GenerateDirtySubStateMachineTasks explictly to make sure
	//      tasks are added to mutable state.
	//   2. Make GenerateDirtySubStateMachineTasks idempotent, so that if the logic
	//      does go through closeTransaction, no duplicate tasks are generated.
	//   3. Explicitly clear transition state for sub state machines when not going through
	//      closeTransaction path.
	//   4. Sub state machine will be marked as dirty, causing task refresh being counted
	//      as a state transition. (if the logic does go through closeTransaction path,
	//      though we don't actually have such cases today)
	// With the implementation below, we avoid the above complexities by directly generating
	// tasks and adding them to mutable state immediately.

	// Task refresher is only meant for regenerating tasks that has been generated before,
	// in previous state transitions, so we can just call generateSubStateMachineTask
	// which uses the last versioned transition.
	// In replication case, task refresher should be called after applying the state from source
	// cluster, which updated the transition history.

	// Reset all the state machine timers, we'll recreate them all.
	mutableState.GetExecutionInfo().StateMachineTimers = nil

	err := mutableState.HSM().Walk(func(node *hsm.Node) error {
		taskRegenerator, err := hsm.MachineData[hsm.TaskRegenerator](node)
		if err != nil {
			if node.Parent == nil && errors.Is(err, hsm.ErrIncompatibleType) {
				// root node is mutable state and doesn't implement TaskRegenerator interface
				return nil
			}
			return err
		}

		tasks, err := taskRegenerator.RegenerateTasks(node)
		if err != nil {
			return err
		}

		for _, task := range tasks {
			if err := generateSubStateMachineTask(
				mutableState,
				r.shard.StateMachineRegistry(),
				node,
				node.Path(),
				task,
			); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	AddNextStateMachineTimerTask(mutableState)

	return nil
}
