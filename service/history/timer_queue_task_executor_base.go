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

package history

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

var (
	errNoTimerFired = serviceerror.NewNotFound("no expired timer to fire found")
)

type (
	timerQueueTaskExecutorBase struct {
		stateMachineEnvironment
		currentClusterName string
		registry           namespace.Registry
		deleteManager      deletemanager.DeleteManager
		matchingRawClient  resource.MatchingRawClient
		config             *configs.Config
		metricHandler      metrics.Handler
		isActive           bool
	}
)

func newTimerQueueTaskExecutorBase(
	shardContext shard.Context,
	workflowCache wcache.Cache,
	deleteManager deletemanager.DeleteManager,
	matchingRawClient resource.MatchingRawClient,
	logger log.Logger,
	metricsHandler metrics.Handler,
	config *configs.Config,
	isActive bool,
) *timerQueueTaskExecutorBase {
	return &timerQueueTaskExecutorBase{
		stateMachineEnvironment: stateMachineEnvironment{
			shardContext:   shardContext,
			cache:          workflowCache,
			logger:         logger,
			metricsHandler: metricsHandler,
		},
		currentClusterName: shardContext.GetClusterMetadata().GetCurrentClusterName(),
		registry:           shardContext.GetNamespaceRegistry(),
		deleteManager:      deleteManager,
		matchingRawClient:  matchingRawClient,
		config:             config,
		metricHandler:      metricsHandler,
		isActive:           isActive,
	}
}

func (t *timerQueueTaskExecutorBase) executeDeleteHistoryEventTask(
	ctx context.Context,
	task *tasks.DeleteHistoryEventTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowID(),
		RunId:      task.GetRunID(),
	}

	weContext, release, err := t.cache.GetOrCreateWorkflowExecution(
		ctx,
		t.shardContext,
		namespace.ID(task.GetNamespaceID()),
		workflowExecution,
		locks.PriorityLow,
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(ctx, t.shardContext, weContext, task, t.metricsHandler, t.logger)
	switch err.(type) {
	case nil:
		if mutableState == nil {
			return nil
		}
	case *serviceerror.NotFound:
		// the mutable state is deleted and delete history branch operation failed.
		// use task branch token to delete the leftover history branch
		return t.deleteHistoryBranch(ctx, task.BranchToken)
	default:
		return err
	}

	if mutableState.GetExecutionState().GetState() != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		// If workflow is running then just ignore DeleteHistoryEventTask timer task.
		// This should almost never happen because DeleteHistoryEventTask is created only for closed workflows.
		// But cross DC replication can resurrect workflow and therefore DeleteHistoryEventTask should be ignored.
		return nil
	}

	closeVersion, err := mutableState.GetCloseVersion()
	if err != nil {
		return err
	}
	if err := CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), closeVersion, task.Version, task); err != nil {
		return err
	}

	return t.deleteManager.DeleteWorkflowExecutionByRetention(
		ctx,
		namespace.ID(task.GetNamespaceID()),
		workflowExecution,
		weContext,
		mutableState,
		&task.ProcessStage,
	)
}

func (t *timerQueueTaskExecutorBase) deleteHistoryBranch(
	ctx context.Context,
	branchToken []byte,
) error {
	if len(branchToken) > 0 {
		return t.shardContext.GetExecutionManager().DeleteHistoryBranch(ctx, &persistence.DeleteHistoryBranchRequest{
			ShardID:     t.shardContext.GetShardID(),
			BranchToken: branchToken,
		})
	}
	return nil
}

func (t *timerQueueTaskExecutorBase) isValidExpirationTime(
	mutableState workflow.MutableState,
	expirationTime *timestamppb.Timestamp,
) bool {
	if expirationTime == nil {
		return false
	}
	if !mutableState.IsWorkflowExecutionRunning() {
		return false
	}

	now := t.shardContext.GetTimeSource().Now()
	expired := queues.IsTimeExpired(now, expirationTime.AsTime())

	return expired

}

func (t *timerQueueTaskExecutorBase) isValidWorkflowRunTimeoutTask(
	mutableState workflow.MutableState,
	task *tasks.WorkflowRunTimeoutTask,
) bool {
	executionInfo := mutableState.GetExecutionInfo()

	// Check if workflow execution timeout is not expired
	// This can happen if the workflow is reset but old timer task is still fired.
	return t.isValidExpirationTime(mutableState, executionInfo.WorkflowExecutionExpirationTime)
}

func (t *timerQueueTaskExecutorBase) isValidWorkflowExecutionTimeoutTask(
	mutableState workflow.MutableState,
	task *tasks.WorkflowExecutionTimeoutTask,
) bool {

	executionInfo := mutableState.GetExecutionInfo()
	if executionInfo.FirstExecutionRunId != task.FirstRunID {
		// current run does not belong to workflow chain the task is generated for
		return false
	}

	// Check if workflow execution timeout is not expired
	// This can happen if the workflow is reset since reset re-calculates
	// the execution timeout but shares the same firstRunID as the base run
	return t.isValidExpirationTime(mutableState, executionInfo.WorkflowExecutionExpirationTime)

	// NOTE: we don't need to do version check here because if we were to do it, we need to compare the task version
	// and the start version in the first run. However, failover & conflict resolution will never change
	// the first event of a workflowID (the history tree model we are using always share at least one node),
	// meaning start version check will always pass.
	// Also there's no way we can perform version check before first run may already be deleted due to retention
}

func (t *timerQueueTaskExecutorBase) executeSingleStateMachineTimer(
	ctx context.Context,
	workflowContext workflow.Context,
	ms workflow.MutableState,
	deadline time.Time,
	timer *persistencespb.StateMachineTaskInfo,
	execute func(node *hsm.Node, task hsm.Task) error,
) error {
	def, ok := t.shardContext.StateMachineRegistry().TaskSerializer(timer.Type)
	if !ok {
		return queues.NewUnprocessableTaskError(fmt.Sprintf("deserializer not registered for task type %v", timer.Type))
	}
	smt, err := def.Deserialize(timer.Data, hsm.TaskKindTimer{Deadline: deadline})
	if err != nil {
		return fmt.Errorf(
			"%w: %w",
			queues.NewUnprocessableTaskError(fmt.Sprintf("cannot deserialize task %v", timer.Type)),
			err,
		)
	}
	ref := hsm.Ref{
		WorkflowKey:     ms.GetWorkflowKey(),
		StateMachineRef: timer.Ref,
	}
	if err := t.validateStateMachineRef(ctx, workflowContext, ms, ref, false); err != nil {
		return err
	}
	node, err := ms.HSM().Child(ref.StateMachinePath())
	if err != nil {
		return err
	}

	if err := execute(node, smt); err != nil {
		return fmt.Errorf("failed to execute task: %w", err)
	}
	return nil
}

// executeStateMachineTimers gets the state machine timers, processed the expired timers,
// and return a slice of unprocessed timers.
func (t *timerQueueTaskExecutorBase) executeStateMachineTimers(
	ctx context.Context,
	workflowContext workflow.Context,
	ms workflow.MutableState,
	execute func(node *hsm.Node, task hsm.Task) error,
) (int, error) {

	// need to specifically check for zombie workflows here instead of workflow running
	// or not since zombie workflows are considered as not running but state machine timers
	// can target closed workflows.
	if ms.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		return 0, consts.ErrWorkflowZombie
	}

	timers := ms.GetExecutionInfo().StateMachineTimers
	processedTimers := 0
	// StateMachineTimers are sorted by Deadline, iterate through them as long as the deadline is expired.
	for len(timers) > 0 {
		group := timers[0]
		if !queues.IsTimeExpired(t.Now(), group.Deadline.AsTime()) {
			break
		}

		for _, timer := range group.Infos {
			err := t.executeSingleStateMachineTimer(ctx, workflowContext, ms, group.Deadline.AsTime(), timer, execute)
			if err != nil {
				if !errors.As(err, new(*serviceerror.NotFound)) {
					metrics.StateMachineTimerProcessingFailuresCounter.With(t.metricHandler).Record(
						1,
						metrics.OperationTag(queues.GetTimerStateMachineTaskTypeTagValue(timer.GetType(), t.isActive)),
						metrics.ServiceErrorTypeTag(err),
					)
					// Return on first error as we don't want to duplicate the Executable's error handling logic.
					// This implies that a single bad task in the mutable state timer sequence will cause all other
					// tasks to be stuck. We'll accept this limitation for now.
					return 0, err
				}
				metrics.StateMachineTimerSkipsCounter.With(t.metricHandler).Record(
					1,
					metrics.OperationTag(queues.GetTimerStateMachineTaskTypeTagValue(timer.GetType(), t.isActive)),
				)
				t.logger.Warn("Skipped state machine timer", tag.Error(err))
			}
		}
		// Remove the processed timer group.
		timers = timers[1:]
		processedTimers++
	}

	if processedTimers > 0 {
		// Update processed timers.
		ms.GetExecutionInfo().StateMachineTimers = timers
	}
	return processedTimers, nil
}
