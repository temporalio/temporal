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
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/vclock"
	"go.temporal.io/server/service/history/workflow"
)

type (
	taskEventIDGetter        func(task tasks.Task) int64
	mutableStateStaleChecker func(task tasks.Task, executionInfo *persistencespb.WorkflowExecutionInfo) bool
)

// CheckTaskVersion will return an error if task version check fails
func CheckTaskVersion(
	shard shard.Context,
	logger log.Logger,
	namespace *namespace.Namespace,
	version int64,
	taskVersion int64,
	task interface{},
) error {

	if !shard.GetClusterMetadata().IsGlobalNamespaceEnabled() {
		return nil
	}

	// the first return value is whether this task is valid for further processing
	if !namespace.IsGlobalNamespace() {
		logger.Debug("NamespaceID is not global, task version check pass", tag.WorkflowNamespaceID(namespace.ID().String()), tag.Task(task))
		return nil
	} else if version != taskVersion {
		logger.Debug("NamespaceID is global, task version != target version", tag.WorkflowNamespaceID(namespace.ID().String()), tag.Task(task), tag.TaskVersion(version))
		return consts.ErrTaskVersionMismatch
	}
	logger.Debug("NamespaceID is global, task version == target version", tag.WorkflowNamespaceID(namespace.ID().String()), tag.Task(task), tag.TaskVersion(version))
	return nil
}

// load mutable state, if mutable state's next event ID <= task ID, will attempt to refresh
// if still mutable state's next event ID <= task ID, will return nil, nil
func loadMutableStateForTransferTask(
	ctx context.Context,
	shardContext shard.Context,
	wfContext workflow.Context,
	transferTask tasks.Task,
	metricsHandler metrics.Handler,
	logger log.Logger,
) (workflow.MutableState, error) {
	logger = tasks.InitializeLogger(transferTask, logger)
	mutableState, err := loadMutableStateForTask(
		ctx,
		shardContext,
		wfContext,
		transferTask,
		tasks.GetTransferTaskEventID,
		transferTaskMutableStateStaleChecker,
		metricsHandler.WithTags(metrics.OperationTag(metrics.OperationTransferQueueProcessorScope)),
		queues.GetActiveTransferTaskTypeTagValue(transferTask),
		logger,
	)
	if err != nil {
		// When standby task executor executes task in active cluster (and vice versa),
		// mutable state might be already deleted by active task executor and NotFound is a valid case which shouldn't be logged.
		// Unfortunately, this will also skip logging of actual errors that might happen due to serious bugs,
		// but these errors, most likely, will happen for other task types too, and will be logged.
		// TODO: remove this logic multi-cursor is implemented and only one task processor is running in each cluster.
		skipNotFoundLog :=
			transferTask.GetType() == enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION ||
				transferTask.GetType() == enumsspb.TASK_TYPE_TRANSFER_DELETE_EXECUTION

		if !skipNotFoundLog {
			switch err.(type) {
			case *serviceerror.NotFound:
				// NotFound error will be ignored by task error handling logic, so log it here
				// for transfer tasks, mutable state should always be available
				logger.Warn("Transfer Task Processor: workflow mutable state not found, skip.")
			case *serviceerror.NamespaceNotFound:
				// NamespaceNotFound error will be ignored by task error handling logic, so log it here
				// for transfer tasks, namespace should always be available.
				logger.Warn("Transfer Task Processor: namespace not found, skip.")
			}
		}
	}
	return mutableState, err
}

// load mutable state, if mutable state's next event ID <= task ID, will attempt to refresh
// if still mutable state's next event ID <= task ID, will return nil, nil
func loadMutableStateForTimerTask(
	ctx context.Context,
	shardContext shard.Context,
	wfContext workflow.Context,
	timerTask tasks.Task,
	metricsHandler metrics.Handler,
	logger log.Logger,
) (workflow.MutableState, error) {
	logger = tasks.InitializeLogger(timerTask, logger)
	return loadMutableStateForTask(
		ctx,
		shardContext,
		wfContext,
		timerTask,
		tasks.GetTimerTaskEventID,
		timerTaskMutableStateStaleChecker,
		metricsHandler.WithTags(metrics.OperationTag(metrics.OperationTimerQueueProcessorScope)),
		queues.GetActiveTimerTaskTypeTagValue(timerTask),
		logger,
	)
}

func loadMutableStateForTask(
	ctx context.Context,
	shardContext shard.Context,
	wfContext workflow.Context,
	task tasks.Task,
	getEventID taskEventIDGetter,
	canMutableStateBeStale mutableStateStaleChecker,
	metricsHandler metrics.Handler,
	taskTypeTag string,
	logger log.Logger,
) (workflow.MutableState, error) {

	if err := validateTaskByClock(shardContext, task); err != nil {
		return nil, err
	}

	mutableState, err := wfContext.LoadMutableState(ctx, shardContext)
	if err != nil {
		return nil, err
	}

	if task.GetRunID() == mutableState.GetWorkflowKey().RunID {
		// Task generation is scoped to a specific run, so only perform the validation if runID matches.
		// Tasks targeting the current run (e.g. workflow execution timeout timer) should bypass the validation.
		if err := validateTaskGeneration(ctx, shardContext, wfContext, mutableState, task.GetTaskID()); err != nil {
			return nil, err
		}
	}

	// TODO: With validateTaskByClock check above, we should never run into the situation where
	// mutable state cache is stale. This is based on the assumption that shard context
	// will never re-acquire the shard after it has been stolen.
	// We should monitor the StaleMutableStateCounter metric and remove the logic below once we are confident.

	// Validation based on eventID is not good enough as certain operation does not generate events.
	// For example, scheduling transient workflow task, or starting activities that have retry policy.
	eventID := getEventID(task)
	if eventID < mutableState.GetNextEventID() {
		return mutableState, nil
	}

	// Depending on task type, there are exceptions when mutable state can't be stale.
	// If this is a case, it is safe to use cached mutable state.
	if !canMutableStateBeStale(task, mutableState.GetExecutionInfo()) {
		return mutableState, nil
	}
	// Otherwise, clear workflow context, reload mutable state from a database and try again.

	metrics.StaleMutableStateCounter.With(metricsHandler).Record(1)
	wfContext.Clear()

	mutableState, err = wfContext.LoadMutableState(ctx, shardContext)
	if err != nil {
		return nil, err
	}

	if err := validateTaskGeneration(ctx, shardContext, wfContext, mutableState, task.GetTaskID()); err != nil {
		return nil, err
	}

	if eventID < mutableState.GetNextEventID() {
		return mutableState, nil
	}
	// After reloading mutable state from a database, task's event ID is still not valid,
	// means that task is obsolete and can be safely skipped.
	getNamespaceTagByID(shardContext.GetNamespaceRegistry(), task.GetNamespaceID())
	metrics.TaskSkipped.With(metricsHandler).Record(1, getNamespaceTagByID(shardContext.GetNamespaceRegistry(), task.GetNamespaceID()), metrics.TaskTypeTag(taskTypeTag))
	logger.Info("Task processor skipping task: task event ID >= MS NextEventID.",
		tag.WorkflowNextEventID(mutableState.GetNextEventID()),
	)
	return nil, nil
}

func validateTaskByClock(
	shardContext shard.Context,
	task tasks.Task,
) error {
	shardID := shardContext.GetShardID()
	taskClock := vclock.NewVectorClock(
		shardContext.GetClusterMetadata().GetClusterID(),
		shardContext.GetShardID(),
		task.GetTaskID(),
	)
	currentClock := shardContext.CurrentVectorClock()
	result, err := vclock.Compare(taskClock, currentClock)
	if err != nil {
		return err
	}
	if result >= 0 {
		shardContext.UnloadForOwnershipLost()
		return &persistence.ShardOwnershipLostError{
			ShardID: shardID,
			Msg:     fmt.Sprintf("Shard: %v task clock validation failed, reloading", shardID),
		}
	}

	return nil
}

func validateTaskGeneration(
	ctx context.Context,
	shardContext shard.Context,
	workflowContext workflow.Context,
	mutableState workflow.MutableState,
	taskID int64,
) error {
	tgClock := mutableState.GetExecutionInfo().TaskGenerationShardClockTimestamp
	if tgClock != 0 && taskID != 0 && taskID < tgClock {

		currentClock := shardContext.CurrentVectorClock().Clock
		if tgClock > currentClock {
			if err := workflowContext.RefreshTasks(ctx, shardContext); err != nil {
				return err
			}
			return fmt.Errorf("%w: fixed task generation logic via workflow refresh", consts.ErrStaleReference)
		}

		return fmt.Errorf("%w: task was generated before mutable state rebuild", consts.ErrStaleReference)
	}
	return nil
}

func transferTaskMutableStateStaleChecker(
	transferTask tasks.Task,
	executionInfo *persistencespb.WorkflowExecutionInfo,
) bool {

	// Check to see if mutable state cache needs to be reloaded from a database.
	// The exception is a transient workflow task that doesn't generate events
	// (check only that it is still current WFT).

	wt, isWt := transferTask.(*tasks.WorkflowTask)
	if !isWt {
		return true
	}

	isTransientWorkflowTask := executionInfo.WorkflowTaskAttempt > 1
	if isTransientWorkflowTask && executionInfo.WorkflowTaskScheduledEventId == wt.ScheduledEventID {
		return false
	}

	return true
}

func timerTaskMutableStateStaleChecker(
	timerTask tasks.Task,
	executionInfo *persistencespb.WorkflowExecutionInfo,
) bool {

	// Check to see if mutable state cache needs to be reloaded from a database.
	// Exceptions are:
	// 1. Transient workflow task that doesn't generate events (check only that it is still current WFT).
	// 2. Speculative workflow task that doesn't generate events.

	wttt, isWttt := timerTask.(*tasks.WorkflowTaskTimeoutTask)
	if !isWttt {
		return true
	}

	isSpeculativeWorkflowTask := wttt.GetCategory() == tasks.CategoryMemoryTimer
	if isSpeculativeWorkflowTask {
		return false
	}

	isTransientWorkflowTask := executionInfo.WorkflowTaskAttempt > 1
	if isTransientWorkflowTask && executionInfo.WorkflowTaskScheduledEventId == wttt.EventID {
		return false
	}

	return true
}

func getNamespaceTagByID(
	registry namespace.Registry,
	namespaceID string,
) metrics.Tag {
	namespaceName, err := registry.GetNamespaceName(namespace.ID(namespaceID))
	if err != nil {
		return metrics.NamespaceUnknownTag()
	}

	return metrics.NamespaceTag(namespaceName.String())
}

func getNamespaceTagAndReplicationStateByID(
	registry namespace.Registry,
	namespaceID string,
) (metrics.Tag, enumspb.ReplicationState) {
	namespace, err := registry.GetNamespaceByID(namespace.ID(namespaceID))
	if err != nil {
		return metrics.NamespaceUnknownTag(), enumspb.REPLICATION_STATE_UNSPECIFIED
	}

	return metrics.NamespaceTag(namespace.Name().String()), namespace.ReplicationState()
}
