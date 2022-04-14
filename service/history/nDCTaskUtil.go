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
	"time"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

const (
	refreshTaskTimeout = 30 * time.Second
)

// VerifyTaskVersion, will return true if failover version check is successful
func VerifyTaskVersion(
	shard shard.Context,
	logger log.Logger,
	namespace *namespace.Namespace,
	version int64,
	taskVersion int64,
	task interface{},
) bool {

	if !shard.GetClusterMetadata().IsGlobalNamespaceEnabled() {
		return true
	}

	// the first return value is whether this task is valid for further processing
	if !namespace.IsGlobalNamespace() {
		logger.Debug("NamespaceID is not global, task version check pass", tag.WorkflowNamespaceID(namespace.ID().String()), tag.Task(task))
		return true
	} else if version != taskVersion {
		logger.Debug("NamespaceID is global, task version != target version", tag.WorkflowNamespaceID(namespace.ID().String()), tag.Task(task), tag.TaskVersion(version))
		return false
	}
	logger.Debug("NamespaceID is global, task version == target version", tag.WorkflowNamespaceID(namespace.ID().String()), tag.Task(task), tag.TaskVersion(version))
	return true
}

// load mutable state, if mutable state's next event ID <= task ID, will attempt to refresh
// if still mutable state's next event ID <= task ID, will return nil, nil
func loadMutableStateForTransferTask(
	ctx context.Context,
	wfContext workflow.Context,
	transferTask tasks.Task,
	metricsClient metrics.Client,
	logger log.Logger,
) (workflow.MutableState, error) {
	logger = initializeLoggerForTask(transferTask, logger)
	mutableState, err := LoadMutableStateForTask(
		ctx,
		wfContext,
		transferTask,
		getTransferTaskEventIDAndRetryable,
		metricsClient.Scope(metrics.TransferQueueProcessorScope),
		logger,
	)
	if _, ok := err.(*serviceerror.NotFound); ok {
		// NotFound error will be ignored by task error handling logic, so log it here
		// for transfer tasks, mutable state should always be available
		logger.Error("Transfer Task Processor: workflow mutable state not found, skip.")
	}

	return mutableState, err
}

// load mutable state, if mutable state's next event ID <= task ID, will attempt to refresh
// if still mutable state's next event ID <= task ID, will return nil, nil
func loadMutableStateForTimerTask(
	ctx context.Context,
	wfContext workflow.Context,
	timerTask tasks.Task,
	metricsClient metrics.Client,
	logger log.Logger,
) (workflow.MutableState, error) {
	logger = initializeLoggerForTask(timerTask, logger)
	return LoadMutableStateForTask(
		ctx,
		wfContext,
		timerTask,
		getTimerTaskEventIDAndRetryable,
		metricsClient.Scope(metrics.TimerQueueProcessorScope),
		logger,
	)
}

func LoadMutableStateForTask(
	ctx context.Context,
	wfContext workflow.Context,
	task tasks.Task,
	taskEventIDAndRetryable func(task tasks.Task, executionInfo *persistencespb.WorkflowExecutionInfo) (int64, bool),
	scope metrics.Scope,
	logger log.Logger,
) (workflow.MutableState, error) {

	mutableState, err := wfContext.LoadWorkflowExecution(ctx)
	if err != nil {
		return nil, err
	}

	// check to see if cache needs to be refreshed as we could potentially have stale workflow execution
	// the exception is workflow task consistently fail
	// there will be no event generated, thus making the workflow task schedule ID == next event ID
	eventID, retryable := taskEventIDAndRetryable(task, mutableState.GetExecutionInfo())
	if eventID < mutableState.GetNextEventID() || !retryable {
		return mutableState, nil
	}

	scope.IncCounter(metrics.StaleMutableStateCounter)
	wfContext.Clear()

	mutableState, err = wfContext.LoadWorkflowExecution(ctx)
	if err != nil {
		return nil, err
	}
	// after refresh, still mutable state's next event ID <= task's event ID
	if eventID >= mutableState.GetNextEventID() {
		scope.IncCounter(metrics.TaskSkipped)
		logger.Info("Task Processor: task event ID >= MS NextEventID, skip.",
			tag.WorkflowNextEventID(mutableState.GetNextEventID()),
		)
		return nil, nil
	}
	return mutableState, nil
}

func initializeLoggerForTask(
	task tasks.Task,
	logger log.Logger,
) log.Logger {
	var taskEventID func(task tasks.Task) int64
	taskCategory := task.GetCategory()
	switch taskCategory.ID() {
	case tasks.CategoryIDTransfer:
		taskEventID = getTransferTaskEventID
	case tasks.CategoryIDTimer:
		taskEventID = getTimerTaskEventID
	case tasks.CategoryIDVisibility:
		// visibility tasks don't have task eventID
		taskEventID = func(task tasks.Task) int64 { return 0 }
	default:
		// replication task won't reach here
		panic(serviceerror.NewInternal("unknown task category"))
	}

	taskLogger := log.With(
		logger,
		tag.WorkflowNamespaceID(task.GetNamespaceID()),
		tag.WorkflowID(task.GetWorkflowID()),
		tag.WorkflowRunID(task.GetRunID()),
		tag.TaskID(task.GetTaskID()),
		tag.TaskVisibilityTimestamp(task.GetVisibilityTime()),
		tag.TaskType(task.GetType()),
		tag.Task(task),
		tag.WorkflowEventID(taskEventID(task)),
	)
	return taskLogger
}

func getTransferTaskEventID(
	transferTask tasks.Task,
) int64 {
	eventID := int64(0)
	switch task := transferTask.(type) {
	case *tasks.ActivityTask:
		eventID = task.ScheduleID
	case *tasks.WorkflowTask:
		eventID = task.ScheduleID
	case *tasks.CloseExecutionTask:
		eventID = common.FirstEventID
	case *tasks.DeleteExecutionTask:
		eventID = common.FirstEventID
	case *tasks.CancelExecutionTask:
		eventID = task.InitiatedID
	case *tasks.SignalExecutionTask:
		eventID = task.InitiatedID
	case *tasks.StartChildExecutionTask:
		eventID = task.InitiatedID
	case *tasks.ResetWorkflowTask:
		eventID = common.FirstEventID
	default:
		panic(errUnknownTransferTask)
	}
	return eventID
}

func getTransferTaskEventIDAndRetryable(
	transferTask tasks.Task,
	executionInfo *persistencespb.WorkflowExecutionInfo,
) (int64, bool) {
	eventID := getTransferTaskEventID(transferTask)
	retryable := true

	if task, ok := transferTask.(*tasks.WorkflowTask); ok {
		retryable = !(executionInfo.WorkflowTaskScheduleId == task.ScheduleID && executionInfo.WorkflowTaskAttempt > 1)
	}

	return eventID, retryable
}

func getTimerTaskEventID(
	timerTask tasks.Task,
) int64 {
	eventID := int64(0)

	switch task := timerTask.(type) {
	case *tasks.UserTimerTask:
		eventID = task.EventID
	case *tasks.ActivityTimeoutTask:
		eventID = task.EventID
	case *tasks.WorkflowTaskTimeoutTask:
		eventID = task.EventID
	case *tasks.WorkflowBackoffTimerTask:
		eventID = common.FirstEventID
	case *tasks.ActivityRetryTimerTask:
		eventID = task.EventID
	case *tasks.WorkflowTimeoutTask:
		eventID = common.FirstEventID
	case *tasks.DeleteHistoryEventTask:
		eventID = common.FirstEventID
	default:
		panic(errUnknownTimerTask)
	}
	return eventID
}

func getTimerTaskEventIDAndRetryable(
	timerTask tasks.Task,
	executionInfo *persistencespb.WorkflowExecutionInfo,
) (int64, bool) {
	eventID := getTimerTaskEventID(timerTask)
	retryable := true

	if task, ok := timerTask.(*tasks.WorkflowTaskTimeoutTask); ok {
		retryable = !(executionInfo.WorkflowTaskScheduleId == task.EventID && executionInfo.WorkflowTaskAttempt > 1)
	}

	return eventID, retryable
}
