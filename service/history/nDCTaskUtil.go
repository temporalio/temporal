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
	"time"

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

// verifyTaskVersion, will return true if failover version check is successful
func verifyTaskVersion(
	shard shard.Context,
	logger log.Logger,
	namespaceID namespace.ID,
	version int64,
	taskVersion int64,
	task interface{},
) (bool, error) {

	if !shard.GetClusterMetadata().IsGlobalNamespaceEnabled() {
		return true, nil
	}

	// the first return value is whether this task is valid for further processing
	namespaceEntry, err := shard.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		logger.Debug("Cannot find namespaceID", tag.WorkflowNamespaceID(namespaceID.String()), tag.Error(err))
		return false, err
	}
	if !namespaceEntry.IsGlobalNamespace() {
		logger.Debug("NamespaceID is not active, task version check pass", tag.WorkflowNamespaceID(namespaceID.String()), tag.Task(task))
		return true, nil
	} else if version != taskVersion {
		logger.Debug("NamespaceID is active, task version != target version", tag.WorkflowNamespaceID(namespaceID.String()), tag.Task(task), tag.TaskVersion(version))
		return false, nil
	}
	logger.Debug("NamespaceID is active, task version == target version", tag.WorkflowNamespaceID(namespaceID.String()), tag.Task(task), tag.TaskVersion(version))
	return true, nil
}

// load mutable state, if mutable state's next event ID <= task ID, will attempt to refresh
// if still mutable state's next event ID <= task ID, will return nil, nil
func loadMutableStateForTransferTask(
	context workflow.Context,
	transferTask tasks.Task,
	metricsClient metrics.Client,
	logger log.Logger,
) (workflow.MutableState, error) {
	return loadMutableStateForTask(
		context,
		transferTask,
		getTransferTaskEventIDAndRetryable,
		metricsClient.Scope(metrics.TransferQueueProcessorScope),
		logger,
	)
}

// load mutable state, if mutable state's next event ID <= task ID, will attempt to refresh
// if still mutable state's next event ID <= task ID, will return nil, nil
func loadMutableStateForTimerTask(
	context workflow.Context,
	timerTask tasks.Task,
	metricsClient metrics.Client,
	logger log.Logger,
) (workflow.MutableState, error) {
	return loadMutableStateForTask(
		context,
		timerTask,
		getTimerTaskEventIDAndRetryable,
		metricsClient.Scope(metrics.TimerQueueProcessorScope),
		logger,
	)
}

func loadMutableStateForTask(
	context workflow.Context,
	task tasks.Task,
	taskEventIDAndRetryable func(task tasks.Task, executionInfo *persistencespb.WorkflowExecutionInfo) (int64, bool),
	scope metrics.Scope,
	logger log.Logger,
) (workflow.MutableState, error) {

	mutableState, err := context.LoadWorkflowExecution()
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
	context.Clear()

	mutableState, err = context.LoadWorkflowExecution()
	if err != nil {
		return nil, err
	}
	// after refresh, still mutable state's next event ID <= task's event ID
	if eventID >= mutableState.GetNextEventID() {
		logger.Info("Timer Task Processor: task event ID >= MS NextEventID, skip.",
			tag.WorkflowEventID(eventID),
			tag.WorkflowNextEventID(mutableState.GetNextEventID()),
		)
		return nil, nil
	}
	return mutableState, nil
}

func initializeLoggerForTask(
	shardID int32,
	task tasks.Task,
	logger log.Logger,
) log.Logger {
	taskLogger := log.With(
		logger,
		tag.ShardID(shardID),
		tag.TaskID(task.GetTaskID()),
		tag.TaskVisibilityTimestamp(task.GetVisibilityTime()),
		tag.Task(task),
	)
	return taskLogger
}

func getTransferTaskEventIDAndRetryable(
	transferTask tasks.Task,
	executionInfo *persistencespb.WorkflowExecutionInfo,
) (int64, bool) {
	eventID := int64(0)
	retryable := true

	switch task := transferTask.(type) {
	case *tasks.ActivityTask:
		eventID = task.ScheduleID
	case *tasks.WorkflowTask:
		eventID = task.ScheduleID
		retryable = !(executionInfo.WorkflowTaskScheduleId == task.ScheduleID && executionInfo.WorkflowTaskAttempt > 1)
	case *tasks.CloseExecutionTask:
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
	return eventID, retryable
}

func getTimerTaskEventIDAndRetryable(
	timerTask tasks.Task,
	executionInfo *persistencespb.WorkflowExecutionInfo,
) (int64, bool) {
	eventID := int64(0)
	retryable := true

	switch task := timerTask.(type) {
	case *tasks.UserTimerTask:
		eventID = task.EventID
	case *tasks.ActivityTimeoutTask:
		eventID = task.EventID
	case *tasks.WorkflowTaskTimeoutTask:
		eventID = task.EventID
		retryable = !(executionInfo.WorkflowTaskScheduleId == task.EventID && executionInfo.WorkflowTaskAttempt > 1)
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
	return eventID, retryable
}
