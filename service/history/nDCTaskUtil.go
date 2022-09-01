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

	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
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
	logger = tasks.InitializeLogger(transferTask, logger)
	mutableState, err := LoadMutableStateForTask(
		ctx,
		wfContext,
		transferTask,
		getTransferTaskEventIDAndRetryable,
		metricsClient.Scope(metrics.TransferQueueProcessorScope),
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
	wfContext workflow.Context,
	timerTask tasks.Task,
	metricsClient metrics.Client,
	logger log.Logger,
) (workflow.MutableState, error) {
	logger = tasks.InitializeLogger(timerTask, logger)
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

func getTransferTaskEventIDAndRetryable(
	transferTask tasks.Task,
	executionInfo *persistencespb.WorkflowExecutionInfo,
) (int64, bool) {
	eventID := tasks.GetTransferTaskEventID(transferTask)
	retryable := true

	if task, ok := transferTask.(*tasks.WorkflowTask); ok {
		retryable = !(executionInfo.WorkflowTaskScheduledEventId == task.ScheduledEventID && executionInfo.WorkflowTaskAttempt > 1)
	}

	return eventID, retryable
}

func getTimerTaskEventIDAndRetryable(
	timerTask tasks.Task,
	executionInfo *persistencespb.WorkflowExecutionInfo,
) (int64, bool) {
	eventID := tasks.GetTimerTaskEventID(timerTask)
	retryable := true

	if task, ok := timerTask.(*tasks.WorkflowTaskTimeoutTask); ok {
		retryable = !(executionInfo.WorkflowTaskScheduledEventId == task.EventID && executionInfo.WorkflowTaskAttempt > 1)
	}

	return eventID, retryable
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
