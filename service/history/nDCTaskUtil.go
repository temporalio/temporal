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
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/shard"
)

const (
	refreshTaskTimeout = 30 * time.Second
)

// verifyTaskVersion, will return true if failover version check is successful
func verifyTaskVersion(
	shard shard.Context,
	logger log.Logger,
	namespaceID string,
	version int64,
	taskVersion int64,
	task interface{},
) (bool, error) {

	if !shard.GetService().GetClusterMetadata().IsGlobalNamespaceEnabled() {
		return true, nil
	}

	// the first return value is whether this task is valid for further processing
	namespaceEntry, err := shard.GetNamespaceCache().GetNamespaceByID(namespaceID)
	if err != nil {
		logger.Debug("Cannot find namespaceID", tag.WorkflowNamespaceID(namespaceID), tag.Error(err))
		return false, err
	}
	if !namespaceEntry.IsGlobalNamespace() {
		logger.Debug("NamespaceID is not active, task version check pass", tag.WorkflowNamespaceID(namespaceID), tag.Task(task))
		return true, nil
	} else if version != taskVersion {
		logger.Debug("NamespaceID is active, task version != target version", tag.WorkflowNamespaceID(namespaceID), tag.Task(task), tag.TaskVersion(version))
		return false, nil
	}
	logger.Debug("NamespaceID is active, task version == target version", tag.WorkflowNamespaceID(namespaceID), tag.Task(task), tag.TaskVersion(version))
	return true, nil
}

// load mutable state, if mutable state's next event ID <= task ID, will attempt to refresh
// if still mutable state's next event ID <= task ID, will return nil, nil
func loadMutableStateForTransferTask(
	context workflowExecutionContext,
	transferTask *persistencespb.TransferTaskInfo,
	metricsClient metrics.Client,
	logger log.Logger,
) (mutableState, error) {

	msBuilder, err := context.loadWorkflowExecution()
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil, nil
		}
		return nil, err
	}
	executionInfo := msBuilder.GetExecutionInfo()

	// check to see if cache needs to be refreshed as we could potentially have stale workflow execution
	// the exception is workflow task consistently fail
	// there will be no event generated, thus making the workflow task schedule ID == next event ID
	isWorkflowTaskRetry := transferTask.TaskType == enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK &&
		executionInfo.WorkflowTaskScheduleId == transferTask.GetScheduleId() &&
		executionInfo.WorkflowTaskAttempt > 1

	if transferTask.GetScheduleId() >= msBuilder.GetNextEventID() && !isWorkflowTaskRetry {
		metricsClient.IncCounter(metrics.TransferQueueProcessorScope, metrics.StaleMutableStateCounter)
		context.clear()

		msBuilder, err = context.loadWorkflowExecution()
		if err != nil {
			return nil, err
		}
		// after refresh, still mutable state's next event ID <= task ID
		if transferTask.GetScheduleId() >= msBuilder.GetNextEventID() {
			logger.Info("Transfer Task Processor: task event ID >= MS NextEventID, skip.",
				tag.WorkflowScheduleID(transferTask.GetScheduleId()),
				tag.WorkflowNextEventID(msBuilder.GetNextEventID()))
			return nil, nil
		}
	}
	return msBuilder, nil
}

// load mutable state, if mutable state's next event ID <= task ID, will attempt to refresh
// if still mutable state's next event ID <= task ID, will return nil, nil
func loadMutableStateForTimerTask(
	context workflowExecutionContext,
	timerTask *persistencespb.TimerTaskInfo,
	metricsClient metrics.Client,
	logger log.Logger,
) (mutableState, error) {

	msBuilder, err := context.loadWorkflowExecution()
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil, nil
		}
		return nil, err
	}
	executionInfo := msBuilder.GetExecutionInfo()

	// check to see if cache needs to be refreshed as we could potentially have stale workflow execution
	// the exception is workflow task consistently fail
	// there will be no event generated, thus making the workflow task schedule ID == next event ID
	isWorkflowTaskRetry := timerTask.TaskType == enumsspb.TASK_TYPE_WORKFLOW_TASK_TIMEOUT &&
		executionInfo.WorkflowTaskScheduleId == timerTask.GetEventId() &&
		executionInfo.WorkflowTaskAttempt > 1

	if timerTask.GetEventId() >= msBuilder.GetNextEventID() && !isWorkflowTaskRetry {
		metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.StaleMutableStateCounter)
		context.clear()

		msBuilder, err = context.loadWorkflowExecution()
		if err != nil {
			return nil, err
		}
		// after refresh, still mutable state's next event ID <= task ID
		if timerTask.GetEventId() >= msBuilder.GetNextEventID() {
			logger.Info("Timer Task Processor: task event ID >= MS NextEventID, skip.",
				tag.WorkflowEventID(timerTask.GetEventId()),
				tag.WorkflowNextEventID(msBuilder.GetNextEventID()))
			return nil, nil
		}
	}
	return msBuilder, nil
}

func initializeLoggerForTask(
	shardID int32,
	task queueTaskInfo,
	logger log.Logger,
) log.Logger {
	taskLogger := log.With(
		logger,
		tag.ShardID(shardID),
		tag.TaskID(task.GetTaskId()),
		tag.TaskVisibilityTimestamp(task.GetVisibilityTime().UnixNano()),
		tag.FailoverVersion(task.GetVersion()),
		tag.TaskType(task.GetTaskType()),
		tag.WorkflowNamespaceID(task.GetNamespaceId()),
		tag.WorkflowID(task.GetWorkflowId()),
		tag.WorkflowRunID(task.GetRunId()),
	)

	switch task := task.(type) {
	case *persistencespb.TimerTaskInfo:
		log.With(taskLogger, tag.WorkflowTimeoutType(task.TimeoutType))
	case *persistencespb.TransferTaskInfo,
		*persistencespb.VisibilityTaskInfo,
		*persistence.ReplicationTaskInfoWrapper:
		// noop
	default:
		taskLogger.Error(fmt.Sprintf("Unknown queue task type: %v", task))
	}

	return taskLogger
}
