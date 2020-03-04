// Copyright (c) 2017 Uber Technologies, Inc.
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

	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
)

// verifyTaskVersion, will return true if failover version check is successful
func verifyTaskVersion(
	shard ShardContext,
	logger log.Logger,
	domainID string,
	version int64,
	taskVersion int64,
	task interface{},
) (bool, error) {

	if !shard.GetService().GetClusterMetadata().IsGlobalDomainEnabled() {
		return true, nil
	}

	// the first return value is whether this task is valid for further processing
	domainEntry, err := shard.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		logger.Debug("Cannot find domainID", tag.WorkflowDomainID(domainID), tag.Error(err))
		return false, err
	}
	if !domainEntry.IsGlobalDomain() {
		logger.Debug("DomainID is not active, task version check pass", tag.WorkflowDomainID(domainID), tag.Task(task))
		return true, nil
	} else if version != taskVersion {
		logger.Debug("DomainID is active, task version != target version", tag.WorkflowDomainID(domainID), tag.Task(task), tag.TaskVersion(version))
		return false, nil
	}
	logger.Debug("DomainID is active, task version == target version", tag.WorkflowDomainID(domainID), tag.Task(task), tag.TaskVersion(version))
	return true, nil
}

// load mutable state, if mutable state's next event ID <= task ID, will attempt to refresh
// if still mutable state's next event ID <= task ID, will return nil, nil
func loadMutableStateForTransferTask(
	context workflowExecutionContext,
	transferTask *persistenceblobs.TransferTaskInfo,
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
	// the exception is decision consistently fail
	// there will be no event generated, thus making the decision schedule ID == next event ID
	isDecisionRetry := transferTask.TaskType == persistence.TransferTaskTypeDecisionTask &&
		executionInfo.DecisionScheduleID == transferTask.ScheduleID &&
		executionInfo.DecisionAttempt > 0

	if transferTask.ScheduleID >= msBuilder.GetNextEventID() && !isDecisionRetry {
		metricsClient.IncCounter(metrics.TransferQueueProcessorScope, metrics.StaleMutableStateCounter)
		context.clear()

		msBuilder, err = context.loadWorkflowExecution()
		if err != nil {
			return nil, err
		}
		// after refresh, still mutable state's next event ID <= task ID
		if transferTask.ScheduleID >= msBuilder.GetNextEventID() {
			logger.Info("Transfer Task Processor: task event ID >= MS NextEventID, skip.",
				tag.WorkflowScheduleID(transferTask.ScheduleID),
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
	timerTask *persistenceblobs.TimerTaskInfo,
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
	// the exception is decision consistently fail
	// there will be no event generated, thus making the decision schedule ID == next event ID
	isDecisionRetry := timerTask.TaskType == persistence.TaskTypeDecisionTimeout &&
		executionInfo.DecisionScheduleID == timerTask.EventID &&
		executionInfo.DecisionAttempt > 0

	if timerTask.EventID >= msBuilder.GetNextEventID() && !isDecisionRetry {
		metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.StaleMutableStateCounter)
		context.clear()

		msBuilder, err = context.loadWorkflowExecution()
		if err != nil {
			return nil, err
		}
		// after refresh, still mutable state's next event ID <= task ID
		if timerTask.EventID >= msBuilder.GetNextEventID() {
			logger.Info("Timer Task Processor: task event ID >= MS NextEventID, skip.",
				tag.WorkflowEventID(timerTask.EventID),
				tag.WorkflowNextEventID(msBuilder.GetNextEventID()))
			return nil, nil
		}
	}
	return msBuilder, nil
}

func initializeLoggerForTask(
	shardID int,
	task queueTaskInfo,
	logger log.Logger,
) log.Logger {

	taskLogger := logger.WithTags(
		tag.ShardID(shardID),
		tag.TaskID(task.GetTaskID()),
		tag.FailoverVersion(task.GetVersion()),
		tag.TaskType(task.GetTaskType()),
		tag.WorkflowDomainIDBytes(task.GetDomainID()),
		tag.WorkflowID(task.GetWorkflowID()),
		tag.WorkflowRunIDBytes(task.GetRunID()),
	)

	switch task := task.(type) {
	case *persistenceblobs.TimerTaskInfo:
		taskLogger = taskLogger.WithTags(
			tag.WorkflowTimeoutType(int64(task.TimeoutType)),
		)
	case *persistenceblobs.TransferTaskInfo:
		// noop
	case *persistence.ReplicationTaskInfoWrapper:
		// noop
	default:
		taskLogger.Error(fmt.Sprintf("Unknown queue task type: %v", task))
	}

	return taskLogger
}
