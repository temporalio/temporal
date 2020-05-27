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

package task

import (
	"fmt"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/shard"
)

// InitializeLoggerForTask creates a new logger with additional tags for task info
func InitializeLoggerForTask(
	shardID int,
	task Info,
	logger log.Logger,
) log.Logger {

	taskLogger := logger.WithTags(
		tag.ShardID(shardID),
		tag.TaskID(task.GetTaskID()),
		tag.TaskVisibilityTimestamp(task.GetVisibilityTimestamp().UnixNano()),
		tag.FailoverVersion(task.GetVersion()),
		tag.TaskType(task.GetTaskType()),
		tag.WorkflowDomainID(task.GetDomainID()),
		tag.WorkflowID(task.GetWorkflowID()),
		tag.WorkflowRunID(task.GetRunID()),
	)

	switch task := task.(type) {
	case *persistence.TimerTaskInfo:
		taskLogger = taskLogger.WithTags(
			tag.WorkflowTimeoutType(int64(task.TimeoutType)),
		)
	case *persistence.TransferTaskInfo:
		// noop
	case *persistence.ReplicationTaskInfo:
		// noop
	default:
		taskLogger.Error(fmt.Sprintf("Unknown queue task type: %v", task))
	}

	return taskLogger
}

// GetTransferTaskMetricsScope returns the metrics scope index for transfer task
func GetTransferTaskMetricsScope(
	taskType int,
	isActive bool,
) int {
	switch taskType {
	case persistence.TransferTaskTypeActivityTask:
		if isActive {
			return metrics.TransferActiveTaskActivityScope
		}
		return metrics.TransferStandbyTaskActivityScope
	case persistence.TransferTaskTypeDecisionTask:
		if isActive {
			return metrics.TransferActiveTaskDecisionScope
		}
		return metrics.TransferStandbyTaskDecisionScope
	case persistence.TransferTaskTypeCloseExecution:
		if isActive {
			return metrics.TransferActiveTaskCloseExecutionScope
		}
		return metrics.TransferStandbyTaskCloseExecutionScope
	case persistence.TransferTaskTypeCancelExecution:
		if isActive {
			return metrics.TransferActiveTaskCancelExecutionScope
		}
		return metrics.TransferStandbyTaskCancelExecutionScope
	case persistence.TransferTaskTypeSignalExecution:
		if isActive {
			return metrics.TransferActiveTaskSignalExecutionScope
		}
		return metrics.TransferStandbyTaskSignalExecutionScope
	case persistence.TransferTaskTypeStartChildExecution:
		if isActive {
			return metrics.TransferActiveTaskStartChildExecutionScope
		}
		return metrics.TransferStandbyTaskStartChildExecutionScope
	case persistence.TransferTaskTypeRecordWorkflowStarted:
		if isActive {
			return metrics.TransferActiveTaskRecordWorkflowStartedScope
		}
		return metrics.TransferStandbyTaskRecordWorkflowStartedScope
	case persistence.TransferTaskTypeResetWorkflow:
		if isActive {
			return metrics.TransferActiveTaskResetWorkflowScope
		}
		return metrics.TransferStandbyTaskResetWorkflowScope
	case persistence.TransferTaskTypeUpsertWorkflowSearchAttributes:
		if isActive {
			return metrics.TransferActiveTaskUpsertWorkflowSearchAttributesScope
		}
		return metrics.TransferStandbyTaskUpsertWorkflowSearchAttributesScope
	default:
		if isActive {
			return metrics.TransferActiveQueueProcessorScope
		}
		return metrics.TransferStandbyQueueProcessorScope
	}
}

// GetTimerTaskMetricScope returns the metrics scope index for timer task
func GetTimerTaskMetricScope(
	taskType int,
	isActive bool,
) int {
	switch taskType {
	case persistence.TaskTypeDecisionTimeout:
		if isActive {
			return metrics.TimerActiveTaskDecisionTimeoutScope
		}
		return metrics.TimerStandbyTaskDecisionTimeoutScope
	case persistence.TaskTypeActivityTimeout:
		if isActive {
			return metrics.TimerActiveTaskActivityTimeoutScope
		}
		return metrics.TimerStandbyTaskActivityTimeoutScope
	case persistence.TaskTypeUserTimer:
		if isActive {
			return metrics.TimerActiveTaskUserTimerScope
		}
		return metrics.TimerStandbyTaskUserTimerScope
	case persistence.TaskTypeWorkflowTimeout:
		if isActive {
			return metrics.TimerActiveTaskWorkflowTimeoutScope
		}
		return metrics.TimerStandbyTaskWorkflowTimeoutScope
	case persistence.TaskTypeDeleteHistoryEvent:
		if isActive {
			return metrics.TimerActiveTaskDeleteHistoryEventScope
		}
		return metrics.TimerStandbyTaskDeleteHistoryEventScope
	case persistence.TaskTypeActivityRetryTimer:
		if isActive {
			return metrics.TimerActiveTaskActivityRetryTimerScope
		}
		return metrics.TimerStandbyTaskActivityRetryTimerScope
	case persistence.TaskTypeWorkflowBackoffTimer:
		if isActive {
			return metrics.TimerActiveTaskWorkflowBackoffTimerScope
		}
		return metrics.TimerStandbyTaskWorkflowBackoffTimerScope
	default:
		if isActive {
			return metrics.TimerActiveQueueProcessorScope
		}
		return metrics.TimerStandbyQueueProcessorScope
	}
}

// verifyTaskVersion, will return true if failover version check is successful
func verifyTaskVersion(
	shard shard.Context,
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
		logger.Debug(fmt.Sprintf("Cannot find domainID: %v, err: %v.", domainID, err))
		return false, err
	}
	if !domainEntry.IsGlobalDomain() {
		logger.Debug(fmt.Sprintf("DomainID: %v is not active, task: %v version check pass", domainID, task))
		return true, nil
	} else if version != taskVersion {
		logger.Debug(fmt.Sprintf("DomainID: %v is active, task: %v version != target version: %v.", domainID, task, version))
		return false, nil
	}
	logger.Debug(fmt.Sprintf("DomainID: %v is active, task: %v version == target version: %v.", domainID, task, version))
	return true, nil
}

// load mutable state, if mutable state's next event ID <= task ID, will attempt to refresh
// if still mutable state's next event ID <= task ID, will return nil, nil
func loadMutableStateForTimerTask(
	context execution.Context,
	timerTask *persistence.TimerTaskInfo,
	metricsClient metrics.Client,
	logger log.Logger,
) (execution.MutableState, error) {

	msBuilder, err := context.LoadWorkflowExecution()
	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
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
		context.Clear()

		msBuilder, err = context.LoadWorkflowExecution()
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

// load mutable state, if mutable state's next event ID <= task ID, will attempt to refresh
// if still mutable state's next event ID <= task ID, will return nil, nil
func loadMutableStateForTransferTask(
	context execution.Context,
	transferTask *persistence.TransferTaskInfo,
	metricsClient metrics.Client,
	logger log.Logger,
) (execution.MutableState, error) {

	msBuilder, err := context.LoadWorkflowExecution()
	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
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
		context.Clear()

		msBuilder, err = context.LoadWorkflowExecution()
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

func timeoutWorkflow(
	mutableState execution.MutableState,
	eventBatchFirstEventID int64,
) error {

	if decision, ok := mutableState.GetInFlightDecision(); ok {
		if err := execution.FailDecision(
			mutableState,
			decision,
			workflow.DecisionTaskFailedCauseForceCloseDecision,
		); err != nil {
			return err
		}
	}

	_, err := mutableState.AddTimeoutWorkflowEvent(
		eventBatchFirstEventID,
	)
	return err
}

func retryWorkflow(
	mutableState execution.MutableState,
	eventBatchFirstEventID int64,
	parentDomainName string,
	continueAsNewAttributes *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes,
) (execution.MutableState, error) {

	if decision, ok := mutableState.GetInFlightDecision(); ok {
		if err := execution.FailDecision(
			mutableState,
			decision,
			workflow.DecisionTaskFailedCauseForceCloseDecision,
		); err != nil {
			return nil, err
		}
	}

	_, newMutableState, err := mutableState.AddContinueAsNewEvent(
		eventBatchFirstEventID,
		common.EmptyEventID,
		parentDomainName,
		continueAsNewAttributes,
	)
	if err != nil {
		return nil, err
	}
	return newMutableState, nil
}
