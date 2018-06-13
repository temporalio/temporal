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
	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

// verifyActiveTask, will return true if task activeness check is successful
func verifyActiveTask(shard ShardContext, logger bark.Logger, taskDomainID string, task interface{}) (bool, error) {
	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	domainEntry, err := shard.GetDomainCache().GetDomainByID(taskDomainID)
	if err != nil {
		// it is possible that the domain is deleted
		// we should treat that domain as active
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			logger.Warnf("Cannot find domainID: %v, err: %v.", taskDomainID)
			return false, err
		}
		logger.Warnf("Cannot find domainID: %v, default to process task: %v.", taskDomainID, task)
		return true, nil
	}
	if domainEntry.IsGlobalDomain() && currentClusterName != domainEntry.GetReplicationConfig().ActiveClusterName {
		// timer task does not belong to cluster name
		logger.Debugf("DomainID: %v is not active, skip task: %v.", taskDomainID, task)
		return false, nil
	}
	logger.Debugf("DomainID: %v is active, process task: %v.", taskDomainID, task)
	return true, nil
}

// verifyFailoverActiveTask, will return true if task activeness check is successful
func verifyFailoverActiveTask(logger bark.Logger, targetDomainID string, taskDomainID string, task interface{}) (bool, error) {
	if targetDomainID == taskDomainID {
		logger.Debugf("Failover DomainID: %v is active, process task: %v.", taskDomainID, task)
		return true, nil
	}
	logger.Debugf("Failover DomainID: %v is not active, skip task: %v.", taskDomainID, task)
	return false, nil
}

// verifyStandbyTask, will return true if task standbyness check is successful
func verifyStandbyTask(shard ShardContext, logger bark.Logger, standbyCluster string, taskDomainID string, task interface{}) (bool, error) {
	domainEntry, err := shard.GetDomainCache().GetDomainByID(taskDomainID)
	if err != nil {
		// it is possible that the domain is deleted
		// we should treat that domain as not active
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			logger.Warnf("Cannot find domainID: %v, err: %v.", taskDomainID)
			return false, err
		}
		logger.Warnf("Cannot find domainID: %v, default to not process task: %v.", taskDomainID, task)
		return false, nil
	}
	if !domainEntry.IsGlobalDomain() {
		// non global domain, timer task does not belong here
		logger.Debugf("DomainID: %v is not global, skip task: %v.", taskDomainID, task)
		return false, nil
	} else if domainEntry.IsGlobalDomain() && domainEntry.GetReplicationConfig().ActiveClusterName != standbyCluster {
		// timer task does not belong here
		logger.Debugf("DomainID: %v is not standby, skip task: %v.", taskDomainID, task)
		return false, nil
	}
	logger.Debugf("DomainID: %v is standby, process task: %v.", taskDomainID, task)
	return true, nil
}

// verifyTaskVersion, will return true if failover version check is successful
func verifyTaskVersion(shard ShardContext, logger bark.Logger, domainID string, version int64, taskVersion int64, task interface{}) (bool, error) {
	if !shard.GetService().GetClusterMetadata().IsGlobalDomainEnabled() {
		return true, nil
	}

	// the first return value is whether this task is valid for further processing
	domainEntry, err := shard.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		logger.Debugf("Cannot find domainID: %v, err: %v.", domainID, task)
		return false, err
	}
	if !domainEntry.IsGlobalDomain() {
		logger.Debugf("DomainID: %v is not active, task: %v version check pass", domainID, task)
		return true, nil
	} else if version != taskVersion {
		logger.Debugf("DomainID: %v is active, task: %v version != target version: %v.", domainID, task, version)
		return false, nil
	}
	logger.Debugf("DomainID: %v is active, task: %v version == target version: %v.", domainID, task, version)
	return true, nil
}

// load mutable state, if mutable state's next event ID <= task ID, will attempt to refresh
// if still mutable state's next event ID <= task ID, will return nil, nil
func loadMutableStateForTransferTask(context *workflowExecutionContext, transferTask *persistence.TransferTaskInfo, metricsClient metrics.Client, logger bark.Logger) (mutableState, error) {
	msBuilder, err := context.loadWorkflowExecution()
	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			logger.Debugf("Cannot find execution: domainID: %v, workflowID: %v, runID: %v when processing transfer taskID: %v, eventID: %v",
				context.domainID, context.workflowExecution.GetWorkflowId(), context.workflowExecution.GetRunId(), transferTask.TaskID, transferTask.ScheduleID)
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
		logger.Debugf("Transfer Task Processor: task event ID: %v >= MS NextEventID: %v.", transferTask.ScheduleID, msBuilder.GetNextEventID())
		context.clear()

		msBuilder, err = context.loadWorkflowExecution()
		if err != nil {
			return nil, err
		}
		// after refresh, still mutable state's next event ID <= task ID
		if transferTask.ScheduleID >= msBuilder.GetNextEventID() {
			logger.Infof("Transfer Task Processor: task event ID: %v >= MS NextEventID: %v, skip.", transferTask.ScheduleID, msBuilder.GetNextEventID())
			return nil, nil
		}
	}
	return msBuilder, nil
}

// load mutable state, if mutable state's next event ID <= task ID, will attempt to refresh
// if still mutable state's next event ID <= task ID, will return nil, nil
func loadMutableStateForTimerTask(context *workflowExecutionContext, timerTask *persistence.TimerTaskInfo, metricsClient metrics.Client, logger bark.Logger) (mutableState, error) {
	msBuilder, err := context.loadWorkflowExecution()
	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			logger.Debugf("Cannot find execution: domainID: %v, workflowID: %v, runID: %v when processing timer timestamp %v, taskID: %v, eventID: %v",
				context.domainID, context.workflowExecution.GetWorkflowId(), context.workflowExecution.GetRunId(), timerTask.VisibilityTimestamp, timerTask.TaskID, timerTask.EventID)
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
		logger.Debugf("Timer Task Processor: task event ID: %v >= MS NextEventID: %v.", timerTask.EventID, msBuilder.GetNextEventID())
		context.clear()

		msBuilder, err = context.loadWorkflowExecution()
		if err != nil {
			return nil, err
		}
		// after refresh, still mutable state's next event ID <= task ID
		if timerTask.EventID >= msBuilder.GetNextEventID() {
			logger.Infof("Timer Task Processor: task event ID: %v >= MS NextEventID: %v, skip.", timerTask.EventID, msBuilder.GetNextEventID())
			return nil, nil
		}
	}
	return msBuilder, nil
}
