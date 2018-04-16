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
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	transferQueueStandbyProcessorImpl struct {
		clusterName    string
		shard          ShardContext
		historyService *historyEngineImpl
		visibilityMgr  persistence.VisibilityManager
		cache          *historyCache
		logger         bark.Logger
		metricsClient  metrics.Client
	}
)

func newTransferQueueStandbyProcessor(clusterName string, shard ShardContext, historyService *historyEngineImpl,
	visibilityMgr persistence.VisibilityManager, logger bark.Logger) *transferQueueStandbyProcessorImpl {
	processor := &transferQueueStandbyProcessorImpl{
		clusterName:    clusterName,
		shard:          shard,
		historyService: historyService,
		visibilityMgr:  visibilityMgr,
		cache:          historyService.historyCache,
		logger:         logger,
		metricsClient:  historyService.metricsClient,
	}
	return processor
}

func (t *transferQueueStandbyProcessorImpl) process(transferTask *persistence.TransferTaskInfo) error {
	var err error
	scope := metrics.TransferQueueProcessorScope
	switch transferTask.TaskType {
	case persistence.TransferTaskTypeActivityTask:
		scope = metrics.TransferTaskActivityScope
		err = t.processActivityTask(transferTask)
	case persistence.TransferTaskTypeDecisionTask:
		scope = metrics.TransferTaskDecisionScope
		err = t.processDecisionTask(transferTask)
	case persistence.TransferTaskTypeCloseExecution:
		scope = metrics.TransferTaskCloseExecutionScope
		err = t.processCloseExecution(transferTask)
	case persistence.TransferTaskTypeCancelExecution:
		scope = metrics.TransferTaskCancelExecutionScope
		err = t.processCancelExecution(transferTask)
	case persistence.TransferTaskTypeSignalExecution:
		scope = metrics.TransferTaskSignalExecutionScope
		err = t.processSignalExecution(transferTask)
	case persistence.TransferTaskTypeStartChildExecution:
		scope = metrics.TransferTaskStartChildExecutionScope
		err = t.processStartChildExecution(transferTask)
	default:
		err = errUnknownTransferTask
	}

	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// Transfer task could fire after the execution is deleted.
			// In which case just ignore the error so we can complete the timer task.
			// TODO complete task
			err = nil
		}
		if err != nil {
			t.metricsClient.IncCounter(scope, metrics.TaskFailures)
		}
	}

	return err
}

func (t *transferQueueStandbyProcessorImpl) processActivityTask(transferTask *persistence.TransferTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TransferTaskActivityScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TransferTaskActivityScope, metrics.TaskLatency)
	defer sw.Stop()

	return t.processTransfer(transferTask, func(msBuilder *mutableStateBuilder) error {
		activityInfo, isPending := msBuilder.GetActivityInfo(transferTask.ScheduleID)
		if isPending && activityInfo.StartedID == emptyEventID {
			// TODO retry task
		} else {
			// the activity information will be deleted once the activity is finished
			// the activity started ID will not be emptyEventID once activity started
			// TODO complete task
		}
		return nil
	})
}

func (t *transferQueueStandbyProcessorImpl) processDecisionTask(transferTask *persistence.TransferTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TransferTaskDecisionScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TransferTaskDecisionScope, metrics.TaskLatency)
	defer sw.Stop()

	return t.processTransfer(transferTask, func(msBuilder *mutableStateBuilder) error {
		decisionInfo, isPending := msBuilder.GetPendingDecision(transferTask.ScheduleID)
		if isPending && decisionInfo.StartedID == emptyEventID {
			// TODO retry task
		} else {
			// the decision information will be deleted once the decision is finished
			// the decision started ID will not be emptyEventID once decision started
			// TODO complete task
		}
		return nil
	})
}

func (t *transferQueueStandbyProcessorImpl) processCloseExecution(transferTask *persistence.TransferTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TransferTaskCloseExecutionScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TransferTaskCloseExecutionScope, metrics.TaskLatency)
	defer sw.Stop()

	return t.processTransfer(transferTask, func(msBuilder *mutableStateBuilder) error {
		// here we actually grab a lock while doing a RPC call to database,
		// this is OK since this workflow is supposed to be closed (nobody is accessing this workflow)
		_, err := t.visibilityMgr.GetClosedWorkflowExecution(&persistence.GetClosedWorkflowExecutionRequest{
			DomainUUID: transferTask.DomainID,
			Execution: workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(transferTask.WorkflowID),
				RunId:      common.StringPtr(transferTask.RunID),
			},
		})
		if err != nil {
			if _, ok := err.(*workflow.EntityNotExistsError); ok {
				err = nil
				// means that we cannot find the closed workflow execution
				// TODO retry task
			}
		} else {
			// means that we can find the closed workflow execution
			// TODO complete task
		}

		return err
	})
}

func (t *transferQueueStandbyProcessorImpl) processCancelExecution(transferTask *persistence.TransferTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TransferTaskCancelExecutionScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TransferTaskCancelExecutionScope, metrics.TaskLatency)
	defer sw.Stop()

	return t.processTransfer(transferTask, func(msBuilder *mutableStateBuilder) error {
		_, isPending := msBuilder.GetRequestCancelInfo(transferTask.ScheduleID)
		if isPending {
			// TODO retry task
		} else {
			// the cancellation information will be deleted once the cancellation is finished
			// TODO complete task
		}
		return nil
	})
}

func (t *transferQueueStandbyProcessorImpl) processSignalExecution(transferTask *persistence.TransferTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TransferTaskSignalExecutionScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TransferTaskSignalExecutionScope, metrics.TaskLatency)
	defer sw.Stop()

	return t.processTransfer(transferTask, func(msBuilder *mutableStateBuilder) error {
		_, isPending := msBuilder.GetSignalInfo(transferTask.ScheduleID)
		if isPending {
			// TODO retry task
		} else {
			// the signal information will be deleted once the cancellation is signal
			// TODO complete task
		}
		return nil
	})
}

func (t *transferQueueStandbyProcessorImpl) processStartChildExecution(transferTask *persistence.TransferTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TransferTaskStartChildExecutionScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TransferTaskStartChildExecutionScope, metrics.TaskLatency)
	defer sw.Stop()

	return t.processTransfer(transferTask, func(msBuilder *mutableStateBuilder) error {
		childWorkflowInfo, isPending := msBuilder.GetChildExecutionInfo(transferTask.ScheduleID)
		if isPending && childWorkflowInfo.StartedID == emptyEventID {
			// TODO retry task
		} else {
			// the child workflow information will be deleted once the child workflow finishes
			// the child workflow started ID will not be emptyEventID once child workflow started
			// TODO complete task
		}
		return nil
	})
}

func (t *transferQueueStandbyProcessorImpl) processTransfer(transferTask *persistence.TransferTaskInfo, fn func(*mutableStateBuilder) error) (retError error) {
	context, release, err := t.cache.getOrCreateWorkflowExecution(t.getDomainIDAndWorkflowExecution(transferTask))
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

Process_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err := context.loadWorkflowExecution()
		if err != nil {
			return err
		}

		// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
		// some extreme cassandra failure cases.
		if transferTask.ScheduleID >= msBuilder.GetNextEventID() {
			t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.StaleMutableStateCounter)
			t.logger.Debugf("processExpiredUserTimer: timer event ID: %v >= MS NextEventID: %v.", transferTask.ScheduleID, msBuilder.GetNextEventID())
			// Reload workflow execution history
			context.clear()
			continue Process_Loop
		}

		if !msBuilder.isWorkflowExecutionRunning() {
			// workflow already finished, no need to process the timer
			// TODO complete task
			return nil
		}

		return fn(msBuilder)
	}
	return ErrMaxAttemptsExceeded
}

func (t *transferQueueStandbyProcessorImpl) getDomainIDAndWorkflowExecution(transferTask *persistence.TransferTaskInfo) (string, workflow.WorkflowExecution) {
	return transferTask.DomainID, workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(transferTask.WorkflowID),
		RunId:      common.StringPtr(transferTask.RunID),
	}
}
