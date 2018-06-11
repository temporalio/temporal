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
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	transferQueueStandbyProcessorImpl struct {
		clusterName        string
		shard              ShardContext
		historyService     *historyEngineImpl
		options            *QueueProcessorOptions
		executionManager   persistence.ExecutionManager
		visibilityMgr      persistence.VisibilityManager
		cache              *historyCache
		transferTaskFilter transferTaskFilter
		logger             bark.Logger
		metricsClient      metrics.Client
		*transferQueueProcessorBase
		*queueProcessorBase
		queueAckMgr
	}
)

func newTransferQueueStandbyProcessor(clusterName string, shard ShardContext, historyService *historyEngineImpl,
	visibilityMgr persistence.VisibilityManager, logger bark.Logger) *transferQueueStandbyProcessorImpl {
	config := shard.GetConfig()
	options := &QueueProcessorOptions{
		BatchSize:            config.TransferTaskBatchSize,
		WorkerCount:          config.TransferTaskWorkerCount,
		MaxPollRPS:           config.TransferProcessorMaxPollRPS,
		MaxPollInterval:      config.TransferProcessorMaxPollInterval,
		UpdateAckInterval:    config.TransferProcessorUpdateAckInterval,
		MaxRetryCount:        config.TransferTaskMaxRetryCount,
		MetricScope:          metrics.TransferQueueProcessorScope,
		UpdateShardTaskCount: config.TransferProcessorUpdateShardTaskCount,
	}
	logger = logger.WithFields(bark.Fields{
		logging.TagWorkflowCluster: clusterName,
	})

	transferTaskFilter := func(task *persistence.TransferTaskInfo) (bool, error) {
		domainEntry, err := shard.GetDomainCache().GetDomainByID(task.DomainID)
		if err != nil {
			return false, err
		}
		if !domainEntry.IsGlobalDomain() {
			// non global domain, timer task does not belong here
			return false, nil
		} else if domainEntry.IsGlobalDomain() && domainEntry.GetReplicationConfig().ActiveClusterName != clusterName {
			// timer task does not belong here
			return false, nil
		}
		return true, nil
	}
	maxReadAckLevel := func() int64 {
		return shard.GetTransferMaxReadLevel()
	}
	updateClusterAckLevel := func(ackLevel int64) error {
		return shard.UpdateTransferClusterAckLevel(clusterName, ackLevel)
	}

	processor := &transferQueueStandbyProcessorImpl{
		clusterName:                clusterName,
		shard:                      shard,
		historyService:             historyService,
		options:                    options,
		executionManager:           shard.GetExecutionManager(),
		visibilityMgr:              visibilityMgr,
		cache:                      historyService.historyCache,
		transferTaskFilter:         transferTaskFilter,
		logger:                     logger,
		metricsClient:              historyService.metricsClient,
		transferQueueProcessorBase: newTransferQueueProcessorBase(shard, options, maxReadAckLevel, updateClusterAckLevel),
	}
	queueAckMgr := newQueueAckMgr(shard, options, processor, shard.GetTransferClusterAckLevel(clusterName), logger)
	queueProcessorBase := newQueueProcessorBase(shard, options, processor, queueAckMgr, logger)
	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase

	return processor
}

func (t *transferQueueStandbyProcessorImpl) notifyNewTask() {
	t.queueProcessorBase.notifyNewTask()
}

func (t *transferQueueStandbyProcessorImpl) process(qTask queueTaskInfo) error {
	task, ok := qTask.(*persistence.TransferTaskInfo)
	if !ok {
		return errUnexpectedQueueTask
	}
	ok, err := t.transferTaskFilter(task)
	if err != nil {
		return err
	} else if !ok {
		t.queueAckMgr.completeQueueTask(task.TaskID)
		return nil
	}

	scope := metrics.TransferQueueProcessorScope
	switch task.TaskType {
	case persistence.TransferTaskTypeActivityTask:
		scope = metrics.TransferTaskActivityScope
		err = t.processActivityTask(task)
	case persistence.TransferTaskTypeDecisionTask:
		scope = metrics.TransferTaskDecisionScope
		err = t.processDecisionTask(task)
	case persistence.TransferTaskTypeCloseExecution:
		scope = metrics.TransferTaskCloseExecutionScope
		err = t.processCloseExecution(task)
	case persistence.TransferTaskTypeCancelExecution:
		scope = metrics.TransferTaskCancelExecutionScope
		err = t.processCancelExecution(task)
	case persistence.TransferTaskTypeSignalExecution:
		scope = metrics.TransferTaskSignalExecutionScope
		err = t.processSignalExecution(task)
	case persistence.TransferTaskTypeStartChildExecution:
		scope = metrics.TransferTaskStartChildExecutionScope
		err = t.processStartChildExecution(task)
	default:
		err = errUnknownTransferTask
	}

	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// Transfer task could fire after the execution is deleted.
			// In which case just ignore the error so we can complete the timer task.
			t.queueAckMgr.completeQueueTask(task.TaskID)
			err = nil
		}
		if err != nil {
			t.metricsClient.IncCounter(scope, metrics.TaskFailures)
		}
	} else {
		t.queueAckMgr.completeQueueTask(task.TaskID)
	}

	return err
}

func (t *transferQueueStandbyProcessorImpl) processActivityTask(transferTask *persistence.TransferTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TransferTaskActivityScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TransferTaskActivityScope, metrics.TaskLatency)
	defer sw.Stop()

	processTaskIfClosed := false
	return t.processTransfer(processTaskIfClosed, transferTask, func(msBuilder mutableState) error {
		activityInfo, isPending := msBuilder.GetActivityInfo(transferTask.ScheduleID)

		if !isPending {
			return nil
		}
		ok, err := verifyTransferTaskVersion(t.shard, transferTask.DomainID, activityInfo.Version, transferTask)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		if activityInfo.StartedID == common.EmptyEventID {
			return ErrTaskRetry
		}
		return nil
	})
}

func (t *transferQueueStandbyProcessorImpl) processDecisionTask(transferTask *persistence.TransferTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TransferTaskDecisionScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TransferTaskDecisionScope, metrics.TaskLatency)
	defer sw.Stop()

	processTaskIfClosed := false
	return t.processTransfer(processTaskIfClosed, transferTask, func(msBuilder mutableState) error {
		decisionInfo, isPending := msBuilder.GetPendingDecision(transferTask.ScheduleID)

		if !isPending {
			if transferTask.ScheduleID == common.FirstEventID+1 {
				return t.recordWorkflowStarted(msBuilder)
			}
			return nil
		}
		ok, err := verifyTransferTaskVersion(t.shard, transferTask.DomainID, decisionInfo.Version, transferTask)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		if decisionInfo.StartedID == common.EmptyEventID {
			return ErrTaskRetry
		}

		if transferTask.ScheduleID == common.FirstEventID+1 {
			return t.recordWorkflowStarted(msBuilder)
		}

		return nil
	})
}

func (t *transferQueueStandbyProcessorImpl) processCloseExecution(transferTask *persistence.TransferTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TransferTaskCloseExecutionScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TransferTaskCloseExecutionScope, metrics.TaskLatency)
	defer sw.Stop()

	processTaskIfClosed := true
	return t.processTransfer(processTaskIfClosed, transferTask, func(msBuilder mutableState) error {

		ok, err := verifyTransferTaskVersion(t.shard, transferTask.DomainID, msBuilder.GetLastWriteVersion(), transferTask)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		// DO NOT REPLY TO PARENT
		// since event replication should be done by active cluster

		// Record closing in visibility store
		retentionSeconds := int64(0)
		domainEntry, err := t.shard.GetDomainCache().GetDomainByID(transferTask.DomainID)
		if err != nil {
			if _, ok := err.(*workflow.EntityNotExistsError); !ok {
				return err
			}
			// it is possible that the domain got deleted. Use default retention.
		} else {
			// retention in domain config is in days, convert to seconds
			retentionSeconds = int64(domainEntry.GetConfig().Retention) * 24 * 60 * 60
		}

		return t.recordWorkflowClosed(msBuilder, retentionSeconds)
	})
}

func (t *transferQueueStandbyProcessorImpl) processCancelExecution(transferTask *persistence.TransferTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TransferTaskCancelExecutionScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TransferTaskCancelExecutionScope, metrics.TaskLatency)
	defer sw.Stop()

	processTaskIfClosed := false
	return t.processTransfer(processTaskIfClosed, transferTask, func(msBuilder mutableState) error {
		requestCancelInfo, isPending := msBuilder.GetRequestCancelInfo(transferTask.ScheduleID)

		if !isPending {
			return nil
		}
		ok, err := verifyTransferTaskVersion(t.shard, transferTask.DomainID, requestCancelInfo.Version, transferTask)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		return ErrTaskRetry
	})
}

func (t *transferQueueStandbyProcessorImpl) processSignalExecution(transferTask *persistence.TransferTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TransferTaskSignalExecutionScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TransferTaskSignalExecutionScope, metrics.TaskLatency)
	defer sw.Stop()

	processTaskIfClosed := false
	return t.processTransfer(processTaskIfClosed, transferTask, func(msBuilder mutableState) error {
		signalInfo, isPending := msBuilder.GetSignalInfo(transferTask.ScheduleID)

		if !isPending {
			return nil
		}
		ok, err := verifyTransferTaskVersion(t.shard, transferTask.DomainID, signalInfo.Version, transferTask)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		return ErrTaskRetry
	})
}

func (t *transferQueueStandbyProcessorImpl) processStartChildExecution(transferTask *persistence.TransferTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TransferTaskStartChildExecutionScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TransferTaskStartChildExecutionScope, metrics.TaskLatency)
	defer sw.Stop()

	processTaskIfClosed := false
	return t.processTransfer(processTaskIfClosed, transferTask, func(msBuilder mutableState) error {
		childWorkflowInfo, isPending := msBuilder.GetChildExecutionInfo(transferTask.ScheduleID)

		if !isPending {
			return nil
		}
		ok, err := verifyTransferTaskVersion(t.shard, transferTask.DomainID, childWorkflowInfo.Version, transferTask)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		if childWorkflowInfo.StartedID == common.EmptyEventID {
			return ErrTaskRetry
		}
		return nil
	})
}

func (t *transferQueueStandbyProcessorImpl) processTransfer(processTaskIfClosed bool, transferTask *persistence.TransferTaskInfo, fn func(mutableState) error) (retError error) {
	context, release, err := t.cache.getOrCreateWorkflowExecution(t.getDomainIDAndWorkflowExecution(transferTask))
	if err != nil {
		return err
	}
	defer func() {
		if retError == ErrTaskRetry {
			release(nil)
		} else {
			release(retError)
		}
	}()

	msBuilder, err := loadMutableStateForTransferTask(context, transferTask, t.metricsClient, t.logger)
	if err != nil {
		return err
	} else if msBuilder == nil {
		return nil
	}

	if !processTaskIfClosed && !msBuilder.IsWorkflowExecutionRunning() {
		// workflow already finished, no need to process the timer
		return nil
	}

	return fn(msBuilder)
}

func (t *transferQueueStandbyProcessorImpl) getDomainIDAndWorkflowExecution(transferTask *persistence.TransferTaskInfo) (string, workflow.WorkflowExecution) {
	return transferTask.DomainID, workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(transferTask.WorkflowID),
		RunId:      common.StringPtr(transferTask.RunID),
	}
}

func (t *transferQueueStandbyProcessorImpl) recordWorkflowStarted(msBuilder mutableState) error {
	executionInfo := msBuilder.GetExecutionInfo()
	return t.visibilityMgr.RecordWorkflowExecutionStarted(&persistence.RecordWorkflowExecutionStartedRequest{
		DomainUUID: executionInfo.DomainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(executionInfo.WorkflowID),
			RunId:      common.StringPtr(executionInfo.RunID),
		},
		WorkflowTypeName: executionInfo.WorkflowTypeName,
		StartTimestamp:   executionInfo.StartTimestamp.UnixNano(),
		WorkflowTimeout:  int64(executionInfo.WorkflowTimeout),
	})
}

func (t *transferQueueStandbyProcessorImpl) recordWorkflowClosed(msBuilder mutableState, retentionSeconds int64) error {
	executionInfo := msBuilder.GetExecutionInfo()
	return t.visibilityMgr.RecordWorkflowExecutionClosed(&persistence.RecordWorkflowExecutionClosedRequest{
		DomainUUID: executionInfo.DomainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(executionInfo.WorkflowID),
			RunId:      common.StringPtr(executionInfo.RunID),
		},
		WorkflowTypeName: executionInfo.WorkflowTypeName,
		StartTimestamp:   executionInfo.StartTimestamp.UnixNano(),
		CloseTimestamp:   msBuilder.GetLastUpdatedTimestamp(),
		Status:           getWorkflowExecutionCloseStatus(executionInfo.CloseStatus),
		HistoryLength:    msBuilder.GetNextEventID(),
		RetentionSeconds: retentionSeconds,
	})
}
