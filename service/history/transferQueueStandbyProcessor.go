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
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/xdc"
)

type (
	transferQueueStandbyProcessorImpl struct {
		clusterName        string
		shard              ShardContext
		historyService     *historyEngineImpl
		options            *QueueProcessorOptions
		executionManager   persistence.ExecutionManager
		cache              *historyCache
		transferTaskFilter queueTaskFilter
		logger             log.Logger
		metricsClient      metrics.Client
		*transferQueueProcessorBase
		*queueProcessorBase
		queueAckMgr
		historyRereplicator xdc.HistoryRereplicator
	}
)

func newTransferQueueStandbyProcessor(clusterName string, shard ShardContext, historyService *historyEngineImpl,
	visibilityMgr persistence.VisibilityManager, matchingClient matching.Client, taskAllocator taskAllocator,
	historyRereplicator xdc.HistoryRereplicator, logger log.Logger) *transferQueueStandbyProcessorImpl {
	config := shard.GetConfig()
	options := &QueueProcessorOptions{
		StartDelay:                         config.TransferProcessorStartDelay,
		BatchSize:                          config.TransferTaskBatchSize,
		WorkerCount:                        config.TransferTaskWorkerCount,
		MaxPollRPS:                         config.TransferProcessorMaxPollRPS,
		MaxPollInterval:                    config.TransferProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:   config.TransferProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                  config.TransferProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient: config.TransferProcessorUpdateAckIntervalJitterCoefficient,
		MaxRetryCount:                      config.TransferTaskMaxRetryCount,
		MetricScope:                        metrics.TransferStandbyQueueProcessorScope,
	}
	logger = logger.WithTags(tag.ClusterName(clusterName))

	transferTaskFilter := func(qTask queueTaskInfo) (bool, error) {
		task, ok := qTask.(*persistence.TransferTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.verifyStandbyTask(clusterName, task.DomainID, task)
	}
	maxReadAckLevel := func() int64 {
		return shard.GetTransferMaxReadLevel()
	}
	updateClusterAckLevel := func(ackLevel int64) error {
		return shard.UpdateTransferClusterAckLevel(clusterName, ackLevel)
	}
	transferQueueShutdown := func() error {
		return nil
	}

	processor := &transferQueueStandbyProcessorImpl{
		clusterName:        clusterName,
		shard:              shard,
		historyService:     historyService,
		options:            options,
		executionManager:   shard.GetExecutionManager(),
		cache:              historyService.historyCache,
		transferTaskFilter: transferTaskFilter,
		logger:             logger,
		metricsClient:      historyService.metricsClient,
		transferQueueProcessorBase: newTransferQueueProcessorBase(
			shard, options, visibilityMgr, matchingClient,
			maxReadAckLevel, updateClusterAckLevel, transferQueueShutdown, logger,
		),
		historyRereplicator: historyRereplicator,
	}

	queueAckMgr := newQueueAckMgr(shard, options, processor, shard.GetTransferClusterAckLevel(clusterName), logger)
	queueProcessorBase := newQueueProcessorBase(clusterName, shard, options, processor, queueAckMgr, logger)
	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase

	return processor
}

func (t *transferQueueStandbyProcessorImpl) getTaskFilter() queueTaskFilter {
	return t.transferTaskFilter
}

func (t *transferQueueStandbyProcessorImpl) notifyNewTask() {
	t.queueProcessorBase.notifyNewTask()
}

func (t *transferQueueStandbyProcessorImpl) process(qTask queueTaskInfo, shouldProcessTask bool) (int, error) {
	task, ok := qTask.(*persistence.TransferTaskInfo)
	if !ok {
		return metrics.TransferStandbyQueueProcessorScope, errUnexpectedQueueTask
	}

	var err error
	lastAttempt := false
	switch task.TaskType {
	case persistence.TransferTaskTypeActivityTask:
		if shouldProcessTask {
			err = t.processActivityTask(task)
		}
		return metrics.TransferStandbyTaskActivityScope, err

	case persistence.TransferTaskTypeDecisionTask:
		if shouldProcessTask {
			err = t.processDecisionTask(task)
		}
		return metrics.TransferStandbyTaskDecisionScope, err

	case persistence.TransferTaskTypeCloseExecution:
		// guarantee the processing of workflow execution close
		err = t.processCloseExecution(task)
		return metrics.TransferStandbyTaskCloseExecutionScope, err

	case persistence.TransferTaskTypeCancelExecution:
		if shouldProcessTask {
			err = t.processCancelExecution(task, lastAttempt)
		}
		return metrics.TransferStandbyTaskCancelExecutionScope, err

	case persistence.TransferTaskTypeSignalExecution:
		if shouldProcessTask {
			err = t.processSignalExecution(task, lastAttempt)
		}
		return metrics.TransferStandbyTaskSignalExecutionScope, err

	case persistence.TransferTaskTypeStartChildExecution:
		if shouldProcessTask {
			err = t.processStartChildExecution(task, lastAttempt)
		}
		return metrics.TransferStandbyTaskStartChildExecutionScope, err

	case persistence.TransferTaskTypeRecordWorkflowStarted:
		if shouldProcessTask {
			err = t.processRecordWorkflowStarted(task)
		}
		return metrics.TransferStandbyTaskRecordWorkflowStartedScope, err

	default:
		return metrics.TransferStandbyQueueProcessorScope, errUnknownTransferTask
	}
}

func (t *transferQueueStandbyProcessorImpl) processActivityTask(transferTask *persistence.TransferTaskInfo) error {

	var activityScheduleToStartTimeout *int32
	processTaskIfClosed := false
	return t.processTransfer(processTaskIfClosed, transferTask, func(msBuilder mutableState) error {
		activityInfo, isPending := msBuilder.GetActivityInfo(transferTask.ScheduleID)

		if !isPending {
			return nil
		}
		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.DomainID, activityInfo.Version, transferTask.Version, transferTask)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		now := t.shard.GetCurrentTime(t.clusterName)
		pushToMatching := now.Sub(transferTask.GetVisibilityTimestamp()) > t.shard.GetConfig().StandbyClusterDelay()
		if activityInfo.StartedID == common.EmptyEventID {
			if !pushToMatching {
				return ErrTaskRetry
			}

			activityScheduleToStartTimeout = common.Int32Ptr(common.MinInt32(activityInfo.ScheduleToStartTimeout, common.MaxTaskTimeout))
			return nil
		}

		return nil
	}, func() error {
		if activityScheduleToStartTimeout == nil {
			return nil
		}

		timeout := common.MinInt32(*activityScheduleToStartTimeout, common.MaxTaskTimeout)
		err := t.pushActivity(transferTask, timeout)
		return err
	})
}

func (t *transferQueueStandbyProcessorImpl) processDecisionTask(transferTask *persistence.TransferTaskInfo) error {
	var decisionScheduleToStartTimeout *int32
	var tasklist *workflow.TaskList
	processTaskIfClosed := false

	return t.processTransfer(processTaskIfClosed, transferTask, func(msBuilder mutableState) error {
		decisionInfo, isPending := msBuilder.GetPendingDecision(transferTask.ScheduleID)
		if !isPending {
			return nil
		}

		executionInfo := msBuilder.GetExecutionInfo()
		workflowTimeout := executionInfo.WorkflowTimeout
		decisionTimeout := common.MinInt32(workflowTimeout, common.MaxTaskTimeout)

		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.DomainID, decisionInfo.Version, transferTask.Version, transferTask)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		now := t.shard.GetCurrentTime(t.clusterName)
		pushToMatching := now.Sub(transferTask.GetVisibilityTimestamp()) > t.shard.GetConfig().StandbyClusterDelay()
		if decisionInfo.StartedID == common.EmptyEventID {
			if !pushToMatching {
				return ErrTaskRetry
			}

			decisionScheduleToStartTimeout = common.Int32Ptr(decisionTimeout)
			tasklist = &workflow.TaskList{Name: &transferTask.TaskList}
			return nil
		}

		return nil
	}, func() error {
		if decisionScheduleToStartTimeout == nil {
			return nil
		}

		timeout := common.MinInt32(*decisionScheduleToStartTimeout, common.MaxTaskTimeout)
		err := t.pushDecision(transferTask, tasklist, timeout)
		return err
	})
}

func (t *transferQueueStandbyProcessorImpl) processCloseExecution(transferTask *persistence.TransferTaskInfo) error {

	processTaskIfClosed := true
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(transferTask.WorkflowID),
		RunId:      common.StringPtr(transferTask.RunID),
	}

	return t.processTransfer(processTaskIfClosed, transferTask, func(msBuilder mutableState) error {

		if msBuilder.IsWorkflowExecutionRunning() {
			// this can happen if workflow is reset.
			return nil
		}

		completionEvent, ok := msBuilder.GetCompletionEvent()
		var wfCloseTime int64
		if !ok {
			// This is need for backwards compatibility
			// TODO: remove usage of getLastUpdatedTimestamp after release 0.5.4, only use completionEvent timestamp
			wfCloseTime = getLastUpdatedTimestamp(msBuilder)
		} else {
			wfCloseTime = completionEvent.GetTimestamp()
		}

		executionInfo := msBuilder.GetExecutionInfo()
		workflowTypeName := executionInfo.WorkflowTypeName
		workflowStartTimestamp := executionInfo.StartTimestamp.UnixNano()
		workflowCloseTimestamp := wfCloseTime
		workflowCloseStatus := getWorkflowExecutionCloseStatus(executionInfo.CloseStatus)
		workflowHistoryLength := msBuilder.GetNextEventID() - 1
		startEvent, _ := msBuilder.GetStartEvent()
		workflowExecutionTimestamp := getWorkflowExecutionTimestamp(msBuilder, startEvent)
		visibilityMemo := getVisibilityMemo(startEvent)

		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.DomainID, msBuilder.GetLastWriteVersion(), transferTask.Version, transferTask)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		// DO NOT REPLY TO PARENT
		// since event replication should be done by active cluster

		return t.recordWorkflowClosed(
			transferTask.DomainID, execution, workflowTypeName, workflowStartTimestamp, workflowExecutionTimestamp.UnixNano(),
			workflowCloseTimestamp, workflowCloseStatus, workflowHistoryLength, transferTask.GetTaskID(), visibilityMemo,
		)
	}, standbyTaskPostActionNoOp) // no op post action, since the entire workflow is finished
}

func (t *transferQueueStandbyProcessorImpl) processCancelExecution(transferTask *persistence.TransferTaskInfo, lastAttempt bool) error {

	var nextEventID *int64
	postProcessingFn := func() error {
		return t.fetchHistoryAndVerifyOnce(transferTask, nextEventID, t.processCancelExecution)
	}
	if lastAttempt {
		postProcessingFn = func() error {
			return standbyTrensferTaskPostActionTaskDiscarded(nextEventID, transferTask, t.logger)
		}
	}

	processTaskIfClosed := false
	return t.processTransfer(processTaskIfClosed, transferTask, func(msBuilder mutableState) error {
		requestCancelInfo, isPending := msBuilder.GetRequestCancelInfo(transferTask.ScheduleID)

		if !isPending {
			return nil
		}
		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.DomainID, requestCancelInfo.Version, transferTask.Version, transferTask)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		if t.discardTask(transferTask) {
			// returning nil and set next event ID
			// the post action function below shall take over
			nextEventID = common.Int64Ptr(msBuilder.GetNextEventID())
			return nil
		}
		return ErrTaskRetry
	}, postProcessingFn)
}

func (t *transferQueueStandbyProcessorImpl) processSignalExecution(transferTask *persistence.TransferTaskInfo, lastAttempt bool) error {

	var nextEventID *int64
	postProcessingFn := func() error {
		return t.fetchHistoryAndVerifyOnce(transferTask, nextEventID, t.processSignalExecution)
	}
	if lastAttempt {
		postProcessingFn = func() error {
			return standbyTrensferTaskPostActionTaskDiscarded(nextEventID, transferTask, t.logger)
		}
	}

	processTaskIfClosed := false
	return t.processTransfer(processTaskIfClosed, transferTask, func(msBuilder mutableState) error {
		signalInfo, isPending := msBuilder.GetSignalInfo(transferTask.ScheduleID)

		if !isPending {
			return nil
		}
		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.DomainID, signalInfo.Version, transferTask.Version, transferTask)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		if t.discardTask(transferTask) {
			// returning nil and set next event ID
			// the post action function below shall take over
			nextEventID = common.Int64Ptr(msBuilder.GetNextEventID())
			return nil
		}
		return ErrTaskRetry
	}, postProcessingFn)
}

func (t *transferQueueStandbyProcessorImpl) processStartChildExecution(transferTask *persistence.TransferTaskInfo, lastAttempt bool) error {

	var nextEventID *int64
	postProcessingFn := func() error {
		return t.fetchHistoryAndVerifyOnce(transferTask, nextEventID, t.processStartChildExecution)
	}
	if lastAttempt {
		postProcessingFn = func() error {
			return standbyTrensferTaskPostActionTaskDiscarded(nextEventID, transferTask, t.logger)
		}
	}

	processTaskIfClosed := false
	return t.processTransfer(processTaskIfClosed, transferTask, func(msBuilder mutableState) error {
		childWorkflowInfo, isPending := msBuilder.GetChildExecutionInfo(transferTask.ScheduleID)

		if !isPending {
			return nil
		}
		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.DomainID, childWorkflowInfo.Version, transferTask.Version, transferTask)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		if childWorkflowInfo.StartedID != common.EmptyEventID {
			return nil
		}

		if t.discardTask(transferTask) {
			// returning nil and set next event ID
			// the post action function below shall take over
			nextEventID = common.Int64Ptr(msBuilder.GetNextEventID())
			return nil
		}
		return ErrTaskRetry
	}, postProcessingFn)
}

func (t *transferQueueStandbyProcessorImpl) processRecordWorkflowStarted(transferTask *persistence.TransferTaskInfo) error {
	processTaskIfClosed := false

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(transferTask.WorkflowID),
		RunId:      common.StringPtr(transferTask.RunID),
	}

	return t.processTransfer(processTaskIfClosed, transferTask, func(msBuilder mutableState) error {
		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.DomainID, msBuilder.GetStartVersion(), transferTask.Version, transferTask)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		executionInfo := msBuilder.GetExecutionInfo()
		workflowTimeout := executionInfo.WorkflowTimeout
		wfTypeName := executionInfo.WorkflowTypeName
		startTimestamp := executionInfo.StartTimestamp.UnixNano()
		startEvent, _ := msBuilder.GetStartEvent()
		executionTimestamp := getWorkflowExecutionTimestamp(msBuilder, startEvent)
		visibilityMemo := getVisibilityMemo(startEvent)

		return t.recordWorkflowStarted(transferTask.DomainID, execution, wfTypeName, startTimestamp, executionTimestamp.UnixNano(),
			workflowTimeout, transferTask.GetTaskID(), visibilityMemo)
	}, standbyTaskPostActionNoOp)
}

func (t *transferQueueStandbyProcessorImpl) processTransfer(processTaskIfClosed bool, transferTask *persistence.TransferTaskInfo,
	action func(mutableState) error, postAction func() error) (retError error) {
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

	err = action(msBuilder)
	if err != nil {
		return err
	}

	release(nil)
	err = postAction()
	return err
}

func (t *transferQueueStandbyProcessorImpl) getDomainIDAndWorkflowExecution(transferTask *persistence.TransferTaskInfo) (string, workflow.WorkflowExecution) {
	return transferTask.DomainID, workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(transferTask.WorkflowID),
		RunId:      common.StringPtr(transferTask.RunID),
	}
}

func (t *transferQueueStandbyProcessorImpl) fetchHistoryAndVerifyOnce(transferTask *persistence.TransferTaskInfo, nextEventID *int64,
	verifyFn func(*persistence.TransferTaskInfo, bool) error) error {

	if nextEventID == nil {
		return nil
	}
	err := t.fetchHistoryFromRemote(transferTask, *nextEventID)
	if err != nil {
		// fail to fetch events from remote, just discard the task
		return ErrTaskDiscarded
	}
	lastAttempt := true
	err = verifyFn(transferTask, lastAttempt)
	if err != nil {
		// task still pending, just discard the task
		return ErrTaskDiscarded
	}
	return nil
}

func (t *transferQueueStandbyProcessorImpl) fetchHistoryFromRemote(transferTask *persistence.TransferTaskInfo, nextEventID int64) error {

	t.metricsClient.IncCounter(metrics.HistoryRereplicationByTransferTaskScope, metrics.CadenceClientRequests)
	stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByTransferTaskScope, metrics.CadenceClientLatency)
	defer stopwatch.Stop()
	err := t.historyRereplicator.SendMultiWorkflowHistory(
		transferTask.DomainID, transferTask.WorkflowID,
		transferTask.RunID, nextEventID,
		transferTask.RunID, common.EndEventID, // use common.EndEventID since we do not know where is the end
	)
	if err != nil {
		t.logger.Error("Error re-replicating history from remote.",
			tag.WorkflowID(transferTask.WorkflowID),
			tag.WorkflowRunID(transferTask.RunID),
			tag.WorkflowDomainID(transferTask.DomainID),
			tag.WorkflowNextEventID(nextEventID),
			tag.SourceCluster(t.clusterName))
	}
	return err
}

func (t *transferQueueStandbyProcessorImpl) discardTask(transferTask *persistence.TransferTaskInfo) bool {
	// the current time got from shard is already delayed by t.shard.GetConfig().StandbyClusterDelay()
	// so discard will be true if task is delayed by 2*t.shard.GetConfig().StandbyClusterDelay()
	now := t.shard.GetCurrentTime(t.clusterName)
	return now.Sub(transferTask.GetVisibilityTimestamp()) > t.shard.GetConfig().StandbyClusterDelay()
}
