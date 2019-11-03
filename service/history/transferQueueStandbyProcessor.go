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
	"time"

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
		*transferQueueProcessorBase
		*queueProcessorBase
		queueAckMgr

		clusterName         string
		shard               ShardContext
		config              *Config
		historyService      *historyEngineImpl
		options             *QueueProcessorOptions
		executionManager    persistence.ExecutionManager
		cache               *historyCache
		transferTaskFilter  taskFilter
		logger              log.Logger
		metricsClient       metrics.Client
		historyRereplicator xdc.HistoryRereplicator
		nDCHistoryResender  xdc.NDCHistoryResender
	}
)

func newTransferQueueStandbyProcessor(
	clusterName string,
	shard ShardContext,
	historyService *historyEngineImpl,
	visibilityMgr persistence.VisibilityManager,
	matchingClient matching.Client,
	taskAllocator taskAllocator,
	historyRereplicator xdc.HistoryRereplicator,
	nDCHistoryResender xdc.NDCHistoryResender,
	logger log.Logger,
) *transferQueueStandbyProcessorImpl {

	config := shard.GetConfig()
	options := &QueueProcessorOptions{
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

	transferTaskFilter := func(taskInfo *taskInfo) (bool, error) {
		task, ok := taskInfo.task.(*persistence.TransferTaskInfo)
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
		config:             shard.GetConfig(),
		historyService:     historyService,
		options:            options,
		executionManager:   shard.GetExecutionManager(),
		cache:              historyService.historyCache,
		transferTaskFilter: transferTaskFilter,
		logger:             logger,
		metricsClient:      historyService.metricsClient,
		transferQueueProcessorBase: newTransferQueueProcessorBase(
			shard,
			options,
			visibilityMgr,
			matchingClient,
			maxReadAckLevel,
			updateClusterAckLevel,
			transferQueueShutdown,
			logger,
		),
		historyRereplicator: historyRereplicator,
		nDCHistoryResender:  nDCHistoryResender,
	}

	queueAckMgr := newQueueAckMgr(shard, options, processor, shard.GetTransferClusterAckLevel(clusterName), logger)
	queueProcessorBase := newQueueProcessorBase(clusterName, shard, options, processor, queueAckMgr, historyService.historyCache, logger)
	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase

	return processor
}

func (t *transferQueueStandbyProcessorImpl) getTaskFilter() taskFilter {
	return t.transferTaskFilter
}

func (t *transferQueueStandbyProcessorImpl) notifyNewTask() {
	t.queueProcessorBase.notifyNewTask()
}

func (t *transferQueueStandbyProcessorImpl) complete(
	taskInfo *taskInfo,
) {

	t.queueProcessorBase.complete(taskInfo.task)
}

func (t *transferQueueStandbyProcessorImpl) process(
	taskInfo *taskInfo,
) (int, error) {

	task, ok := taskInfo.task.(*persistence.TransferTaskInfo)
	if !ok {
		return metrics.TransferStandbyQueueProcessorScope, errUnexpectedQueueTask
	}

	var err error
	switch task.TaskType {
	case persistence.TransferTaskTypeActivityTask:
		if taskInfo.shouldProcessTask {
			err = t.processActivityTask(taskInfo)
		}
		return metrics.TransferStandbyTaskActivityScope, err

	case persistence.TransferTaskTypeDecisionTask:
		if taskInfo.shouldProcessTask {
			err = t.processDecisionTask(taskInfo)
		}
		return metrics.TransferStandbyTaskDecisionScope, err

	case persistence.TransferTaskTypeCloseExecution:
		// guarantee the processing of workflow execution close
		err = t.processCloseExecution(taskInfo)
		return metrics.TransferStandbyTaskCloseExecutionScope, err

	case persistence.TransferTaskTypeCancelExecution:
		if taskInfo.shouldProcessTask {
			err = t.processCancelExecution(taskInfo)
		}
		return metrics.TransferStandbyTaskCancelExecutionScope, err

	case persistence.TransferTaskTypeSignalExecution:
		if taskInfo.shouldProcessTask {
			err = t.processSignalExecution(taskInfo)
		}
		return metrics.TransferStandbyTaskSignalExecutionScope, err

	case persistence.TransferTaskTypeStartChildExecution:
		if taskInfo.shouldProcessTask {
			err = t.processStartChildExecution(taskInfo)
		}
		return metrics.TransferStandbyTaskStartChildExecutionScope, err

	case persistence.TransferTaskTypeRecordWorkflowStarted:
		if taskInfo.shouldProcessTask {
			err = t.processRecordWorkflowStarted(taskInfo)
		}
		return metrics.TransferStandbyTaskRecordWorkflowStartedScope, err

	case persistence.TransferTaskTypeResetWorkflow:
		// no reset needed for standby
		return metrics.TransferStandbyTaskResetWorkflowScope, err

	case persistence.TransferTaskTypeUpsertWorkflowSearchAttributes:
		if taskInfo.shouldProcessTask {
			err = t.processUpsertWorkflowSearchAttributes(taskInfo)
		}
		return metrics.TransferStandbyTaskUpsertWorkflowSearchAttributesScope, err

	default:
		return metrics.TransferStandbyQueueProcessorScope, errUnknownTransferTask
	}
}

func (t *transferQueueStandbyProcessorImpl) processActivityTask(
	taskInfo *taskInfo,
) error {

	processTaskIfClosed := false
	actionFn := func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

		transferTask := taskInfo.task.(*persistence.TransferTaskInfo)

		activityInfo, ok := mutableState.GetActivityInfo(transferTask.ScheduleID)
		if !ok {
			return nil, nil
		}

		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.DomainID, activityInfo.Version, transferTask.Version, transferTask)
		if err != nil || !ok {
			return nil, err
		}

		if activityInfo.StartedID == common.EmptyEventID {
			return newPushActivityToMatchingInfo(
				activityInfo.ScheduleToStartTimeout,
			), nil
		}

		return nil, nil
	}

	return t.processTransfer(
		processTaskIfClosed,
		taskInfo,
		actionFn,
		getStandbyPostActionFn(
			taskInfo,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(),
			t.config.StandbyTaskMissingEventsDiscardDelay(),
			t.pushActivity,
			t.pushActivity,
		),
	)
}

func (t *transferQueueStandbyProcessorImpl) processDecisionTask(
	taskInfo *taskInfo,
) error {

	processTaskIfClosed := false
	actionFn := func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

		transferTask := taskInfo.task.(*persistence.TransferTaskInfo)

		decisionInfo, ok := mutableState.GetDecisionInfo(transferTask.ScheduleID)
		if !ok {
			return nil, nil
		}

		executionInfo := mutableState.GetExecutionInfo()
		workflowTimeout := executionInfo.WorkflowTimeout
		decisionTimeout := common.MinInt32(workflowTimeout, common.MaxTaskTimeout)

		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.DomainID, decisionInfo.Version, transferTask.Version, transferTask)
		if err != nil || !ok {
			return nil, err
		}

		if decisionInfo.StartedID == common.EmptyEventID {
			return newPushDecisionToMatchingInfo(
				decisionTimeout,
				workflow.TaskList{Name: &transferTask.TaskList},
			), nil
		}

		return nil, nil
	}

	return t.processTransfer(
		processTaskIfClosed,
		taskInfo,
		actionFn,
		getStandbyPostActionFn(
			taskInfo,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(),
			t.config.StandbyTaskMissingEventsDiscardDelay(),
			t.pushDecision,
			t.pushDecision,
		),
	)
}

func (t *transferQueueStandbyProcessorImpl) processCloseExecution(
	taskInfo *taskInfo,
) error {

	processTaskIfClosed := true
	actionFn := func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

		transferTask := taskInfo.task.(*persistence.TransferTaskInfo)

		if mutableState.IsWorkflowExecutionRunning() {
			// this can happen if workflow is reset.
			return nil, nil
		}

		completionEvent, err := mutableState.GetCompletionEvent()
		if err != nil {
			return nil, err
		}
		wfCloseTime := completionEvent.GetTimestamp()

		executionInfo := mutableState.GetExecutionInfo()
		workflowTypeName := executionInfo.WorkflowTypeName
		workflowStartTimestamp := executionInfo.StartTimestamp.UnixNano()
		workflowCloseTimestamp := wfCloseTime
		workflowCloseStatus := persistence.ToThriftWorkflowExecutionCloseStatus(executionInfo.CloseStatus)
		workflowHistoryLength := mutableState.GetNextEventID() - 1
		startEvent, err := mutableState.GetStartEvent()
		if err != nil {
			return nil, err
		}
		workflowExecutionTimestamp := getWorkflowExecutionTimestamp(mutableState, startEvent)
		visibilityMemo := getWorkflowMemo(executionInfo.Memo)
		searchAttr := executionInfo.SearchAttributes

		lastWriteVersion, err := mutableState.GetLastWriteVersion()
		if err != nil {
			return nil, err
		}
		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.DomainID, lastWriteVersion, transferTask.Version, transferTask)
		if err != nil || !ok {
			return nil, err
		}

		// DO NOT REPLY TO PARENT
		// since event replication should be done by active cluster
		return nil, t.recordWorkflowClosed(
			transferTask.DomainID,
			transferTask.WorkflowID,
			transferTask.RunID,
			workflowTypeName,
			workflowStartTimestamp,
			workflowExecutionTimestamp.UnixNano(),
			workflowCloseTimestamp,
			workflowCloseStatus,
			workflowHistoryLength,
			transferTask.GetTaskID(),
			visibilityMemo,
			searchAttr,
		)
	}

	return t.processTransfer(
		processTaskIfClosed,
		taskInfo,
		actionFn,
		standbyTaskPostActionNoOp,
	) // no op post action, since the entire workflow is finished
}

func (t *transferQueueStandbyProcessorImpl) processCancelExecution(
	taskInfo *taskInfo,
) error {

	processTaskIfClosed := false
	actionFn := func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

		transferTask := taskInfo.task.(*persistence.TransferTaskInfo)

		requestCancelInfo, ok := mutableState.GetRequestCancelInfo(transferTask.ScheduleID)
		if !ok {
			return nil, nil
		}

		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.DomainID, requestCancelInfo.Version, transferTask.Version, transferTask)
		if err != nil || !ok {
			return nil, err
		}

		return getHistoryResendInfo(mutableState)
	}

	return t.processTransfer(
		processTaskIfClosed,
		taskInfo,
		actionFn,
		getStandbyPostActionFn(
			taskInfo,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(),
			t.config.StandbyTaskMissingEventsDiscardDelay(),
			t.fetchHistoryFromRemote,
			standbyTransferTaskPostActionTaskDiscarded,
		),
	)
}

func (t *transferQueueStandbyProcessorImpl) processSignalExecution(
	taskInfo *taskInfo,
) error {

	processTaskIfClosed := false
	actionFn := func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

		transferTask := taskInfo.task.(*persistence.TransferTaskInfo)

		signalInfo, ok := mutableState.GetSignalInfo(transferTask.ScheduleID)
		if !ok {
			return nil, nil
		}

		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.DomainID, signalInfo.Version, transferTask.Version, transferTask)
		if err != nil || !ok {
			return nil, err
		}

		return getHistoryResendInfo(mutableState)
	}

	return t.processTransfer(
		processTaskIfClosed,
		taskInfo,
		actionFn,
		getStandbyPostActionFn(
			taskInfo,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(),
			t.config.StandbyTaskMissingEventsDiscardDelay(),
			t.fetchHistoryFromRemote,
			standbyTransferTaskPostActionTaskDiscarded,
		),
	)
}

func (t *transferQueueStandbyProcessorImpl) processStartChildExecution(
	taskInfo *taskInfo,
) error {

	processTaskIfClosed := false
	actionFn := func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

		transferTask := taskInfo.task.(*persistence.TransferTaskInfo)

		childWorkflowInfo, ok := mutableState.GetChildExecutionInfo(transferTask.ScheduleID)
		if !ok {
			return nil, nil
		}

		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.DomainID, childWorkflowInfo.Version, transferTask.Version, transferTask)
		if err != nil || !ok {
			return nil, err
		}

		if childWorkflowInfo.StartedID != common.EmptyEventID {
			return nil, nil
		}

		return getHistoryResendInfo(mutableState)
	}

	return t.processTransfer(
		processTaskIfClosed,
		taskInfo,
		actionFn,
		getStandbyPostActionFn(
			taskInfo,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(),
			t.config.StandbyTaskMissingEventsDiscardDelay(),
			t.fetchHistoryFromRemote,
			standbyTransferTaskPostActionTaskDiscarded,
		),
	)
}

func (t *transferQueueStandbyProcessorImpl) processRecordWorkflowStarted(
	taskInfo *taskInfo,
) error {

	processTaskIfClosed := false
	return t.processTransfer(
		processTaskIfClosed,
		taskInfo,
		func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

			transferTask := taskInfo.task.(*persistence.TransferTaskInfo)

			return nil, t.processRecordWorkflowStartedOrUpsertHelper(transferTask, mutableState, true)
		},
		standbyTaskPostActionNoOp,
	)
}

func (t *transferQueueStandbyProcessorImpl) processUpsertWorkflowSearchAttributes(
	taskInfo *taskInfo,
) error {

	processTaskIfClosed := false
	return t.processTransfer(
		processTaskIfClosed,
		taskInfo,
		func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

			transferTask := taskInfo.task.(*persistence.TransferTaskInfo)
			return nil, t.processRecordWorkflowStartedOrUpsertHelper(transferTask, mutableState, false)
		},
		standbyTaskPostActionNoOp,
	)
}

func (t *transferQueueStandbyProcessorImpl) processRecordWorkflowStartedOrUpsertHelper(
	transferTask *persistence.TransferTaskInfo,
	mutableState mutableState,
	isRecordStart bool,
) error {

	// verify task version for RecordWorkflowStarted.
	// upsert doesn't require verifyTask, because it is just a sync of mutableState.
	if isRecordStart {
		startVersion, err := mutableState.GetStartVersion()
		if err != nil {
			return err
		}
		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.DomainID, startVersion, transferTask.Version, transferTask)
		if err != nil || !ok {
			return err
		}
	}

	executionInfo := mutableState.GetExecutionInfo()
	workflowTimeout := executionInfo.WorkflowTimeout
	wfTypeName := executionInfo.WorkflowTypeName
	startTimestamp := executionInfo.StartTimestamp.UnixNano()
	startEvent, err := mutableState.GetStartEvent()
	if err != nil {
		return err
	}
	executionTimestamp := getWorkflowExecutionTimestamp(mutableState, startEvent)
	visibilityMemo := getWorkflowMemo(executionInfo.Memo)
	searchAttr := copySearchAttributes(executionInfo.SearchAttributes)

	if isRecordStart {
		return t.recordWorkflowStarted(
			transferTask.DomainID,
			transferTask.WorkflowID,
			transferTask.RunID,
			wfTypeName,
			startTimestamp,
			executionTimestamp.UnixNano(),
			workflowTimeout,
			transferTask.GetTaskID(),
			visibilityMemo,
			searchAttr,
		)
	}
	return t.upsertWorkflowExecution(
		transferTask.DomainID,
		transferTask.WorkflowID,
		transferTask.RunID,
		wfTypeName,
		startTimestamp,
		executionTimestamp.UnixNano(),
		workflowTimeout,
		transferTask.GetTaskID(),
		visibilityMemo,
		searchAttr,
	)

}

func (t *transferQueueStandbyProcessorImpl) processTransfer(
	processTaskIfClosed bool,
	taskInfo *taskInfo,
	actionFn standbyActionFn,
	postActionFn standbyPostActionFn,
) (retError error) {

	transferTask := taskInfo.task.(*persistence.TransferTaskInfo)
	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(transferTask),
	)
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

	mutableState, err := loadMutableStateForTransferTask(context, transferTask, t.metricsClient, t.logger)
	if err != nil || mutableState == nil {
		return err
	}

	if !mutableState.IsWorkflowExecutionRunning() && !processTaskIfClosed {
		// workflow already finished, no need to process the timer
		return nil
	}

	historyResendInfo, err := actionFn(context, mutableState)
	if err != nil {
		return err
	}

	release(nil)
	return postActionFn(taskInfo, historyResendInfo, taskInfo.logger)
}

func (t *transferQueueStandbyProcessorImpl) pushActivity(
	task *taskInfo,
	postActionInfo interface{},
	logger log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	pushActivityInfo := postActionInfo.(*pushActivityToMatchingInfo)
	timeout := common.MinInt32(pushActivityInfo.activityScheduleToStartTimeout, common.MaxTaskTimeout)
	return t.transferQueueProcessorBase.pushActivity(
		task.task.(*persistence.TransferTaskInfo),
		timeout,
	)
}

func (t *transferQueueStandbyProcessorImpl) pushDecision(
	task *taskInfo,
	postActionInfo interface{},
	logger log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	pushDecisionInfo := postActionInfo.(*pushDecisionToMatchingInfo)
	timeout := common.MinInt32(pushDecisionInfo.decisionScheduleToStartTimeout, common.MaxTaskTimeout)
	return t.transferQueueProcessorBase.pushDecision(
		task.task.(*persistence.TransferTaskInfo),
		&pushDecisionInfo.tasklist,
		timeout,
	)
}

func (t *transferQueueStandbyProcessorImpl) fetchHistoryFromRemote(
	taskInfo *taskInfo,
	postActionInfo interface{},
	log log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	transferTask := taskInfo.task.(*persistence.TransferTaskInfo)
	resendInfo := postActionInfo.(*historyResendInfo)

	t.metricsClient.IncCounter(metrics.HistoryRereplicationByTransferTaskScope, metrics.CadenceClientRequests)
	stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByTransferTaskScope, metrics.CadenceClientLatency)
	defer stopwatch.Stop()

	var err error
	if resendInfo.lastEventID != nil && resendInfo.lastEventVersion != nil {
		err = t.nDCHistoryResender.SendSingleWorkflowHistory(
			transferTask.DomainID,
			transferTask.WorkflowID,
			transferTask.RunID,
			resendInfo.lastEventID,
			resendInfo.lastEventVersion,
			nil,
			nil,
		)
	} else if resendInfo.nextEventID != nil {
		err = t.historyRereplicator.SendMultiWorkflowHistory(
			transferTask.DomainID,
			transferTask.WorkflowID,
			transferTask.RunID,
			*resendInfo.nextEventID,
			transferTask.RunID,
			common.EndEventID, // use common.EndEventID since we do not know where is the end
		)
	} else {
		err = &workflow.InternalServiceError{
			Message: "transferQueueStandbyProcessor encounter empty historyResendInfo",
		}
	}

	if err != nil {
		t.logger.Error("Error re-replicating history from remote.",
			tag.ShardID(t.shard.GetShardID()),
			tag.WorkflowDomainID(transferTask.DomainID),
			tag.WorkflowID(transferTask.WorkflowID),
			tag.WorkflowRunID(transferTask.RunID),
			tag.SourceCluster(t.clusterName))
	}

	// return error so task processing logic will retry
	return ErrTaskRetry
}

func (t *transferQueueStandbyProcessorImpl) getCurrentTime() time.Time {
	return t.shard.GetCurrentTime(t.clusterName)
}
