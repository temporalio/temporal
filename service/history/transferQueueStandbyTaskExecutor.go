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
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
)

type (
	transferQueueStandbyTaskExecutor struct {
		*transferQueueTaskExecutorBase

		clusterName        string
		adminClient        adminservice.AdminServiceClient
		nDCHistoryResender xdc.NDCHistoryResender
	}
)

func newTransferQueueStandbyTaskExecutor(
	shard shard.Context,
	historyService *historyEngineImpl,
	nDCHistoryResender xdc.NDCHistoryResender,
	logger log.Logger,
	metricsClient metrics.Client,
	clusterName string,
	config *configs.Config,
) queueTaskExecutor {
	return &transferQueueStandbyTaskExecutor{
		transferQueueTaskExecutorBase: newTransferQueueTaskExecutorBase(
			shard,
			historyService,
			logger,
			metricsClient,
			config,
		),
		clusterName:        clusterName,
		adminClient:        shard.GetService().GetRemoteAdminClient(clusterName),
		nDCHistoryResender: nDCHistoryResender,
	}
}

func (t *transferQueueStandbyTaskExecutor) execute(
	taskInfo queueTaskInfo,
	shouldProcessTask bool,
) error {

	transferTask, ok := taskInfo.(*persistencespb.TransferTaskInfo)
	if !ok {
		return errUnexpectedQueueTask
	}

	if !shouldProcessTask &&
		transferTask.TaskType != enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION {
		// guarantee the processing of workflow execution close
		return nil
	}

	switch transferTask.TaskType {
	case enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK:
		return t.processActivityTask(transferTask)
	case enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK:
		return t.processWorkflowTask(transferTask)
	case enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION:
		return t.processCloseExecution(transferTask)
	case enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION:
		return t.processCancelExecution(transferTask)
	case enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION:
		return t.processSignalExecution(transferTask)
	case enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION:
		return t.processStartChildExecution(transferTask)
	case enumsspb.TASK_TYPE_TRANSFER_RESET_WORKFLOW:
		// no reset needed for standby
		// TODO: add error logs
		return nil
	default:
		return errUnknownTransferTask
	}
}

func (t *transferQueueStandbyTaskExecutor) processActivityTask(
	transferTask *persistencespb.TransferTaskInfo,
) error {

	processTaskIfClosed := false
	actionFn := func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

		activityInfo, ok := mutableState.GetActivityInfo(transferTask.GetScheduleId())
		if !ok {
			return nil, nil
		}

		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.GetNamespaceId(), activityInfo.Version, transferTask.Version, transferTask)
		if err != nil || !ok {
			return nil, err
		}

		if activityInfo.StartedId == common.EmptyEventID {
			return newPushActivityToMatchingInfo(*activityInfo.ScheduleToStartTimeout), nil
		}

		return nil, nil
	}

	return t.processTransfer(
		processTaskIfClosed,
		transferTask,
		actionFn,
		getStandbyPostActionFn(
			transferTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(),
			t.config.StandbyTaskMissingEventsDiscardDelay(),
			t.pushActivity,
			t.pushActivity,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processWorkflowTask(
	transferTask *persistencespb.TransferTaskInfo,
) error {

	processTaskIfClosed := false
	actionFn := func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

		wtInfo, ok := mutableState.GetWorkflowTaskInfo(transferTask.GetScheduleId())
		if !ok {
			return nil, nil
		}

		executionInfo := mutableState.GetExecutionInfo()

		var taskQueue *taskqueuepb.TaskQueue
		var taskScheduleToStartTimeoutSeconds = int64(0)
		if mutableState.GetExecutionInfo().TaskQueue != transferTask.TaskQueue {
			// this workflowTask is an sticky workflowTask
			// there shall already be an timer set
			taskQueue = &taskqueuepb.TaskQueue{
				Name: transferTask.TaskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_STICKY,
			}
			taskScheduleToStartTimeoutSeconds = int64(timestamp.DurationValue(executionInfo.StickyScheduleToStartTimeout).Seconds())
		} else {
			taskQueue = &taskqueuepb.TaskQueue{
				Name: transferTask.TaskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			}
			workflowRunTimeout := timestamp.DurationValue(executionInfo.WorkflowRunTimeout)
			taskScheduleToStartTimeoutSeconds = int64(workflowRunTimeout.Round(time.Second).Seconds())
		}

		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.GetNamespaceId(), wtInfo.Version, transferTask.Version, transferTask)
		if err != nil || !ok {
			return nil, err
		}

		if wtInfo.StartedID == common.EmptyEventID {
			return newPushWorkflowTaskToMatchingInfo(
				taskScheduleToStartTimeoutSeconds,
				*taskQueue,
			), nil
		}

		return nil, nil
	}

	return t.processTransfer(
		processTaskIfClosed,
		transferTask,
		actionFn,
		getStandbyPostActionFn(
			transferTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(),
			t.config.StandbyTaskMissingEventsDiscardDelay(),
			t.pushWorkflowTask,
			t.pushWorkflowTask,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processCloseExecution(
	transferTask *persistencespb.TransferTaskInfo,
) error {

	processTaskIfClosed := true
	actionFn := func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

		if mutableState.IsWorkflowExecutionRunning() {
			// this can happen if workflow is reset.
			return nil, nil
		}

		completionEvent, err := mutableState.GetCompletionEvent()
		if err != nil {
			return nil, err
		}
		wfCloseTime := timestamp.TimeValue(completionEvent.GetEventTime())

		executionInfo := mutableState.GetExecutionInfo()
		executionState := mutableState.GetExecutionState()
		workflowTypeName := executionInfo.WorkflowTypeName
		workflowCloseTime := wfCloseTime
		workflowStatus := executionState.Status
		workflowHistoryLength := mutableState.GetNextEventID() - 1
		startEvent, err := mutableState.GetStartEvent()
		if err != nil {
			return nil, err
		}
		workflowStartTime := timestamp.TimeValue(startEvent.GetEventTime())
		workflowExecutionTimestamp := getWorkflowExecutionTime(mutableState, startEvent)
		visibilityMemo := getWorkflowMemo(executionInfo.Memo)
		searchAttr := getSearchAttributes(executionInfo.SearchAttributes)

		lastWriteVersion, err := mutableState.GetLastWriteVersion()
		if err != nil {
			return nil, err
		}
		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.GetNamespaceId(), lastWriteVersion, transferTask.Version, transferTask)
		if err != nil || !ok {
			return nil, err
		}

		// DO NOT REPLY TO PARENT
		// since event replication should be done by active cluster
		return nil, t.recordWorkflowClosed(
			transferTask.GetNamespaceId(),
			transferTask.GetWorkflowId(),
			transferTask.GetRunId(),
			workflowTypeName,
			workflowStartTime,
			workflowExecutionTimestamp,
			workflowCloseTime,
			workflowStatus,
			workflowHistoryLength,
			transferTask.GetTaskId(),
			visibilityMemo,
			executionInfo.TaskQueue,
			searchAttr,
		)
	}

	return t.processTransfer(
		processTaskIfClosed,
		transferTask,
		actionFn,
		standbyTaskPostActionNoOp,
	) // no op post action, since the entire workflow is finished
}

func (t *transferQueueStandbyTaskExecutor) processCancelExecution(
	transferTask *persistencespb.TransferTaskInfo,
) error {

	processTaskIfClosed := false
	actionFn := func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

		requestCancelInfo, ok := mutableState.GetRequestCancelInfo(transferTask.GetScheduleId())
		if !ok {
			return nil, nil
		}

		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.GetNamespaceId(), requestCancelInfo.Version, transferTask.Version, transferTask)
		if err != nil || !ok {
			return nil, err
		}

		return getHistoryResendInfo(mutableState)
	}

	return t.processTransfer(
		processTaskIfClosed,
		transferTask,
		actionFn,
		getStandbyPostActionFn(
			transferTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(),
			t.config.StandbyTaskMissingEventsDiscardDelay(),
			t.fetchHistoryFromRemote,
			standbyTransferTaskPostActionTaskDiscarded,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processSignalExecution(
	transferTask *persistencespb.TransferTaskInfo,
) error {

	processTaskIfClosed := false
	actionFn := func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

		signalInfo, ok := mutableState.GetSignalInfo(transferTask.GetScheduleId())
		if !ok {
			return nil, nil
		}

		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.GetNamespaceId(), signalInfo.Version, transferTask.Version, transferTask)
		if err != nil || !ok {
			return nil, err
		}

		return getHistoryResendInfo(mutableState)
	}

	return t.processTransfer(
		processTaskIfClosed,
		transferTask,
		actionFn,
		getStandbyPostActionFn(
			transferTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(),
			t.config.StandbyTaskMissingEventsDiscardDelay(),
			t.fetchHistoryFromRemote,
			standbyTransferTaskPostActionTaskDiscarded,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processStartChildExecution(
	transferTask *persistencespb.TransferTaskInfo,
) error {

	processTaskIfClosed := false
	actionFn := func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

		childWorkflowInfo, ok := mutableState.GetChildExecutionInfo(transferTask.GetScheduleId())
		if !ok {
			return nil, nil
		}

		ok, err := verifyTaskVersion(t.shard, t.logger, transferTask.GetNamespaceId(), childWorkflowInfo.Version, transferTask.Version, transferTask)
		if err != nil || !ok {
			return nil, err
		}

		if childWorkflowInfo.StartedId != common.EmptyEventID {
			return nil, nil
		}

		return getHistoryResendInfo(mutableState)
	}

	return t.processTransfer(
		processTaskIfClosed,
		transferTask,
		actionFn,
		getStandbyPostActionFn(
			transferTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(),
			t.config.StandbyTaskMissingEventsDiscardDelay(),
			t.fetchHistoryFromRemote,
			standbyTransferTaskPostActionTaskDiscarded,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processTransfer(
	processTaskIfClosed bool,
	taskInfo queueTaskInfo,
	actionFn standbyActionFn,
	postActionFn standbyPostActionFn,
) (retError error) {

	transferTask := taskInfo.(*persistencespb.TransferTaskInfo)
	ctx, cancel := context.WithTimeout(context.Background(), taskTimeout)
	defer cancel()
	namespaceID, execution := t.getNamespaceIDAndWorkflowExecution(transferTask)
	context, release, err := t.cache.getOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		callerTypeTask,
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

	// NOTE: do not access anything related mutable state after this lock release
	release(nil)
	return postActionFn(taskInfo, historyResendInfo, t.logger)
}

func (t *transferQueueStandbyTaskExecutor) pushActivity(
	task queueTaskInfo,
	postActionInfo interface{},
	logger log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	pushActivityInfo := postActionInfo.(*pushActivityTaskToMatchingInfo)
	timeout := pushActivityInfo.activityTaskScheduleToStartTimeout
	return t.transferQueueTaskExecutorBase.pushActivity(
		task.(*persistencespb.TransferTaskInfo),
		&timeout,
	)
}

func (t *transferQueueStandbyTaskExecutor) pushWorkflowTask(
	task queueTaskInfo,
	postActionInfo interface{},
	logger log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	pushwtInfo := postActionInfo.(*pushWorkflowTaskToMatchingInfo)
	timeout := pushwtInfo.workflowTaskScheduleToStartTimeout
	return t.transferQueueTaskExecutorBase.pushWorkflowTask(
		task.(*persistencespb.TransferTaskInfo),
		&pushwtInfo.taskqueue,
		timestamp.DurationFromSeconds(timeout),
	)
}

func (t *transferQueueStandbyTaskExecutor) fetchHistoryFromRemote(
	taskInfo queueTaskInfo,
	postActionInfo interface{},
	log log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	transferTask := taskInfo.(*persistencespb.TransferTaskInfo)
	resendInfo := postActionInfo.(*historyResendInfo)

	t.metricsClient.IncCounter(metrics.HistoryRereplicationByTransferTaskScope, metrics.ClientRequests)
	stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByTransferTaskScope, metrics.ClientLatency)
	defer stopwatch.Stop()

	var err error
	if resendInfo.lastEventID != common.EmptyEventID && resendInfo.lastEventVersion != common.EmptyVersion {
		if err := refreshTasks(
			t.adminClient,
			t.shard.GetNamespaceCache(),
			transferTask.GetNamespaceId(),
			transferTask.GetWorkflowId(),
			transferTask.GetRunId(),
		); err != nil {
			t.logger.Error("Error refresh tasks from remote.",
				tag.ShardID(t.shard.GetShardID()),
				tag.WorkflowNamespaceID(transferTask.GetNamespaceId()),
				tag.WorkflowID(transferTask.GetWorkflowId()),
				tag.WorkflowRunID(transferTask.GetRunId()),
				tag.ClusterName(t.clusterName))
		}
		err = t.nDCHistoryResender.SendSingleWorkflowHistory(
			transferTask.GetNamespaceId(),
			transferTask.GetWorkflowId(),
			transferTask.GetRunId(),
			resendInfo.lastEventID,
			resendInfo.lastEventVersion,
			0,
			0,
		)
	} else {
		err = &serviceerror.Internal{
			Message: "transferQueueStandbyProcessor encounter empty historyResendInfo",
		}
	}

	if err != nil {
		t.logger.Error("Error re-replicating history from remote.",
			tag.ShardID(t.shard.GetShardID()),
			tag.WorkflowNamespaceID(transferTask.GetNamespaceId()),
			tag.WorkflowID(transferTask.GetWorkflowId()),
			tag.WorkflowRunID(transferTask.GetRunId()),
			tag.SourceCluster(t.clusterName))
	}

	// return error so task processing logic will retry
	return ErrTaskRetry
}

func (t *transferQueueStandbyTaskExecutor) getCurrentTime() time.Time {
	return t.shard.GetCurrentTime(t.clusterName)
}
