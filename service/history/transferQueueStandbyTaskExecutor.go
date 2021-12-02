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
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
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
	historyEngine *historyEngineImpl,
	nDCHistoryResender xdc.NDCHistoryResender,
	logger log.Logger,
	metricsClient metrics.Client,
	clusterName string,
	config *configs.Config,
	clientBean client.Bean,
	matchingClient matchingservice.MatchingServiceClient,
) queueTaskExecutor {
	return &transferQueueStandbyTaskExecutor{
		transferQueueTaskExecutorBase: newTransferQueueTaskExecutorBase(
			shard,
			historyEngine,
			logger,
			metricsClient,
			config,
			matchingClient,
		),
		clusterName:        clusterName,
		adminClient:        clientBean.GetRemoteAdminClient(clusterName),
		nDCHistoryResender: nDCHistoryResender,
	}
}

func (t *transferQueueStandbyTaskExecutor) execute(
	ctx context.Context,
	taskInfo tasks.Task,
	shouldProcessTask bool,
) error {
	switch task := taskInfo.(type) {
	case *tasks.ActivityTask:
		if !shouldProcessTask {
			return nil
		}
		return t.processActivityTask(ctx, task)
	case *tasks.WorkflowTask:
		if !shouldProcessTask {
			return nil
		}
		return t.processWorkflowTask(ctx, task)
	case *tasks.CancelExecutionTask:
		if !shouldProcessTask {
			return nil
		}
		return t.processCancelExecution(ctx, task)
	case *tasks.SignalExecutionTask:
		if !shouldProcessTask {
			return nil
		}
		return t.processSignalExecution(ctx, task)
	case *tasks.StartChildExecutionTask:
		if !shouldProcessTask {
			return nil
		}
		return t.processStartChildExecution(ctx, task)
	case *tasks.ResetWorkflowTask:
		// no reset needed for standby
		// TODO: add error logs
		return nil
	case *tasks.CloseExecutionTask:
		return t.processCloseExecution(ctx, task)
	default:
		return errUnknownTransferTask
	}
}

func (t *transferQueueStandbyTaskExecutor) processActivityTask(
	ctx context.Context,
	transferTask *tasks.ActivityTask,
) error {

	processTaskIfClosed := false
	actionFn := func(context workflow.Context, mutableState workflow.MutableState) (interface{}, error) {

		activityInfo, ok := mutableState.GetActivityInfo(transferTask.ScheduleID)
		if !ok {
			return nil, nil
		}

		ok, err := verifyTaskVersion(t.shard, t.logger, namespace.ID(transferTask.NamespaceID), activityInfo.Version, transferTask.Version, transferTask)
		if err != nil || !ok {
			return nil, err
		}

		if activityInfo.StartedId == common.EmptyEventID {
			return newPushActivityToMatchingInfo(*activityInfo.ScheduleToStartTimeout), nil
		}

		return nil, nil
	}

	return t.processTransfer(
		ctx,
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
	ctx context.Context,
	transferTask *tasks.WorkflowTask,
) error {

	processTaskIfClosed := false
	actionFn := func(context workflow.Context, mutableState workflow.MutableState) (interface{}, error) {

		wtInfo, ok := mutableState.GetWorkflowTaskInfo(transferTask.ScheduleID)
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

		ok, err := verifyTaskVersion(t.shard, t.logger, namespace.ID(transferTask.NamespaceID), wtInfo.Version, transferTask.Version, transferTask)
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
		ctx,
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
	ctx context.Context,
	transferTask *tasks.CloseExecutionTask,
) error {

	processTaskIfClosed := true
	actionFn := func(context workflow.Context, mutableState workflow.MutableState) (interface{}, error) {

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
		workflowStartTime := timestamp.TimeValue(mutableState.GetExecutionInfo().GetStartTime())
		workflowExecutionTime := timestamp.TimeValue(mutableState.GetExecutionInfo().GetExecutionTime())
		visibilityMemo := getWorkflowMemo(executionInfo.Memo)
		searchAttr := getSearchAttributes(executionInfo.SearchAttributes)

		lastWriteVersion, err := mutableState.GetLastWriteVersion()
		if err != nil {
			return nil, err
		}
		ok, err := verifyTaskVersion(t.shard, t.logger, namespace.ID(transferTask.NamespaceID), lastWriteVersion, transferTask.Version, transferTask)
		if err != nil || !ok {
			return nil, err
		}

		// DO NOT REPLY TO PARENT
		// since event replication should be done by active cluster
		return nil, t.recordWorkflowClosed(
			namespace.ID(transferTask.NamespaceID),
			transferTask.WorkflowID,
			transferTask.RunID,
			workflowTypeName,
			workflowStartTime,
			workflowExecutionTime,
			workflowCloseTime,
			workflowStatus,
			workflowHistoryLength,
			visibilityMemo,
			searchAttr,
		)
	}

	return t.processTransfer(
		ctx,
		processTaskIfClosed,
		transferTask,
		actionFn,
		standbyTaskPostActionNoOp,
	) // no op post action, since the entire workflow is finished
}

func (t *transferQueueStandbyTaskExecutor) processCancelExecution(
	ctx context.Context,
	transferTask *tasks.CancelExecutionTask,
) error {

	processTaskIfClosed := false
	actionFn := func(context workflow.Context, mutableState workflow.MutableState) (interface{}, error) {

		requestCancelInfo, ok := mutableState.GetRequestCancelInfo(transferTask.InitiatedID)
		if !ok {
			return nil, nil
		}

		ok, err := verifyTaskVersion(t.shard, t.logger, namespace.ID(transferTask.NamespaceID), requestCancelInfo.Version, transferTask.Version, transferTask)
		if err != nil || !ok {
			return nil, err
		}

		return getHistoryResendInfo(mutableState)
	}

	return t.processTransfer(
		ctx,
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
	ctx context.Context,
	transferTask *tasks.SignalExecutionTask,
) error {

	processTaskIfClosed := false
	actionFn := func(context workflow.Context, mutableState workflow.MutableState) (interface{}, error) {

		signalInfo, ok := mutableState.GetSignalInfo(transferTask.InitiatedID)
		if !ok {
			return nil, nil
		}

		ok, err := verifyTaskVersion(t.shard, t.logger, namespace.ID(transferTask.NamespaceID), signalInfo.Version, transferTask.Version, transferTask)
		if err != nil || !ok {
			return nil, err
		}

		return getHistoryResendInfo(mutableState)
	}

	return t.processTransfer(
		ctx,
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
	ctx context.Context,
	transferTask *tasks.StartChildExecutionTask,
) error {

	processTaskIfClosed := false
	actionFn := func(context workflow.Context, mutableState workflow.MutableState) (interface{}, error) {

		childWorkflowInfo, ok := mutableState.GetChildExecutionInfo(transferTask.InitiatedID)
		if !ok {
			return nil, nil
		}

		ok, err := verifyTaskVersion(t.shard, t.logger, namespace.ID(transferTask.NamespaceID), childWorkflowInfo.Version, transferTask.Version, transferTask)
		if err != nil || !ok {
			return nil, err
		}

		if childWorkflowInfo.StartedId != common.EmptyEventID {
			return nil, nil
		}

		return getHistoryResendInfo(mutableState)
	}

	return t.processTransfer(
		ctx,
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
	ctx context.Context,
	processTaskIfClosed bool,
	taskInfo tasks.Task,
	actionFn standbyActionFn,
	postActionFn standbyPostActionFn,
) (retError error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, taskTimeout)
	defer cancel()
	namespaceID, execution := t.getNamespaceIDAndWorkflowExecution(taskInfo)
	context, release, err := t.cache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		workflow.CallerTypeTask,
	)
	if err != nil {
		return err
	}
	defer func() {
		if retError == consts.ErrTaskRetry {
			release(nil)
		} else {
			release(retError)
		}
	}()

	mutableState, err := loadMutableStateForTransferTask(context, taskInfo, t.metricsClient, t.logger)
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
	task tasks.Task,
	postActionInfo interface{},
	logger log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	pushActivityInfo := postActionInfo.(*pushActivityTaskToMatchingInfo)
	timeout := pushActivityInfo.activityTaskScheduleToStartTimeout
	return t.transferQueueTaskExecutorBase.pushActivity(
		task.(*tasks.ActivityTask),
		&timeout,
	)
}

func (t *transferQueueStandbyTaskExecutor) pushWorkflowTask(
	task tasks.Task,
	postActionInfo interface{},
	logger log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	pushwtInfo := postActionInfo.(*pushWorkflowTaskToMatchingInfo)
	timeout := pushwtInfo.workflowTaskScheduleToStartTimeout
	return t.transferQueueTaskExecutorBase.pushWorkflowTask(
		task.(*tasks.WorkflowTask),
		&pushwtInfo.taskqueue,
		timestamp.DurationFromSeconds(timeout),
	)
}

func (t *transferQueueStandbyTaskExecutor) fetchHistoryFromRemote(
	taskInfo tasks.Task,
	postActionInfo interface{},
	log log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	resendInfo := postActionInfo.(*historyResendInfo)

	t.metricsClient.IncCounter(metrics.HistoryRereplicationByTransferTaskScope, metrics.ClientRequests)
	stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByTransferTaskScope, metrics.ClientLatency)
	defer stopwatch.Stop()

	var err error
	if resendInfo.lastEventID != common.EmptyEventID && resendInfo.lastEventVersion != common.EmptyVersion {
		if err := refreshTasks(
			t.adminClient,
			t.shard.GetNamespaceRegistry(),
			namespace.ID(taskInfo.GetNamespaceID()),
			taskInfo.GetWorkflowID(),
			taskInfo.GetRunID(),
		); err != nil {
			t.logger.Error("Error refresh tasks from remote.",
				tag.ShardID(t.shard.GetShardID()),
				tag.WorkflowNamespaceID(taskInfo.GetNamespaceID()),
				tag.WorkflowID(taskInfo.GetWorkflowID()),
				tag.WorkflowRunID(taskInfo.GetRunID()),
				tag.ClusterName(t.clusterName))
		}
		err = t.nDCHistoryResender.SendSingleWorkflowHistory(
			namespace.ID(taskInfo.GetNamespaceID()),
			taskInfo.GetWorkflowID(),
			taskInfo.GetRunID(),
			resendInfo.lastEventID,
			resendInfo.lastEventVersion,
			0,
			0,
		)
	} else {
		err = serviceerror.NewInternal(
			"transferQueueStandbyProcessor encountered empty historyResendInfo",
		)
	}

	if err != nil {
		t.logger.Error("Error re-replicating history from remote.",
			tag.ShardID(t.shard.GetShardID()),
			tag.WorkflowNamespaceID(taskInfo.GetNamespaceID()),
			tag.WorkflowID(taskInfo.GetWorkflowID()),
			tag.WorkflowRunID(taskInfo.GetRunID()),
			tag.SourceCluster(t.clusterName))
	}

	// return error so task processing logic will retry
	return consts.ErrTaskRetry
}

func (t *transferQueueStandbyTaskExecutor) getCurrentTime() time.Time {
	return t.shard.GetCurrentTime(t.clusterName)
}
