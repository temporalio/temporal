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
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type (
	timerQueueStandbyTaskExecutor struct {
		*timerQueueTaskExecutorBase

		clusterName        string
		adminClient        adminservice.AdminServiceClient
		nDCHistoryResender xdc.NDCHistoryResender
	}
)

func newTimerQueueStandbyTaskExecutor(
	shard shard.Context,
	historyService *historyEngineImpl,
	nDCHistoryResender xdc.NDCHistoryResender,
	logger log.Logger,
	metricsClient metrics.Client,
	clusterName string,
	config *configs.Config,
) queueTaskExecutor {
	return &timerQueueStandbyTaskExecutor{
		timerQueueTaskExecutorBase: newTimerQueueTaskExecutorBase(
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

func (t *timerQueueStandbyTaskExecutor) execute(
	ctx context.Context,
	taskInfo queueTaskInfo,
	shouldProcessTask bool,
) error {

	timerTask, ok := taskInfo.(*persistencespb.TimerTaskInfo)
	if !ok {
		return errUnexpectedQueueTask
	}

	if !shouldProcessTask &&
		timerTask.TaskType != enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT &&
		timerTask.TaskType != enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT {
		// guarantee the processing of workflow execution history deletion
		return nil
	}

	switch timerTask.TaskType {
	case enumsspb.TASK_TYPE_USER_TIMER:
		return t.executeUserTimerTimeoutTask(timerTask)
	case enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT:
		return t.executeActivityTimeoutTask(timerTask)
	case enumsspb.TASK_TYPE_WORKFLOW_TASK_TIMEOUT:
		return t.executeWorkflowTaskTimeoutTask(timerTask)
	case enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT:
		return t.executeWorkflowTimeoutTask(timerTask)
	case enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER:
		// retry backoff timer should not get created on passive cluster
		// TODO: add error logs
		return nil
	case enumsspb.TASK_TYPE_WORKFLOW_BACKOFF_TIMER:
		return t.executeWorkflowBackoffTimerTask(timerTask)
	case enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT:
		return t.executeDeleteHistoryEventTask(ctx, timerTask)
	default:
		return errUnknownTimerTask
	}
}

func (t *timerQueueStandbyTaskExecutor) executeUserTimerTimeoutTask(
	timerTask *persistencespb.TimerTaskInfo,
) error {

	actionFn := func(context workflow.Context, mutableState workflow.MutableState) (interface{}, error) {

		timerSequence := t.getTimerSequence(mutableState)

	Loop:
		for _, timerSequenceID := range timerSequence.LoadAndSortUserTimers() {
			_, ok := mutableState.GetUserTimerInfoByEventID(timerSequenceID.EventID)
			if !ok {
				errString := fmt.Sprintf("failed to find in user timer event ID: %v", timerSequenceID.EventID)
				t.logger.Error(errString)
				return nil, serviceerror.NewInternal(errString)
			}

			if isExpired := timerSequence.IsExpired(
				timestamp.TimeValue(timerTask.VisibilityTime),
				timerSequenceID,
			); isExpired {
				return getHistoryResendInfo(mutableState)
			}
			// since the user timer are already sorted, so if there is one timer which will not expired
			// all user timer after this timer will not expired
			break Loop //nolint:staticcheck
		}
		// if there is no user timer expired, then we are good
		return nil, nil
	}

	return t.processTimer(
		timerTask,
		actionFn,
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(),
			t.config.StandbyTaskMissingEventsDiscardDelay(),
			t.fetchHistoryFromRemote,
			standbyTimerTaskPostActionTaskDiscarded,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeActivityTimeoutTask(
	timerTask *persistencespb.TimerTaskInfo,
) error {

	// activity heartbeat timer task is a special snowflake.
	// normal activity timer task on the passive side will be generated by events related to activity in history replicator,
	// and the standby timer processor will only need to verify whether the timer task can be safely throw away.
	//
	// activity heartbeat timer task cannot be handled in the way mentioned above.
	// the reason is, there is no event driving the creation of new activity heartbeat timer.
	// although there will be an task syncing activity from remote, the task is not an event,
	// and cannot attempt to recreate a new activity timer task.
	//
	// the overall solution is to attempt to generate a new activity timer task whenever the
	// task passed in is safe to be throw away.

	actionFn := func(context workflow.Context, mutableState workflow.MutableState) (interface{}, error) {

		timerSequence := t.getTimerSequence(mutableState)
		updateMutableState := false

	Loop:
		for _, timerSequenceID := range timerSequence.LoadAndSortActivityTimers() {
			_, ok := mutableState.GetActivityInfo(timerSequenceID.EventID)
			if !ok {
				errString := fmt.Sprintf("failed to find in memory activity timer: %v", timerSequenceID.EventID)
				t.logger.Error(errString)
				return nil, serviceerror.NewInternal(errString)
			}

			if isExpired := timerSequence.IsExpired(
				timestamp.TimeValue(timerTask.VisibilityTime),
				timerSequenceID,
			); isExpired {
				return getHistoryResendInfo(mutableState)
			}
			// since the activity timer are already sorted, so if there is one timer which will not expired
			// all activity timer after this timer will not expired
			break Loop //nolint:staticcheck
		}

		// for reason to update mutable state & generate a new activity task,
		// see comments at the beginning of this function.
		// NOTE: this is the only place in the standby logic where mutable state can be updated

		// need to clear the activity heartbeat timer task marks
		lastWriteVersion, err := mutableState.GetLastWriteVersion()
		if err != nil {
			return nil, err
		}

		// NOTE: LastHeartbeatTimeoutVisibilityInSeconds is for deduping heartbeat timer creation as it's possible
		// one heartbeat task was persisted multiple times with different taskIDs due to the retry logic
		// for updating workflow execution. In that case, only one new heartbeat timeout task should be
		// created.
		isHeartBeatTask := timerTask.TimeoutType == enumspb.TIMEOUT_TYPE_HEARTBEAT
		activityInfo, heartbeatTimeoutVis, ok := mutableState.GetActivityInfoWithTimerHeartbeat(timerTask.GetEventId())
		goTS := timestamp.TimeValue(timerTask.VisibilityTime)
		if isHeartBeatTask && ok && (heartbeatTimeoutVis.Before(goTS) || heartbeatTimeoutVis.Equal(goTS)) {
			activityInfo.TimerTaskStatus = activityInfo.TimerTaskStatus &^ workflow.TimerTaskStatusCreatedHeartbeat
			if err := mutableState.UpdateActivity(activityInfo); err != nil {
				return nil, err
			}
			updateMutableState = true
		}

		// passive logic need to explicitly call create timer
		modified, err := timerSequence.CreateNextActivityTimer()
		if err != nil {
			return nil, err
		}
		updateMutableState = updateMutableState || modified

		if !updateMutableState {
			return nil, nil
		}

		now := t.getStandbyClusterTime()
		// we need to handcraft some of the variables
		// since the job being done here is update the activity and possibly write a timer task to DB
		// also need to reset the current version.
		if err := mutableState.UpdateCurrentVersion(lastWriteVersion, true); err != nil {
			return nil, err
		}

		err = context.UpdateWorkflowExecutionAsPassive(now)
		return nil, err
	}

	return t.processTimer(
		timerTask,
		actionFn,
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(),
			t.config.StandbyTaskMissingEventsDiscardDelay(),
			t.fetchHistoryFromRemote,
			standbyTimerTaskPostActionTaskDiscarded,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeWorkflowTaskTimeoutTask(
	timerTask *persistencespb.TimerTaskInfo,
) error {

	// workflow task schedule to start timer task is a special snowflake.
	// the schedule to start timer is for sticky workflow task, which is
	// not applicable on the passive cluster
	if timerTask.TimeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START {
		return nil
	}

	actionFn := func(context workflow.Context, mutableState workflow.MutableState) (interface{}, error) {

		workflowTask, isPending := mutableState.GetWorkflowTaskInfo(timerTask.GetEventId())
		if !isPending {
			return nil, nil
		}

		ok, err := verifyTaskVersion(t.shard, t.logger, timerTask.GetNamespaceId(), workflowTask.Version, timerTask.Version, timerTask)
		if err != nil || !ok {
			return nil, err
		}

		return getHistoryResendInfo(mutableState)
	}

	return t.processTimer(
		timerTask,
		actionFn,
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(),
			t.config.StandbyTaskMissingEventsDiscardDelay(),
			t.fetchHistoryFromRemote,
			standbyTimerTaskPostActionTaskDiscarded,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeWorkflowBackoffTimerTask(
	timerTask *persistencespb.TimerTaskInfo,
) error {

	actionFn := func(context workflow.Context, mutableState workflow.MutableState) (interface{}, error) {

		if mutableState.HasProcessedOrPendingWorkflowTask() {
			// if there is one workflow task already been processed
			// or has pending workflow task, meaning workflow has already running
			return nil, nil
		}

		// Note: do not need to verify task version here
		// logic can only go here if mutable state build's next event ID is 2
		// meaning history only contains workflow started event.
		// we can do the checking of task version vs workflow started version
		// however, workflow started version is immutable

		// active cluster will add first workflow task after backoff timeout.
		// standby cluster should just call ack manager to retry this task
		// since we are stilling waiting for the first WorkflowTaskScheduledEvent to be replicated from active side.

		return getHistoryResendInfo(mutableState)
	}

	return t.processTimer(
		timerTask,
		actionFn,
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(),
			t.config.StandbyTaskMissingEventsDiscardDelay(),
			t.fetchHistoryFromRemote,
			standbyTimerTaskPostActionTaskDiscarded,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeWorkflowTimeoutTask(
	timerTask *persistencespb.TimerTaskInfo,
) error {

	actionFn := func(context workflow.Context, mutableState workflow.MutableState) (interface{}, error) {

		// we do not need to notify new timer to base, since if there is no new event being replicated
		// checking again if the timer can be completed is meaningless

		startVersion, err := mutableState.GetStartVersion()
		if err != nil {
			return nil, err
		}
		ok, err := verifyTaskVersion(t.shard, t.logger, timerTask.GetNamespaceId(), startVersion, timerTask.Version, timerTask)
		if err != nil || !ok {
			return nil, err
		}

		return getHistoryResendInfo(mutableState)
	}

	return t.processTimer(
		timerTask,
		actionFn,
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(),
			t.config.StandbyTaskMissingEventsDiscardDelay(),
			t.fetchHistoryFromRemote,
			standbyTimerTaskPostActionTaskDiscarded,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) getStandbyClusterTime() time.Time {
	// time of remote cluster in the shard is delayed by "StandbyClusterDelay"
	// so to get the current accurate remote cluster time, need to add it back
	return t.shard.GetCurrentTime(t.clusterName).Add(t.shard.GetConfig().StandbyClusterDelay())
}

func (t *timerQueueStandbyTaskExecutor) getTimerSequence(
	mutableState workflow.MutableState,
) workflow.TimerSequence {

	timeSource := clock.NewEventTimeSource()
	now := t.getStandbyClusterTime()
	timeSource.Update(now)
	return workflow.NewTimerSequence(timeSource, mutableState)
}

func (t *timerQueueStandbyTaskExecutor) processTimer(
	timerTask *persistencespb.TimerTaskInfo,
	actionFn standbyActionFn,
	postActionFn standbyPostActionFn,
) (retError error) {

	ctx, cancel := context.WithTimeout(context.Background(), taskTimeout)
	defer cancel()
	namespaceID, execution := t.getNamespaceIDAndWorkflowExecution(timerTask)
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

	mutableState, err := loadMutableStateForTimerTask(context, timerTask, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil {
		return nil
	}

	if !mutableState.IsWorkflowExecutionRunning() {
		// workflow already finished, no need to process the timer
		return nil
	}

	historyResendInfo, err := actionFn(context, mutableState)
	if err != nil {
		return err
	}

	// NOTE: do not access anything related mutable state after this lock release
	release(nil)
	return postActionFn(timerTask, historyResendInfo, t.logger)
}

func (t *timerQueueStandbyTaskExecutor) fetchHistoryFromRemote(
	taskInfo queueTaskInfo,
	postActionInfo interface{},
	log log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	timerTask := taskInfo.(*persistencespb.TimerTaskInfo)
	resendInfo := postActionInfo.(*historyResendInfo)

	t.metricsClient.IncCounter(metrics.HistoryRereplicationByTimerTaskScope, metrics.ClientRequests)
	stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByTimerTaskScope, metrics.ClientLatency)
	defer stopwatch.Stop()

	var err error
	if resendInfo.lastEventID != common.EmptyEventID && resendInfo.lastEventVersion != common.EmptyVersion {
		if err := refreshTasks(
			t.adminClient,
			t.shard.GetNamespaceCache(),
			timerTask.GetNamespaceId(),
			timerTask.GetWorkflowId(),
			timerTask.GetRunId(),
		); err != nil {
			t.logger.Error("Error refresh tasks from remote.",
				tag.ShardID(t.shard.GetShardID()),
				tag.WorkflowNamespaceID(timerTask.GetNamespaceId()),
				tag.WorkflowID(timerTask.GetWorkflowId()),
				tag.WorkflowRunID(timerTask.GetRunId()),
				tag.ClusterName(t.clusterName))
		}

		err = t.nDCHistoryResender.SendSingleWorkflowHistory(
			timerTask.GetNamespaceId(),
			timerTask.GetWorkflowId(),
			timerTask.GetRunId(),
			resendInfo.lastEventID,
			resendInfo.lastEventVersion,
			common.EmptyEventID,
			common.EmptyVersion,
		)
	} else {
		err = serviceerror.NewInternal("timerQueueStandbyProcessor encounter empty historyResendInfo")
	}

	if err != nil {
		t.logger.Error("Error re-replicating history from remote.",
			tag.ShardID(t.shard.GetShardID()),
			tag.WorkflowNamespaceID(timerTask.GetNamespaceId()),
			tag.WorkflowID(timerTask.GetWorkflowId()),
			tag.WorkflowRunID(timerTask.GetRunId()),
			tag.ClusterName(t.clusterName))
	}

	// return error so task processing logic will retry
	return consts.ErrTaskRetry
}

func (t *timerQueueStandbyTaskExecutor) getCurrentTime() time.Time {
	return t.shard.GetCurrentTime(t.clusterName)
}
