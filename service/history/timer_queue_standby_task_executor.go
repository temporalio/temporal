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

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/vclock"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	timerQueueStandbyTaskExecutor struct {
		*timerQueueTaskExecutorBase

		clusterName        string
		nDCHistoryResender xdc.NDCHistoryResender
	}
)

func newTimerQueueStandbyTaskExecutor(
	shard shard.Context,
	workflowCache wcache.Cache,
	workflowDeleteManager deletemanager.DeleteManager,
	nDCHistoryResender xdc.NDCHistoryResender,
	matchingRawClient resource.MatchingRawClient,
	logger log.Logger,
	metricProvider metrics.Handler,
	clusterName string,
	config *configs.Config,
) queues.Executor {
	return &timerQueueStandbyTaskExecutor{
		timerQueueTaskExecutorBase: newTimerQueueTaskExecutorBase(
			shard,
			workflowCache,
			workflowDeleteManager,
			matchingRawClient,
			logger,
			metricProvider,
			config,
		),
		clusterName:        clusterName,
		nDCHistoryResender: nDCHistoryResender,
	}
}

func (t *timerQueueStandbyTaskExecutor) Execute(
	ctx context.Context,
	executable queues.Executable,
) queues.ExecuteResponse {
	task := executable.GetTask()
	taskType := queues.GetStandbyTimerTaskTypeTagValue(task)
	metricsTags := []metrics.Tag{
		getNamespaceTagByID(t.shardContext.GetNamespaceRegistry(), task.GetNamespaceID()),
		metrics.TaskTypeTag(taskType),
		metrics.OperationTag(taskType), // for backward compatibility
	}

	var err error
	switch task := task.(type) {
	case *tasks.UserTimerTask:
		err = t.executeUserTimerTimeoutTask(ctx, task)
	case *tasks.ActivityTimeoutTask:
		err = t.executeActivityTimeoutTask(ctx, task)
	case *tasks.WorkflowTaskTimeoutTask:
		err = t.executeWorkflowTaskTimeoutTask(ctx, task)
	case *tasks.WorkflowBackoffTimerTask:
		err = t.executeWorkflowBackoffTimerTask(ctx, task)
	case *tasks.ActivityRetryTimerTask:
		err = t.executeActivityRetryTimerTask(ctx, task)
	case *tasks.WorkflowTimeoutTask:
		err = t.executeWorkflowTimeoutTask(ctx, task)
	case *tasks.DeleteHistoryEventTask:
		err = t.executeDeleteHistoryEventTask(ctx, task)
	default:
		err = errUnknownTimerTask
	}

	return queues.ExecuteResponse{
		ExecutionMetricTags: metricsTags,
		ExecutedAsActive:    false,
		ExecutionErr:        err,
	}
}

func (t *timerQueueStandbyTaskExecutor) executeUserTimerTimeoutTask(
	ctx context.Context,
	timerTask *tasks.UserTimerTask,
) error {
	actionFn := func(_ context.Context, wfContext workflow.Context, mutableState workflow.MutableState) (interface{}, error) {
		timerSequence := t.getTimerSequence(mutableState)
		timerSequenceIDs := timerSequence.LoadAndSortUserTimers()
		if len(timerSequenceIDs) > 0 {
			timerSequenceID := timerSequenceIDs[0]
			_, ok := mutableState.GetUserTimerInfoByEventID(timerSequenceID.EventID)
			if !ok {
				errString := fmt.Sprintf("failed to find in user timer event ID: %v", timerSequenceID.EventID)
				t.logger.Error(errString)
				return nil, serviceerror.NewInternal(errString)
			}

			if queues.IsTimeExpired(
				timerTask.GetVisibilityTime(),
				timerSequenceID.Timestamp,
			) {
				return getHistoryResendInfo(mutableState)
			}
			// Since the user timers are already sorted, then if there is one timer which is not expired,
			// all user timers after that timer are not expired.
		}
		// If there is no user timer expired, then we are good.
		return nil, nil
	}

	return t.processTimer(
		ctx,
		timerTask,
		actionFn,
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(timerTask.GetType()),
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.fetchHistoryFromRemote,
			standbyTimerTaskPostActionTaskDiscarded,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeActivityTimeoutTask(
	ctx context.Context,
	timerTask *tasks.ActivityTimeoutTask,
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
	actionFn := func(ctx context.Context, wfContext workflow.Context, mutableState workflow.MutableState) (interface{}, error) {
		timerSequence := t.getTimerSequence(mutableState)
		updateMutableState := false
		timerSequenceIDs := timerSequence.LoadAndSortActivityTimers()
		if len(timerSequenceIDs) > 0 {
			timerSequenceID := timerSequenceIDs[0]
			_, ok := mutableState.GetActivityInfo(timerSequenceID.EventID)
			if !ok {
				errString := fmt.Sprintf("failed to find in memory activity timer: %v", timerSequenceID.EventID)
				t.logger.Error(errString)
				return nil, serviceerror.NewInternal(errString)
			}

			if queues.IsTimeExpired(
				timerTask.GetVisibilityTime(),
				timerSequenceID.Timestamp,
			) {
				return getHistoryResendInfo(mutableState)
			}
			// Since the activity timers are already sorted, then if there is one timer which is not expired,
			// all activity timers after that timer are not expired.
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
		activityInfo, heartbeatTimeoutVis, ok := mutableState.GetActivityInfoWithTimerHeartbeat(timerTask.EventID)
		if isHeartBeatTask && ok && queues.IsTimeExpired(timerTask.GetVisibilityTime(), heartbeatTimeoutVis) {
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

		// we need to handcraft some of the variables
		// since the job being done here is update the activity and possibly write a timer task to DB
		// also need to reset the current version.
		if err := mutableState.UpdateCurrentVersion(lastWriteVersion, true); err != nil {
			return nil, err
		}

		err = wfContext.UpdateWorkflowExecutionAsPassive(ctx, t.shardContext)
		return nil, err
	}

	return t.processTimer(
		ctx,
		timerTask,
		actionFn,
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(timerTask.GetType()),
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.fetchHistoryFromRemote,
			standbyTimerTaskPostActionTaskDiscarded,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeActivityRetryTimerTask(
	ctx context.Context,
	task *tasks.ActivityRetryTimerTask,
) (retError error) {
	actionFn := func(_ context.Context, wfContext workflow.Context, mutableState workflow.MutableState) (interface{}, error) {
		activityInfo, ok := mutableState.GetActivityInfo(task.EventID) // activity schedule ID
		if !ok {
			return nil, nil
		}

		err := CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), activityInfo.Version, task.Version, task)
		if err != nil {
			return nil, err
		}

		if activityInfo.Attempt > task.Attempt {
			return nil, nil
		}

		if activityInfo.StartedEventId != common.EmptyEventID {
			return nil, nil
		}

		return newActivityRetryTimePostActionInfo(mutableState, activityInfo.TaskQueue, activityInfo.ScheduleToStartTimeout.AsDuration(), activityInfo.UseCompatibleVersion)
	}

	return t.processTimer(
		ctx,
		task,
		actionFn,
		getStandbyPostActionFn(
			task,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(task.GetType()),
			t.config.StandbyTaskMissingEventsDiscardDelay(task.GetType()),
			t.fetchHistoryFromRemote,
			t.pushActivity,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeWorkflowTaskTimeoutTask(
	ctx context.Context,
	timerTask *tasks.WorkflowTaskTimeoutTask,
) error {
	// workflow task schedule to start timer task is a special snowflake.
	// the schedule to start timer is for sticky workflow task, which is
	// not applicable on the passive cluster
	if timerTask.TimeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START {
		return nil
	}

	actionFn := func(_ context.Context, wfContext workflow.Context, mutableState workflow.MutableState) (interface{}, error) {
		workflowTask := mutableState.GetWorkflowTaskByID(timerTask.EventID)
		if workflowTask == nil {
			return nil, nil
		}

		err := CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), workflowTask.Version, timerTask.Version, timerTask)
		if err != nil {
			return nil, err
		}

		return getHistoryResendInfo(mutableState)
	}

	return t.processTimer(
		ctx,
		timerTask,
		actionFn,
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(timerTask.GetType()),
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.fetchHistoryFromRemote,
			standbyTimerTaskPostActionTaskDiscarded,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeWorkflowBackoffTimerTask(
	ctx context.Context,
	timerTask *tasks.WorkflowBackoffTimerTask,
) error {
	actionFn := func(_ context.Context, wfContext workflow.Context, mutableState workflow.MutableState) (interface{}, error) {
		if mutableState.HadOrHasWorkflowTask() {
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
		ctx,
		timerTask,
		actionFn,
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(timerTask.GetType()),
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.fetchHistoryFromRemote,
			standbyTimerTaskPostActionTaskDiscarded,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeWorkflowTimeoutTask(
	ctx context.Context,
	timerTask *tasks.WorkflowTimeoutTask,
) error {
	actionFn := func(_ context.Context, wfContext workflow.Context, mutableState workflow.MutableState) (interface{}, error) {
		// we do not need to notify new timer to base, since if there is no new event being replicated
		// checking again if the timer can be completed is meaningless

		startVersion, err := mutableState.GetStartVersion()
		if err != nil {
			return nil, err
		}
		err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), startVersion, timerTask.Version, timerTask)
		if err != nil {
			return nil, err
		}

		return getHistoryResendInfo(mutableState)
	}

	return t.processTimer(
		ctx,
		timerTask,
		actionFn,
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(timerTask.GetType()),
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.fetchHistoryFromRemote,
			standbyTimerTaskPostActionTaskDiscarded,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) getTimerSequence(
	mutableState workflow.MutableState,
) workflow.TimerSequence {
	return workflow.NewTimerSequence(mutableState)
}

func (t *timerQueueStandbyTaskExecutor) processTimer(
	ctx context.Context,
	timerTask tasks.Task,
	actionFn standbyActionFn,
	postActionFn standbyPostActionFn,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	nsRecord, err := t.shardContext.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(timerTask.GetNamespaceID()))
	if err != nil {
		return err
	}
	if !nsRecord.IsOnCluster(t.clusterName) {
		// namespace is not replicated to local cluster, ignore corresponding tasks
		return nil
	}

	executionContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, timerTask)
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

	mutableState, err := loadMutableStateForTimerTask(ctx, t.shardContext, executionContext, timerTask, t.metricHandler, t.logger)
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

	historyResendInfo, err := actionFn(ctx, executionContext, mutableState)
	if err != nil {
		return err
	}

	// NOTE: do not access anything related mutable state after this lock release
	release(nil)
	return postActionFn(ctx, timerTask, historyResendInfo, t.logger)
}

func (t *timerQueueStandbyTaskExecutor) fetchHistoryFromRemote(
	ctx context.Context,
	taskInfo tasks.Task,
	postActionInfo interface{},
	logger log.Logger,
) error {
	var resendInfo *historyResendInfo
	switch postActionInfo := postActionInfo.(type) {
	case nil:
		return nil
	case *historyResendInfo:
		resendInfo = postActionInfo
	case *activityTaskPostActionInfo:
		resendInfo = postActionInfo.historyResendInfo
	default:
		logger.Fatal("unknown post action info for fetching remote history", tag.Value(postActionInfo))
	}

	remoteClusterName, err := getRemoteClusterName(
		t.currentClusterName,
		t.registry,
		taskInfo.GetNamespaceID(),
	)
	if err != nil {
		return err
	}

	scope := t.metricHandler.WithTags(metrics.OperationTag(metrics.HistoryRereplicationByTimerTaskScope))
	scope.Counter(metrics.ClientRequests.Name()).Record(1)
	startTime := time.Now()
	defer func() { scope.Timer(metrics.ClientLatency.Name()).Record(time.Since(startTime)) }()

	if resendInfo.lastEventID == common.EmptyEventID || resendInfo.lastEventVersion == common.EmptyVersion {
		t.logger.Error("Error re-replicating history from remote: timerQueueStandbyProcessor encountered empty historyResendInfo.",
			tag.ShardID(t.shardContext.GetShardID()),
			tag.WorkflowNamespaceID(taskInfo.GetNamespaceID()),
			tag.WorkflowID(taskInfo.GetWorkflowID()),
			tag.WorkflowRunID(taskInfo.GetRunID()),
			tag.ClusterName(remoteClusterName))

		return consts.ErrTaskRetry
	}

	// NOTE: history resend may take long time and its timeout is currently
	// controlled by a separate dynamicconfig config: StandbyTaskReReplicationContextTimeout
	if err = t.nDCHistoryResender.SendSingleWorkflowHistory(
		ctx,
		remoteClusterName,
		namespace.ID(taskInfo.GetNamespaceID()),
		taskInfo.GetWorkflowID(),
		taskInfo.GetRunID(),
		resendInfo.lastEventID,
		resendInfo.lastEventVersion,
		common.EmptyEventID,
		common.EmptyVersion,
	); err != nil {
		if _, isNotFound := err.(*serviceerror.NamespaceNotFound); isNotFound {
			// Don't log NamespaceNotFound error because it is valid case, and return error to stop retrying.
			return err
		}
		t.logger.Error("Error re-replicating history from remote.",
			tag.ShardID(t.shardContext.GetShardID()),
			tag.WorkflowNamespaceID(taskInfo.GetNamespaceID()),
			tag.WorkflowID(taskInfo.GetWorkflowID()),
			tag.WorkflowRunID(taskInfo.GetRunID()),
			tag.ClusterName(remoteClusterName),
			tag.Error(err))
	}

	// Return retryable error, so task processing will retry.
	return consts.ErrTaskRetry
}

func (t *timerQueueStandbyTaskExecutor) pushActivity(
	ctx context.Context,
	task tasks.Task,
	postActionInfo interface{},
	logger log.Logger,
) error {
	if postActionInfo == nil {
		return nil
	}

	pushActivityInfo := postActionInfo.(*activityTaskPostActionInfo)
	activityScheduleToStartTimeout := pushActivityInfo.activityTaskScheduleToStartTimeout
	activityTask := task.(*tasks.ActivityRetryTimerTask)

	_, err := t.matchingRawClient.AddActivityTask(ctx, &matchingservice.AddActivityTaskRequest{
		NamespaceId: activityTask.NamespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: activityTask.WorkflowID,
			RunId:      activityTask.RunID,
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: pushActivityInfo.taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		ScheduledEventId:       activityTask.EventID,
		ScheduleToStartTimeout: durationpb.New(activityScheduleToStartTimeout),
		Clock:                  vclock.NewVectorClock(t.shardContext.GetClusterMetadata().GetClusterID(), t.shardContext.GetShardID(), activityTask.TaskID),
		VersionDirective:       pushActivityInfo.versionDirective,
	})
	return err
}

func (t *timerQueueStandbyTaskExecutor) getCurrentTime() time.Time {
	return t.shardContext.GetCurrentTime(t.clusterName)
}
