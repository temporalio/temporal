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
	"fmt"
	"time"

	"github.com/gogo/protobuf/types"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/xdc"
)

type (
	timerQueueStandbyTaskExecutor struct {
		*timerQueueTaskExecutorBase

		clusterName         string
		historyRereplicator xdc.HistoryRereplicator
		nDCHistoryResender  xdc.NDCHistoryResender
	}
)

func newTimerQueueStandbyTaskExecutor(
	shard ShardContext,
	historyService *historyEngineImpl,
	historyRereplicator xdc.HistoryRereplicator,
	nDCHistoryResender xdc.NDCHistoryResender,
	logger log.Logger,
	metricsClient metrics.Client,
	clusterName string,
	config *Config,
) queueTaskExecutor {
	return &timerQueueStandbyTaskExecutor{
		timerQueueTaskExecutorBase: newTimerQueueTaskExecutorBase(
			shard,
			historyService,
			logger,
			metricsClient,
			config,
		),
		clusterName:         clusterName,
		historyRereplicator: historyRereplicator,
		nDCHistoryResender:  nDCHistoryResender,
	}
}

func (t *timerQueueStandbyTaskExecutor) execute(
	taskInfo queueTaskInfo,
	shouldProcessTask bool,
) error {

	timerTask, ok := taskInfo.(*persistenceblobs.TimerTaskInfo)
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
	case enumsspb.TASK_TYPE_DECISION_TIMEOUT:
		return t.executeDecisionTimeoutTask(timerTask)
	case enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT:
		return t.executeWorkflowTimeoutTask(timerTask)
	case enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER:
		// retry backoff timer should not get created on passive cluster
		// TODO: add error logs
		return nil
	case enumsspb.TASK_TYPE_WORKFLOW_BACKOFF_TIMER:
		return t.executeWorkflowBackoffTimerTask(timerTask)
	case enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT:
		return t.executeDeleteHistoryEventTask(timerTask)
	default:
		return errUnknownTimerTask
	}
}

func (t *timerQueueStandbyTaskExecutor) executeUserTimerTimeoutTask(
	timerTask *persistenceblobs.TimerTaskInfo,
) error {

	actionFn := func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

		timerSequence := t.getTimerSequence(mutableState)

	Loop:
		for _, timerSequenceID := range timerSequence.loadAndSortUserTimers() {
			_, ok := mutableState.GetUserTimerInfoByEventID(timerSequenceID.eventID)
			if !ok {
				errString := fmt.Sprintf("failed to find in user timer event ID: %v", timerSequenceID.eventID)
				t.logger.Error(errString)
				return nil, serviceerror.NewInternal(errString)
			}

			t, _ := types.TimestampFromProto(timerTask.VisibilityTime)
			if isExpired := timerSequence.isExpired(
				t,
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
	timerTask *persistenceblobs.TimerTaskInfo,
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

	actionFn := func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

		timerSequence := t.getTimerSequence(mutableState)
		updateMutableState := false

	Loop:
		for _, timerSequenceID := range timerSequence.loadAndSortActivityTimers() {
			_, ok := mutableState.GetActivityInfo(timerSequenceID.eventID)
			if !ok {
				errString := fmt.Sprintf("failed to find in memory activity timer: %v", timerSequenceID.eventID)
				t.logger.Error(errString)
				return nil, serviceerror.NewInternal(errString)
			}

			t, _ := types.TimestampFromProto(timerTask.VisibilityTime)
			if isExpired := timerSequence.isExpired(
				t,
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
		activityInfo, ok := mutableState.GetActivityInfo(timerTask.GetEventId())
		goTS, _ := types.TimestampFromProto(timerTask.VisibilityTime)
		if isHeartBeatTask && ok && activityInfo.LastHeartbeatTimeoutVisibilityInSeconds <= goTS.Unix() {
			activityInfo.TimerTaskStatus = activityInfo.TimerTaskStatus &^ timerTaskStatusCreatedHeartbeat
			if err := mutableState.UpdateActivity(activityInfo); err != nil {
				return nil, err
			}
			updateMutableState = true
		}

		// passive logic need to explicitly call create timer
		modified, err := timerSequence.createNextActivityTimer()
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

		err = context.updateWorkflowExecutionAsPassive(now)
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

func (t *timerQueueStandbyTaskExecutor) executeDecisionTimeoutTask(
	timerTask *persistenceblobs.TimerTaskInfo,
) error {

	// decision schedule to start timer task is a special snowflake.
	// the schedule to start timer is for sticky decision, which is
	// not applicable on the passive cluster
	if timerTask.TimeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START {
		return nil
	}

	actionFn := func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

		decision, isPending := mutableState.GetDecisionInfo(timerTask.GetEventId())
		if !isPending {
			return nil, nil
		}

		ok, err := verifyTaskVersion(t.shard, t.logger, timerTask.GetNamespaceId(), decision.Version, timerTask.Version, timerTask)
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
	timerTask *persistenceblobs.TimerTaskInfo,
) error {

	actionFn := func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

		if mutableState.HasProcessedOrPendingDecision() {
			// if there is one decision already been processed
			// or has pending decision, meaning workflow has already running
			return nil, nil
		}

		// Note: do not need to verify task version here
		// logic can only go here if mutable state build's next event ID is 2
		// meaning history only contains workflow started event.
		// we can do the checking of task version vs workflow started version
		// however, workflow started version is immutable

		// active cluster will add first workflow task after backoff timeout.
		// standby cluster should just call ack manager to retry this task
		// since we are stilling waiting for the first DecisionScheduledEvent to be replicated from active side.

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
	timerTask *persistenceblobs.TimerTaskInfo,
) error {

	actionFn := func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

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
	mutableState mutableState,
) timerSequence {

	timeSource := clock.NewEventTimeSource()
	now := t.getStandbyClusterTime()
	timeSource.Update(now)
	return newTimerSequence(timeSource, mutableState)
}

func (t *timerQueueStandbyTaskExecutor) processTimer(
	timerTask *persistenceblobs.TimerTaskInfo,
	actionFn standbyActionFn,
	postActionFn standbyPostActionFn,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getNamespaceIDAndWorkflowExecution(timerTask),
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

	timerTask := taskInfo.(*persistenceblobs.TimerTaskInfo)
	resendInfo := postActionInfo.(*historyResendInfo)

	t.metricsClient.IncCounter(metrics.HistoryRereplicationByTimerTaskScope, metrics.ClientRequests)
	stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByTimerTaskScope, metrics.ClientLatency)
	defer stopwatch.Stop()

	var err error
	if resendInfo.lastEventID != common.EmptyEventID && resendInfo.lastEventVersion != common.EmptyVersion {
		err = t.nDCHistoryResender.SendSingleWorkflowHistory(
			timerTask.GetNamespaceId(),
			timerTask.GetWorkflowId(),
			timerTask.GetRunId(),
			resendInfo.lastEventID,
			resendInfo.lastEventVersion,
			common.EmptyEventID,
			common.EmptyVersion,
		)
	} else if resendInfo.nextEventID != nil {
		err = t.historyRereplicator.SendMultiWorkflowHistory(
			timerTask.GetNamespaceId(),
			timerTask.GetWorkflowId(),
			timerTask.GetRunId(),
			*resendInfo.nextEventID,
			timerTask.GetRunId(),
			common.EndEventID, // use common.EndEventID since we do not know where is the end
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
	return ErrTaskRetry
}

func (t *timerQueueStandbyTaskExecutor) getCurrentTime() time.Time {
	return t.shard.GetCurrentTime(t.clusterName)
}
