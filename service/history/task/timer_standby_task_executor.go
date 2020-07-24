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

package task

import (
	"errors"
	"fmt"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/xdc"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/worker/archiver"
)

var (
	errUnexpectedTask   = errors.New("unexpected task")
	errUnknownTimerTask = errors.New("unknown timer task")
)

type (
	timerStandbyTaskExecutor struct {
		*timerTaskExecutorBase

		clusterName         string
		historyRereplicator xdc.HistoryRereplicator
		nDCHistoryResender  xdc.NDCHistoryResender
	}
)

// NewTimerStandbyTaskExecutor creates a new task executor for standby timer task
func NewTimerStandbyTaskExecutor(
	shard shard.Context,
	archiverClient archiver.Client,
	executionCache *execution.Cache,
	historyRereplicator xdc.HistoryRereplicator,
	nDCHistoryResender xdc.NDCHistoryResender,
	logger log.Logger,
	metricsClient metrics.Client,
	clusterName string,
	config *config.Config,
) Executor {
	return &timerStandbyTaskExecutor{
		timerTaskExecutorBase: newTimerTaskExecutorBase(
			shard,
			archiverClient,
			executionCache,
			logger,
			metricsClient,
			config,
		),
		clusterName:         clusterName,
		historyRereplicator: historyRereplicator,
		nDCHistoryResender:  nDCHistoryResender,
	}
}

func (t *timerStandbyTaskExecutor) Execute(
	taskInfo Info,
	shouldProcessTask bool,
) error {

	timerTask, ok := taskInfo.(*persistence.TimerTaskInfo)
	if !ok {
		return errUnexpectedTask
	}

	if !shouldProcessTask &&
		timerTask.TaskType != persistence.TaskTypeWorkflowTimeout &&
		timerTask.TaskType != persistence.TaskTypeDeleteHistoryEvent {
		// guarantee the processing of workflow execution history deletion
		return nil
	}

	switch timerTask.TaskType {
	case persistence.TaskTypeUserTimer:
		return t.executeUserTimerTimeoutTask(timerTask)
	case persistence.TaskTypeActivityTimeout:
		return t.executeActivityTimeoutTask(timerTask)
	case persistence.TaskTypeDecisionTimeout:
		return t.executeDecisionTimeoutTask(timerTask)
	case persistence.TaskTypeWorkflowTimeout:
		return t.executeWorkflowTimeoutTask(timerTask)
	case persistence.TaskTypeActivityRetryTimer:
		// retry backoff timer should not get created on passive cluster
		// TODO: add error logs
		return nil
	case persistence.TaskTypeWorkflowBackoffTimer:
		return t.executeWorkflowBackoffTimerTask(timerTask)
	case persistence.TaskTypeDeleteHistoryEvent:
		return t.executeDeleteHistoryEventTask(timerTask)
	default:
		return errUnknownTimerTask
	}
}

func (t *timerStandbyTaskExecutor) executeUserTimerTimeoutTask(
	timerTask *persistence.TimerTaskInfo,
) error {

	actionFn := func(context execution.Context, mutableState execution.MutableState) (interface{}, error) {

		timerSequence := t.getTimerSequence(mutableState)

	Loop:
		for _, timerSequenceID := range timerSequence.LoadAndSortUserTimers() {
			_, ok := mutableState.GetUserTimerInfoByEventID(timerSequenceID.EventID)
			if !ok {
				errString := fmt.Sprintf("failed to find in user timer event ID: %v", timerSequenceID.EventID)
				t.logger.Error(errString)
				return nil, &workflow.InternalServiceError{Message: errString}
			}

			if isExpired := timerSequence.IsExpired(
				timerTask.VisibilityTimestamp,
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

func (t *timerStandbyTaskExecutor) executeActivityTimeoutTask(
	timerTask *persistence.TimerTaskInfo,
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

	actionFn := func(context execution.Context, mutableState execution.MutableState) (interface{}, error) {

		timerSequence := t.getTimerSequence(mutableState)
		updateMutableState := false

	Loop:
		for _, timerSequenceID := range timerSequence.LoadAndSortActivityTimers() {
			_, ok := mutableState.GetActivityInfo(timerSequenceID.EventID)
			if !ok {
				errString := fmt.Sprintf("failed to find in memory activity timer: %v", timerSequenceID.EventID)
				t.logger.Error(errString)
				return nil, &workflow.InternalServiceError{Message: errString}
			}

			if isExpired := timerSequence.IsExpired(
				timerTask.VisibilityTimestamp,
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
		isHeartBeatTask := timerTask.TimeoutType == int(workflow.TimeoutTypeHeartbeat)
		activityInfo, ok := mutableState.GetActivityInfo(timerTask.EventID)
		if isHeartBeatTask && ok && activityInfo.LastHeartbeatTimeoutVisibilityInSeconds <= timerTask.VisibilityTimestamp.Unix() {
			activityInfo.TimerTaskStatus = activityInfo.TimerTaskStatus &^ execution.TimerTaskStatusCreatedHeartbeat
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

func (t *timerStandbyTaskExecutor) executeDecisionTimeoutTask(
	timerTask *persistence.TimerTaskInfo,
) error {

	// decision schedule to start timer task is a special snowflake.
	// the schedule to start timer is for sticky decision, which is
	// not applicable on the passive cluster
	if timerTask.TimeoutType == int(workflow.TimeoutTypeScheduleToStart) {
		return nil
	}

	actionFn := func(context execution.Context, mutableState execution.MutableState) (interface{}, error) {

		decision, isPending := mutableState.GetDecisionInfo(timerTask.EventID)
		if !isPending {
			return nil, nil
		}

		ok, err := verifyTaskVersion(t.shard, t.logger, timerTask.DomainID, decision.Version, timerTask.Version, timerTask)
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

func (t *timerStandbyTaskExecutor) executeWorkflowBackoffTimerTask(
	timerTask *persistence.TimerTaskInfo,
) error {

	actionFn := func(context execution.Context, mutableState execution.MutableState) (interface{}, error) {

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

		// active cluster will add first decision task after backoff timeout.
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

func (t *timerStandbyTaskExecutor) executeWorkflowTimeoutTask(
	timerTask *persistence.TimerTaskInfo,
) error {

	actionFn := func(context execution.Context, mutableState execution.MutableState) (interface{}, error) {

		// we do not need to notify new timer to base, since if there is no new event being replicated
		// checking again if the timer can be completed is meaningless

		startVersion, err := mutableState.GetStartVersion()
		if err != nil {
			return nil, err
		}
		ok, err := verifyTaskVersion(t.shard, t.logger, timerTask.DomainID, startVersion, timerTask.Version, timerTask)
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

func (t *timerStandbyTaskExecutor) getStandbyClusterTime() time.Time {
	// time of remote cluster in the shard is delayed by "StandbyClusterDelay"
	// so to get the current accurate remote cluster time, need to add it back
	return t.shard.GetCurrentTime(t.clusterName).Add(t.shard.GetConfig().StandbyClusterDelay())
}

func (t *timerStandbyTaskExecutor) getTimerSequence(
	mutableState execution.MutableState,
) execution.TimerSequence {

	timeSource := clock.NewEventTimeSource()
	now := t.getStandbyClusterTime()
	timeSource.Update(now)
	return execution.NewTimerSequence(timeSource, mutableState)
}

func (t *timerStandbyTaskExecutor) processTimer(
	timerTask *persistence.TimerTaskInfo,
	actionFn standbyActionFn,
	postActionFn standbyPostActionFn,
) (retError error) {

	context, release, err := t.executionCache.GetOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(timerTask),
	)
	if err != nil {
		return err
	}
	defer func() {
		if retError == ErrTaskRedispatch {
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

func (t *timerStandbyTaskExecutor) fetchHistoryFromRemote(
	taskInfo Info,
	postActionInfo interface{},
	log log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	timerTask := taskInfo.(*persistence.TimerTaskInfo)
	resendInfo := postActionInfo.(*historyResendInfo)

	t.metricsClient.IncCounter(metrics.HistoryRereplicationByTimerTaskScope, metrics.CadenceClientRequests)
	stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByTimerTaskScope, metrics.CadenceClientLatency)
	defer stopwatch.Stop()

	var err error
	if resendInfo.lastEventID != nil && resendInfo.lastEventVersion != nil {
		err = t.nDCHistoryResender.SendSingleWorkflowHistory(
			timerTask.DomainID,
			timerTask.WorkflowID,
			timerTask.RunID,
			resendInfo.lastEventID,
			resendInfo.lastEventVersion,
			nil,
			nil,
		)
	} else if resendInfo.nextEventID != nil {
		err = t.historyRereplicator.SendMultiWorkflowHistory(
			timerTask.DomainID,
			timerTask.WorkflowID,
			timerTask.RunID,
			*resendInfo.nextEventID,
			timerTask.RunID,
			common.EndEventID, // use common.EndEventID since we do not know where is the end
		)
	} else {
		err = &workflow.InternalServiceError{
			Message: "timerQueueStandbyProcessor encounter empty historyResendInfo",
		}
	}

	if err != nil {
		t.logger.Error("Error re-replicating history from remote.",
			tag.ShardID(t.shard.GetShardID()),
			tag.WorkflowDomainID(timerTask.DomainID),
			tag.WorkflowID(timerTask.WorkflowID),
			tag.WorkflowRunID(timerTask.RunID),
			tag.ClusterName(t.clusterName))
	}

	// return error so task processing logic will retry
	return ErrTaskRedispatch
}

func (t *timerStandbyTaskExecutor) getCurrentTime() time.Time {
	return t.shard.GetCurrentTime(t.clusterName)
}
