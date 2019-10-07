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
	"fmt"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/xdc"
)

const (
	historyRereplicationTimeout = 30 * time.Second
)

type (
	timerQueueStandbyProcessorImpl struct {
		shard                   ShardContext
		clusterMetadata         cluster.Metadata
		historyService          *historyEngineImpl
		cache                   *historyCache
		timerTaskFilter         queueTaskFilter
		logger                  log.Logger
		metricsClient           metrics.Client
		clusterName             string
		timerGate               RemoteTimerGate
		timerQueueProcessorBase *timerQueueProcessorBase
		historyRereplicator     xdc.HistoryRereplicator
	}
)

func newTimerQueueStandbyProcessor(
	shard ShardContext,
	historyService *historyEngineImpl,
	clusterName string,
	taskAllocator taskAllocator,
	historyRereplicator xdc.HistoryRereplicator,
	logger log.Logger,
) *timerQueueStandbyProcessorImpl {

	timeNow := func() time.Time {
		return shard.GetCurrentTime(clusterName)
	}
	updateShardAckLevel := func(ackLevel TimerSequenceID) error {
		return shard.UpdateTimerClusterAckLevel(clusterName, ackLevel.VisibilityTimestamp)
	}
	logger = logger.WithTags(tag.ClusterName(clusterName))
	timerTaskFilter := func(qTask queueTaskInfo) (bool, error) {
		timer, ok := qTask.(*persistence.TimerTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.verifyStandbyTask(clusterName, timer.DomainID, timer)
	}

	timerGate := NewRemoteTimerGate()
	timerGate.SetCurrentTime(shard.GetCurrentTime(clusterName))
	timerQueueAckMgr := newTimerQueueAckMgr(
		metrics.TimerStandbyQueueProcessorScope,
		shard,
		historyService.metricsClient,
		shard.GetTimerClusterAckLevel(clusterName),
		timeNow,
		updateShardAckLevel,
		logger,
		clusterName,
	)
	processor := &timerQueueStandbyProcessorImpl{
		shard:           shard,
		clusterMetadata: shard.GetService().GetClusterMetadata(),
		historyService:  historyService,
		cache:           historyService.historyCache,
		timerTaskFilter: timerTaskFilter,
		logger:          logger,
		metricsClient:   historyService.metricsClient,
		clusterName:     clusterName,
		timerGate:       timerGate,
		timerQueueProcessorBase: newTimerQueueProcessorBase(
			metrics.TimerStandbyQueueProcessorScope,
			shard,
			historyService,
			timerQueueAckMgr,
			timerGate,
			shard.GetConfig().TimerProcessorMaxPollRPS,
			logger,
		),
		historyRereplicator: historyRereplicator,
	}
	processor.timerQueueProcessorBase.timerProcessor = processor
	return processor
}

func (t *timerQueueStandbyProcessorImpl) Start() {
	t.timerQueueProcessorBase.Start()
}

func (t *timerQueueStandbyProcessorImpl) Stop() {
	t.timerQueueProcessorBase.Stop()
}

func (t *timerQueueStandbyProcessorImpl) getTimerFiredCount() uint64 {
	return t.timerQueueProcessorBase.getTimerFiredCount()
}

func (t *timerQueueStandbyProcessorImpl) setCurrentTime(
	currentTime time.Time,
) {

	t.timerGate.SetCurrentTime(currentTime)
}

func (t *timerQueueStandbyProcessorImpl) retryTasks() {
	t.timerQueueProcessorBase.retryTasks()
}

func (t *timerQueueStandbyProcessorImpl) getTaskFilter() queueTaskFilter {
	return t.timerTaskFilter
}

func (t *timerQueueStandbyProcessorImpl) getAckLevel() TimerSequenceID {
	return t.timerQueueProcessorBase.timerQueueAckMgr.getAckLevel()
}

func (t *timerQueueStandbyProcessorImpl) getReadLevel() TimerSequenceID {
	return t.timerQueueProcessorBase.timerQueueAckMgr.getReadLevel()
}

// NotifyNewTimers - Notify the processor about the new standby timer events arrival.
// This should be called each time new timer events arrives, otherwise timers maybe fired unexpected.
func (t *timerQueueStandbyProcessorImpl) notifyNewTimers(
	timerTasks []persistence.Task,
) {

	t.timerQueueProcessorBase.notifyNewTimers(timerTasks)
}

func (t *timerQueueStandbyProcessorImpl) complete(
	qTask queueTaskInfo,
) {
	timerTask, ok := qTask.(*persistence.TimerTaskInfo)
	if !ok {
		return
	}
	t.timerQueueProcessorBase.complete(timerTask)
}

func (t *timerQueueStandbyProcessorImpl) process(
	qTask queueTaskInfo,
	shouldProcessTask bool,
) (int, error) {
	timerTask, ok := qTask.(*persistence.TimerTaskInfo)
	if !ok {
		return metrics.TimerStandbyQueueProcessorScope, errUnexpectedQueueTask
	}

	var err error
	lastAttempt := false
	switch timerTask.TaskType {
	case persistence.TaskTypeUserTimer:
		if shouldProcessTask {
			err = t.processExpiredUserTimer(timerTask, lastAttempt)
		}
		return metrics.TimerStandbyTaskUserTimerScope, err

	case persistence.TaskTypeActivityTimeout:
		if shouldProcessTask {
			err = t.processActivityTimeout(timerTask, lastAttempt)
		}
		return metrics.TimerStandbyTaskActivityTimeoutScope, err

	case persistence.TaskTypeDecisionTimeout:
		if shouldProcessTask {
			err = t.processDecisionTimeout(timerTask, lastAttempt)
		}
		return metrics.TimerStandbyTaskDecisionTimeoutScope, err

	case persistence.TaskTypeWorkflowTimeout:
		// guarantee the processing of workflow execution history deletion
		err = t.processWorkflowTimeout(timerTask, lastAttempt)
		return metrics.TimerStandbyTaskWorkflowTimeoutScope, err

	case persistence.TaskTypeActivityRetryTimer:
		// retry backoff timer should not get created on passive cluster
		return metrics.TimerStandbyTaskActivityRetryTimerScope, err

	case persistence.TaskTypeWorkflowBackoffTimer:
		if shouldProcessTask {
			err = t.processWorkflowBackoffTimer(timerTask, lastAttempt)
		}
		return metrics.TimerStandbyTaskWorkflowBackoffTimerScope, err

	case persistence.TaskTypeDeleteHistoryEvent:
		// guarantee the processing of workflow execution history deletion
		return metrics.TimerStandbyTaskDeleteHistoryEventScope, t.timerQueueProcessorBase.processDeleteHistoryEvent(timerTask)

	default:
		return metrics.TimerStandbyQueueProcessorScope, errUnknownTimerTask
	}
}

func (t *timerQueueStandbyProcessorImpl) processExpiredUserTimer(
	timerTask *persistence.TimerTaskInfo,
	lastAttempt bool,
) error {

	var nextEventID *int64
	postProcessingFn := func() error {
		return t.fetchHistoryAndVerifyOnce(timerTask, nextEventID, t.processExpiredUserTimer)
	}
	if lastAttempt {
		postProcessingFn = func() error {
			return standbyTimerTaskPostActionTaskDiscarded(nextEventID, timerTask, t.logger)
		}
	}

	return t.processTimer(timerTask, func(context workflowExecutionContext, msBuilder mutableState) error {
		tBuilder := t.getTimerBuilder()

	ExpireUserTimers:
		for _, td := range tBuilder.GetUserTimers(msBuilder) {
			hasTimer, _ := tBuilder.GetUserTimer(td.TimerID)
			if !hasTimer {
				t.logger.Debug(fmt.Sprintf("Failed to find in memory user timer: %s", td.TimerID))
				return fmt.Errorf("Failed to find in memory user timer: %s", td.TimerID)
			}

			if isExpired := tBuilder.IsTimerExpired(td, timerTask.VisibilityTimestamp); isExpired {
				// active cluster will add an timer fired event and schedule a decision if necessary
				// standby cluster should just call ack manager to retry this task
				// since we are stilling waiting for the fired event to be replicated
				//
				// we do not need to notity new timer to base, since if there is no new event being replicated
				// checking again if the timer can be completed is meaningless

				if t.discardTask(timerTask) {
					// returning nil and set next event ID
					// the post action function below shall take over
					nextEventID = common.Int64Ptr(msBuilder.GetNextEventID())
					return nil
				}
				return ErrTaskRetry
			}
			// since the user timer are already sorted, so if there is one timer which will not expired
			// all user timer after this timer will not expired
			break ExpireUserTimers
		}
		// if there is no user timer expired, then we are good
		return nil
	}, postProcessingFn)
}

func (t *timerQueueStandbyProcessorImpl) processActivityTimeout(
	timerTask *persistence.TimerTaskInfo,
	lastAttempt bool,
) error {

	// activity heartbeat timer task is a special snowflake.
	// normal activity timer task on the passive side will be generated by events related to activity in history replicator,
	// and the standby timer processor will only need to verify whether the timer task can be safely throw away.
	//
	// activity hearbeat timer task cannot be handled in the way mentioned above.
	// the reason is, there is no event driving the creation of new activity heartbeat timer.
	// although there will be an task syncing activity from remote, the task is not an event,
	// and cannot attempt to recreate a new activity timer task.
	//
	// the overall solution is to attempt to generate a new activity timer task whenever the
	// task passed in is safe to be throw away.

	var nextEventID *int64
	postProcessingFn := func() error {
		return t.fetchHistoryAndVerifyOnce(timerTask, nextEventID, t.processActivityTimeout)
	}
	if lastAttempt {
		postProcessingFn = func() error {
			return standbyTimerTaskPostActionTaskDiscarded(nextEventID, timerTask, t.logger)
		}
	}

	return t.processTimer(timerTask, func(context workflowExecutionContext, msBuilder mutableState) error {
		tBuilder := t.getTimerBuilder()

	ExpireActivityTimers:
		for _, td := range tBuilder.GetActivityTimers(msBuilder) {
			_, ok := msBuilder.GetActivityInfo(td.ActivityID)
			if !ok {
				//  We might have time out this activity already.
				continue ExpireActivityTimers
			}

			if isExpired := tBuilder.IsTimerExpired(td, timerTask.VisibilityTimestamp); isExpired {
				if t.discardTask(timerTask) {
					// returning nil and set next event ID
					// the post action function below shall take over
					nextEventID = common.Int64Ptr(msBuilder.GetNextEventID())
					return nil
				}
				return ErrTaskRetry
			}

			// since the activity timer are already sorted, so if there is one timer which will not expired
			// all activity timer after this timer will not expired
			break ExpireActivityTimers
		}

		// for reason to update mutable state & generate a new activity task,
		// see comments at the beginning of this function.
		// NOTE: this is the only place in the standby logic where mutable state can be updated

		// need to clear the activity heartbeat timer task marks
		doUpdate := false
		lastWriteVersion, err := msBuilder.GetLastWriteVersion()
		if err != nil {
			return err
		}
		isHeartBeatTask := timerTask.TimeoutType == int(workflow.TimeoutTypeHeartbeat)
		if activityInfo, ok := msBuilder.GetActivityInfo(timerTask.EventID); isHeartBeatTask && ok {
			doUpdate = true
			activityInfo.TimerTaskStatus = activityInfo.TimerTaskStatus &^ TimerTaskStatusCreatedHeartbeat
			if err := msBuilder.UpdateActivity(activityInfo); err != nil {
				return err
			}
		}
		newTimerTasks := []persistence.Task{}
		if newTimerTask := t.getTimerBuilder().GetActivityTimerTaskIfNeeded(msBuilder); newTimerTask != nil {
			doUpdate = true
			newTimerTasks = append(newTimerTasks, newTimerTask)
		}

		if !doUpdate {
			return nil
		}

		now := t.getStandbyClusterTime()
		// we need to handcraft some of the variables
		// since the job being done here is update the activity and possibly write a timer task to DB
		// also need to reset the current version.
		if err := msBuilder.UpdateCurrentVersion(lastWriteVersion, true); err != nil {
			return err
		}

		msBuilder.AddTimerTasks(newTimerTasks...)
		err = context.updateWorkflowExecutionAsPassive(now)
		if err == nil {
			t.notifyNewTimers(newTimerTasks)
		}
		return err
	}, postProcessingFn)
}

func (t *timerQueueStandbyProcessorImpl) processDecisionTimeout(
	timerTask *persistence.TimerTaskInfo,
	lastAttempt bool,
) error {

	// decision schedule to start timer task is a special snowflake.
	// the schedule to start timer is for sticky decision, which is
	// not applicable on the passive cluster
	if timerTask.TimeoutType == int(workflow.TimeoutTypeScheduleToStart) {
		return nil
	}

	var nextEventID *int64
	postProcessingFn := func() error {
		return t.fetchHistoryAndVerifyOnce(timerTask, nextEventID, t.processDecisionTimeout)
	}
	if lastAttempt {
		postProcessingFn = func() error {
			return standbyTimerTaskPostActionTaskDiscarded(nextEventID, timerTask, t.logger)
		}
	}

	return t.processTimer(timerTask, func(context workflowExecutionContext, msBuilder mutableState) error {
		decision, isPending := msBuilder.GetDecisionInfo(timerTask.EventID)

		if !isPending {
			return nil
		}

		ok, err := verifyTaskVersion(t.shard, t.logger, timerTask.DomainID, decision.Version, timerTask.Version, timerTask)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		// active cluster will add an decision timeout event and schedule a decision
		// standby cluster should just call ack manager to retry this task
		// since we are stilling waiting for the decision timeout event / decision completion to be replicated
		//
		// we do not need to notify new timer to base, since if there is no new event being replicated
		// checking again if the timer can be completed is meaningless

		if t.discardTask(timerTask) {
			// returning nil and set next event ID
			// the post action function below shall take over
			nextEventID = common.Int64Ptr(msBuilder.GetNextEventID())
			return nil
		}
		return ErrTaskRetry
	}, postProcessingFn)
}

func (t *timerQueueStandbyProcessorImpl) processWorkflowBackoffTimer(
	timerTask *persistence.TimerTaskInfo,
	lastAttempt bool,
) error {

	var nextEventID *int64
	postProcessingFn := func() error {
		return t.fetchHistoryAndVerifyOnce(timerTask, nextEventID, t.processWorkflowBackoffTimer)
	}
	if lastAttempt {
		postProcessingFn = func() error {
			return standbyTimerTaskPostActionTaskDiscarded(nextEventID, timerTask, t.logger)
		}
	}

	return t.processTimer(timerTask, func(context workflowExecutionContext, msBuilder mutableState) error {

		if msBuilder.HasProcessedOrPendingDecision() {
			// if there is one decision already been processed
			// or has pending decision, meaning workflow has already running
			return nil
		}

		// Note: do not need to verify task version here
		// logic can only go here if mutable state build's next event ID is 2
		// meaning history only contains workflow started event.
		// we can do the checking of task version vs workflow started version
		// however, workflow started version is immutable

		// active cluster will add first decision task after backoff timeout.
		// standby cluster should just call ack manager to retry this task
		// since we are stilling waiting for the first DecisionSchedueldEvent to be replicated from active side.
		//
		// we do not need to notity new timer to base, since if there is no new event being replicated
		// checking again if the timer can be completed is meaningless

		if t.discardTask(timerTask) {
			// returning nil and set next event ID
			// the post action function below shall take over
			nextEventID = common.Int64Ptr(msBuilder.GetNextEventID())
			return nil
		}
		return ErrTaskRetry
	}, postProcessingFn)
}

func (t *timerQueueStandbyProcessorImpl) processWorkflowTimeout(
	timerTask *persistence.TimerTaskInfo,
	lastAttempt bool,
) error {

	var nextEventID *int64
	postProcessingFn := func() error {
		return t.fetchHistoryAndVerifyOnce(timerTask, nextEventID, t.processWorkflowTimeout)
	}
	if lastAttempt {
		postProcessingFn = func() error {
			return standbyTimerTaskPostActionTaskDiscarded(nextEventID, timerTask, t.logger)
		}
	}

	return t.processTimer(timerTask, func(context workflowExecutionContext, msBuilder mutableState) error {
		// we do not need to notity new timer to base, since if there is no new event being replicated
		// checking again if the timer can be completed is meaningless

		startVersion, err := msBuilder.GetStartVersion()
		if err != nil {
			return err
		}
		ok, err := verifyTaskVersion(t.shard, t.logger, timerTask.DomainID, startVersion, timerTask.Version, timerTask)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		if t.discardTask(timerTask) {
			// returning nil and set next event ID
			// the post action function below shall take over
			nextEventID = common.Int64Ptr(msBuilder.GetNextEventID())
			return nil
		}
		return ErrTaskRetry
	}, postProcessingFn)
}

func (t *timerQueueStandbyProcessorImpl) getStandbyClusterTime() time.Time {
	// time of remote cluster in the shard is delayed by "StandbyClusterDelay"
	// so to get the current accurate remote cluster time, need to add it back
	return t.shard.GetCurrentTime(t.clusterName).Add(t.shard.GetConfig().StandbyClusterDelay())
}

func (t *timerQueueStandbyProcessorImpl) getTimerBuilder() *timerBuilder {
	timeSource := clock.NewEventTimeSource()
	now := t.getStandbyClusterTime()
	timeSource.Update(now)
	return newTimerBuilder(timeSource)
}

func (t *timerQueueStandbyProcessorImpl) processTimer(
	timerTask *persistence.TimerTaskInfo,
	action func(workflowExecutionContext, mutableState) error,
	postAction func() error,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.timerQueueProcessorBase.getDomainIDAndWorkflowExecution(timerTask),
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

	msBuilder, err := loadMutableStateForTimerTask(context, timerTask, t.metricsClient, t.logger)
	if err != nil {
		return err
	} else if msBuilder == nil {
		return nil
	}

	if !msBuilder.IsWorkflowExecutionRunning() {
		// workflow already finished, no need to process the timer
		return nil
	}

	err = action(context, msBuilder)
	if err != nil {
		return err
	}

	release(nil)
	err = postAction()
	return err
}

func (t *timerQueueStandbyProcessorImpl) fetchHistoryAndVerifyOnce(
	timerTask *persistence.TimerTaskInfo,
	nextEventID *int64,
	verifyFn func(*persistence.TimerTaskInfo, bool) error,
) error {

	if nextEventID == nil {
		return nil
	}
	err := t.fetchHistoryFromRemote(timerTask, *nextEventID)
	if err != nil {
		// fail to fetch events from remote, just discard the task
		return ErrTaskDiscarded
	}
	lastAttempt := true
	err = verifyFn(timerTask, lastAttempt)
	if err != nil {
		// task still pending, just discard the task
		return ErrTaskDiscarded
	}
	return nil
}

func (t *timerQueueStandbyProcessorImpl) fetchHistoryFromRemote(
	timerTask *persistence.TimerTaskInfo,
	nextEventID int64,
) error {

	t.metricsClient.IncCounter(metrics.HistoryRereplicationByTimerTaskScope, metrics.CadenceClientRequests)
	stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByTimerTaskScope, metrics.CadenceClientLatency)
	defer stopwatch.Stop()
	err := t.historyRereplicator.SendMultiWorkflowHistory(
		timerTask.DomainID, timerTask.WorkflowID,
		timerTask.RunID, nextEventID,
		timerTask.RunID, common.EndEventID, // use common.EndEventID since we do not know where is the end
	)
	if err != nil {
		t.logger.Error("Error re-replicating history from remote.",
			tag.WorkflowID(timerTask.WorkflowID),
			tag.WorkflowRunID(timerTask.RunID),
			tag.WorkflowDomainID(timerTask.DomainID),
			tag.ShardID(t.shard.GetShardID()),
			tag.WorkflowNextEventID(nextEventID),
			tag.ClusterName(t.clusterName))
	}
	return err
}

func (t *timerQueueStandbyProcessorImpl) discardTask(
	timerTask *persistence.TimerTaskInfo,
) bool {

	// the current time got from shard is already delayed by t.shard.GetConfig().StandbyClusterDelay()
	// so discard will be true if task is delayed by 4*t.shard.GetConfig().StandbyClusterDelay()
	now := t.shard.GetCurrentTime(t.clusterName)
	return now.Sub(timerTask.GetVisibilityTimestamp()) > 3*t.shard.GetConfig().StandbyClusterDelay()
}
