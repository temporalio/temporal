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

	"github.com/pborman/uuid"
	m "github.com/uber/cadence/.gen/go/matching"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	timerQueueActiveProcessorImpl struct {
		shard                   ShardContext
		historyService          *historyEngineImpl
		cache                   *historyCache
		timerTaskFilter         timerTaskFilter
		now                     timeNow
		logger                  log.Logger
		metricsClient           metrics.Client
		currentClusterName      string
		matchingClient          matching.Client
		timerQueueProcessorBase *timerQueueProcessorBase
		config                  *Config
	}
)

func newTimerQueueActiveProcessor(
	shard ShardContext,
	historyService *historyEngineImpl,
	matchingClient matching.Client,
	taskAllocator taskAllocator,
	logger log.Logger,
) *timerQueueActiveProcessorImpl {

	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	timeNow := func() time.Time {
		return shard.GetCurrentTime(currentClusterName)
	}
	updateShardAckLevel := func(ackLevel TimerSequenceID) error {
		return shard.UpdateTimerClusterAckLevel(currentClusterName, ackLevel.VisibilityTimestamp)
	}
	logger = logger.WithTags(tag.ClusterName(currentClusterName))
	timerTaskFilter := func(timer *persistence.TimerTaskInfo) (bool, error) {
		return taskAllocator.verifyActiveTask(timer.DomainID, timer)
	}

	timerQueueAckMgr := newTimerQueueAckMgr(
		metrics.TimerActiveQueueProcessorScope,
		shard,
		historyService.metricsClient,
		shard.GetTimerClusterAckLevel(currentClusterName),
		timeNow,
		updateShardAckLevel,
		logger,
		currentClusterName,
	)

	timerGate := NewLocalTimerGate(shard.GetTimeSource())
	processor := &timerQueueActiveProcessorImpl{
		shard:              shard,
		historyService:     historyService,
		cache:              historyService.historyCache,
		timerTaskFilter:    timerTaskFilter,
		now:                timeNow,
		logger:             logger,
		matchingClient:     matchingClient,
		metricsClient:      historyService.metricsClient,
		currentClusterName: currentClusterName,
		timerQueueProcessorBase: newTimerQueueProcessorBase(
			metrics.TimerActiveQueueProcessorScope,
			shard,
			historyService,
			timerQueueAckMgr,
			timerGate,
			shard.GetConfig().TimerProcessorMaxPollRPS,
			logger,
		),
		config: shard.GetConfig(),
	}
	processor.timerQueueProcessorBase.timerProcessor = processor
	return processor
}

func newTimerQueueFailoverProcessor(
	shard ShardContext,
	historyService *historyEngineImpl,
	domainIDs map[string]struct{},
	standbyClusterName string,
	minLevel time.Time,
	maxLevel time.Time,
	matchingClient matching.Client,
	taskAllocator taskAllocator,
	logger log.Logger,
) (func(ackLevel TimerSequenceID) error, *timerQueueActiveProcessorImpl) {

	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	timeNow := func() time.Time {
		// should use current cluster's time when doing domain failover
		return shard.GetCurrentTime(currentClusterName)
	}
	failoverStartTime := shard.GetTimeSource().Now()
	failoverUUID := uuid.New()

	updateShardAckLevel := func(ackLevel TimerSequenceID) error {
		return shard.UpdateTimerFailoverLevel(
			failoverUUID,
			persistence.TimerFailoverLevel{
				StartTime:    failoverStartTime,
				MinLevel:     minLevel,
				CurrentLevel: ackLevel.VisibilityTimestamp,
				MaxLevel:     maxLevel,
				DomainIDs:    domainIDs,
			},
		)
	}
	timerAckMgrShutdown := func() error {
		return shard.DeleteTimerFailoverLevel(failoverUUID)
	}

	logger = logger.WithTags(
		tag.ClusterName(currentClusterName),
		tag.WorkflowDomainIDs(domainIDs),
		tag.FailoverMsg("from: "+standbyClusterName),
	)
	timerTaskFilter := func(timer *persistence.TimerTaskInfo) (bool, error) {
		return taskAllocator.verifyFailoverActiveTask(domainIDs, timer.DomainID, timer)
	}

	timerQueueAckMgr := newTimerQueueFailoverAckMgr(
		shard,
		historyService.metricsClient,
		minLevel,
		maxLevel,
		timeNow,
		updateShardAckLevel,
		timerAckMgrShutdown,
		logger,
	)

	timerGate := NewLocalTimerGate(shard.GetTimeSource())
	processor := &timerQueueActiveProcessorImpl{
		shard:              shard,
		historyService:     historyService,
		cache:              historyService.historyCache,
		timerTaskFilter:    timerTaskFilter,
		now:                timeNow,
		logger:             logger,
		metricsClient:      historyService.metricsClient,
		matchingClient:     matchingClient,
		currentClusterName: currentClusterName,
		timerQueueProcessorBase: newTimerQueueProcessorBase(
			metrics.TimerActiveQueueProcessorScope,
			shard,
			historyService,
			timerQueueAckMgr,
			timerGate,
			shard.GetConfig().TimerProcessorFailoverMaxPollRPS,
			logger,
		),
	}
	processor.timerQueueProcessorBase.timerProcessor = processor
	return updateShardAckLevel, processor
}

func (t *timerQueueActiveProcessorImpl) Start() {
	t.timerQueueProcessorBase.Start()
}

func (t *timerQueueActiveProcessorImpl) Stop() {
	t.timerQueueProcessorBase.Stop()
}

func (t *timerQueueActiveProcessorImpl) getTimerFiredCount() uint64 {
	return t.timerQueueProcessorBase.getTimerFiredCount()
}

func (t *timerQueueActiveProcessorImpl) getTaskFilter() timerTaskFilter {
	return t.timerTaskFilter
}

func (t *timerQueueActiveProcessorImpl) getAckLevel() TimerSequenceID {
	return t.timerQueueProcessorBase.timerQueueAckMgr.getAckLevel()
}

func (t *timerQueueActiveProcessorImpl) getReadLevel() TimerSequenceID {
	return t.timerQueueProcessorBase.timerQueueAckMgr.getReadLevel()
}

// NotifyNewTimers - Notify the processor about the new active timer events arrival.
// This should be called each time new timer events arrives, otherwise timers maybe fired unexpected.
func (t *timerQueueActiveProcessorImpl) notifyNewTimers(
	timerTasks []persistence.Task,
) {
	t.timerQueueProcessorBase.notifyNewTimers(timerTasks)
}

func (t *timerQueueActiveProcessorImpl) process(
	timerTask *persistence.TimerTaskInfo,
	shouldProcessTask bool,
) (int, error) {

	var err error
	switch timerTask.TaskType {
	case persistence.TaskTypeUserTimer:
		if shouldProcessTask {
			err = t.processExpiredUserTimer(timerTask)
		}
		return metrics.TimerActiveTaskUserTimerScope, err

	case persistence.TaskTypeActivityTimeout:
		if shouldProcessTask {
			err = t.processActivityTimeout(timerTask)
		}
		return metrics.TimerActiveTaskActivityTimeoutScope, err

	case persistence.TaskTypeDecisionTimeout:
		if shouldProcessTask {
			err = t.processDecisionTimeout(timerTask)
		}
		return metrics.TimerActiveTaskDecisionTimeoutScope, err

	case persistence.TaskTypeWorkflowTimeout:
		if shouldProcessTask {
			err = t.processWorkflowTimeout(timerTask)
		}
		return metrics.TimerActiveTaskWorkflowTimeoutScope, err

	case persistence.TaskTypeActivityRetryTimer:
		if shouldProcessTask {
			err = t.processActivityRetryTimer(timerTask)
		}
		return metrics.TimerActiveTaskActivityRetryTimerScope, err

	case persistence.TaskTypeWorkflowBackoffTimer:
		if shouldProcessTask {
			err = t.processWorkflowBackoffTimer(timerTask)
		}
		return metrics.TimerActiveTaskWorkflowBackoffTimerScope, err

	case persistence.TaskTypeDeleteHistoryEvent:
		if shouldProcessTask {
			err = t.timerQueueProcessorBase.processDeleteHistoryEvent(timerTask)
		}
		return metrics.TimerActiveTaskDeleteHistoryEventScope, err

	default:
		return metrics.TimerActiveQueueProcessorScope, errUnknownTimerTask
	}
}

func (t *timerQueueActiveProcessorImpl) processExpiredUserTimer(
	task *persistence.TimerTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.timerQueueProcessorBase.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	msBuilder, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	} else if msBuilder == nil || !msBuilder.IsWorkflowExecutionRunning() {
		return nil
	}
	tBuilder := t.historyService.getTimerBuilder(context.getExecution())

	var timerTasks []persistence.Task
	scheduleNewDecision := false

ExpireUserTimers:
	for _, td := range tBuilder.GetUserTimers(msBuilder) {
		hasTimer, ti := tBuilder.GetUserTimer(td.TimerID)
		if !hasTimer {
			t.logger.Debug(fmt.Sprintf("Failed to find in memory user timer: %s", td.TimerID))
			return fmt.Errorf("Failed to find in memory user timer: %s", td.TimerID)
		}

		if isExpired := tBuilder.IsTimerExpired(td, task.VisibilityTimestamp); isExpired {
			// Add TimerFired event to history.
			if _, err := msBuilder.AddTimerFiredEvent(ti.StartedID, ti.TimerID); err != nil {
				return err
			}

			scheduleNewDecision = !msBuilder.HasPendingDecisionTask()
		} else {
			// See if we have next timer in list to be created.
			if !td.TaskCreated {
				nextTask := tBuilder.createNewTask(td)
				timerTasks = []persistence.Task{nextTask}

				// Update the task ID tracking the corresponding timer task.
				ti.TaskID = TimerTaskStatusCreated
				msBuilder.UpdateUserTimer(ti.TimerID, ti)
			}

			// Done!
			break ExpireUserTimers
		}
	}

	// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
	// the history and try the operation again.
	return t.updateWorkflowExecution(context, msBuilder, scheduleNewDecision, false, timerTasks)
}

func (t *timerQueueActiveProcessorImpl) processActivityTimeout(
	task *persistence.TimerTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.timerQueueProcessorBase.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	referenceTime := t.now()

	msBuilder, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	} else if msBuilder == nil || !msBuilder.IsWorkflowExecutionRunning() {
		return nil
	}
	tBuilder := t.historyService.getTimerBuilder(context.getExecution())

	var timerTasks []persistence.Task
	updateHistory := false
	updateState := false
	ai, running := msBuilder.GetActivityInfo(task.EventID)
	if running {
		// If current one is HB task then we may need to create the next heartbeat timer.  Clear the create flag for this
		// heartbeat timer so we can create it again if needed.
		// NOTE: When record activity HB comes in we only update last heartbeat timestamp, this is the place
		// where we create next timer task based on that new updated timestamp.
		isHeartBeatTask := task.TimeoutType == int(workflow.TimeoutTypeHeartbeat)
		if isHeartBeatTask && ai.LastHeartbeatTimeoutVisibility <= task.VisibilityTimestamp.Unix() {
			ai.TimerTaskStatus = ai.TimerTaskStatus &^ TimerTaskStatusCreatedHeartbeat
			msBuilder.UpdateActivity(ai)
			updateState = true
		}

		// No need to check for attempt on the timer task.  ExpireActivityTimer logic below already checks if the
		// activity should be timedout and it will not let the timer expire for earlier attempts.  And creation of
		// duplicate timer task is protected by Created flag.
	}

ExpireActivityTimers:
	for _, td := range tBuilder.GetActivityTimers(msBuilder) {
		ai, isRunning := msBuilder.GetActivityInfo(td.ActivityID)
		if !isRunning {
			//  We might have time out this activity already.
			continue ExpireActivityTimers
		}

		if isExpired := tBuilder.IsTimerExpired(td, referenceTime); !isExpired {
			break ExpireActivityTimers
		}

		timeoutType := td.TimeoutType
		t.logger.Debug(fmt.Sprintf("Activity TimeoutType: %v, scheduledID: %v, startedId: %v. \n",
			timeoutType, ai.ScheduleID, ai.StartedID))

		if td.Attempt < ai.Attempt {
			// retry could update ai.Attempt, and we should ignore further timeouts for previous attempt
			t.logger.Info("Retry attempt mismatch, skip activity timeout processing",
				tag.WorkflowID(msBuilder.GetExecutionInfo().WorkflowID),
				tag.WorkflowRunID(msBuilder.GetExecutionInfo().RunID),
				tag.WorkflowDomainID(msBuilder.GetExecutionInfo().DomainID),
				tag.WorkflowScheduleID(ai.ScheduleID),
				tag.Attempt(ai.Attempt),
				tag.FailoverVersion(ai.Version),
				tag.TimerTaskStatus(ai.TimerTaskStatus),
				tag.WorkflowTimeoutType(int64(timeoutType)))
			continue
		}

		if timeoutType != workflow.TimeoutTypeScheduleToStart {
			// ScheduleToStart (queue timeout) is not retriable. Instead of retry, customer should set larger
			// ScheduleToStart timeout.
			retryTask := msBuilder.CreateActivityRetryTimer(ai, getTimeoutErrorReason(timeoutType))
			if retryTask != nil {
				timerTasks = append(timerTasks, retryTask)
				updateState = true

				t.logger.Info("Ignore activity timeout due to retry",
					tag.WorkflowID(msBuilder.GetExecutionInfo().WorkflowID),
					tag.WorkflowRunID(msBuilder.GetExecutionInfo().RunID),
					tag.WorkflowDomainID(msBuilder.GetExecutionInfo().DomainID),
					tag.WorkflowScheduleID(ai.ScheduleID),
					tag.Attempt(ai.Attempt),
					tag.FailoverVersion(ai.Version),
					tag.TimerTaskStatus(ai.TimerTaskStatus),
					tag.WorkflowTimeoutType(int64(timeoutType)))

				continue
			}
		}

		switch timeoutType {
		case workflow.TimeoutTypeScheduleToClose:
			{
				t.metricsClient.IncCounter(metrics.TimerActiveTaskActivityTimeoutScope, metrics.ScheduleToCloseTimeoutCounter)
				if _, err := msBuilder.AddActivityTaskTimedOutEvent(ai.ScheduleID, ai.StartedID, timeoutType, ai.Details); err != nil {
					return err
				}
				updateHistory = true
			}

		case workflow.TimeoutTypeStartToClose:
			{
				t.metricsClient.IncCounter(metrics.TimerActiveTaskActivityTimeoutScope, metrics.StartToCloseTimeoutCounter)
				if ai.StartedID != common.EmptyEventID {
					if _, err := msBuilder.AddActivityTaskTimedOutEvent(ai.ScheduleID, ai.StartedID, timeoutType, ai.Details); err != nil {
						return err
					}
					updateHistory = true
				}
			}

		case workflow.TimeoutTypeHeartbeat:
			{
				t.metricsClient.IncCounter(metrics.TimerActiveTaskActivityTimeoutScope, metrics.HeartbeatTimeoutCounter)
				if _, err := msBuilder.AddActivityTaskTimedOutEvent(ai.ScheduleID, ai.StartedID, timeoutType, ai.Details); err != nil {
					return err
				}
				updateHistory = true
			}

		case workflow.TimeoutTypeScheduleToStart:
			{
				t.metricsClient.IncCounter(metrics.TimerActiveTaskActivityTimeoutScope, metrics.ScheduleToStartTimeoutCounter)
				if ai.StartedID == common.EmptyEventID {
					if _, err := msBuilder.AddActivityTaskTimedOutEvent(ai.ScheduleID, ai.StartedID, timeoutType, ai.Details); err != nil {
						return err
					}
					updateHistory = true
				}
			}
		}
	}

	// use a new timer builder, since during the above for loop, the some timer definitions can be invalid
	if tt := t.historyService.getTimerBuilder(context.getExecution()).GetActivityTimerTaskIfNeeded(msBuilder); tt != nil {
		updateState = true
		timerTasks = append(timerTasks, tt)
	}

	if updateHistory || updateState {
		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		scheduleNewDecision := updateHistory && !msBuilder.HasPendingDecisionTask()
		return t.updateWorkflowExecution(context, msBuilder, scheduleNewDecision, false, timerTasks)
	}
	return nil
}

func (t *timerQueueActiveProcessorImpl) processDecisionTimeout(
	task *persistence.TimerTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.timerQueueProcessorBase.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	msBuilder, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	} else if msBuilder == nil || !msBuilder.IsWorkflowExecutionRunning() {
		return nil
	}

	scheduleID := task.EventID
	di, found := msBuilder.GetPendingDecision(scheduleID)
	if !found {
		t.logger.Debug("Potentially duplicate task.", tag.TaskID(task.TaskID), tag.WorkflowScheduleID(scheduleID), tag.TaskType(persistence.TaskTypeDecisionTimeout))
		return nil
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, di.Version, task.Version, task)
	if err != nil {
		return err
	} else if !ok {
		return nil
	}

	scheduleNewDecision := false
	switch task.TimeoutType {
	case int(workflow.TimeoutTypeStartToClose):
		t.metricsClient.IncCounter(metrics.TimerActiveTaskDecisionTimeoutScope, metrics.StartToCloseTimeoutCounter)
		if di.Attempt == task.ScheduleAttempt {
			// Add a decision task timeout event.
			msBuilder.AddDecisionTaskTimedOutEvent(scheduleID, di.StartedID)
			scheduleNewDecision = true
		}
	case int(workflow.TimeoutTypeScheduleToStart):
		t.metricsClient.IncCounter(metrics.TimerActiveTaskDecisionTimeoutScope, metrics.ScheduleToStartTimeoutCounter)
		// check if scheduled decision still pending and not started yet
		if di.Attempt == task.ScheduleAttempt && di.StartedID == common.EmptyEventID {
			_, err := msBuilder.AddDecisionTaskScheduleToStartTimeoutEvent(scheduleID)
			if err != nil {
				// Unable to add DecisionTaskTimeout event to history
				return &workflow.InternalServiceError{Message: "Unable to add DecisionTaskScheduleToStartTimeout event to history."}
			}

			// reschedule decision, which will be on its original task list
			scheduleNewDecision = true
		}
	}

	if scheduleNewDecision {
		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		return t.updateWorkflowExecution(context, msBuilder, scheduleNewDecision, false, nil)
	}
	return nil
}

func (t *timerQueueActiveProcessorImpl) processWorkflowBackoffTimer(
	task *persistence.TimerTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.timerQueueProcessorBase.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	if task.TimeoutType == persistence.WorkflowBackoffTimeoutTypeRetry {
		t.metricsClient.IncCounter(metrics.TimerActiveTaskWorkflowBackoffTimerScope, metrics.WorkflowRetryBackoffTimerCount)
	} else {
		t.metricsClient.IncCounter(metrics.TimerActiveTaskWorkflowBackoffTimerScope, metrics.WorkflowCronBackoffTimerCount)
	}

	msBuilder, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	} else if msBuilder == nil || !msBuilder.IsWorkflowExecutionRunning() {
		return nil
	}

	if msBuilder.HasProcessedOrPendingDecisionTask() {
		// already has decision task
		return nil
	}

	// schedule first decision task
	return t.updateWorkflowExecution(context, msBuilder, true, false, nil)
}

func (t *timerQueueActiveProcessorImpl) processActivityRetryTimer(
	task *persistence.TimerTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.timerQueueProcessorBase.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	msBuilder, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	} else if msBuilder == nil || !msBuilder.IsWorkflowExecutionRunning() {
		return nil
	}

	// generate activity task
	scheduledID := task.EventID
	ai, running := msBuilder.GetActivityInfo(scheduledID)
	if !running || task.ScheduleAttempt < int64(ai.Attempt) {
		if running && ai != nil {
			t.logger.Info("Duplicate activity retry timer task",
				tag.WorkflowID(msBuilder.GetExecutionInfo().WorkflowID),
				tag.WorkflowRunID(msBuilder.GetExecutionInfo().RunID),
				tag.WorkflowDomainID(msBuilder.GetExecutionInfo().DomainID),
				tag.WorkflowScheduleID(ai.ScheduleID),
				tag.Attempt(ai.Attempt),
				tag.FailoverVersion(ai.Version),
				tag.TimerTaskStatus(ai.TimerTaskStatus),
				tag.ScheduleAttempt(task.ScheduleAttempt))
		}
		return nil
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, ai.Version, task.Version, task)
	if err != nil {
		return err
	} else if !ok {
		return nil
	}

	domainID := task.DomainID
	targetDomainID := domainID
	scheduledEvent, ok := msBuilder.GetActivityScheduledEvent(scheduledID)
	if !ok {
		return &workflow.InternalServiceError{Message: "Unable to get activity schedule event."}
	}
	if scheduledEvent.ActivityTaskScheduledEventAttributes.Domain != nil {
		domainEntry, err := t.shard.GetDomainCache().GetDomain(scheduledEvent.ActivityTaskScheduledEventAttributes.GetDomain())
		if err != nil {
			return &workflow.InternalServiceError{Message: "Unable to re-schedule activity across domain."}
		}
		targetDomainID = domainEntry.GetInfo().ID
	}

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID)}
	taskList := &workflow.TaskList{
		Name: &ai.TaskList,
	}
	scheduleToStartTimeout := ai.ScheduleToStartTimeout

	release(nil) // release earlier as we don't need the lock anymore

	return t.matchingClient.AddActivityTask(nil, &m.AddActivityTaskRequest{
		DomainUUID:                    common.StringPtr(targetDomainID),
		SourceDomainUUID:              common.StringPtr(domainID),
		Execution:                     &execution,
		TaskList:                      taskList,
		ScheduleId:                    &scheduledID,
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(scheduleToStartTimeout),
	})
}

func (t *timerQueueActiveProcessorImpl) processWorkflowTimeout(
	task *persistence.TimerTaskInfo,
) (retError error) {

	domainID, execution := t.timerQueueProcessorBase.getDomainIDAndWorkflowExecution(task)
	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(domainID, execution)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	msBuilder, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	} else if msBuilder == nil || !msBuilder.IsWorkflowExecutionRunning() {
		return nil
	}

	// do version check for global domain task
	if msBuilder.GetReplicationState() != nil {
		ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, msBuilder.GetReplicationState().StartVersion, task.Version, task)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}
	}

	timeoutReason := getTimeoutErrorReason(workflow.TimeoutTypeStartToClose)
	backoffInterval := msBuilder.GetRetryBackoffDuration(timeoutReason)
	continueAsNewInitiator := workflow.ContinueAsNewInitiatorRetryPolicy
	if backoffInterval == backoff.NoBackoff {
		// check if a cron backoff is needed
		backoffInterval, err = msBuilder.GetCronBackoffDuration()
		if err != nil {
			return err
		}
		continueAsNewInitiator = workflow.ContinueAsNewInitiatorCronSchedule
	}
	if backoffInterval == backoff.NoBackoff {
		if _, err := msBuilder.AddTimeoutWorkflowEvent(); err != nil {
			return err
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		return t.updateWorkflowExecution(context, msBuilder, false, true, nil)
	}

	// workflow timeout, but a retry or cron is needed, so we do continue as new to retry or cron
	startEvent, found := msBuilder.GetStartEvent()
	if !found {
		return &workflow.InternalServiceError{Message: "Failed to load start event."}
	}

	startAttributes := startEvent.WorkflowExecutionStartedEventAttributes
	continueAsnewAttributes := &workflow.ContinueAsNewWorkflowExecutionDecisionAttributes{
		WorkflowType:                        startAttributes.WorkflowType,
		TaskList:                            startAttributes.TaskList,
		RetryPolicy:                         startAttributes.RetryPolicy,
		Input:                               startAttributes.Input,
		Header:                              startAttributes.Header,
		ExecutionStartToCloseTimeoutSeconds: startAttributes.ExecutionStartToCloseTimeoutSeconds,
		TaskStartToCloseTimeoutSeconds:      startAttributes.TaskStartToCloseTimeoutSeconds,
		BackoffStartIntervalInSeconds:       common.Int32Ptr(int32(backoffInterval.Seconds())),
		Initiator:                           continueAsNewInitiator.Ptr(),
		FailureReason:                       common.StringPtr(timeoutReason),
		CronSchedule:                        common.StringPtr(msBuilder.GetExecutionInfo().CronSchedule),
	}
	domainEntry, err := getActiveDomainEntryFromShard(t.shard, &domainID)
	if err != nil {
		return err
	}
	var eventStoreVersion int32
	if t.config.EnableEventsV2(domainEntry.GetInfo().Name) {
		eventStoreVersion = persistence.EventStoreVersionV2
	}
	_, newMutableState, err := msBuilder.AddContinueAsNewEvent(msBuilder.GetNextEventID(), common.EmptyEventID, domainEntry, startAttributes.GetParentWorkflowDomain(), continueAsnewAttributes, eventStoreVersion)
	if err != nil {
		return err
	}

	executionInfo := context.getExecution()
	tBuilder := t.historyService.getTimerBuilder(executionInfo)
	transferTask, timerTask, err := getWorkflowHistoryCleanupTasksFromShard(
		t.shard,
		domainID,
		executionInfo.GetWorkflowId(),
		tBuilder)
	if err != nil {
		return err
	}

	msBuilder.AddTransferTasks(transferTask)
	msBuilder.AddTimerTasks(timerTask)

	newExecutionInfo := newMutableState.GetExecutionInfo()
	return context.updateWorkflowExecutionWithNewAsActive(
		t.shard.GetTimeSource().Now(),
		newWorkflowExecutionContext(
			newExecutionInfo.DomainID,
			workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(newExecutionInfo.WorkflowID),
				RunId:      common.StringPtr(newExecutionInfo.RunID),
			},
			t.shard,
			t.shard.GetExecutionManager(),
			t.logger,
		),
		newMutableState,
	)
}

func (t *timerQueueActiveProcessorImpl) updateWorkflowExecution(
	context workflowExecutionContext,
	msBuilder mutableState,
	scheduleNewDecision bool,
	createDeletionTask bool,
	timerTasks []persistence.Task,
) error {
	executionInfo := msBuilder.GetExecutionInfo()
	var err error
	if scheduleNewDecision {
		// Schedule a new decision.
		err = scheduleDecision(msBuilder, t.shard.GetTimeSource(), t.logger)
		if err != nil {
			return err
		}
	}

	if createDeletionTask {
		tBuilder := t.historyService.getTimerBuilder(context.getExecution())
		transferTask, timerTask, err := t.historyService.getWorkflowHistoryCleanupTasks(
			executionInfo.DomainID,
			executionInfo.WorkflowID,
			tBuilder)
		if err != nil {
			return err
		}
		msBuilder.AddTransferTasks(transferTask)
		msBuilder.AddTimerTasks(timerTask)
	}
	msBuilder.AddTimerTasks(timerTasks...)

	now := t.shard.GetTimeSource().Now()
	err = context.updateWorkflowExecutionAsActive(now)
	if err != nil {
		if isShardOwnershiptLostError(err) {
			// Shard is stolen.  Stop timer processing to reduce duplicates
			t.timerQueueProcessorBase.Stop()
			return err
		}
		return err
	}

	return nil
}
