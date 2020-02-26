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

	m "github.com/uber/cadence/.gen/go/matching"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	timerQueueActiveTaskExecutor struct {
		*timerQueueTaskExecutorBase

		queueProcessor *timerQueueActiveProcessorImpl
	}
)

func newTimerQueueActiveTaskExecutor(
	shard ShardContext,
	historyService *historyEngineImpl,
	queueProcessor *timerQueueActiveProcessorImpl,
	logger log.Logger,
	metricsClient metrics.Client,
	config *Config,
) queueTaskExecutor {
	return &timerQueueActiveTaskExecutor{
		timerQueueTaskExecutorBase: newTimerQueueTaskExecutorBase(
			shard,
			historyService,
			logger,
			metricsClient,
			config,
		),
		queueProcessor: queueProcessor,
	}
}

func (t *timerQueueActiveTaskExecutor) execute(
	taskInfo queueTaskInfo,
	shouldProcessTask bool,
) error {
	timerTask, ok := taskInfo.(*persistence.TimerTaskInfo)
	if !ok {
		return errUnexpectedQueueTask
	}

	if !shouldProcessTask {
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
		return t.executeActivityRetryTimerTask(timerTask)
	case persistence.TaskTypeWorkflowBackoffTimer:
		return t.executeWorkflowBackoffTimerTask(timerTask)
	case persistence.TaskTypeDeleteHistoryEvent:
		return t.executeDeleteHistoryEventTask(timerTask)
	default:
		return errUnknownTimerTask
	}
}

func (t *timerQueueActiveTaskExecutor) executeUserTimerTimeoutTask(
	task *persistence.TimerTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	timerSequence := t.getTimerSequence(mutableState)
	referenceTime := t.shard.GetTimeSource().Now()
	timerFired := false

Loop:
	for _, timerSequenceID := range timerSequence.loadAndSortUserTimers() {
		timerInfo, ok := mutableState.GetUserTimerInfoByEventID(timerSequenceID.eventID)
		if !ok {
			errString := fmt.Sprintf("failed to find in user timer event ID: %v", timerSequenceID.eventID)
			t.logger.Error(errString)
			return &workflow.InternalServiceError{Message: errString}
		}

		if expired := timerSequence.isExpired(referenceTime, timerSequenceID); !expired {
			// timer sequence IDs are sorted, once there is one timer
			// sequence ID not expired, all after that wil not expired
			break Loop
		}

		if _, err := mutableState.AddTimerFiredEvent(timerInfo.TimerID); err != nil {
			return err
		}
		timerFired = true
	}

	if !timerFired {
		return nil
	}

	return t.updateWorkflowExecution(context, mutableState, timerFired)
}

func (t *timerQueueActiveTaskExecutor) executeActivityTimeoutTask(
	task *persistence.TimerTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	timerSequence := t.getTimerSequence(mutableState)
	referenceTime := t.shard.GetTimeSource().Now()
	updateMutableState := false
	scheduleDecision := false

	// need to clear activity heartbeat timer task mask for new activity timer task creation
	// NOTE: LastHeartbeatTimeoutVisibility is for deduping heartbeat timer creation as it's possible
	// one heartbeat task was persisted multiple times with different taskIDs due to the retry logic
	// for updating workflow execution. In that case, only one new heartbeat timeout task should be
	// created.
	isHeartBeatTask := task.TimeoutType == int(workflow.TimeoutTypeHeartbeat)
	activityInfo, ok := mutableState.GetActivityInfo(task.EventID)
	if isHeartBeatTask && ok && activityInfo.LastHeartbeatTimeoutVisibility <= task.VisibilityTimestamp.Unix() {
		activityInfo.TimerTaskStatus = activityInfo.TimerTaskStatus &^ timerTaskStatusCreatedHeartbeat
		if err := mutableState.UpdateActivity(activityInfo); err != nil {
			return err
		}
		updateMutableState = true
	}

Loop:
	for _, timerSequenceID := range timerSequence.loadAndSortActivityTimers() {
		activityInfo, ok := mutableState.GetActivityInfo(timerSequenceID.eventID)
		if !ok || timerSequenceID.attempt < activityInfo.Attempt {
			// handle 2 cases:
			// 1. !ok
			//  this case can happen since each activity can have 4 timers
			//  and one of those 4 timers may have fired in this loop
			// 2. timerSequenceID.attempt < activityInfo.Attempt
			//  retry could update activity attempt, should not timeouts new attempt
			continue Loop
		}

		if expired := timerSequence.isExpired(referenceTime, timerSequenceID); !expired {
			// timer sequence IDs are sorted, once there is one timer
			// sequence ID not expired, all after that wil not expired
			break Loop
		}

		if timerSequenceID.timerType != timerTypeScheduleToStart {
			// schedule to start timeout is not retriable
			// customer should set larger schedule to start timeout if necessary
			if ok, err := mutableState.RetryActivity(
				activityInfo,
				timerTypeToReason(timerSequenceID.timerType),
				nil,
			); err != nil {
				return err
			} else if ok {
				updateMutableState = true
				continue Loop
			}
		}

		t.emitTimeoutMetricScopeWithDomainTag(
			mutableState.GetExecutionInfo().DomainID,
			metrics.TimerActiveTaskActivityTimeoutScope,
			timerSequenceID.timerType,
		)
		if _, err := mutableState.AddActivityTaskTimedOutEvent(
			activityInfo.ScheduleID,
			activityInfo.StartedID,
			timerTypeToThrift(timerSequenceID.timerType),
			activityInfo.Details,
		); err != nil {
			return err
		}
		updateMutableState = true
		scheduleDecision = true
	}

	if !updateMutableState {
		return nil
	}
	return t.updateWorkflowExecution(context, mutableState, scheduleDecision)
}

func (t *timerQueueActiveTaskExecutor) executeDecisionTimeoutTask(
	task *persistence.TimerTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	scheduleID := task.EventID
	decision, ok := mutableState.GetDecisionInfo(scheduleID)
	if !ok {
		t.logger.Debug("Potentially duplicate task.", tag.TaskID(task.TaskID), tag.WorkflowScheduleID(scheduleID), tag.TaskType(persistence.TaskTypeDecisionTimeout))
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, task.DomainID, decision.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	if decision.Attempt != task.ScheduleAttempt {
		return nil
	}

	scheduleDecision := false
	switch timerTypeFromThrift(workflow.TimeoutType(task.TimeoutType)) {
	case timerTypeStartToClose:
		t.emitTimeoutMetricScopeWithDomainTag(
			mutableState.GetExecutionInfo().DomainID,
			metrics.TimerActiveTaskDecisionTimeoutScope,
			timerTypeStartToClose,
		)
		if _, err := mutableState.AddDecisionTaskTimedOutEvent(
			decision.ScheduleID,
			decision.StartedID,
		); err != nil {
			return err
		}
		scheduleDecision = true

	case timerTypeScheduleToStart:
		if decision.StartedID != common.EmptyEventID {
			// decision has already started
			return nil
		}

		t.emitTimeoutMetricScopeWithDomainTag(
			mutableState.GetExecutionInfo().DomainID,
			metrics.TimerActiveTaskDecisionTimeoutScope,
			timerTypeScheduleToStart,
		)
		_, err := mutableState.AddDecisionTaskScheduleToStartTimeoutEvent(scheduleID)
		if err != nil {
			return err
		}
		scheduleDecision = true
	}

	return t.updateWorkflowExecution(context, mutableState, scheduleDecision)
}

func (t *timerQueueActiveTaskExecutor) executeWorkflowBackoffTimerTask(
	task *persistence.TimerTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	if task.TimeoutType == persistence.WorkflowBackoffTimeoutTypeRetry {
		t.metricsClient.IncCounter(metrics.TimerActiveTaskWorkflowBackoffTimerScope, metrics.WorkflowRetryBackoffTimerCount)
	} else {
		t.metricsClient.IncCounter(metrics.TimerActiveTaskWorkflowBackoffTimerScope, metrics.WorkflowCronBackoffTimerCount)
	}

	if mutableState.HasProcessedOrPendingDecision() {
		// already has decision task
		return nil
	}

	// schedule first decision task
	return t.updateWorkflowExecution(context, mutableState, true)
}

func (t *timerQueueActiveTaskExecutor) executeActivityRetryTimerTask(
	task *persistence.TimerTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	// generate activity task
	scheduledID := task.EventID
	activityInfo, ok := mutableState.GetActivityInfo(scheduledID)
	if !ok || task.ScheduleAttempt < int64(activityInfo.Attempt) || activityInfo.StartedID != common.EmptyEventID {
		if ok {
			t.logger.Info("Duplicate activity retry timer task",
				tag.WorkflowID(mutableState.GetExecutionInfo().WorkflowID),
				tag.WorkflowRunID(mutableState.GetExecutionInfo().RunID),
				tag.WorkflowDomainID(mutableState.GetExecutionInfo().DomainID),
				tag.WorkflowScheduleID(activityInfo.ScheduleID),
				tag.Attempt(activityInfo.Attempt),
				tag.FailoverVersion(activityInfo.Version),
				tag.TimerTaskStatus(activityInfo.TimerTaskStatus),
				tag.ScheduleAttempt(task.ScheduleAttempt))
		}
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, task.DomainID, activityInfo.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	domainID := task.DomainID
	targetDomainID := domainID
	if activityInfo.DomainID != "" {
		targetDomainID = activityInfo.DomainID
	} else {
		// TODO remove this block after Mar, 1th, 2020
		//  previously, DomainID in activity info is not used, so need to get
		//  schedule event from DB checking whether activity to be scheduled
		//  belongs to this domain
		scheduledEvent, err := mutableState.GetActivityScheduledEvent(scheduledID)
		if err != nil {
			return err
		}
		if scheduledEvent.ActivityTaskScheduledEventAttributes.Domain != nil {
			domainEntry, err := t.shard.GetDomainCache().GetDomain(scheduledEvent.ActivityTaskScheduledEventAttributes.GetDomain())
			if err != nil {
				return &workflow.InternalServiceError{Message: "unable to re-schedule activity across domain."}
			}
			targetDomainID = domainEntry.GetInfo().ID
		}
	}

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID)}
	taskList := &workflow.TaskList{
		Name: common.StringPtr(activityInfo.TaskList),
	}
	scheduleToStartTimeout := activityInfo.ScheduleToStartTimeout

	release(nil) // release earlier as we don't need the lock anymore

	return t.shard.GetService().GetMatchingClient().AddActivityTask(nil, &m.AddActivityTaskRequest{
		DomainUUID:                    common.StringPtr(targetDomainID),
		SourceDomainUUID:              common.StringPtr(domainID),
		Execution:                     &execution,
		TaskList:                      taskList,
		ScheduleId:                    common.Int64Ptr(scheduledID),
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(scheduleToStartTimeout),
	})
}

func (t *timerQueueActiveTaskExecutor) executeWorkflowTimeoutTask(
	task *persistence.TimerTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	startVersion, err := mutableState.GetStartVersion()
	if err != nil {
		return err
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, startVersion, task.Version, task)
	if err != nil || !ok {
		return err
	}

	eventBatchFirstEventID := mutableState.GetNextEventID()

	timeoutReason := timerTypeToReason(timerTypeStartToClose)
	backoffInterval := mutableState.GetRetryBackoffDuration(timeoutReason)
	continueAsNewInitiator := workflow.ContinueAsNewInitiatorRetryPolicy
	if backoffInterval == backoff.NoBackoff {
		// check if a cron backoff is needed
		backoffInterval, err = mutableState.GetCronBackoffDuration()
		if err != nil {
			return err
		}
		continueAsNewInitiator = workflow.ContinueAsNewInitiatorCronSchedule
	}
	if backoffInterval == backoff.NoBackoff {
		if err := timeoutWorkflow(mutableState, eventBatchFirstEventID); err != nil {
			return err
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		return t.updateWorkflowExecution(context, mutableState, false)
	}

	// workflow timeout, but a retry or cron is needed, so we do continue as new to retry or cron
	startEvent, err := mutableState.GetStartEvent()
	if err != nil {
		return err
	}

	startAttributes := startEvent.WorkflowExecutionStartedEventAttributes
	continueAsnewAttributes := &workflow.ContinueAsNewWorkflowExecutionDecisionAttributes{
		WorkflowType:                        startAttributes.WorkflowType,
		TaskList:                            startAttributes.TaskList,
		Input:                               startAttributes.Input,
		ExecutionStartToCloseTimeoutSeconds: startAttributes.ExecutionStartToCloseTimeoutSeconds,
		TaskStartToCloseTimeoutSeconds:      startAttributes.TaskStartToCloseTimeoutSeconds,
		BackoffStartIntervalInSeconds:       common.Int32Ptr(int32(backoffInterval.Seconds())),
		RetryPolicy:                         startAttributes.RetryPolicy,
		Initiator:                           continueAsNewInitiator.Ptr(),
		FailureReason:                       common.StringPtr(timeoutReason),
		CronSchedule:                        common.StringPtr(mutableState.GetExecutionInfo().CronSchedule),
		Header:                              startAttributes.Header,
		Memo:                                startAttributes.Memo,
		SearchAttributes:                    startAttributes.SearchAttributes,
	}
	newMutableState, err := retryWorkflow(
		mutableState,
		eventBatchFirstEventID,
		startAttributes.GetParentWorkflowDomain(),
		continueAsnewAttributes,
	)
	if err != nil {
		return err
	}

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

func (t *timerQueueActiveTaskExecutor) getTimerSequence(
	mutableState mutableState,
) timerSequence {

	timeSource := t.shard.GetTimeSource()
	return newTimerSequence(timeSource, mutableState)
}

func (t *timerQueueActiveTaskExecutor) updateWorkflowExecution(
	context workflowExecutionContext,
	mutableState mutableState,
	scheduleNewDecision bool,
) error {

	var err error
	if scheduleNewDecision {
		// Schedule a new decision.
		err = scheduleDecision(mutableState)
		if err != nil {
			return err
		}
	}

	now := t.shard.GetTimeSource().Now()
	err = context.updateWorkflowExecutionAsActive(now)
	if err != nil {
		if isShardOwnershiptLostError(err) {
			// Shard is stolen.  Stop timer processing to reduce duplicates
			t.queueProcessor.Stop()
			return err
		}
		return err
	}

	return nil
}

func (t *timerQueueActiveTaskExecutor) emitTimeoutMetricScopeWithDomainTag(
	domainID string,
	scope int,
	timerType timerType,
) {

	domainEntry, err := t.shard.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		return
	}
	metricsScope := t.metricsClient.Scope(scope).Tagged(metrics.DomainTag(domainEntry.GetInfo().Name))
	switch timerType {
	case timerTypeScheduleToStart:
		metricsScope.IncCounter(metrics.ScheduleToStartTimeoutCounter)
	case timerTypeScheduleToClose:
		metricsScope.IncCounter(metrics.ScheduleToCloseTimeoutCounter)
	case timerTypeStartToClose:
		metricsScope.IncCounter(metrics.StartToCloseTimeoutCounter)
	case timerTypeHeartbeat:
		metricsScope.IncCounter(metrics.HeartbeatTimeoutCounter)
	}
}
