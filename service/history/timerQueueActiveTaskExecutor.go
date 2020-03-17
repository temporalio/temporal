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

	"github.com/gogo/protobuf/types"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/matchingservice"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
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
	timerTask, ok := taskInfo.(*persistenceblobs.TimerTaskInfo)
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
	task *persistenceblobs.TimerTaskInfo,
) (retError error) {

	weContext, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(weContext, task, t.metricsClient, t.logger)
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
			return serviceerror.NewInternal(errString)
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

	return t.updateWorkflowExecution(weContext, mutableState, timerFired)
}

func (t *timerQueueActiveTaskExecutor) executeActivityTimeoutTask(
	task *persistenceblobs.TimerTaskInfo,
) (retError error) {

	weContext, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(weContext, task, t.metricsClient, t.logger)
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
	isHeartBeatTask := task.TimeoutType == int32(enums.TimeoutTypeHeartbeat)
	activityInfo, ok := mutableState.GetActivityInfo(task.EventID)
	goVisibilityTS, _ := types.TimestampFromProto(task.VisibilityTimestamp)
	if isHeartBeatTask && ok && activityInfo.LastHeartbeatTimeoutVisibility <= goVisibilityTS.Unix() {
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
			timerTypeToProto(timerSequenceID.timerType),
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
	return t.updateWorkflowExecution(weContext, mutableState, scheduleDecision)
}

func (t *timerQueueActiveTaskExecutor) executeDecisionTimeoutTask(
	task *persistenceblobs.TimerTaskInfo,
) (retError error) {

	weContext, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(weContext, task, t.metricsClient, t.logger)
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
	switch timerTypeFromProto(enums.TimeoutType(task.TimeoutType)) {
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

	return t.updateWorkflowExecution(weContext, mutableState, scheduleDecision)
}

func (t *timerQueueActiveTaskExecutor) executeWorkflowBackoffTimerTask(
	task *persistenceblobs.TimerTaskInfo,
) (retError error) {

	weContext, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(weContext, task, t.metricsClient, t.logger)
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
	return t.updateWorkflowExecution(weContext, mutableState, true)
}

func (t *timerQueueActiveTaskExecutor) executeActivityRetryTimerTask(
	task *persistenceblobs.TimerTaskInfo,
) (retError error) {

	weContext, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(weContext, task, t.metricsClient, t.logger)
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

	domainID := primitives.UUIDString(task.DomainID)
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
		if scheduledEvent.GetActivityTaskScheduledEventAttributes().GetDomain() != "" {
			domainEntry, err := t.shard.GetDomainCache().GetDomain(scheduledEvent.GetActivityTaskScheduledEventAttributes().GetDomain())
			if err != nil {
				return serviceerror.NewInternal("unable to re-schedule activity across domain.")
			}
			targetDomainID = domainEntry.GetInfo().ID
		}
	}

	execution := &commonproto.WorkflowExecution{
		WorkflowId: task.WorkflowID,
		RunId:      primitives.UUIDString(task.RunID)}
	taskList := &commonproto.TaskList{
		Name: activityInfo.TaskList,
	}
	scheduleToStartTimeout := activityInfo.ScheduleToStartTimeout

	release(nil) // release earlier as we don't need the lock anymore

	_, retError = t.shard.GetService().GetMatchingClient().AddActivityTask(context.Background(), &matchingservice.AddActivityTaskRequest{
		DomainUUID:                    targetDomainID,
		SourceDomainUUID:              domainID,
		Execution:                     execution,
		TaskList:                      taskList,
		ScheduleId:                    scheduledID,
		ScheduleToStartTimeoutSeconds: scheduleToStartTimeout,
	})

	return retError
}

func (t *timerQueueActiveTaskExecutor) executeWorkflowTimeoutTask(
	task *persistenceblobs.TimerTaskInfo,
) (retError error) {

	weContext, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(weContext, task, t.metricsClient, t.logger)
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
	continueAsNewInitiator := enums.ContinueAsNewInitiatorRetryPolicy
	if backoffInterval == backoff.NoBackoff {
		// check if a cron backoff is needed
		backoffInterval, err = mutableState.GetCronBackoffDuration()
		if err != nil {
			return err
		}
		continueAsNewInitiator = enums.ContinueAsNewInitiatorCronSchedule
	}
	if backoffInterval == backoff.NoBackoff {
		if err := timeoutWorkflow(mutableState, eventBatchFirstEventID); err != nil {
			return err
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		return t.updateWorkflowExecution(weContext, mutableState, false)
	}

	// workflow timeout, but a retry or cron is needed, so we do continue as new to retry or cron
	startEvent, err := mutableState.GetStartEvent()
	if err != nil {
		return err
	}

	startAttributes := startEvent.GetWorkflowExecutionStartedEventAttributes()
	continueAsnewAttributes := &commonproto.ContinueAsNewWorkflowExecutionDecisionAttributes{
		WorkflowType:                        startAttributes.WorkflowType,
		TaskList:                            startAttributes.TaskList,
		Input:                               startAttributes.Input,
		ExecutionStartToCloseTimeoutSeconds: startAttributes.ExecutionStartToCloseTimeoutSeconds,
		TaskStartToCloseTimeoutSeconds:      startAttributes.TaskStartToCloseTimeoutSeconds,
		BackoffStartIntervalInSeconds:       int32(backoffInterval.Seconds()),
		RetryPolicy:                         startAttributes.RetryPolicy,
		Initiator:                           continueAsNewInitiator,
		FailureReason:                       timeoutReason,
		CronSchedule:                        mutableState.GetExecutionInfo().CronSchedule,
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
	return weContext.updateWorkflowExecutionWithNewAsActive(
		t.shard.GetTimeSource().Now(),
		newWorkflowExecutionContext(
			newExecutionInfo.DomainID,
			commonproto.WorkflowExecution{
				WorkflowId: newExecutionInfo.WorkflowID,
				RunId:      newExecutionInfo.RunID,
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
