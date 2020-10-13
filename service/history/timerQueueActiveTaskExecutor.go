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

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
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
	case enumsspb.TASK_TYPE_USER_TIMER:
		return t.executeUserTimerTimeoutTask(timerTask)
	case enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT:
		return t.executeActivityTimeoutTask(timerTask)
	case enumsspb.TASK_TYPE_WORKFLOW_TASK_TIMEOUT:
		return t.executeWorkflowTaskTimeoutTask(timerTask)
	case enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT:
		return t.executeWorkflowTimeoutTask(timerTask)
	case enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER:
		return t.executeActivityRetryTimerTask(timerTask)
	case enumsspb.TASK_TYPE_WORKFLOW_BACKOFF_TIMER:
		return t.executeWorkflowBackoffTimerTask(timerTask)
	case enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT:
		return t.executeDeleteHistoryEventTask(timerTask)
	default:
		return errUnknownTimerTask
	}
}

func (t *timerQueueActiveTaskExecutor) executeUserTimerTimeoutTask(
	task *persistenceblobs.TimerTaskInfo,
) (retError error) {

	weContext, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getNamespaceIDAndWorkflowExecution(task),
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

		if _, err := mutableState.AddTimerFiredEvent(timerInfo.GetTimerId()); err != nil {
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
		t.getNamespaceIDAndWorkflowExecution(task),
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
	scheduleWorkflowTask := false

	// need to clear activity heartbeat timer task mask for new activity timer task creation
	// NOTE: LastHeartbeatTimeoutVisibilityInSeconds is for deduping heartbeat timer creation as it's possible
	// one heartbeat task was persisted multiple times with different taskIDs due to the retry logic
	// for updating workflow execution. In that case, only one new heartbeat timeout task should be
	// created.
	isHeartBeatTask := task.TimeoutType == enumspb.TIMEOUT_TYPE_HEARTBEAT
	activityInfo, heartbeatTimeoutVis, ok := mutableState.GetActivityInfoWithTimerHeartbeat(task.GetEventId())
	goVisibilityTS := timestamp.TimeValue(task.VisibilityTime)
	if isHeartBeatTask && ok && (heartbeatTimeoutVis.Before(goVisibilityTS) || heartbeatTimeoutVis.Equal(goVisibilityTS)) {
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

		timeoutFailure := failure.NewTimeoutFailure("activity timeout", timerSequenceID.timerType)
		var retryState enumspb.RetryState
		if retryState, err = mutableState.RetryActivity(
			activityInfo,
			timeoutFailure,
		); err != nil {
			return err
		} else if retryState == enumspb.RETRY_STATE_IN_PROGRESS {
			updateMutableState = true
			continue Loop
		}

		timeoutFailure.GetTimeoutFailureInfo().LastHeartbeatDetails = activityInfo.LastHeartbeatDetails
		// If retryState is Timeout then it means that expirationTime is expired.
		// ExpirationTime is expired when ScheduleToClose timeout is expired.
		if retryState == enumspb.RETRY_STATE_TIMEOUT {
			timeoutFailure.GetTimeoutFailureInfo().TimeoutType = enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE
		}

		t.emitTimeoutMetricScopeWithNamespaceTag(
			mutableState.GetExecutionInfo().NamespaceId,
			metrics.TimerActiveTaskActivityTimeoutScope,
			timerSequenceID.timerType,
		)
		if _, err := mutableState.AddActivityTaskTimedOutEvent(
			activityInfo.ScheduleId,
			activityInfo.StartedId,
			timeoutFailure,
			retryState,
		); err != nil {
			return err
		}
		updateMutableState = true
		scheduleWorkflowTask = true
	}

	if !updateMutableState {
		return nil
	}
	return t.updateWorkflowExecution(weContext, mutableState, scheduleWorkflowTask)
}

func (t *timerQueueActiveTaskExecutor) executeWorkflowTaskTimeoutTask(
	task *persistenceblobs.TimerTaskInfo,
) (retError error) {

	weContext, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getNamespaceIDAndWorkflowExecution(task),
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

	scheduleID := task.GetEventId()
	workflowTask, ok := mutableState.GetWorkflowTaskInfo(scheduleID)
	if !ok {
		t.logger.Debug("Potentially duplicate task.", tag.TaskID(task.GetTaskId()), tag.WorkflowScheduleID(scheduleID), tag.TaskType(enumsspb.TASK_TYPE_WORKFLOW_TASK_TIMEOUT))
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, task.GetNamespaceId(), workflowTask.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	if workflowTask.Attempt != task.ScheduleAttempt {
		return nil
	}

	scheduleWorkflowTask := false
	switch task.TimeoutType {
	case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		t.emitTimeoutMetricScopeWithNamespaceTag(
			mutableState.GetExecutionInfo().NamespaceId,
			metrics.TimerActiveTaskWorkflowTaskTimeoutScope,
			enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		)
		if _, err := mutableState.AddWorkflowTaskTimedOutEvent(
			workflowTask.ScheduleID,
			workflowTask.StartedID,
		); err != nil {
			return err
		}
		scheduleWorkflowTask = true

	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START:
		if workflowTask.StartedID != common.EmptyEventID {
			// workflowTask has already started
			return nil
		}

		t.emitTimeoutMetricScopeWithNamespaceTag(
			mutableState.GetExecutionInfo().NamespaceId,
			metrics.TimerActiveTaskWorkflowTaskTimeoutScope,
			enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		)
		_, err := mutableState.AddWorkflowTaskScheduleToStartTimeoutEvent(scheduleID)
		if err != nil {
			return err
		}
		scheduleWorkflowTask = true
	}

	return t.updateWorkflowExecution(weContext, mutableState, scheduleWorkflowTask)
}

func (t *timerQueueActiveTaskExecutor) executeWorkflowBackoffTimerTask(
	task *persistenceblobs.TimerTaskInfo,
) (retError error) {

	weContext, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getNamespaceIDAndWorkflowExecution(task),
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

	if task.WorkflowBackoffType == enumsspb.WORKFLOW_BACKOFF_TYPE_RETRY {
		t.metricsClient.IncCounter(metrics.TimerActiveTaskWorkflowBackoffTimerScope, metrics.WorkflowRetryBackoffTimerCount)
	} else if task.WorkflowBackoffType == enumsspb.WORKFLOW_BACKOFF_TYPE_CRON {
		t.metricsClient.IncCounter(metrics.TimerActiveTaskWorkflowBackoffTimerScope, metrics.WorkflowCronBackoffTimerCount)
	}

	if mutableState.HasProcessedOrPendingWorkflowTask() {
		// already has workflow task
		return nil
	}

	// schedule first workflow task
	return t.updateWorkflowExecution(weContext, mutableState, true)
}

func (t *timerQueueActiveTaskExecutor) executeActivityRetryTimerTask(
	task *persistenceblobs.TimerTaskInfo,
) (retError error) {

	weContext, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getNamespaceIDAndWorkflowExecution(task),
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
	scheduledID := task.GetEventId()
	activityInfo, ok := mutableState.GetActivityInfo(scheduledID)
	if !ok || task.ScheduleAttempt < activityInfo.Attempt || activityInfo.StartedId != common.EmptyEventID {
		if ok {
			t.logger.Info("Duplicate activity retry timer task",
				tag.WorkflowID(mutableState.GetExecutionInfo().WorkflowId),
				tag.WorkflowRunID(mutableState.GetExecutionInfo().GetRunId()),
				tag.WorkflowNamespaceID(mutableState.GetExecutionInfo().NamespaceId),
				tag.WorkflowScheduleID(activityInfo.ScheduleId),
				tag.Attempt(activityInfo.Attempt),
				tag.FailoverVersion(activityInfo.Version),
				tag.TimerTaskStatus(activityInfo.TimerTaskStatus),
				tag.ScheduleAttempt(task.ScheduleAttempt))
		}
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, task.GetNamespaceId(), activityInfo.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	namespaceID := task.GetNamespaceId()
	targetNamespaceID := namespaceID
	if activityInfo.NamespaceId != "" {
		targetNamespaceID = activityInfo.NamespaceId
	} else {
		// TODO remove this block after Mar, 1th, 2020
		//  previously, NamespaceID in activity info is not used, so need to get
		//  schedule event from DB checking whether activity to be scheduled
		//  belongs to this namespace
		scheduledEvent, err := mutableState.GetActivityScheduledEvent(scheduledID)
		if err != nil {
			return err
		}
		if scheduledEvent.GetActivityTaskScheduledEventAttributes().GetNamespace() != "" {
			namespaceEntry, err := t.shard.GetNamespaceCache().GetNamespace(scheduledEvent.GetActivityTaskScheduledEventAttributes().GetNamespace())
			if err != nil {
				return serviceerror.NewInternal("unable to re-schedule activity across namespace.")
			}
			targetNamespaceID = namespaceEntry.GetInfo().Id
		}
	}

	execution := &commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      task.GetRunId()}
	taskQueue := &taskqueuepb.TaskQueue{
		Name: activityInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	scheduleToStartTimeout := activityInfo.ScheduleToStartTimeout

	release(nil) // release earlier as we don't need the lock anymore

	_, retError = t.shard.GetService().GetMatchingClient().AddActivityTask(context.Background(), &matchingservice.AddActivityTaskRequest{
		NamespaceId:            targetNamespaceID,
		SourceNamespaceId:      namespaceID,
		Execution:              execution,
		TaskQueue:              taskQueue,
		ScheduleId:             scheduledID,
		ScheduleToStartTimeout: scheduleToStartTimeout,
	})

	return retError
}

func (t *timerQueueActiveTaskExecutor) executeWorkflowTimeoutTask(
	task *persistenceblobs.TimerTaskInfo,
) (retError error) {

	weContext, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getNamespaceIDAndWorkflowExecution(task),
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
	ok, err := verifyTaskVersion(t.shard, t.logger, task.GetNamespaceId(), startVersion, task.Version, task)
	if err != nil || !ok {
		return err
	}

	eventBatchFirstEventID := mutableState.GetNextEventID()

	timeoutFailure := failure.NewTimeoutFailure("workflow timeout", enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	backoffInterval := backoff.NoBackoff
	retryState := enumspb.RETRY_STATE_TIMEOUT
	continueAsNewInitiator := enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY

	wfExpTime := timestamp.TimeValue(mutableState.GetExecutionInfo().WorkflowExpirationTime)
	// Retry if WorkflowExpirationTime is not set or workflow is not expired.
	if wfExpTime.IsZero() || wfExpTime.After(t.shard.GetTimeSource().Now()) {
		backoffInterval, retryState = mutableState.GetRetryBackoffDuration(timeoutFailure)

		if backoffInterval == backoff.NoBackoff {
			// Check if a cron backoff is needed.
			backoffInterval, err = mutableState.GetCronBackoffDuration()
			if err != nil {
				return err
			}
			continueAsNewInitiator = enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE
		}
	}

	// No more retries, or workflow is expired.
	if backoffInterval == backoff.NoBackoff {
		if err := timeoutWorkflow(mutableState, eventBatchFirstEventID, retryState); err != nil {
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
	continueAsNewAttributes := &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
		WorkflowType:         startAttributes.WorkflowType,
		TaskQueue:            startAttributes.TaskQueue,
		Input:                startAttributes.Input,
		WorkflowRunTimeout:   startAttributes.WorkflowRunTimeout,
		WorkflowTaskTimeout:  startAttributes.WorkflowTaskTimeout,
		BackoffStartInterval: &backoffInterval,
		RetryPolicy:          startAttributes.RetryPolicy,
		Initiator:            continueAsNewInitiator,
		Failure:              timeoutFailure,
		CronSchedule:         mutableState.GetExecutionInfo().CronSchedule,
		Header:               startAttributes.Header,
		Memo:                 startAttributes.Memo,
		SearchAttributes:     startAttributes.SearchAttributes,
	}
	newMutableState, err := retryWorkflow(
		mutableState,
		eventBatchFirstEventID,
		startAttributes.GetParentWorkflowNamespace(),
		continueAsNewAttributes,
	)
	if err != nil {
		return err
	}

	newExecutionInfo := newMutableState.GetExecutionInfo()
	return weContext.updateWorkflowExecutionWithNewAsActive(
		t.shard.GetTimeSource().Now(),
		newWorkflowExecutionContext(
			newExecutionInfo.NamespaceId,
			commonpb.WorkflowExecution{
				WorkflowId: newExecutionInfo.WorkflowId,
				RunId:      newExecutionInfo.ExecutionState.RunId,
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
	scheduleNewWorkflowTask bool,
) error {

	var err error
	if scheduleNewWorkflowTask {
		// Schedule a new workflow task.
		err = scheduleWorkflowTask(mutableState)
		if err != nil {
			return err
		}
	}

	now := t.shard.GetTimeSource().Now()
	err = context.updateWorkflowExecutionAsActive(now)
	if err != nil {
		if isShardOwnershipLostError(err) {
			// Shard is stolen.  Stop timer processing to reduce duplicates
			t.queueProcessor.Stop()
			return err
		}
		return err
	}

	return nil
}

func (t *timerQueueActiveTaskExecutor) emitTimeoutMetricScopeWithNamespaceTag(
	namespaceID string,
	scope int,
	timerType enumspb.TimeoutType,
) {

	namespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(namespaceID)
	if err != nil {
		return
	}
	metricsScope := t.metricsClient.Scope(scope).Tagged(metrics.NamespaceTag(namespaceEntry.GetInfo().Name))
	switch timerType {
	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START:
		metricsScope.IncCounter(metrics.ScheduleToStartTimeoutCounter)
	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
		metricsScope.IncCounter(metrics.ScheduleToCloseTimeoutCounter)
	case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		metricsScope.IncCounter(metrics.StartToCloseTimeoutCounter)
	case enumspb.TIMEOUT_TYPE_HEARTBEAT:
		metricsScope.IncCounter(metrics.HeartbeatTimeoutCounter)
	}
}
