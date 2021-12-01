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

	"github.com/pborman/uuid"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

type (
	timerQueueActiveTaskExecutor struct {
		*timerQueueTaskExecutorBase

		queueProcessor *timerQueueActiveProcessorImpl
		matchingClient matchingservice.MatchingServiceClient
	}
)

func newTimerQueueActiveTaskExecutor(
	shard shard.Context,
	historyService *historyEngineImpl,
	queueProcessor *timerQueueActiveProcessorImpl,
	logger log.Logger,
	metricsClient metrics.Client,
	config *configs.Config,
	matchingClient matchingservice.MatchingServiceClient,
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
		matchingClient: matchingClient,
	}
}

func (t *timerQueueActiveTaskExecutor) execute(
	ctx context.Context,
	taskInfo tasks.Task,
	shouldProcessTask bool,
) error {

	if !shouldProcessTask {
		return nil
	}

	switch task := taskInfo.(type) {
	case *tasks.UserTimerTask:
		return t.executeUserTimerTimeoutTask(ctx, task)
	case *tasks.ActivityTimeoutTask:
		return t.executeActivityTimeoutTask(ctx, task)
	case *tasks.WorkflowTaskTimeoutTask:
		return t.executeWorkflowTaskTimeoutTask(ctx, task)
	case *tasks.WorkflowTimeoutTask:
		return t.executeWorkflowTimeoutTask(ctx, task)
	case *tasks.ActivityRetryTimerTask:
		return t.executeActivityRetryTimerTask(ctx, task)
	case *tasks.WorkflowBackoffTimerTask:
		return t.executeWorkflowBackoffTimerTask(ctx, task)
	case *tasks.DeleteHistoryEventTask:
		return t.executeDeleteHistoryEventTask(ctx, task)
	default:
		return errUnknownTimerTask
	}
}

func (t *timerQueueActiveTaskExecutor) executeUserTimerTimeoutTask(
	ctx context.Context,
	task *tasks.UserTimerTask,
) (retError error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, taskTimeout)

	defer cancel()
	namespaceID, execution := t.getNamespaceIDAndWorkflowExecution(task)
	weContext, release, err := t.cache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		workflow.CallerTypeTask,
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
	for _, timerSequenceID := range timerSequence.LoadAndSortUserTimers() {
		timerInfo, ok := mutableState.GetUserTimerInfoByEventID(timerSequenceID.EventID)
		if !ok {
			errString := fmt.Sprintf("failed to find in user timer event ID: %v", timerSequenceID.EventID)
			t.logger.Error(errString)
			return serviceerror.NewInternal(errString)
		}

		if expired := timerSequence.IsExpired(referenceTime, timerSequenceID); !expired {
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
	ctx context.Context,
	task *tasks.ActivityTimeoutTask,
) (retError error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, taskTimeout)

	defer cancel()
	namespaceID, execution := t.getNamespaceIDAndWorkflowExecution(task)
	weContext, release, err := t.cache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		workflow.CallerTypeTask,
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
	activityInfo, heartbeatTimeoutVis, ok := mutableState.GetActivityInfoWithTimerHeartbeat(task.EventID)
	if isHeartBeatTask && ok && (heartbeatTimeoutVis.Before(task.GetVisibilityTime()) || heartbeatTimeoutVis.Equal(task.GetVisibilityTime())) {
		activityInfo.TimerTaskStatus = activityInfo.TimerTaskStatus &^ workflow.TimerTaskStatusCreatedHeartbeat
		if err := mutableState.UpdateActivity(activityInfo); err != nil {
			return err
		}
		updateMutableState = true
	}

Loop:
	for _, timerSequenceID := range timerSequence.LoadAndSortActivityTimers() {
		activityInfo, ok := mutableState.GetActivityInfo(timerSequenceID.EventID)
		if !ok || timerSequenceID.Attempt < activityInfo.Attempt {
			// handle 2 cases:
			// 1. !ok
			//  this case can happen since each activity can have 4 timers
			//  and one of those 4 timers may have fired in this loop
			// 2. timerSequenceID.attempt < activityInfo.Attempt
			//  retry could update activity attempt, should not timeouts new attempt
			continue Loop
		}

		if expired := timerSequence.IsExpired(referenceTime, timerSequenceID); !expired {
			// timer sequence IDs are sorted, once there is one timer
			// sequence ID not expired, all after that wil not expired
			break Loop
		}

		timeoutFailure := failure.NewTimeoutFailure("activity timeout", timerSequenceID.TimerType)
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
			namespace.ID(mutableState.GetExecutionInfo().NamespaceId),
			metrics.TimerActiveTaskActivityTimeoutScope,
			timerSequenceID.TimerType,
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
	ctx context.Context,
	task *tasks.WorkflowTaskTimeoutTask,
) (retError error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, taskTimeout)

	defer cancel()
	namespaceID, execution := t.getNamespaceIDAndWorkflowExecution(task)
	weContext, release, err := t.cache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		workflow.CallerTypeTask,
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

	workflowTask, ok := mutableState.GetWorkflowTaskInfo(task.EventID)
	if !ok {
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, namespace.ID(task.NamespaceID), workflowTask.Version, task.Version, task)
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
			namespace.ID(mutableState.GetExecutionInfo().NamespaceId),
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
			namespace.ID(mutableState.GetExecutionInfo().NamespaceId),
			metrics.TimerActiveTaskWorkflowTaskTimeoutScope,
			enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		)
		_, err := mutableState.AddWorkflowTaskScheduleToStartTimeoutEvent(task.EventID)
		if err != nil {
			return err
		}
		scheduleWorkflowTask = true
	}

	return t.updateWorkflowExecution(weContext, mutableState, scheduleWorkflowTask)
}

func (t *timerQueueActiveTaskExecutor) executeWorkflowBackoffTimerTask(
	ctx context.Context,
	task *tasks.WorkflowBackoffTimerTask,
) (retError error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, taskTimeout)

	defer cancel()
	namespaceID, execution := t.getNamespaceIDAndWorkflowExecution(task)
	weContext, release, err := t.cache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		workflow.CallerTypeTask,
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
	ctx context.Context,
	task *tasks.ActivityRetryTimerTask,
) (retError error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, taskTimeout)

	defer cancel()
	namespaceID, execution := t.getNamespaceIDAndWorkflowExecution(task)
	weContext, release, err := t.cache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		workflow.CallerTypeTask,
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
	activityInfo, ok := mutableState.GetActivityInfo(task.EventID)
	if !ok || task.Attempt < activityInfo.Attempt || activityInfo.StartedId != common.EmptyEventID {
		if ok {
			t.logger.Info("Duplicate activity retry timer task",
				tag.WorkflowID(mutableState.GetExecutionInfo().WorkflowId),
				tag.WorkflowRunID(mutableState.GetExecutionState().GetRunId()),
				tag.WorkflowNamespaceID(mutableState.GetExecutionInfo().NamespaceId),
				tag.WorkflowScheduleID(activityInfo.ScheduleId),
				tag.Attempt(activityInfo.Attempt),
				tag.FailoverVersion(activityInfo.Version),
				tag.TimerTaskStatus(activityInfo.TimerTaskStatus),
				tag.ScheduleAttempt(task.Attempt))
		}
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, namespace.ID(task.NamespaceID), activityInfo.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	targetNamespaceID := activityInfo.NamespaceId
	taskQueue := &taskqueuepb.TaskQueue{
		Name: activityInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	scheduleToStartTimeout := timestamp.DurationValue(activityInfo.ScheduleToStartTimeout)

	// NOTE: do not access anything related mutable state after this lock release
	release(nil) // release earlier as we don't need the lock anymore

	ctx, cancel = context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()
	_, retError = t.matchingClient.AddActivityTask(ctx, &matchingservice.AddActivityTaskRequest{
		NamespaceId:            targetNamespaceID,
		SourceNamespaceId:      namespaceID.String(),
		Execution:              &execution,
		TaskQueue:              taskQueue,
		ScheduleId:             task.EventID,
		ScheduleToStartTimeout: timestamp.DurationPtr(scheduleToStartTimeout),
	})

	return retError
}

func (t *timerQueueActiveTaskExecutor) executeWorkflowTimeoutTask(
	ctx context.Context,
	task *tasks.WorkflowTimeoutTask,
) (retError error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, taskTimeout)

	defer cancel()

	namespaceID, execution := t.getNamespaceIDAndWorkflowExecution(task)
	weContext, release, err := t.cache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		workflow.CallerTypeTask,
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
	ok, err := verifyTaskVersion(t.shard, t.logger, namespace.ID(task.NamespaceID), startVersion, task.Version, task)
	if err != nil || !ok {
		return err
	}

	eventBatchFirstEventID := mutableState.GetNextEventID()
	timeoutFailure := failure.NewTimeoutFailure("workflow timeout", enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	backoffInterval := backoff.NoBackoff
	retryState := enumspb.RETRY_STATE_TIMEOUT
	initiator := enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED

	wfExpTime := timestamp.TimeValue(mutableState.GetExecutionInfo().WorkflowExecutionExpirationTime)
	if wfExpTime.IsZero() || wfExpTime.After(t.shard.GetTimeSource().Now()) {
		backoffInterval, retryState = mutableState.GetRetryBackoffDuration(timeoutFailure)
		if backoffInterval != backoff.NoBackoff {
			// We have a retry policy and we should retry.
			initiator = enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY
		} else if backoffInterval = mutableState.GetCronBackoffDuration(); backoffInterval != backoff.NoBackoff {
			// We have a cron schedule.
			initiator = enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE
		}
	}

	var newRunID string
	if initiator != enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED {
		newRunID = uuid.New()
	}

	// First add timeout workflow event, no matter what we're doing next.
	if err := workflow.TimeoutWorkflow(
		mutableState,
		eventBatchFirstEventID,
		retryState,
		newRunID,
	); err != nil {
		return err
	}

	// No more retries, or workflow is expired.
	if initiator == enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED {
		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		return t.updateWorkflowExecution(weContext, mutableState, false)
	}

	startEvent, err := mutableState.GetStartEvent()
	if err != nil {
		return err
	}
	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()

	newMutableState, err := createMutableState(
		t.shard,
		mutableState.GetNamespaceEntry(),
		newRunID,
	)
	if err != nil {
		return err
	}
	err = workflow.SetupNewWorkflowForRetryOrCron(
		mutableState,
		newMutableState,
		newRunID,
		startAttr,
		startAttr.LastCompletionResult,
		timeoutFailure,
		backoffInterval,
		initiator,
	)
	if err != nil {
		return err
	}

	newExecutionInfo := newMutableState.GetExecutionInfo()
	newExecutionState := newMutableState.GetExecutionState()
	return weContext.UpdateWorkflowExecutionWithNewAsActive(
		t.shard.GetTimeSource().Now(),
		workflow.NewContext(
			namespace.ID(newExecutionInfo.NamespaceId),
			commonpb.WorkflowExecution{
				WorkflowId: newExecutionInfo.WorkflowId,
				RunId:      newExecutionState.RunId,
			},
			t.shard,
			t.logger,
		),
		newMutableState,
	)
}

func (t *timerQueueActiveTaskExecutor) getTimerSequence(
	mutableState workflow.MutableState,
) workflow.TimerSequence {

	timeSource := t.shard.GetTimeSource()
	return workflow.NewTimerSequence(timeSource, mutableState)
}

func (t *timerQueueActiveTaskExecutor) updateWorkflowExecution(
	context workflow.Context,
	mutableState workflow.MutableState,
	scheduleNewWorkflowTask bool,
) error {

	var err error
	if scheduleNewWorkflowTask {
		// Schedule a new workflow task.
		err = workflow.ScheduleWorkflowTask(mutableState)
		if err != nil {
			return err
		}
	}

	now := t.shard.GetTimeSource().Now()
	err = context.UpdateWorkflowExecutionAsActive(now)
	if err != nil {
		if shard.IsShardOwnershipLostError(err) {
			// Shard is stolen.  Stop timer processing to reduce duplicates
			t.queueProcessor.Stop()
			return err
		}
		return err
	}

	return nil
}

func (t *timerQueueActiveTaskExecutor) emitTimeoutMetricScopeWithNamespaceTag(
	namespaceID namespace.ID,
	scope int,
	timerType enumspb.TimeoutType,
) {
	namespaceEntry, err := t.shard.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return
	}
	metricsScope := t.metricsClient.Scope(scope).Tagged(metrics.NamespaceTag(namespaceEntry.Name().String()))
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
