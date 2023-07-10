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
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/worker_versioning"
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
	timerQueueActiveTaskExecutor struct {
		*timerQueueTaskExecutorBase
	}
)

func newTimerQueueActiveTaskExecutor(
	shard shard.Context,
	workflowCache wcache.Cache,
	workflowDeleteManager deletemanager.DeleteManager,
	logger log.Logger,
	metricProvider metrics.Handler,
	config *configs.Config,
	matchingClient matchingservice.MatchingServiceClient,
) queues.Executor {
	return &timerQueueActiveTaskExecutor{
		timerQueueTaskExecutorBase: newTimerQueueTaskExecutorBase(
			shard,
			workflowCache,
			workflowDeleteManager,
			matchingClient,
			logger,
			metricProvider,
			config,
		),
	}
}

func (t *timerQueueActiveTaskExecutor) Execute(
	ctx context.Context,
	executable queues.Executable,
) ([]metrics.Tag, bool, error) {
	taskTypeTagValue := queues.GetActiveTimerTaskTypeTagValue(executable)
	namespaceTag, replicationState := getNamespaceTagAndReplicationStateByID(
		t.shard.GetNamespaceRegistry(),
		executable.GetNamespaceID(),
	)
	metricsTags := []metrics.Tag{
		namespaceTag,
		metrics.TaskTypeTag(taskTypeTagValue),
		metrics.OperationTag(taskTypeTagValue), // for backward compatibility
	}

	if replicationState == enumspb.REPLICATION_STATE_HANDOVER {
		// TODO: exclude task types here if we believe it's safe & necessary to execute
		//  them during namespace handover.
		// TODO: move this logic to queues.Executable when metrics tag doesn't need to
		//  be returned from task executor
		return metricsTags, true, consts.ErrNamespaceHandover
	}

	var err error
	switch task := executable.GetTask().(type) {
	case *tasks.UserTimerTask:
		err = t.executeUserTimerTimeoutTask(ctx, task)
	case *tasks.ActivityTimeoutTask:
		err = t.executeActivityTimeoutTask(ctx, task)
	case *tasks.WorkflowTaskTimeoutTask:
		err = t.executeWorkflowTaskTimeoutTask(ctx, task)
	case *tasks.WorkflowTimeoutTask:
		err = t.executeWorkflowTimeoutTask(ctx, task)
	case *tasks.ActivityRetryTimerTask:
		err = t.executeActivityRetryTimerTask(ctx, task)
	case *tasks.WorkflowBackoffTimerTask:
		err = t.executeWorkflowBackoffTimerTask(ctx, task)
	case *tasks.DeleteHistoryEventTask:
		err = t.executeDeleteHistoryEventTask(ctx, task)
	default:
		err = errUnknownTimerTask
	}

	return metricsTags, true, err
}

func (t *timerQueueActiveTaskExecutor) executeUserTimerTimeoutTask(
	ctx context.Context,
	task *tasks.UserTimerTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(ctx, weContext, task, t.metricHandler, t.logger)
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

		if !queues.IsTimeExpired(referenceTime, timerSequenceID.Timestamp) {
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

	return t.updateWorkflowExecution(ctx, weContext, mutableState, timerFired)
}

func (t *timerQueueActiveTaskExecutor) executeActivityTimeoutTask(
	ctx context.Context,
	task *tasks.ActivityTimeoutTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(ctx, weContext, task, t.metricHandler, t.logger)
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
	if isHeartBeatTask && ok && queues.IsTimeExpired(task.GetVisibilityTime(), heartbeatTimeoutVis) {
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

		if !queues.IsTimeExpired(referenceTime, timerSequenceID.Timestamp) {
			// timer sequence IDs are sorted, once there is one timer
			// sequence ID not expired, all after that wil not expired
			break Loop
		}

		failureMsg := fmt.Sprintf("activity %v timeout", timerSequenceID.TimerType.String())
		timeoutFailure := failure.NewTimeoutFailure(failureMsg, timerSequenceID.TimerType)
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
			activityInfo.ScheduledEventId,
			activityInfo.StartedEventId,
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
	return t.updateWorkflowExecution(ctx, weContext, mutableState, scheduleWorkflowTask)
}

func (t *timerQueueActiveTaskExecutor) executeWorkflowTaskTimeoutTask(
	ctx context.Context,
	task *tasks.WorkflowTaskTimeoutTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(ctx, weContext, task, t.metricHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	workflowTask := mutableState.GetWorkflowTaskByID(task.EventID)
	if workflowTask == nil {
		return nil
	}

	if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		// Check if mutable state still points to this task.
		// Mutable state can lost speculative WT or even has another one there if, for example, workflow was evicted from cache.
		if !mutableState.CheckSpeculativeWorkflowTaskTimeoutTask(task) {
			return nil
		}
	} else {
		err = CheckTaskVersion(t.shard, t.logger, mutableState.GetNamespaceEntry(), workflowTask.Version, task.Version, task)
		if err != nil {
			return err
		}

		if workflowTask.Attempt != task.ScheduleAttempt {
			return nil
		}
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
			workflowTask,
		); err != nil {
			return err
		}
		scheduleWorkflowTask = true

	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START:
		if workflowTask.StartedEventID != common.EmptyEventID {
			// workflowTask has already started
			return nil
		}

		t.emitTimeoutMetricScopeWithNamespaceTag(
			namespace.ID(mutableState.GetExecutionInfo().NamespaceId),
			metrics.TimerActiveTaskWorkflowTaskTimeoutScope,
			enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		)
		_, err := mutableState.AddWorkflowTaskScheduleToStartTimeoutEvent(workflowTask)
		if err != nil {
			return err
		}
		scheduleWorkflowTask = true
	}

	return t.updateWorkflowExecution(ctx, weContext, mutableState, scheduleWorkflowTask)
}

func (t *timerQueueActiveTaskExecutor) executeWorkflowBackoffTimerTask(
	ctx context.Context,
	task *tasks.WorkflowBackoffTimerTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(ctx, weContext, task, t.metricHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	if task.WorkflowBackoffType == enumsspb.WORKFLOW_BACKOFF_TYPE_RETRY {
		t.metricHandler.Counter(metrics.WorkflowRetryBackoffTimerCount.GetMetricName()).Record(
			1,
			metrics.OperationTag(metrics.TimerActiveTaskWorkflowBackoffTimerScope),
		)
	} else if task.WorkflowBackoffType == enumsspb.WORKFLOW_BACKOFF_TYPE_CRON {
		t.metricHandler.Counter(metrics.WorkflowCronBackoffTimerCount.GetMetricName()).Record(
			1,
			metrics.OperationTag(metrics.TimerActiveTaskWorkflowBackoffTimerScope),
		)
	} else if task.WorkflowBackoffType == enumsspb.WORKFLOW_BACKOFF_TYPE_DELAY_START {
		t.metricHandler.Counter(metrics.WorkflowDelayedStartBackoffTimerCount.GetMetricName()).Record(
			1,
			metrics.OperationTag(metrics.TimerActiveTaskWorkflowBackoffTimerScope),
		)
	}

	if mutableState.HadOrHasWorkflowTask() {
		// already has workflow task
		return nil
	}

	// schedule first workflow task
	return t.updateWorkflowExecution(ctx, weContext, mutableState, true)
}

func (t *timerQueueActiveTaskExecutor) executeActivityRetryTimerTask(
	ctx context.Context,
	task *tasks.ActivityRetryTimerTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(ctx, weContext, task, t.metricHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	// generate activity task
	activityInfo, ok := mutableState.GetActivityInfo(task.EventID)
	if !ok || task.Attempt < activityInfo.Attempt || activityInfo.StartedEventId != common.EmptyEventID {
		if ok {
			t.logger.Info("Duplicate activity retry timer task",
				tag.WorkflowID(mutableState.GetExecutionInfo().WorkflowId),
				tag.WorkflowRunID(mutableState.GetExecutionState().GetRunId()),
				tag.WorkflowNamespaceID(mutableState.GetExecutionInfo().NamespaceId),
				tag.WorkflowScheduledEventID(activityInfo.ScheduledEventId),
				tag.Attempt(activityInfo.Attempt),
				tag.FailoverVersion(activityInfo.Version),
				tag.TimerTaskStatus(activityInfo.TimerTaskStatus),
				tag.ScheduleAttempt(task.Attempt))
		}
		return nil
	}
	err = CheckTaskVersion(t.shard, t.logger, mutableState.GetNamespaceEntry(), activityInfo.Version, task.Version, task)
	if err != nil {
		return err
	}

	taskQueue := &taskqueuepb.TaskQueue{
		Name: activityInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	scheduleToStartTimeout := timestamp.DurationValue(activityInfo.ScheduleToStartTimeout)
	directive := worker_versioning.MakeDirectiveForActivityTask(mutableState.GetWorkerVersionStamp(), activityInfo.UseCompatibleVersion)

	// NOTE: do not access anything related mutable state after this lock release
	release(nil) // release earlier as we don't need the lock anymore

	_, retError = t.matchingClient.AddActivityTask(ctx, &matchingservice.AddActivityTaskRequest{
		NamespaceId: task.GetNamespaceID(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: task.GetWorkflowID(),
			RunId:      task.GetRunID(),
		},
		TaskQueue:              taskQueue,
		ScheduledEventId:       task.EventID,
		ScheduleToStartTimeout: timestamp.DurationPtr(scheduleToStartTimeout),
		Clock:                  vclock.NewVectorClock(t.shard.GetClusterMetadata().GetClusterID(), t.shard.GetShardID(), task.TaskID),
		VersionDirective:       directive,
	})

	return retError
}

func (t *timerQueueActiveTaskExecutor) executeWorkflowTimeoutTask(
	ctx context.Context,
	task *tasks.WorkflowTimeoutTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(ctx, weContext, task, t.metricHandler, t.logger)
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
	err = CheckTaskVersion(t.shard, t.logger, mutableState.GetNamespaceEntry(), startVersion, task.Version, task)
	if err != nil {
		return err
	}

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
		retryState,
		newRunID,
	); err != nil {
		return err
	}

	// No more retries, or workflow is expired.
	if initiator == enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED {
		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		return t.updateWorkflowExecution(ctx, weContext, mutableState, false)
	}

	startEvent, err := mutableState.GetStartEvent(ctx)
	if err != nil {
		return err
	}
	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()

	newMutableState := workflow.NewMutableState(
		t.shard,
		t.shard.GetEventsCache(),
		t.shard.GetLogger(),
		mutableState.GetNamespaceEntry(),
		t.shard.GetTimeSource().Now(),
	)
	err = workflow.SetupNewWorkflowForRetryOrCron(
		ctx,
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

	err = newMutableState.SetHistoryTree(
		ctx,
		newMutableState.GetExecutionInfo().WorkflowExecutionTimeout,
		newMutableState.GetExecutionInfo().WorkflowRunTimeout,
		newRunID)
	if err != nil {
		return err
	}

	newExecutionInfo := newMutableState.GetExecutionInfo()
	newExecutionState := newMutableState.GetExecutionState()
	return weContext.UpdateWorkflowExecutionWithNewAsActive(
		ctx,
		workflow.NewContext(
			t.shard,
			definition.NewWorkflowKey(
				newExecutionInfo.NamespaceId,
				newExecutionInfo.WorkflowId,
				newExecutionState.RunId,
			),
			t.logger,
		),
		newMutableState,
	)
}

func (t *timerQueueActiveTaskExecutor) getTimerSequence(
	mutableState workflow.MutableState,
) workflow.TimerSequence {
	return workflow.NewTimerSequence(mutableState)
}

func (t *timerQueueActiveTaskExecutor) updateWorkflowExecution(
	ctx context.Context,
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
	return context.UpdateWorkflowExecutionAsActive(ctx)
}

func (t *timerQueueActiveTaskExecutor) emitTimeoutMetricScopeWithNamespaceTag(
	namespaceID namespace.ID,
	operation string,
	timerType enumspb.TimeoutType,
) {
	namespaceEntry, err := t.registry.GetNamespaceByID(namespaceID)
	if err != nil {
		return
	}
	metricsScope := t.metricHandler.WithTags(
		metrics.OperationTag(operation),
		metrics.NamespaceTag(namespaceEntry.Name().String()),
	)
	switch timerType {
	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START:
		metricsScope.Counter(metrics.ScheduleToStartTimeoutCounter.GetMetricName()).Record(1)
	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
		metricsScope.Counter(metrics.ScheduleToCloseTimeoutCounter.GetMetricName()).Record(1)
	case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		metricsScope.Counter(metrics.StartToCloseTimeoutCounter.GetMetricName()).Record(1)
	case enumspb.TIMEOUT_TYPE_HEARTBEAT:
		metricsScope.Counter(metrics.HeartbeatTimeoutCounter.GetMetricName()).Record(1)
	}
}
