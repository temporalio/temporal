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
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/priorities"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/vclock"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/history/workflow/update"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	timerQueueActiveTaskExecutor struct {
		*timerQueueTaskExecutorBase
	}
)

func newTimerQueueActiveTaskExecutor(
	shard historyi.ShardContext,
	workflowCache wcache.Cache,
	workflowDeleteManager deletemanager.DeleteManager,
	logger log.Logger,
	metricProvider metrics.Handler,
	config *configs.Config,
	matchingRawClient resource.MatchingRawClient,
	chasmEngine chasm.Engine,
) queues.Executor {
	return &timerQueueActiveTaskExecutor{
		timerQueueTaskExecutorBase: newTimerQueueTaskExecutorBase(
			shard,
			workflowCache,
			workflowDeleteManager,
			matchingRawClient,
			chasmEngine,
			logger,
			metricProvider,
			config,
			true,
		),
	}
}

func (t *timerQueueActiveTaskExecutor) Execute(
	ctx context.Context,
	executable queues.Executable,
) queues.ExecuteResponse {
	taskTypeTagValue := queues.GetActiveTimerTaskTypeTagValue(executable.GetTask())

	namespaceTag, replicationState := getNamespaceTagAndReplicationStateByID(
		t.shardContext.GetNamespaceRegistry(),
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
		//  be returned from task executor. Also check the standby queue logic.
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutedAsActive:    true,
			ExecutionErr:        consts.ErrNamespaceHandover,
		}
	}

	var err error

	switch task := executable.GetTask().(type) {
	case *tasks.UserTimerTask:
		err = t.executeUserTimerTimeoutTask(ctx, task)
	case *tasks.ActivityTimeoutTask:
		err = t.executeActivityTimeoutTask(ctx, task)
	case *tasks.WorkflowTaskTimeoutTask:
		err = t.executeWorkflowTaskTimeoutTask(ctx, task)
	case *tasks.WorkflowRunTimeoutTask:
		err = t.executeWorkflowRunTimeoutTask(ctx, task)
	case *tasks.WorkflowExecutionTimeoutTask:
		err = t.executeWorkflowExecutionTimeoutTask(ctx, task)
	case *tasks.ActivityRetryTimerTask:
		err = t.executeActivityRetryTimerTask(ctx, task)
	case *tasks.WorkflowBackoffTimerTask:
		err = t.executeWorkflowBackoffTimerTask(ctx, task)
	case *tasks.DeleteHistoryEventTask:
		err = t.executeDeleteHistoryEventTask(ctx, task)
	case *tasks.StateMachineTimerTask:
		err = t.executeStateMachineTimerTask(ctx, task)
	case *tasks.ChasmTaskPure:
		err = t.executeChasmPureTimerTask(ctx, task)
	case *tasks.ChasmTask:
		err = t.executeChasmSideEffectTimerTask(ctx, task)
	default:
		err = queues.NewUnprocessableTaskError("unknown task type")
	}

	return queues.ExecuteResponse{
		ExecutionMetricTags: metricsTags,
		ExecutedAsActive:    true,
		ExecutionErr:        err,
	}
}

func (t *timerQueueActiveTaskExecutor) executeUserTimerTimeoutTask(
	ctx context.Context,
	task *tasks.UserTimerTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(ctx, t.shardContext, weContext, task, t.metricsHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil {
		release(nil) // release(nil) so mutable state is not unloaded from cache
		return consts.ErrWorkflowExecutionNotFound
	}

	timerSequence := t.getTimerSequence(mutableState)
	referenceTime := t.Now()
	timerFired := false
Loop:
	for _, timerSequenceID := range timerSequence.LoadAndSortUserTimers() {
		timerInfo, ok := mutableState.GetUserTimerInfoByEventID(timerSequenceID.EventID)
		if !ok {
			errString := fmt.Sprintf("failed to find in user timer event ID: %v", timerSequenceID.EventID)
			t.logger.Error(errString)
			return serviceerror.NewInternal(errString)
		}

		if !queues.IsTimeExpired(task, referenceTime, timerSequenceID.Timestamp) {
			// Timer sequence IDs are sorted; once we encounter a timer whose
			// sequence ID has not expired, all subsequent timers will not have
			// expired.
			break Loop
		}

		if !mutableState.IsWorkflowExecutionRunning() {
			release(nil) // so mutable state is not unloaded from cache
			return consts.ErrWorkflowCompleted
		}

		if _, err := mutableState.AddTimerFiredEvent(timerInfo.GetTimerId()); err != nil {
			return err
		}
		timerFired = true
	}

	if !timerFired {
		release(nil) // so mutable state is not unloaded from cache
		return errNoTimerFired
	}

	return t.updateWorkflowExecution(ctx, weContext, mutableState, timerFired)
}

func (t *timerQueueActiveTaskExecutor) executeActivityTimeoutTask(
	ctx context.Context,
	task *tasks.ActivityTimeoutTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(ctx, t.shardContext, weContext, task, t.metricsHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	timerSequence := t.getTimerSequence(mutableState)
	referenceTime := t.Now()
	updateMutableState := false
	scheduleWorkflowTask := false

	// Need to clear activity heartbeat timer task mask for new activity timer task creation.
	// NOTE: LastHeartbeatTimeoutVisibilityInSeconds is for deduping heartbeat timer creation as it's possible
	// one heartbeat task was persisted multiple times with different taskIDs due to the retry logic
	// for updating workflow execution. In that case, only one new heartbeat timeout task should be
	// created.
	isHeartBeatTask := task.TimeoutType == enumspb.TIMEOUT_TYPE_HEARTBEAT
	ai, heartbeatTimeoutVis, ok := mutableState.GetActivityInfoWithTimerHeartbeat(task.EventID)
	if isHeartBeatTask && ok && queues.IsTimeExpired(task, task.GetVisibilityTime(), heartbeatTimeoutVis) {
		if err := mutableState.UpdateActivityTaskStatusWithTimerHeartbeat(
			ai.ScheduledEventId, ai.TimerTaskStatus&^workflow.TimerTaskStatusCreatedHeartbeat, nil); err != nil {
			return err
		}
		updateMutableState = true
	}

Loop:
	for _, timerSequenceID := range timerSequence.LoadAndSortActivityTimers() {
		if !queues.IsTimeExpired(task, referenceTime, timerSequenceID.Timestamp) {
			// timer sequence IDs are sorted, once there is one timer
			// sequence ID not expired, all after that wil not expired
			break Loop
		}

		activityInfo, ok := mutableState.GetActivityInfo(timerSequenceID.EventID)
		if !ok {
			//  this case can happen since each activity can have 4 timers
			//  and one of those 4 timers may have fired in this loop
			continue Loop
		}

		result, err := t.processSingleActivityTimeoutTask(mutableState, timerSequenceID, activityInfo)

		if err != nil {
			return err
		}

		updateMutableState = updateMutableState || result.shouldUpdateMutableState
		scheduleWorkflowTask = scheduleWorkflowTask || result.shouldScheduleWorkflowTask

	}

	if !updateMutableState {
		return nil
	}
	return t.updateWorkflowExecution(ctx, weContext, mutableState, scheduleWorkflowTask)
}

type processingActivityTimeoutResult struct {
	shouldUpdateMutableState   bool
	shouldScheduleWorkflowTask bool
}

func (t *timerQueueActiveTaskExecutor) processSingleActivityTimeoutTask(
	mutableState historyi.MutableState,
	timerSequenceID workflow.TimerSequenceID,
	ai *persistencespb.ActivityInfo,
) (processingActivityTimeoutResult, error) {

	result := processingActivityTimeoutResult{
		shouldUpdateMutableState:   false,
		shouldScheduleWorkflowTask: false,
	}

	if timerSequenceID.Attempt < ai.Attempt {
		//  The RetryActivity call below could update activity attempt, in which case we do not want to apply a timeout for the previous attempt.
		return result, nil
	}

	// Note: we don't need to check activity Stamps.
	// This is because for the same attempts calls are idempotent.

	failureMsg := fmt.Sprintf("activity %v timeout", timerSequenceID.TimerType.String())
	timeoutFailure := failure.NewTimeoutFailure(failureMsg, timerSequenceID.TimerType)
	retryState, err := mutableState.RetryActivity(ai, timeoutFailure)
	if err != nil {
		return result, nil
	}

	workflow.RecordActivityCompletionMetrics(
		t.shardContext,
		mutableState.GetNamespaceEntry().Name(),
		ai.TaskQueue,
		workflow.ActivityCompletionMetrics{
			Status:             workflow.ActivityStatusTimeout,
			AttemptStartedTime: timestamp.TimeValue(ai.StartedTime),
			FirstScheduledTime: timestamp.TimeValue(ai.FirstScheduledTime),
			Closed:             retryState != enumspb.RETRY_STATE_IN_PROGRESS,
			TimerType:          timerSequenceID.TimerType,
		},
		metrics.OperationTag(metrics.TimerActiveTaskActivityTimeoutScope),
		metrics.WorkflowTypeTag(mutableState.GetWorkflowType().GetName()),
		metrics.ActivityTypeTag(ai.ActivityType.GetName()),
		metrics.VersioningBehaviorTag(mutableState.GetEffectiveVersioningBehavior()))

	if retryState == enumspb.RETRY_STATE_IN_PROGRESS {
		// TODO uncommment once RETRY_STATE_PAUSED is supported
		// || retryState == enumspb.RETRY_STATE_PAUSED {
		result.shouldUpdateMutableState = true
		return result, nil
	}

	if retryState == enumspb.RETRY_STATE_TIMEOUT {
		// If retryState is Timeout then it means that expirationTime is expired.
		// ExpirationTime is expired when ScheduleToClose timeout is expired.
		const timeoutType = enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE
		var failureMsg = fmt.Sprintf("activity %v timeout", timeoutType.String())
		timeoutFailure = failure.NewTimeoutFailure(failureMsg, timeoutType)
	}
	timeoutFailure.GetTimeoutFailureInfo().LastHeartbeatDetails = ai.LastHeartbeatDetails

	t.emitTimeoutMetricScopeWithNamespaceTag(
		namespace.ID(mutableState.GetExecutionInfo().NamespaceId),
		metrics.TimerActiveTaskActivityTimeoutScope,
		timerSequenceID.TimerType,
		mutableState.GetEffectiveVersioningBehavior(),
		ai.Attempt,
	)
	if _, err = mutableState.AddActivityTaskTimedOutEvent(
		ai.ScheduledEventId,
		ai.StartedEventId,
		timeoutFailure,
		retryState,
	); err != nil {
		return result, err
	}

	result.shouldUpdateMutableState = true
	result.shouldScheduleWorkflowTask = true
	return result, nil
}

func (t *timerQueueActiveTaskExecutor) executeWorkflowTaskTimeoutTask(
	ctx context.Context,
	task *tasks.WorkflowTaskTimeoutTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(ctx, t.shardContext, weContext, task, t.metricsHandler, t.logger)
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
	if task.Stamp != workflowTask.Stamp {
		release(nil) // release(nil) so that the mutable state is not unloaded from cache
		return consts.ErrStaleReference
	}

	var operationMetricsTag string
	if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		// Check if mutable state still points to this task.
		// Mutable state can lost speculative WT or even has another one there if, for example, workflow was evicted from cache.
		if !mutableState.CheckSpeculativeWorkflowTaskTimeoutTask(task) {
			return nil
		}
		operationMetricsTag = metrics.TaskTypeTimerActiveTaskSpeculativeWorkflowTaskTimeout
	} else {
		err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), workflowTask.Version, task.Version, task)
		if err != nil {
			return err
		}

		if workflowTask.Attempt != task.ScheduleAttempt {
			return nil
		}
		operationMetricsTag = metrics.TimerActiveTaskWorkflowTaskTimeoutScope
	}

	scheduleWorkflowTask := false
	switch task.TimeoutType {
	case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		t.emitTimeoutMetricScopeWithNamespaceTag(
			namespace.ID(mutableState.GetExecutionInfo().NamespaceId),
			operationMetricsTag,
			enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			mutableState.GetEffectiveVersioningBehavior(),
			workflowTask.Attempt,
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
			operationMetricsTag,
			enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
			mutableState.GetEffectiveVersioningBehavior(),
			workflowTask.Attempt,
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

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(ctx, t.shardContext, weContext, task, t.metricsHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil {
		release(nil)
		return consts.ErrWorkflowExecutionNotFound
	}
	if !mutableState.IsWorkflowExecutionRunning() {
		release(nil)
		return consts.ErrWorkflowCompleted
	}

	// TODO: deprecated, remove below 3 metrics after v1.25
	if task.WorkflowBackoffType == enumsspb.WORKFLOW_BACKOFF_TYPE_RETRY {
		metrics.WorkflowRetryBackoffTimerCount.With(t.metricsHandler).Record(
			1,
			metrics.OperationTag(metrics.TimerActiveTaskWorkflowBackoffTimerScope),
		)
	} else if task.WorkflowBackoffType == enumsspb.WORKFLOW_BACKOFF_TYPE_CRON {
		metrics.WorkflowCronBackoffTimerCount.With(t.metricsHandler).Record(
			1,
			metrics.OperationTag(metrics.TimerActiveTaskWorkflowBackoffTimerScope),
		)
	} else if task.WorkflowBackoffType == enumsspb.WORKFLOW_BACKOFF_TYPE_DELAY_START {
		metrics.WorkflowDelayedStartBackoffTimerCount.With(t.metricsHandler).Record(
			1,
			metrics.OperationTag(metrics.TimerActiveTaskWorkflowBackoffTimerScope),
		)
	}

	nsName := mutableState.GetNamespaceEntry().Name().String()
	metrics.WorkflowBackoffCount.With(t.metricsHandler).Record(
		1,
		metrics.NamespaceTag(nsName),
		metrics.StringTag("backoff_type", task.WorkflowBackoffType.String()))

	if mutableState.HadOrHasWorkflowTask() {
		// already has workflow task
		release(nil)
		return errNoTimerFired
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

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(ctx, t.shardContext, weContext, task, t.metricsHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil {
		release(nil) // release(nil) so mutable state is not unloaded from cache
		return consts.ErrWorkflowExecutionNotFound
	}

	// generate activity task
	activityInfo, ok := mutableState.GetActivityInfo(task.EventID)

	if !ok {
		release(nil) // release(nil) so mutable state is not unloaded from cache
		return consts.ErrActivityTaskNotFound
	}

	if task.Stamp != activityInfo.Stamp || activityInfo.Paused {
		// if retry task event is from an old stamp of if activity is paused we should ignore the event.
		release(nil) // release(nil) so mutable state is not unloaded from cache
		return consts.ErrActivityTaskNotFound
	}

	if task.Attempt < activityInfo.Attempt || activityInfo.StartedEventId != common.EmptyEventID {
		t.logger.Info("Duplicate activity retry timer task",
			tag.WorkflowID(mutableState.GetExecutionInfo().WorkflowId),
			tag.WorkflowRunID(mutableState.GetExecutionState().GetRunId()),
			tag.WorkflowNamespaceID(mutableState.GetExecutionInfo().NamespaceId),
			tag.WorkflowScheduledEventID(activityInfo.ScheduledEventId),
			tag.Attempt(activityInfo.Attempt),
			tag.FailoverVersion(activityInfo.Version),
			tag.TimerTaskStatus(activityInfo.TimerTaskStatus),
			tag.ScheduleAttempt(task.Attempt))
		release(nil) // release(nil) so mutable state is not unloaded from cache
		return consts.ErrActivityTaskNotFound
	}
	err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), activityInfo.Version, task.Version, task)
	if err != nil {
		return err
	}

	if !mutableState.IsWorkflowExecutionRunning() {
		release(nil) // release(nil) so mutable state is not unloaded from cache
		return consts.ErrWorkflowCompleted
	}

	err = t.processActivityWorkflowRules(ctx, weContext, mutableState, activityInfo)
	if err != nil {
		return err
	}

	// task can be paused as the result of processing activity workflow rules, so we need to check again
	if task.Stamp != activityInfo.Stamp || activityInfo.Paused {
		// if retry task event is from an old stamp of if activity is paused we should ignore the event.
		release(nil) // release(nil) so mutable state is not unloaded from cache
		return consts.ErrActivityTaskNotFound
	}

	taskQueue := &taskqueuepb.TaskQueue{
		Name: activityInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	scheduleToStartTimeout := timestamp.DurationValue(activityInfo.ScheduleToStartTimeout)
	directive := MakeDirectiveForActivityTask(mutableState, activityInfo)
	useWfBuildId := activityInfo.GetUseWorkflowBuildIdInfo() != nil
	priority := priorities.Merge(mutableState.GetExecutionInfo().Priority, activityInfo.Priority)

	// NOTE: do not access anything related mutable state after this lock release
	release(nil) // release earlier as we don't need the lock anymore

	resp, err := t.matchingRawClient.AddActivityTask(ctx, &matchingservice.AddActivityTaskRequest{
		NamespaceId: task.GetNamespaceID(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: task.GetWorkflowID(),
			RunId:      task.GetRunID(),
		},
		TaskQueue:              taskQueue,
		ScheduledEventId:       task.EventID,
		ScheduleToStartTimeout: durationpb.New(scheduleToStartTimeout),
		Clock:                  vclock.NewVectorClock(t.shardContext.GetClusterMetadata().GetClusterID(), t.shardContext.GetShardID(), task.TaskID),
		VersionDirective:       directive,
		Stamp:                  task.Stamp,
		Priority:               priority,
	})
	if err != nil {
		return err
	}

	if useWfBuildId {
		// activity's build ID is the same as WF's, so no need to update MS
		return nil
	}

	return updateIndependentActivityBuildId(
		ctx,
		task,
		resp.AssignedBuildId,
		t.shardContext,
		historyi.TransactionPolicyActive,
		t.cache,
		t.metricsHandler,
		t.logger,
	)
}

func (t *timerQueueActiveTaskExecutor) executeWorkflowRunTimeoutTask(
	ctx context.Context,
	task *tasks.WorkflowRunTimeoutTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(ctx, t.shardContext, weContext, task, t.metricsHandler, t.logger)
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
	err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), startVersion, task.Version, task)
	if err != nil {
		return err
	}

	if !t.isValidWorkflowRunTimeoutTask(mutableState, task) {
		return nil
	}

	timeoutFailure := failure.NewTimeoutFailure("workflow timeout", enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	backoffInterval := backoff.NoBackoff
	retryState := enumspb.RETRY_STATE_TIMEOUT
	initiator := enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED

	wfExpTime := mutableState.GetExecutionInfo().WorkflowExecutionExpirationTime
	if wfExpTime == nil || wfExpTime.AsTime().IsZero() || wfExpTime.AsTime().After(t.Now()) {
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
		updateErr := t.updateWorkflowExecution(ctx, weContext, mutableState, false)
		if updateErr != nil {
			return updateErr
		}
		weContext.UpdateRegistry(ctx).Abort(update.AbortReasonWorkflowCompleted)
		return nil
	}

	startEvent, err := mutableState.GetStartEvent(ctx)
	if err != nil {
		return err
	}
	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()

	newMutableState, err := workflow.NewMutableStateInChain(
		t.shardContext,
		t.shardContext.GetEventsCache(),
		t.shardContext.GetLogger(),
		mutableState.GetNamespaceEntry(),
		mutableState.GetWorkflowKey().WorkflowID,
		newRunID,
		t.Now(),
		mutableState,
	)
	if err != nil {
		return err
	}

	err = workflow.SetupNewWorkflowForRetryOrCron(
		ctx,
		mutableState,
		newMutableState,
		newRunID,
		startAttr,
		startEvent.Links,
		startAttr.LastCompletionResult,
		timeoutFailure,
		backoffInterval,
		initiator,
	)
	if err != nil {
		return err
	}

	err = newMutableState.SetHistoryTree(
		newMutableState.GetExecutionInfo().WorkflowExecutionTimeout,
		newMutableState.GetExecutionInfo().WorkflowRunTimeout,
		newRunID,
	)
	if err != nil {
		return err
	}

	newExecutionInfo := newMutableState.GetExecutionInfo()
	newExecutionState := newMutableState.GetExecutionState()
	updateErr := weContext.UpdateWorkflowExecutionWithNewAsActive(
		ctx,
		t.shardContext,
		workflow.NewContext(
			t.shardContext.GetConfig(),
			definition.NewWorkflowKey(
				newExecutionInfo.NamespaceId,
				newExecutionInfo.WorkflowId,
				newExecutionState.RunId,
			),
			t.logger,
			t.shardContext.GetThrottledLogger(),
			t.shardContext.GetMetricsHandler(),
		),
		newMutableState,
	)

	if updateErr != nil {
		return updateErr
	}

	// A new run was created after the previous run timed out. Running Updates
	// for this WF are aborted with a retryable error.
	// Internal server retries will retry the API call, and the Update will be sent to the new run.
	weContext.UpdateRegistry(ctx).Abort(update.AbortReasonWorkflowContinuing)
	return nil
}

func (t *timerQueueActiveTaskExecutor) executeWorkflowExecutionTimeoutTask(
	ctx context.Context,
	task *tasks.WorkflowExecutionTimeoutTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(ctx, t.shardContext, weContext, task, t.metricsHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil {
		return nil
	}

	if !t.isValidWorkflowExecutionTimeoutTask(mutableState, task) {
		return nil
	}

	if err := workflow.TimeoutWorkflow(mutableState, enumspb.RETRY_STATE_TIMEOUT, ""); err != nil {
		return err
	}

	updateErr := t.updateWorkflowExecution(ctx, weContext, mutableState, false)
	if updateErr != nil {
		return updateErr
	}

	weContext.UpdateRegistry(ctx).Abort(update.AbortReasonWorkflowCompleted)
	return nil
}

func (t *timerQueueActiveTaskExecutor) executeStateMachineTimerTask(
	ctx context.Context,
	task *tasks.StateMachineTimerTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	wfCtx, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	ms, err := loadMutableStateForTimerTask(ctx, t.shardContext, wfCtx, task, t.metricsHandler, t.logger)
	if err != nil {
		return err
	}
	if ms == nil {
		return nil
	}

	processedTimers, err := t.executeStateMachineTimers(
		ctx,
		wfCtx,
		ms,
		task,
		func(node *hsm.Node, task hsm.Task) error {
			return t.shardContext.StateMachineRegistry().ExecuteTimerTask(t, node, task)
		},
	)
	if err != nil {
		return err
	}

	// We haven't done any work, return without committing.
	if processedTimers == 0 {
		return nil
	}

	if t.config.EnableUpdateWorkflowModeIgnoreCurrent() {
		return wfCtx.UpdateWorkflowExecutionAsActive(ctx, t.shardContext)
	}

	// TODO: remove following code once EnableUpdateWorkflowModeIgnoreCurrent config is deprecated.
	if ms.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		// Can't use UpdateWorkflowExecutionAsActive since it updates the current run, and we are operating on a
		// closed workflow.
		return wfCtx.SubmitClosedWorkflowSnapshot(ctx, t.shardContext, historyi.TransactionPolicyActive)
	}
	return wfCtx.UpdateWorkflowExecutionAsActive(ctx, t.shardContext)
}

func (t *timerQueueActiveTaskExecutor) getTimerSequence(
	mutableState historyi.MutableState,
) workflow.TimerSequence {
	return workflow.NewTimerSequence(mutableState)
}

func (t *timerQueueActiveTaskExecutor) updateWorkflowExecution(
	ctx context.Context,
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
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
	return wfContext.UpdateWorkflowExecutionAsActive(ctx, t.shardContext)
}

func (t *timerQueueActiveTaskExecutor) emitTimeoutMetricScopeWithNamespaceTag(
	namespaceID namespace.ID,
	operation string,
	timerType enumspb.TimeoutType,
	effectiveVersioningBehavior enumspb.VersioningBehavior,
	taskAttempt int32,
) {
	namespaceEntry, err := t.registry.GetNamespaceByID(namespaceID)
	if err != nil {
		return
	}
	metricsScope := t.metricsHandler.WithTags(
		metrics.OperationTag(operation),
		metrics.NamespaceTag(namespaceEntry.Name().String()),
		metrics.VersioningBehaviorTag(effectiveVersioningBehavior),
		metrics.FirstAttemptTag(taskAttempt),
	)
	switch timerType {
	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START:
		metrics.ScheduleToStartTimeoutCounter.With(metricsScope).Record(1)
	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
		metrics.ScheduleToCloseTimeoutCounter.With(metricsScope).Record(1)
	case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		metrics.StartToCloseTimeoutCounter.With(metricsScope).Record(1)
	case enumspb.TIMEOUT_TYPE_HEARTBEAT:
		metrics.HeartbeatTimeoutCounter.With(metricsScope).Record(1)
	}
}

func (t *timerQueueActiveTaskExecutor) processActivityWorkflowRules(
	ctx context.Context,
	weContext historyi.WorkflowContext,
	ms historyi.MutableState,
	ai *persistencespb.ActivityInfo,
) error {
	if ai.Paused {
		return nil
	}

	activityChanged := workflow.ActivityMatchWorkflowRules(ms, t.shardContext.GetTimeSource(), t.logger, ai)
	if !activityChanged {
		return nil
	}
	if ai.Paused {
		// need to update activity
		if err := ms.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencespb.ActivityInfo, _ historyi.MutableState) error {
			activityInfo.StartedEventId = common.EmptyEventID
			activityInfo.StartVersion = common.EmptyVersion
			activityInfo.StartedTime = nil
			activityInfo.RequestId = ""
			return nil
		}); err != nil {
			return err
		}

		// need to update mutable state
		err := weContext.UpdateWorkflowExecutionAsActive(
			ctx,
			t.shardContext,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *timerQueueActiveTaskExecutor) executeChasmSideEffectTimerTask(
	ctx context.Context,
	task *tasks.ChasmTask,
) error {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	wfCtx, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(err) }()

	ms, err := loadMutableStateForTimerTask(ctx, t.shardContext, wfCtx, task, t.metricsHandler, t.logger)
	if err != nil {
		return err
	}
	if ms == nil {
		return errNoChasmMutableState
	}

	tree := ms.ChasmTree()
	if tree == nil {
		return errNoChasmTree
	}

	// Now that we've loaded the CHASM tree, we can release the lock before task
	// execution. The task's executor must do its own locking as needed, and additional
	// mutable state validations will run at access time.
	release(nil)

	return executeChasmSideEffectTask(
		ctx,
		t.chasmEngine,
		t.shardContext.ChasmRegistry(),
		tree,
		task,
	)
}

func (t *timerQueueActiveTaskExecutor) executeChasmPureTimerTask(
	ctx context.Context,
	task *tasks.ChasmTaskPure,
) error {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	wfCtx, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(err) }()

	ms, err := loadMutableStateForTimerTask(ctx, t.shardContext, wfCtx, task, t.metricsHandler, t.logger)
	if err != nil {
		return err
	}
	if ms == nil {
		return errNoChasmMutableState
	}

	// Execute all fired pure tasks for a component while holding the workflow lock.
	processedTimers := 0
	err = t.executeChasmPureTimers(
		ms,
		task,
		func(executor chasm.NodePureTask, taskAttributes chasm.TaskAttributes, taskInstance any) (bool, error) {
			// ExecutePureTask also calls the task's validator. Invalid tasks will no-op
			// succeed.
			executed, err := executor.ExecutePureTask(ctx, taskAttributes, taskInstance)
			if err == nil {
				processedTimers += 1

			}

			return executed, err
		},
	)
	if err != nil {
		return err
	}

	// Commit changes only if we processed any timers.
	if processedTimers == 0 {
		return nil
	}

	if t.config.EnableUpdateWorkflowModeIgnoreCurrent() {
		return wfCtx.UpdateWorkflowExecutionAsActive(ctx, t.shardContext)
	}

	// TODO: remove following code once EnableUpdateWorkflowModeIgnoreCurrent config is deprecated.
	if ms.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		// Can't use UpdateWorkflowExecutionAsActive since it updates the current run, and we are operating on a
		// closed workflow.
		return wfCtx.SubmitClosedWorkflowSnapshot(ctx, t.shardContext, historyi.TransactionPolicyActive)
	}
	return wfCtx.UpdateWorkflowExecutionAsActive(ctx, t.shardContext)
}
