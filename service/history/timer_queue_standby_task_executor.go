package history

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
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
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	timerQueueStandbyTaskExecutor struct {
		*timerQueueTaskExecutorBase
		clusterName string
		clientBean  client.Bean
	}
)

func newTimerQueueStandbyTaskExecutor(
	shard historyi.ShardContext,
	workflowCache wcache.Cache,
	workflowDeleteManager deletemanager.DeleteManager,
	matchingRawClient resource.MatchingRawClient,
	chasmEngine chasm.Engine,
	logger log.Logger,
	metricProvider metrics.Handler,
	clusterName string,
	config *configs.Config,
	clientBean client.Bean,
) queues.Executor {
	return &timerQueueStandbyTaskExecutor{
		timerQueueTaskExecutorBase: newTimerQueueTaskExecutorBase(
			shard,
			workflowCache,
			workflowDeleteManager,
			matchingRawClient,
			chasmEngine,
			logger,
			metricProvider,
			config,
			false,
		),
		clusterName: clusterName,
		clientBean:  clientBean,
	}
}

func (t *timerQueueStandbyTaskExecutor) Execute(
	ctx context.Context,
	executable queues.Executable,
) queues.ExecuteResponse {
	task := executable.GetTask()
	taskTypeTagValue := queues.GetStandbyTimerTaskTypeTagValue(task)

	metricsTags := []metrics.Tag{
		getNamespaceTagByID(t.shardContext.GetNamespaceRegistry(), task.GetNamespaceID()),
		metrics.TaskTypeTag(taskTypeTagValue),
		metrics.OperationTag(taskTypeTagValue), // for backward compatibility
	}

	var err error

	switch task := task.(type) {
	case *tasks.UserTimerTask:
		err = t.executeUserTimerTimeoutTask(ctx, task)
	case *tasks.ActivityTimeoutTask:
		err = t.executeActivityTimeoutTask(ctx, task)
	case *tasks.WorkflowTaskTimeoutTask:
		err = t.executeWorkflowTaskTimeoutTask(ctx, task)
	case *tasks.WorkflowBackoffTimerTask:
		err = t.executeWorkflowBackoffTimerTask(ctx, task)
	case *tasks.ActivityRetryTimerTask:
		err = t.executeActivityRetryTimerTask(ctx, task)
	case *tasks.WorkflowRunTimeoutTask:
		err = t.executeWorkflowRunTimeoutTask(ctx, task)
	case *tasks.WorkflowExecutionTimeoutTask:
		err = t.executeWorkflowExecutionTimeoutTask(ctx, task)
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
		ExecutedAsActive:    false,
		ExecutionErr:        err,
	}
}

func (t *timerQueueStandbyTaskExecutor) executeChasmPureTimerTask(
	ctx context.Context,
	task *tasks.ChasmTaskPure,
) error {
	actionFn := func(
		ctx context.Context,
		wfContext historyi.WorkflowContext,
		mutableState historyi.MutableState,
	) (any, error) {
		err := t.executeChasmPureTimers(
			mutableState,
			task,
			func(node chasm.NodePureTask, taskAttributes chasm.TaskAttributes, task any) (bool, error) {
				ok, err := node.ValidatePureTask(ctx, taskAttributes, task)
				if err != nil {
					return false, err
				}

				// When Validate succeeds, the task is still expected to run. Return ErrTaskRetry
				// to wait for the task to complete on the active cluster, after which Validate
				// will begin returning false.
				if ok {
					return false, consts.ErrTaskRetry
				}

				return false, nil
			},
		)
		if err != nil && errors.Is(err, consts.ErrTaskRetry) {
			return &struct{}{}, nil
		}

		return nil, err
	}

	return t.processTimer(
		ctx,
		task,
		actionFn,
		getStandbyPostActionFn(
			task,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(task.GetType()),
			t.checkWorkflowStillExistOnSourceBeforeDiscard,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeChasmSideEffectTimerTask(
	ctx context.Context,
	task *tasks.ChasmTask,
) error {
	actionFn := func(
		ctx context.Context,
		wfContext historyi.WorkflowContext,
		ms historyi.MutableState,
	) (any, error) {
		return validateChasmSideEffectTask(
			ctx,
			ms,
			task,
		)
	}

	return t.processTimer(
		ctx,
		task,
		actionFn,
		getStandbyPostActionFn(
			task,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(task.GetType()),
			// TODO - replace this with a method for CHASM components
			t.checkWorkflowStillExistOnSourceBeforeDiscard,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeUserTimerTimeoutTask(
	ctx context.Context,
	timerTask *tasks.UserTimerTask,
) error {
	referenceTime := t.Now()
	actionFn := func(_ context.Context, wfContext historyi.WorkflowContext, mutableState historyi.MutableState) (interface{}, error) {
		if !mutableState.IsWorkflowExecutionRunning() {
			// workflow already finished, no need to process the timer
			return nil, nil
		}

		timerSequence := t.getTimerSequence(mutableState)
		timerSequenceIDs := timerSequence.LoadAndSortUserTimers()
		if len(timerSequenceIDs) > 0 {
			timerSequenceID := timerSequenceIDs[0]
			_, ok := mutableState.GetUserTimerInfoByEventID(timerSequenceID.EventID)
			if !ok {
				errString := fmt.Sprintf("failed to find in user timer event ID: %v", timerSequenceID.EventID)
				t.logger.Error(errString)
				return nil, serviceerror.NewInternal(errString)
			}

			if queues.IsTimeExpired(
				timerTask,
				referenceTime,
				timerSequenceID.Timestamp,
			) {
				return &struct{}{}, nil
			}
			// Since the user timers are already sorted, then if there is one timer which is not expired,
			// all user timers after that timer are not expired.
		}
		// If there is no user timer expired, then we are good.
		return nil, nil
	}

	return t.processTimer(
		ctx,
		timerTask,
		actionFn,
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.checkWorkflowStillExistOnSourceBeforeDiscard,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeActivityTimeoutTask(
	ctx context.Context,
	timerTask *tasks.ActivityTimeoutTask,
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
	referenceTime := t.Now()
	actionFn := func(ctx context.Context, wfContext historyi.WorkflowContext, mutableState historyi.MutableState) (interface{}, error) {
		if !mutableState.IsWorkflowExecutionRunning() {
			// workflow already finished, no need to process the timer
			return nil, nil
		}

		timerSequence := t.getTimerSequence(mutableState)
		updateMutableState := false
		timerSequenceIDs := timerSequence.LoadAndSortActivityTimers()
		if len(timerSequenceIDs) > 0 {
			timerSequenceID := timerSequenceIDs[0]
			_, ok := mutableState.GetActivityInfo(timerSequenceID.EventID)
			if !ok {
				errString := fmt.Sprintf("failed to find in memory activity timer: %v", timerSequenceID.EventID)
				t.logger.Error(errString)
				return nil, serviceerror.NewInternal(errString)
			}

			if queues.IsTimeExpired(
				timerTask,
				referenceTime,
				timerSequenceID.Timestamp,
			) {
				return &struct{}{}, nil
			}
			// Since the activity timers are already sorted, then if there is one timer which is not expired,
			// all activity timers after that timer are not expired.
		}

		// for reason to update mutable state & generate a new activity task,
		// see comments at the beginning of this function.
		// NOTE: this is the only place in the standby logic where mutable state can be updated

		// need to clear the activity heartbeat timer task marks
		// NOTE: LastHeartbeatTimeoutVisibilityInSeconds is for deduping heartbeat timer creation as it's possible
		// one heartbeat task was persisted multiple times with different taskIDs due to the retry logic
		// for updating workflow execution. In that case, only one new heartbeat timeout task should be
		// created.
		isHeartBeatTask := timerTask.TimeoutType == enumspb.TIMEOUT_TYPE_HEARTBEAT
		ai, heartbeatTimeoutVis, ok := mutableState.GetActivityInfoWithTimerHeartbeat(timerTask.EventID)
		if isHeartBeatTask && ok && queues.IsTimeExpired(timerTask, timerTask.GetVisibilityTime(), heartbeatTimeoutVis) {
			if err := mutableState.UpdateActivityTaskStatusWithTimerHeartbeat(ai.ScheduledEventId, ai.TimerTaskStatus&^workflow.TimerTaskStatusCreatedHeartbeat, nil); err != nil {
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

		// TODO: why do we need to update the current version here?
		// Neither UpdateActivity nor CreateNextActivityTimer uses the current version.
		// Current version also not used when closing the transaction as passive
		//
		// we need to handcraft some of the variables
		// since the job being done here is update the activity and possibly write a timer task to DB
		// also need to reset the current version.
		lastWriteVersion, err := mutableState.GetLastWriteVersion()
		if err != nil {
			return nil, err
		}
		if err := mutableState.UpdateCurrentVersion(lastWriteVersion, true); err != nil {
			return nil, err
		}

		err = wfContext.UpdateWorkflowExecutionAsPassive(ctx, t.shardContext)
		return nil, err
	}

	return t.processTimer(
		ctx,
		timerTask,
		actionFn,
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.checkWorkflowStillExistOnSourceBeforeDiscard,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeActivityRetryTimerTask(
	ctx context.Context,
	task *tasks.ActivityRetryTimerTask,
) (retError error) {
	actionFn := func(_ context.Context, wfContext historyi.WorkflowContext, mutableState historyi.MutableState) (interface{}, error) {
		if !mutableState.IsWorkflowExecutionRunning() {
			// workflow already finished, no need to process the timer
			return nil, nil
		}

		activityInfo, ok := mutableState.GetActivityInfo(task.EventID) // activity schedule ID
		if !ok {
			return nil, nil
		}

		err := CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), activityInfo.Version, task.Version, task)
		if err != nil {
			return nil, err
		}

		// we ignore retry timer task if:
		// * this retry task is from old Stamp.
		// * attempts is not the same as recorded in activity info.
		// * activity is already started.
		if activityInfo.Attempt > task.Attempt ||
			activityInfo.Stamp != task.Stamp ||
			activityInfo.StartedEventId != common.EmptyEventID ||
			activityInfo.Paused {
			return nil, nil
		}

		return newActivityRetryTimePostActionInfo(mutableState, activityInfo.TaskQueue, activityInfo.ScheduleToStartTimeout.AsDuration(), activityInfo)
	}

	return t.processTimer(
		ctx,
		task,
		actionFn,
		getStandbyPostActionFn(
			task,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(task.GetType()),
			t.pushActivity,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeWorkflowTaskTimeoutTask(
	ctx context.Context,
	timerTask *tasks.WorkflowTaskTimeoutTask,
) error {
	// workflow task schedule to start timer task is a special snowflake.
	// the schedule to start timer is for sticky workflow task, which is
	// not applicable on the passive cluster
	if timerTask.TimeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START {
		return nil
	}

	actionFn := func(_ context.Context, wfContext historyi.WorkflowContext, mutableState historyi.MutableState) (interface{}, error) {
		if !mutableState.IsWorkflowExecutionRunning() {
			// workflow already finished, no need to process the timer
			return nil, nil
		}

		workflowTask := mutableState.GetWorkflowTaskByID(timerTask.EventID)
		if workflowTask == nil {
			return nil, nil
		}
		if timerTask.Stamp != workflowTask.Stamp {
			return nil, consts.ErrStaleReference
		}

		err := CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), workflowTask.Version, timerTask.Version, timerTask)
		if err != nil {
			return nil, err
		}

		if workflowTask.Attempt != timerTask.ScheduleAttempt {
			return nil, nil
		}

		// We could check if workflow task is started state (since the timeout type here is START_TO_CLOSE)
		// but that's unnecessary.
		//
		// Ifthe  workflow task is in scheduled state, it must have a higher attempt
		// count and will be captured by the attempt check above.

		return &struct{}{}, nil
	}

	return t.processTimer(
		ctx,
		timerTask,
		actionFn,
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.checkWorkflowStillExistOnSourceBeforeDiscard,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeWorkflowBackoffTimerTask(
	ctx context.Context,
	timerTask *tasks.WorkflowBackoffTimerTask,
) error {
	actionFn := func(_ context.Context, wfContext historyi.WorkflowContext, mutableState historyi.MutableState) (interface{}, error) {
		if !mutableState.IsWorkflowExecutionRunning() {
			// workflow already finished, no need to process the timer
			return nil, nil
		}

		if mutableState.HadOrHasWorkflowTask() {
			// if there is one workflow task already been processed
			// or has pending workflow task, meaning workflow has already running
			return nil, nil
		}

		// Note: do not need to verify task version here
		// logic can only go here if mutable state build's next event ID is 2
		// meaning history only contains workflow started event.
		// we can do the checking of task version vs workflow started version
		// however, workflow started version is immutable

		// active cluster will add first workflow task after backoff timeout.
		// standby cluster should just call ack manager to retry this task
		// since we are stilling waiting for the first WorkflowTaskScheduledEvent to be replicated from active side.

		return &struct{}{}, nil
	}

	return t.processTimer(
		ctx,
		timerTask,
		actionFn,
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.checkWorkflowStillExistOnSourceBeforeDiscard,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeWorkflowRunTimeoutTask(
	ctx context.Context,
	timerTask *tasks.WorkflowRunTimeoutTask,
) error {

	actionFn := func(_ context.Context, wfContext historyi.WorkflowContext, mutableState historyi.MutableState) (interface{}, error) {
		if !t.isValidWorkflowRunTimeoutTask(mutableState, timerTask) {
			return nil, nil
		}

		startVersion, err := mutableState.GetStartVersion()
		if err != nil {
			return nil, err
		}
		err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), startVersion, timerTask.Version, timerTask)
		if err != nil {
			return nil, err
		}

		return &struct{}{}, nil
	}

	return t.processTimer(
		ctx,
		timerTask,
		actionFn,
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.checkWorkflowStillExistOnSourceBeforeDiscard,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeWorkflowExecutionTimeoutTask(
	ctx context.Context,
	timerTask *tasks.WorkflowExecutionTimeoutTask,
) error {
	actionFn := func(
		_ context.Context,
		wfContext historyi.WorkflowContext,
		mutableState historyi.MutableState,
	) (interface{}, error) {
		if !t.isValidWorkflowExecutionTimeoutTask(mutableState, timerTask) {
			return nil, nil
		}

		// If the task is valid, it means the workflow should be timed out but it is still running.
		// The standby logic should continue to wait for the workflow timeout event to be replicated from the active side.
		//
		// Return non-nil post action info to indicate that verification is not done yet.
		// The returned post action info can be used to resend history fron active side.

		return newExecutionTimerPostActionInfo(mutableState)
	}

	return t.processTimer(
		ctx,
		timerTask,
		actionFn,
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.checkWorkflowStillExistOnSourceBeforeDiscard,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeStateMachineTimerTask(
	ctx context.Context,
	timerTask *tasks.StateMachineTimerTask,
) error {
	actionFn := func(
		ctx context.Context,
		wfContext historyi.WorkflowContext,
		mutableState historyi.MutableState,
	) (any, error) {
		processedTimers, err := t.executeStateMachineTimers(
			ctx,
			wfContext,
			mutableState,
			timerTask,
			func(node *hsm.Node, task hsm.Task) error {
				// If this line of code is reached, the task's Validate() function returned no error, which indicates
				// that it is still expected to run. Return ErrTaskRetry to wait the machine to transition on the active
				// cluster.
				return consts.ErrTaskRetry
			},
		)
		if err != nil {
			if errors.Is(err, consts.ErrTaskRetry) {
				// This handles the ErrTaskRetry error returned by executeStateMachineTimers.
				return &struct{}{}, nil
			}
			return nil, err
		}

		// We haven't done any work, return without committing.
		if processedTimers == 0 {
			return nil, nil
		}

		if t.config.EnableUpdateWorkflowModeIgnoreCurrent() {
			return nil, wfContext.UpdateWorkflowExecutionAsPassive(ctx, t.shardContext)
		}

		// TODO: remove following code once EnableUpdateWorkflowModeIgnoreCurrent config is deprecated.
		if mutableState.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
			// Can't use UpdateWorkflowExecutionAsPassive since it updates the current run,
			// and we are operating on a closed workflow.
			return nil, wfContext.SubmitClosedWorkflowSnapshot(
				ctx,
				t.shardContext,
				historyi.TransactionPolicyPassive,
			)
		}
		return nil, wfContext.UpdateWorkflowExecutionAsPassive(ctx, t.shardContext)
	}

	return t.processTimer(
		ctx,
		timerTask,
		actionFn,
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.checkWorkflowStillExistOnSourceBeforeDiscard,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) getTimerSequence(
	mutableState historyi.MutableState,
) workflow.TimerSequence {
	return workflow.NewTimerSequence(mutableState)
}

func (t *timerQueueStandbyTaskExecutor) processTimer(
	ctx context.Context,
	timerTask tasks.Task,
	actionFn standbyActionFn,
	postActionFn standbyPostActionFn,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	nsRecord, err := t.shardContext.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(timerTask.GetNamespaceID()))
	if err != nil {
		return err
	}
	if !nsRecord.IsOnCluster(t.clusterName) {
		// namespace is not replicated to local cluster, ignore corresponding tasks
		return nil
	}

	executionContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, timerTask)
	if err != nil {
		return err
	}
	defer func() {
		if errors.Is(retError, consts.ErrTaskRetry) {
			release(nil)
		} else {
			release(retError)
		}
	}()

	mutableState, err := loadMutableStateForTimerTask(ctx, t.shardContext, executionContext, timerTask, t.metricsHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil {
		return nil
	}

	historyResendInfo, err := actionFn(ctx, executionContext, mutableState)
	if err != nil {
		return err
	}

	// NOTE: do not access anything related mutable state after this lock release
	release(nil)
	return postActionFn(ctx, timerTask, historyResendInfo, t.logger)
}

func (t *timerQueueStandbyTaskExecutor) pushActivity(
	ctx context.Context,
	task tasks.Task,
	postActionInfo interface{},
	logger log.Logger,
) error {
	if postActionInfo == nil {
		return nil
	}

	pushActivityInfo := postActionInfo.(*activityTaskPostActionInfo)
	activityScheduleToStartTimeout := pushActivityInfo.activityTaskScheduleToStartTimeout
	activityTask := task.(*tasks.ActivityRetryTimerTask)

	resp, err := t.matchingRawClient.AddActivityTask(ctx, &matchingservice.AddActivityTaskRequest{
		NamespaceId: activityTask.NamespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: activityTask.WorkflowID,
			RunId:      activityTask.RunID,
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: pushActivityInfo.taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		ScheduledEventId:       activityTask.EventID,
		ScheduleToStartTimeout: durationpb.New(activityScheduleToStartTimeout),
		Clock:                  vclock.NewVectorClock(t.shardContext.GetClusterMetadata().GetClusterID(), t.shardContext.GetShardID(), activityTask.TaskID),
		VersionDirective:       pushActivityInfo.versionDirective,
		Stamp:                  activityTask.Stamp,
	})

	if err != nil {
		return err
	}

	if pushActivityInfo.versionDirective.GetUseAssignmentRules() == nil {
		// activity is not getting a new build ID, so no need to update MS
		return nil
	}

	return updateIndependentActivityBuildId(
		ctx,
		activityTask,
		resp.AssignedBuildId,
		t.shardContext,
		historyi.TransactionPolicyPassive,
		t.cache,
		t.metricsHandler,
		t.logger,
	)
}

// TODO: deprecate this function and always use t.Now()
// Only test code sets t.clusterName to be non-current cluster name
// and advance the time by setting calling shardContext.SetCurrentTime.
func (t *timerQueueStandbyTaskExecutor) getCurrentTime() time.Time {
	return t.shardContext.GetCurrentTime(t.clusterName)
}

func (t *timerQueueStandbyTaskExecutor) checkWorkflowStillExistOnSourceBeforeDiscard(
	ctx context.Context,
	taskInfo tasks.Task,
	postActionInfo interface{},
	logger log.Logger,
) error {
	if postActionInfo == nil {
		return nil
	}
	if !isWorkflowExistOnSource(ctx, taskWorkflowKey(taskInfo), logger, t.clusterName, t.clientBean, t.shardContext.GetNamespaceRegistry()) {
		return standbyTimerTaskPostActionTaskDiscarded(ctx, taskInfo, nil, logger)
	}
	return standbyTimerTaskPostActionTaskDiscarded(ctx, taskInfo, postActionInfo, logger)
}
