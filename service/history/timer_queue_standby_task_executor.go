package history

import (
	"context"
	"errors"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/deletemanager"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
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
	taskTypeTagValue := queues.GetStandbyTimerTaskTypeTagValue(task, t.shardContext.ChasmRegistry())

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
		task.Attempt = executable.Attempt()
		err = t.executeChasmSideEffectTimerTask(ctx, task)
	case *tasks.TimeSkippingFastForwardTimerTask:
		err = t.executeTimeSkippingTimerTask(ctx, task)
	default:
		err = queueserrors.NewUnprocessableTaskError("unknown task type")
	}
	emitStandbyTaskError(t.shardContext, executable, taskTypeTagValue, nil, err)

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

	return t.processTimer(
		ctx,
		task,
		standbyTaskVerifier(t.verifyChasmPureTimerTask),
		getStandbyPostActionFn(
			task,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(task.GetType()),
			t.checkExecutionStillExistsOnSourceBeforeDiscard,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeChasmSideEffectTimerTask(
	ctx context.Context,
	task *tasks.ChasmTask,
) error {

	chasmTaskType, _ := t.shardContext.ChasmRegistry().TaskFqnByID(task.Info.GetTypeId())
	return t.processTimer(
		ctx,
		task,
		standbyTaskVerifier(t.verifyChasmSideEffectTimerTask),
		getStandbyPostActionFn(
			task,
			t.getCurrentTime,
			t.config.ChasmStandbyTaskDiscardDelay(chasmTaskType),
			t.discardChasmTask,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) discardChasmTask(
	ctx context.Context,
	taskInfo tasks.Task,
	postActionInfo any,
	logger log.Logger,
) error {
	if postActionInfo == nil {
		return nil
	}
	chasmTree, ok := postActionInfo.(historyi.ChasmTree)
	if !ok {
		return serviceerror.NewInternal("postActionInfo is not a ChasmTree")
	}
	chasmTask, ok := taskInfo.(*tasks.ChasmTask)
	if !ok {
		return serviceerror.NewInternal("taskInfo is not a ChasmTask")
	}

	return discardChasmSideEffectTask(
		ctx,
		t.chasmEngine,
		t.shardContext.ChasmRegistry(),
		chasmTree,
		chasmTask,
		logger,
		t.clusterName,
		t.clientBean,
		t.shardContext.GetNamespaceRegistry(),
	)
}

// executeTimeSkippingTimerTask waits on the standby until the active cluster
// replicates the fast-forward transition. If the fast-forward this task was
// generated for is still pending (same source event and not yet reached), the
// task is retried until the discard delay elapses; otherwise it is acked.
func (t *timerQueueStandbyTaskExecutor) executeTimeSkippingTimerTask(
	ctx context.Context,
	timerTask *tasks.TimeSkippingFastForwardTimerTask,
) error {

	return t.processTimer(
		ctx,
		timerTask,
		standbyTaskVerifier(t.verifyTimeSkippingTimerTask),
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.checkExecutionStillExistsOnSourceBeforeDiscard,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeUserTimerTimeoutTask(
	ctx context.Context,
	timerTask *tasks.UserTimerTask,
) error {

	return t.processTimer(
		ctx,
		timerTask,
		standbyTaskVerifier(t.verifyUserTimerTimeoutTask),
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.checkExecutionStillExistsOnSourceBeforeDiscard,
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

	return t.processTimer(
		ctx,
		timerTask,
		standbyTaskVerifier(t.verifyActivityTimeoutTask),
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.checkExecutionStillExistsOnSourceBeforeDiscard,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeActivityRetryTimerTask(
	ctx context.Context,
	task *tasks.ActivityRetryTimerTask,
) (retError error) {

	return t.processTimer(
		ctx,
		task,
		standbyTaskVerifier(t.verifyActivityRetryTimerTask),
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

	return t.processTimer(
		ctx,
		timerTask,
		standbyTaskVerifier(t.verifyWorkflowTaskTimeoutTask),
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.checkExecutionStillExistsOnSourceBeforeDiscard,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeWorkflowBackoffTimerTask(
	ctx context.Context,
	timerTask *tasks.WorkflowBackoffTimerTask,
) error {

	return t.processTimer(
		ctx,
		timerTask,
		standbyTaskVerifier(t.verifyWorkflowBackoffTimerTask),
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.checkExecutionStillExistsOnSourceBeforeDiscard,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeWorkflowRunTimeoutTask(
	ctx context.Context,
	timerTask *tasks.WorkflowRunTimeoutTask,
) error {

	return t.processTimer(
		ctx,
		timerTask,
		standbyTaskVerifier(t.verifyWorkflowRunTimeoutTask),
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.checkExecutionStillExistsOnSourceBeforeDiscard,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeWorkflowExecutionTimeoutTask(
	ctx context.Context,
	timerTask *tasks.WorkflowExecutionTimeoutTask,
) error {

	return t.processTimer(
		ctx,
		timerTask,
		standbyTaskVerifier(t.verifyWorkflowExecutionTimeoutTask),
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.checkExecutionStillExistsOnSourceBeforeDiscard,
		),
	)
}

func (t *timerQueueStandbyTaskExecutor) executeStateMachineTimerTask(
	ctx context.Context,
	timerTask *tasks.StateMachineTimerTask,
) error {

	return t.processTimer(
		ctx,
		timerTask,
		standbyTaskVerifier(t.verifyStateMachineTimerTask),
		getStandbyPostActionFn(
			timerTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(timerTask.GetType()),
			t.checkExecutionStillExistsOnSourceBeforeDiscard,
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
	verify standbyVerificationFn,
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

	historyResendInfo, err := verify(ctx, timerTask, mutableState)
	if err != nil {
		return err
	}

	historyResendInfo, err = t.applyTimerMaintenance(ctx, executionContext, mutableState, historyResendInfo)
	if err != nil {
		return err
	}

	// Do not access mutable state after releasing the workflow lock.
	release(nil)
	return postActionFn(ctx, timerTask, historyResendInfo, t.logger)
}

func (t *timerQueueStandbyTaskExecutor) pushActivity(
	ctx context.Context,
	task tasks.Task,
	postActionInfo any,
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

// getCurrentTime returns the shard's wall-clock view of "now" for t.clusterName.
// Must stay wall-clock: it gates standby task-retry timing against VisibilityTime
// (also wall-clock); mutableState.Now() is virtual time and would force-discard
// time-skipping workflows. Timer actionFn closures also compare against
// wall-clock timestamps after converting their virtual deadlines.
//
// TODO: deprecate this function and always use t.Now()
// Only test code sets t.clusterName to be non-current cluster name
// and advance the time by setting calling shardContext.SetCurrentTime.
func (t *timerQueueStandbyTaskExecutor) getCurrentTime() time.Time {
	return t.shardContext.GetCurrentTime(t.clusterName)
}

func (t *timerQueueStandbyTaskExecutor) checkExecutionStillExistsOnSourceBeforeDiscard(
	ctx context.Context,
	taskInfo tasks.Task,
	postActionInfo any,
	logger log.Logger,
) error {
	if postActionInfo == nil {
		return nil
	}
	exists, err := executionExistsOnSource(
		ctx,
		taskInfo,
		taskWorkflowKey(taskInfo),
		getTaskArchetypeID(taskInfo),
		logger,
		t.clusterName,
		t.clientBean,
		t.shardContext.GetNamespaceRegistry(),
		t.shardContext.ChasmRegistry(),
	)
	if err != nil {
		return err
	}
	if !exists {
		return standbyTimerTaskPostActionTaskDiscarded(ctx, taskInfo, nil, logger)
	}
	return standbyTimerTaskPostActionTaskDiscarded(ctx, taskInfo, postActionInfo, logger)
}
