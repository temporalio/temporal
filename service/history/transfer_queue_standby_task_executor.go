package history

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const (
	recordChildCompletionVerificationFailedMsg = "Failed to verify child execution completion recorded"
	verifyFirstWorkflowTaskScheduledFailedMsg  = "Failed to verify first workflow task scheduled"
)

type (
	transferQueueStandbyTaskExecutor struct {
		*transferQueueTaskExecutorBase

		clusterName string
		clientBean  client.Bean
	}

	verificationErr struct {
		msg string
		err error
	}
)

func newTransferQueueStandbyTaskExecutor(
	shard historyi.ShardContext,
	workflowCache wcache.Cache,
	logger log.Logger,
	metricProvider metrics.Handler,
	clusterName string,
	historyRawClient resource.HistoryRawClient,
	matchingRawClient resource.MatchingRawClient,
	visibilityManager manager.VisibilityManager,
	chasmEngine chasm.Engine,
	clientBean client.Bean,
) queues.Executor {
	return &transferQueueStandbyTaskExecutor{
		transferQueueTaskExecutorBase: newTransferQueueTaskExecutorBase(
			shard,
			workflowCache,
			logger,
			metricProvider,
			historyRawClient,
			matchingRawClient,
			visibilityManager,
			chasmEngine,
		),
		clusterName: clusterName,
		clientBean:  clientBean,
	}
}

func (t *transferQueueStandbyTaskExecutor) Execute(
	ctx context.Context,
	executable queues.Executable,
) queues.ExecuteResponse {
	task := executable.GetTask()
	taskType := queues.GetStandbyTransferTaskTypeTagValue(task, t.shardContext.ChasmRegistry())
	metricsTags := []metrics.Tag{
		getNamespaceTagByID(t.shardContext.GetNamespaceRegistry(), task.GetNamespaceID()),
		metrics.TaskTypeTag(taskType),
		metrics.OperationTag(taskType), // for backward compatibility
	}

	var (
		err          error
		eventDetails map[string]any
	)
	switch task := task.(type) {
	case *tasks.ActivityTask:
		err = t.processActivityTask(ctx, task)
	case *tasks.WorkflowTask:
		err = t.processWorkflowTask(ctx, task)
	case *tasks.CancelExecutionTask:
		err = t.processCancelExecution(ctx, task, executable)
	case *tasks.SignalExecutionTask:
		err = t.processSignalExecution(ctx, task, executable)
	case *tasks.StartChildExecutionTask:
		eventDetails = make(map[string]any)
		err = t.processStartChildExecution(ctx, task, eventDetails)
	case *tasks.ResetWorkflowTask:
		// no reset needed for standby
		// TODO: add error logs
		err = nil
	case *tasks.CloseExecutionTask:
		eventDetails = make(map[string]any)
		err = t.processCloseExecution(ctx, task, executable.Attempt(), eventDetails)
	case *tasks.DeleteExecutionTask:
		err = t.processDeleteExecutionTask(ctx, task, false)
	case *tasks.ReleaseLimiterTask:
		// TODO(fc): add standby release validation in later PR
		err = nil
	case *tasks.ChasmTask:
		task.Attempt = executable.Attempt()
		err = t.executeChasmSideEffectTransferTask(ctx, task)
	default:
		err = errUnknownTransferTask
	}
	emitStandbyTaskError(t.shardContext, executable, taskType, eventDetails, err)

	return queues.ExecuteResponse{
		ExecutionMetricTags: metricsTags,
		ExecutedAsActive:    false,
		ExecutionErr:        err,
	}
}

func (t *transferQueueStandbyTaskExecutor) executeChasmSideEffectTransferTask(
	ctx context.Context,
	task *tasks.ChasmTask,
) error {

	chasmTaskType, _ := t.shardContext.ChasmRegistry().TaskFqnByID(task.Info.GetTypeId())
	return t.processTransfer(
		ctx,
		true,
		task,
		standbyTaskVerifier(t.verifyChasmSideEffectTransferTask),
		getStandbyPostActionFn(
			task,
			t.getCurrentTime,
			t.config.ChasmStandbyTaskDiscardDelay(chasmTaskType),
			t.discardChasmTask,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) discardChasmTask(
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

func (t *transferQueueStandbyTaskExecutor) processActivityTask(
	ctx context.Context,
	transferTask *tasks.ActivityTask,
) error {
	processTaskIfClosed := false

	return t.processTransfer(
		ctx,
		processTaskIfClosed,
		transferTask,
		standbyTaskVerifier(t.verifyActivityTask),
		getStandbyPostActionFn(
			transferTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(transferTask.GetType()),
			t.pushActivity,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processWorkflowTask(
	ctx context.Context,
	transferTask *tasks.WorkflowTask,
) error {

	return t.processTransfer(
		ctx,
		false,
		transferTask,
		standbyTaskVerifier(t.verifyWorkflowTask),
		getStandbyPostActionFn(
			transferTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(transferTask.GetType()),
			t.pushWorkflowTask,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processCloseExecution(
	ctx context.Context,
	transferTask *tasks.CloseExecutionTask,
	attempt int,
	eventDetails map[string]any,
) error {
	postAction := getStandbyPostActionFn(
		transferTask, t.getCurrentTime, t.config.StandbyTaskMissingEventsDiscardDelay(transferTask.GetType()),
		func(ctx context.Context, task tasks.Task, info any, logger log.Logger) error {
			return t.checkParentWorkflowStillExistOnSourceBeforeDiscard(ctx, task, info, logger, eventDetails)
		},
	)
	return t.processTransfer(ctx, true, transferTask, standbyTaskVerifier(t.verifyCloseExecution),
		func(ctx context.Context, task tasks.Task, info any, logger log.Logger) error {
			if info == nil {
				return nil
			}
			verification, ok := info.(*closeExecutionVerificationInfo)
			if !ok {
				return serviceerror.NewInternal("unexpected close execution verification info")
			}
			result, err := t.checkChildCompletionRecorded(ctx, transferTask, verification, attempt)
			if err != nil {
				return err
			}
			return postAction(ctx, task, result, logger)
		},
	)
}

func (t *transferQueueStandbyTaskExecutor) processCancelExecution(
	ctx context.Context,
	transferTask *tasks.CancelExecutionTask,
	executable queues.Executable,
) error {
	processTaskIfClosed := false

	return t.processTransfer(
		ctx,
		processTaskIfClosed,
		transferTask,
		standbyTaskVerifier(t.verifyCancelExecution),
		getStandbyPostActionFn(
			transferTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(transferTask.GetType()),
			t.verifyPendingExternalTaskOnSource(executable),
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processSignalExecution(
	ctx context.Context,
	transferTask *tasks.SignalExecutionTask,
	executable queues.Executable,
) error {
	processTaskIfClosed := false

	return t.processTransfer(
		ctx,
		processTaskIfClosed,
		transferTask,
		standbyTaskVerifier(t.verifySignalExecution),
		getStandbyPostActionFn(
			transferTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(transferTask.GetType()),
			t.verifyPendingExternalTaskOnSource(executable),
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processStartChildExecution(
	ctx context.Context,
	transferTask *tasks.StartChildExecutionTask,
	eventDetails map[string]any,
) error {
	postAction := getStandbyPostActionFn(
		transferTask, t.getCurrentTime, t.config.StandbyTaskMissingEventsDiscardDelay(transferTask.GetType()),
		func(ctx context.Context, task tasks.Task, info any, logger log.Logger) error {
			return t.checkStartChildExecutionStillExistsOnSourceBeforeDiscard(ctx, task, info, logger, eventDetails)
		},
	)
	return t.processTransfer(ctx, true, transferTask, standbyTaskVerifier(t.verifyStartChildExecution),
		func(ctx context.Context, task tasks.Task, info any, logger log.Logger) error {
			if verification, ok := info.(*startChildExecutionVerificationInfo); ok {
				var err error
				info, err = t.checkChildStarted(ctx, transferTask, verification)
				if err != nil {
					return err
				}
			}
			return postAction(ctx, task, info, logger)
		},
	)
}

func (t *transferQueueStandbyTaskExecutor) processTransfer(
	ctx context.Context,
	processTaskIfClosed bool,
	taskInfo tasks.Task,
	verify standbyVerificationFn,
	postActionFn standbyPostActionFn,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	nsRecord, err := t.shardContext.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(taskInfo.GetNamespaceID()))
	if err != nil {
		return err
	}
	if !nsRecord.IsOnCluster(t.clusterName) {
		// namespace is not replicated to local cluster, ignore corresponding tasks
		return nil
	}

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, taskInfo)
	if err != nil {
		return err
	}
	defer func() {
		var verificationErr *verificationErr
		switch {
		case retError == consts.ErrTaskRetry,
			errors.Is(retError, consts.ErrStaleReference),
			errors.As(retError, &verificationErr):
			release(nil)
		default:
			release(retError)
		}
	}()

	mutableState, err := loadMutableStateForTransferTask(ctx, t.shardContext, weContext, taskInfo, t.metricHandler, t.logger)
	if err != nil || mutableState == nil {
		return err
	}

	if !processTaskIfClosed && !mutableState.IsWorkflowExecutionRunning() {
		// workflow already finished, no need to process transfer task.
		return nil
	}

	postActionInfo, err := verify(ctx, taskInfo, mutableState)
	if err != nil {
		return err
	}

	// NOTE: do not access anything related mutable state after this lock release
	release(nil)
	return postActionFn(ctx, taskInfo, postActionInfo, t.logger)
}

func (t *transferQueueStandbyTaskExecutor) pushActivity(
	ctx context.Context,
	task tasks.Task,
	postActionInfo any,
	logger log.Logger,
) error {
	if postActionInfo == nil {
		return nil
	}

	activityTask, ok := task.(*tasks.ActivityTask)
	if !ok {
		return serviceerror.NewInternal("task is not an ActivityTask")
	}
	pushActivityInfo := postActionInfo.(*activityTaskPostActionInfo)
	return t.transferQueueTaskExecutorBase.pushActivity(
		ctx,
		activityTask,
		pushActivityInfo.activityTaskScheduleToStartTimeout,
		pushActivityInfo.versionDirective,
		pushActivityInfo.priority,
		historyi.TransactionPolicyPassive,
	)
}

func (t *transferQueueStandbyTaskExecutor) pushWorkflowTask(
	ctx context.Context,
	task tasks.Task,
	postActionInfo any,
	logger log.Logger,
) error {
	if postActionInfo == nil {
		return nil
	}

	pushwtInfo := postActionInfo.(*workflowTaskPostActionInfo)
	return t.transferQueueTaskExecutorBase.pushWorkflowTask(
		ctx,
		task.(*tasks.WorkflowTask),
		pushwtInfo.taskqueue,
		pushwtInfo.workflowTaskScheduleToStartTimeout,
		pushwtInfo.versionDirective,
		pushwtInfo.priority,
		historyi.TransactionPolicyPassive,
	)
}

// TODO: deprecate this function and always use t.Now()
// Only test code sets t.clusterName to be non-current cluster name
// and advance the time by setting calling shardContext.SetCurrentTime.
func (t *transferQueueStandbyTaskExecutor) getCurrentTime() time.Time {
	return t.shardContext.GetCurrentTime(t.clusterName)
}

func (e *verificationErr) Error() string {
	return fmt.Sprintf("%v: %v", e.msg, e.err.Error())
}

func (e *verificationErr) Unwrap() error {
	return e.err
}

func (t *transferQueueStandbyTaskExecutor) checkExecutionStillExistsOnSourceBeforeDiscard(
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
		return standbyTransferTaskPostActionTaskDiscarded(ctx, taskInfo, nil, logger, nil)
	}
	return standbyTransferTaskPostActionTaskDiscarded(ctx, taskInfo, postActionInfo, logger, nil)
}

func (t *transferQueueStandbyTaskExecutor) checkStartChildExecutionStillExistsOnSourceBeforeDiscard(
	ctx context.Context,
	taskInfo tasks.Task,
	postActionInfo any,
	logger log.Logger,
	eventDetails map[string]any,
) error {
	if postActionInfo == nil {
		return nil
	}
	startChildInfo, ok := postActionInfo.(*startChildExecutionPostActionInfo)
	if !ok || startChildInfo.childWorkflowKey == nil {
		return t.checkExecutionStillExistsOnSourceBeforeDiscard(ctx, taskInfo, postActionInfo, logger)
	}

	exists, err := executionExistsOnSource(
		ctx,
		taskInfo,
		*startChildInfo.childWorkflowKey,
		chasm.WorkflowArchetypeID,
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
		return standbyTransferTaskPostActionTaskDiscarded(ctx, taskInfo, nil, logger, nil)
	}
	return standbyTransferTaskPostActionTaskDiscarded(ctx, taskInfo, postActionInfo, logger, eventDetails)
}

func (t *transferQueueStandbyTaskExecutor) checkParentWorkflowStillExistOnSourceBeforeDiscard(
	ctx context.Context,
	taskInfo tasks.Task,
	postActionInfo any,
	logger log.Logger,
	eventDetails map[string]any,
) error {
	if postActionInfo == nil {
		return nil
	}
	verifyCompletionInfo, ok := postActionInfo.(*verifyCompletionRecordedPostActionInfo)
	if !ok || verifyCompletionInfo.parentWorkflowKey == nil {
		return standbyTransferTaskPostActionTaskDiscarded(ctx, taskInfo, postActionInfo, logger, eventDetails)
	}

	exists, err := executionExistsOnSource(
		ctx,
		taskInfo,
		*verifyCompletionInfo.parentWorkflowKey,
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
		return standbyTransferTaskPostActionTaskDiscarded(ctx, taskInfo, nil, logger, nil)
	}
	return standbyTransferTaskPostActionTaskDiscarded(ctx, taskInfo, postActionInfo, logger, eventDetails)
}
