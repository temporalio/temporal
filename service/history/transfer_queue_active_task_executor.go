package history

import (
	"context"
	"errors"
	"fmt"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/priorities"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/sdk"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/vclock"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/worker/parentclosepolicy"
)

type (
	transferQueueActiveTaskExecutor struct {
		*transferQueueTaskExecutorBase

		workflowResetter        ndc.WorkflowResetter
		parentClosePolicyClient parentclosepolicy.Client
	}
)

func newTransferQueueActiveTaskExecutor(
	shard historyi.ShardContext,
	workflowCache wcache.Cache,
	sdkClientFactory sdk.ClientFactory,
	logger log.Logger,
	metricProvider metrics.Handler,
	config *configs.Config,
	historyRawClient resource.HistoryRawClient,
	matchingRawClient resource.MatchingRawClient,
	visibilityManager manager.VisibilityManager,
	chasmEngine chasm.Engine,
) queues.Executor {
	return &transferQueueActiveTaskExecutor{
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
		workflowResetter: ndc.NewWorkflowResetter(
			shard,
			workflowCache,
			logger,
		),
		parentClosePolicyClient: parentclosepolicy.NewClient(
			shard.GetMetricsHandler(),
			shard.GetLogger(),
			sdkClientFactory,
			config.NumParentClosePolicySystemWorkflows(),
		),
	}
}

func (t *transferQueueActiveTaskExecutor) Execute(
	ctx context.Context,
	executable queues.Executable,
) queues.ExecuteResponse {
	task := executable.GetTask()
	taskType := queues.GetActiveTransferTaskTypeTagValue(task)
	namespaceTag, replicationState := getNamespaceTagAndReplicationStateByID(
		t.shardContext.GetNamespaceRegistry(),
		task.GetNamespaceID(),
	)
	metricsTags := []metrics.Tag{
		namespaceTag,
		metrics.TaskTypeTag(taskType),
		metrics.OperationTag(taskType), // for backward compatibility
	}

	if replicationState == enumspb.REPLICATION_STATE_HANDOVER {
		// TODO: exclude task types here if we believe it's safe & necessary to execute
		// them during namespace handover.
		// TODO: move this logic to queues.Executable when metrics tag doesn't need to
		// be returned from task executor
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutedAsActive:    true,
			ExecutionErr:        consts.ErrNamespaceHandover,
		}
	}

	var err error
	switch task := task.(type) {
	case *tasks.ActivityTask:
		err = t.processActivityTask(ctx, task)
	case *tasks.WorkflowTask:
		err = t.processWorkflowTask(ctx, task)
	case *tasks.CloseExecutionTask:
		err = t.processCloseExecution(ctx, task)
	case *tasks.CancelExecutionTask:
		err = t.processCancelExecution(ctx, task)
	case *tasks.SignalExecutionTask:
		err = t.processSignalExecution(ctx, task)
	case *tasks.StartChildExecutionTask:
		err = t.processStartChildExecution(ctx, task)
	case *tasks.ResetWorkflowTask:
		err = t.processResetWorkflow(ctx, task)
	case *tasks.DeleteExecutionTask:
		err = t.processDeleteExecutionTask(ctx, task)
	case *tasks.ChasmTask:
		err = t.executeChasmSideEffectTransferTask(ctx, task)
	default:
		err = errUnknownTransferTask
	}

	return queues.ExecuteResponse{
		ExecutionMetricTags: metricsTags,
		ExecutedAsActive:    true,
		ExecutionErr:        err,
	}
}

func (t *transferQueueActiveTaskExecutor) executeChasmSideEffectTransferTask(
	ctx context.Context,
	task *tasks.ChasmTask,
) error {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(err) }()

	ms, err := loadMutableStateForTransferTask(ctx, t.shardContext, weContext, task, t.metricHandler, t.logger)
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

func (t *transferQueueActiveTaskExecutor) processDeleteExecutionTask(ctx context.Context,
	task *tasks.DeleteExecutionTask) error {
	return t.transferQueueTaskExecutorBase.processDeleteExecutionTask(ctx, task,
		t.config.TransferProcessorEnsureCloseBeforeDelete())
}

func (t *transferQueueActiveTaskExecutor) processActivityTask(
	ctx context.Context,
	task *tasks.ActivityTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, t.shardContext, weContext, task, t.metricHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil {
		release(nil) // release(nil) so that the mutable state is not unloaded from cache
		return consts.ErrWorkflowExecutionNotFound
	}

	ai, ok := mutableState.GetActivityInfo(task.ScheduledEventID)
	if !ok {
		release(nil) // release(nil) so that the mutable state is not unloaded from cache
		return consts.ErrActivityTaskNotFound
	}

	if ai.Stamp != task.Stamp || ai.Paused {
		release(nil)                    // release(nil) so that the mutable state is not unloaded from cache
		return consts.ErrStaleReference // drop the task
	}

	err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), ai.Version, task.Version, task)
	if err != nil {
		return err
	}

	if !mutableState.IsWorkflowExecutionRunning() {
		release(nil) // release(nil) so that the mutable state is not unloaded from cache
		return consts.ErrWorkflowCompleted
	}

	timeout := timestamp.DurationValue(ai.ScheduleToStartTimeout)
	directive := MakeDirectiveForActivityTask(mutableState, ai)
	priority := priorities.Merge(mutableState.GetExecutionInfo().Priority, ai.Priority)

	// NOTE: do not access anything related mutable state after this lock release
	// release the context lock since we no longer need mutable state and
	// the rest of logic is making RPC call, which takes time.
	release(nil)

	return t.pushActivity(ctx, task, timeout, directive, priority, historyi.TransactionPolicyActive)
}

func (t *transferQueueActiveTaskExecutor) processWorkflowTask(
	ctx context.Context,
	transferTask *tasks.WorkflowTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, transferTask)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, t.shardContext, weContext, transferTask, t.metricHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	workflowTask := mutableState.GetWorkflowTaskByID(transferTask.ScheduledEventID)
	if workflowTask == nil {
		return nil
	}
	err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), workflowTask.Version, transferTask.Version, transferTask)
	if err != nil {
		return err
	}

	// Task queue from transfer task (not current one from mutable state) must be used here.
	// If current task queue becomes sticky since this transfer task was created,
	// it can't be used here, because timeout timer was not created for it,
	// because it used to be non-sticky when this transfer task was created .
	taskQueue, scheduleToStartTimeout := mutableState.TaskQueueScheduleToStartTimeout(transferTask.TaskQueue)

	normalTaskQueueName := mutableState.GetExecutionInfo().TaskQueue

	directive := MakeDirectiveForWorkflowTask(mutableState)
	priority := mutableState.GetExecutionInfo().Priority

	// NOTE: Do not access mutableState after this lock is released.
	// It is important to release the workflow lock here, because pushWorkflowTask will call matching,
	// which will call history back (with RecordWorkflowTaskStarted), and it will try to get workflow lock again.
	release(nil)

	err = t.pushWorkflowTask(
		ctx,
		transferTask,
		taskQueue,
		scheduleToStartTimeout.AsDuration(),
		directive,
		priority,
		historyi.TransactionPolicyActive,
	)

	if _, ok := err.(*serviceerrors.StickyWorkerUnavailable); ok {
		// sticky worker is unavailable, switch to original normal task queue
		taskQueue = &taskqueuepb.TaskQueue{
			// do not use task.TaskQueue which is sticky, use original normal task queue from mutable state
			Name: normalTaskQueueName,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}

		// Continue to use sticky schedule_to_start timeout as TTL for the matching task. Because the schedule_to_start
		// timeout timer task is already created which will timeout this task if no worker pick it up in 5s anyway.
		// There is no need to reset sticky, because if this task is picked by new worker, the new worker will reset
		// the sticky queue to a new one. However, if worker is completely down, that schedule_to_start timeout task
		// will re-create a new non-sticky task and reset sticky.
		err = t.pushWorkflowTask(
			ctx,
			transferTask,
			taskQueue,
			scheduleToStartTimeout.AsDuration(),
			directive,
			priority,
			historyi.TransactionPolicyActive,
		)
	}

	return err
}

func (t *transferQueueActiveTaskExecutor) processCloseExecution(
	ctx context.Context,
	task *tasks.CloseExecutionTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, t.shardContext, weContext, task, t.metricHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	// DeleteAfterClose is set to true when this close execution task was generated as part of delete open workflow execution procedure.
	// Delete workflow execution is started by user API call and should be done regardless of current workflow version.
	if !task.DeleteAfterClose {
		closeVersion, err := mutableState.GetCloseVersion()
		if err != nil {
			return err
		}
		err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), closeVersion, task.Version, task)
		if err != nil {
			return err
		}
	}

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowID(),
		RunId:      task.GetRunID(),
	}
	executionInfo := mutableState.GetExecutionInfo()
	children := copyChildWorkflowInfos(mutableState.GetPendingChildExecutionInfos())
	var completionEvent *historypb.HistoryEvent // needed to report close event to parent workflow
	replyToParentWorkflow := mutableState.HasParentExecution() && executionInfo.NewExecutionRunId == ""
	if replyToParentWorkflow || len(children) > 0 {
		// only load close event if needed.
		completionEvent, err = mutableState.GetCompletionEvent(ctx)
		if err != nil {
			return err
		}
		replyToParentWorkflow = replyToParentWorkflow && !ndc.IsTerminatedByResetter(completionEvent)
	}
	parentNamespaceID := executionInfo.ParentNamespaceId
	parentWorkflowID := executionInfo.ParentWorkflowId
	parentRunID := executionInfo.ParentRunId
	parentInitiatedID := executionInfo.ParentInitiatedId
	parentInitiatedVersion := executionInfo.ParentInitiatedVersion
	var parentClock *clockspb.VectorClock
	if executionInfo.ParentClock != nil {
		parentClock = vclock.NewVectorClock(
			executionInfo.ParentClock.ClusterId,
			executionInfo.ParentClock.ShardId,
			executionInfo.ParentClock.Clock,
		)
	}

	namespaceName := mutableState.GetNamespaceEntry().Name()

	firstRunID, err := mutableState.GetFirstRunID(ctx)
	if err != nil {
		return err
	}

	// NOTE: do not access anything related mutable state after this lock release.
	// Release lock immediately since mutable state is not needed
	// and the rest of logic is RPC calls, which can take time.
	release(nil)

	// Communicate the result to parent execution if this is Child Workflow execution
	if replyToParentWorkflow {
		_, err := t.historyRawClient.RecordChildExecutionCompleted(ctx, &historyservice.RecordChildExecutionCompletedRequest{
			NamespaceId: parentNamespaceID,
			ParentExecution: &commonpb.WorkflowExecution{
				WorkflowId: parentWorkflowID,
				RunId:      parentRunID,
			},
			ParentInitiatedId:        parentInitiatedID,
			ParentInitiatedVersion:   parentInitiatedVersion,
			ChildExecution:           &workflowExecution,
			Clock:                    parentClock,
			CompletionEvent:          completionEvent,
			ChildFirstExecutionRunId: firstRunID,
		})
		switch err.(type) {
		case nil:
			// noop
		case *serviceerror.NotFound, *serviceerror.NamespaceNotFound:
			// parent gone, noop
		default:
			return err
		}
	}

	// process parentClosePolicy except when the execution was reset. In case of reset, we need to keep the children around so that we can reconnect to them.
	// We know an execution was reset when ResetRunId was populated in it.
	// Note: Sometimes the reset operation might race with this task processing. i.e the WF was closed and before this task can be executed, a reset operation is recorded.
	// So we need to additionally check the termination reason for this parent to determine if this task was indeed created due to reset or due to normal completion of the WF.
	// Also, checking the dynamic config is not strictly safe since by definition it can change at any time. However this reduces the chance of us skipping the parent close policy when we shouldn't.
	allowResetWithPendingChildren := t.config.AllowResetWithPendingChildren(namespaceName.String())
	shouldSkipParentClosePolicy := false
	isParentTerminatedDueToReset := (completionEvent != nil) && ndc.IsTerminatedByResetter(completionEvent)
	if isParentTerminatedDueToReset && executionInfo.GetResetRunId() != "" && allowResetWithPendingChildren {
		// TODO (Chetan): update this condition as new reset policies/cases are added.
		shouldSkipParentClosePolicy = true // only skip if the parent is reset and we are using the new flow.
	}
	if !shouldSkipParentClosePolicy {
		if err := t.processParentClosePolicy(
			ctx,
			namespaceName.String(),
			&workflowExecution,
			children,
		); err != nil {
			// This is some retryable error, not NotFound or NamespaceNotFound.
			return err
		}
	}

	if task.DeleteAfterClose {
		err = t.deleteExecution(
			ctx,
			task,
			false,
			&task.DeleteProcessStage,
		)
	}
	return err
}

func (t *transferQueueActiveTaskExecutor) processCancelExecution(
	ctx context.Context,
	task *tasks.CancelExecutionTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, t.shardContext, weContext, task, t.metricHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	requestCancelInfo, ok := mutableState.GetRequestCancelInfo(task.InitiatedEventID)
	if !ok {
		return nil
	}
	err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), requestCancelInfo.Version, task.Version, task)
	if err != nil {
		return err
	}

	initiatedEvent, err := mutableState.GetRequesteCancelExternalInitiatedEvent(ctx, task.InitiatedEventID)
	if err != nil {
		return err
	}
	attributes := initiatedEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes()

	var targetNamespaceName namespace.Name
	var targetNamespaceID namespace.ID
	targetNamespaceEntry, err := t.targetNamespaceEntryHelper(
		namespace.ID(attributes.NamespaceId),
		namespace.Name(attributes.Namespace),
	)
	if err != nil {
		return err
	}

	if targetNamespaceEntry == nil {
		return t.requestCancelExternalExecutionFailed(
			ctx,
			task,
			weContext,
			namespace.Name(attributes.Namespace),
			namespace.ID(attributes.NamespaceId),
			attributes.GetWorkflowExecution().GetWorkflowId(),
			attributes.GetWorkflowExecution().GetRunId(),
			enumspb.CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND)
	}

	targetNamespaceID = targetNamespaceEntry.ID()
	targetNamespaceName = targetNamespaceEntry.Name()

	// handle workflow cancel itself
	if task.NamespaceID == targetNamespaceID.String() && task.WorkflowID == attributes.GetWorkflowExecution().GetWorkflowId() {
		// it does not matter if the run ID is a mismatch
		err = t.requestCancelExternalExecutionFailed(
			ctx,
			task,
			weContext,
			targetNamespaceName,
			targetNamespaceID,
			attributes.GetWorkflowExecution().GetWorkflowId(),
			attributes.GetWorkflowExecution().GetRunId(),
			enumspb.CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND)
		return err
	}

	if err = t.requestCancelExternalExecution(
		ctx,
		task,
		targetNamespaceName,
		targetNamespaceID,
		requestCancelInfo,
		attributes,
	); err != nil {
		t.logger.Debug("Failed to cancel external workflow execution", tag.Error(err))

		// Check to see if the error is non-transient, in which case add RequestCancelFailed
		// event and complete transfer task by returning nil error.
		if common.IsServiceTransientError(err) || common.IsContextDeadlineExceededErr(err) {
			// for retryable error just return
			return err
		}
		var failedCause enumspb.CancelExternalWorkflowExecutionFailedCause
		switch err.(type) {
		case *serviceerror.NotFound:
			failedCause = enumspb.CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND
		case *serviceerror.NamespaceNotFound:
			failedCause = enumspb.CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND
		default:
			t.logger.Error("Unexpected error type returned from RequestCancelWorkflowExecution API call.", tag.ServiceErrorType(err), tag.Error(err))
			return err
		}
		return t.requestCancelExternalExecutionFailed(
			ctx,
			task,
			weContext,
			targetNamespaceName,
			targetNamespaceID,
			attributes.GetWorkflowExecution().GetWorkflowId(),
			attributes.GetWorkflowExecution().GetRunId(),
			failedCause,
		)
	}

	// Record ExternalWorkflowExecutionCancelRequested in source execution
	return t.requestCancelExternalExecutionCompleted(
		ctx,
		task,
		weContext,
		targetNamespaceName,
		targetNamespaceID,
		attributes.GetWorkflowExecution().GetWorkflowId(),
		attributes.GetWorkflowExecution().GetRunId(),
	)
}

func (t *transferQueueActiveTaskExecutor) processSignalExecution(
	ctx context.Context,
	task *tasks.SignalExecutionTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, t.shardContext, weContext, task, t.metricHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil {
		release(nil) // release(nil) so that the mutable state is not unloaded from cache
		return consts.ErrWorkflowExecutionNotFound
	}

	signalInfo, ok := mutableState.GetSignalInfo(task.InitiatedEventID)
	if !ok {
		// TODO: here we should also RemoveSignalMutableState from target workflow
		// Otherwise, target SignalRequestID still can leak if shard restart after signalExternalExecutionCompleted
		// To do that, probably need to add the SignalRequestID in transfer task.
		return nil
	}
	err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), signalInfo.Version, task.Version, task)
	if err != nil {
		return err
	}

	if !mutableState.IsWorkflowExecutionRunning() {
		release(nil) // release(nil) so that the mutable state is not unloaded from cache
		return consts.ErrWorkflowCompleted
	}

	initiatedEvent, err := mutableState.GetSignalExternalInitiatedEvent(ctx, task.InitiatedEventID)
	if err != nil {
		return err
	}
	attributes := initiatedEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes()

	var targetNamespaceName namespace.Name
	var targetNamespaceID namespace.ID
	targetNamespaceEntry, err := t.targetNamespaceEntryHelper(
		namespace.ID(attributes.NamespaceId),
		namespace.Name(attributes.Namespace),
	)
	if err != nil {
		return err
	}

	if targetNamespaceEntry == nil {
		return t.signalExternalExecutionFailed(
			ctx,
			task,
			weContext,
			namespace.Name(attributes.Namespace),
			namespace.ID(attributes.NamespaceId),
			attributes.GetWorkflowExecution().GetWorkflowId(),
			attributes.GetWorkflowExecution().GetRunId(),
			attributes.Control,
			enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND,
		)
	}

	targetNamespaceID = targetNamespaceEntry.ID()
	targetNamespaceName = targetNamespaceEntry.Name()

	// handle workflow signal itself
	if task.NamespaceID == targetNamespaceID.String() && task.WorkflowID == attributes.GetWorkflowExecution().GetWorkflowId() {
		// it does not matter if the run ID is a mismatch
		return t.signalExternalExecutionFailed(
			ctx,
			task,
			weContext,
			targetNamespaceName,
			targetNamespaceID,
			attributes.GetWorkflowExecution().GetWorkflowId(),
			attributes.GetWorkflowExecution().GetRunId(),
			attributes.Control,
			enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
		)
	}

	if err = t.signalExternalExecution(
		ctx,
		task,
		targetNamespaceName,
		targetNamespaceID,
		signalInfo,
		attributes,
	); err != nil {
		t.logger.Debug("Failed to signal external workflow execution", tag.Error(err))

		// Check to see if the error is non-transient, in which case add SignalFailed
		// event and complete transfer task by returning nil error.
		if common.IsServiceTransientError(err) || common.IsContextDeadlineExceededErr(err) {
			// for retryable error just return
			return err
		}
		var failedCause enumspb.SignalExternalWorkflowExecutionFailedCause
		switch err.(type) {
		case *serviceerror.NotFound:
			failedCause = enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND
		case *serviceerror.NamespaceNotFound:
			failedCause = enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND
		case *serviceerror.InvalidArgument:
			failedCause = enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_SIGNAL_COUNT_LIMIT_EXCEEDED
		default:
			t.logger.Error("Unexpected error type returned from SignalWorkflowExecution API call.", tag.ServiceErrorType(err), tag.Error(err))
			return err
		}
		return t.signalExternalExecutionFailed(
			ctx,
			task,
			weContext,
			targetNamespaceName,
			targetNamespaceID,
			attributes.GetWorkflowExecution().GetWorkflowId(),
			attributes.GetWorkflowExecution().GetRunId(),
			attributes.Control,
			failedCause,
		)
	}

	err = t.signalExternalExecutionCompleted(
		ctx,
		task,
		weContext,
		targetNamespaceName,
		targetNamespaceID,
		attributes.GetWorkflowExecution().GetWorkflowId(),
		attributes.GetWorkflowExecution().GetRunId(),
		attributes.Control,
	)
	if err != nil {
		return err
	}

	signalRequestID := signalInfo.GetRequestId()

	// release the weContext lock since we no longer need mutable state and
	// the rest of logic is making RPC call, which takes time.
	release(retError)
	// remove signalRequestedID from target workflow, after Signal detail is removed from source workflow
	_, err = t.historyRawClient.RemoveSignalMutableState(ctx, &historyservice.RemoveSignalMutableStateRequest{
		NamespaceId:       targetNamespaceID.String(),
		WorkflowExecution: attributes.GetWorkflowExecution(),
		RequestId:         signalRequestID,
	})
	return err
}

func (t *transferQueueActiveTaskExecutor) processStartChildExecution(
	ctx context.Context,
	task *tasks.StartChildExecutionTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, t.shardContext, weContext, task, t.metricHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil {
		release(nil) // release(nil) so that the mutable state is not unloaded from cache
		return consts.ErrWorkflowExecutionNotFound
	}

	childInfo, ok := mutableState.GetChildExecutionInfo(task.InitiatedEventID)
	if !ok {
		release(nil) // release(nil) so that the mutable state is not unloaded from cache
		return consts.ErrChildExecutionNotFound
	}
	err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), childInfo.Version, task.Version, task)
	if err != nil {
		return err
	}

	var targetNamespaceName namespace.Name
	var targetNamespaceID namespace.ID
	targetNamespaceEntry, err := t.targetNamespaceEntryHelper(
		namespace.ID(childInfo.NamespaceId),
		namespace.Name(childInfo.Namespace),
	)
	if err != nil {
		return err
	}
	if targetNamespaceEntry != nil {
		targetNamespaceID = targetNamespaceEntry.ID()
		targetNamespaceName = targetNamespaceEntry.Name()
	}
	// Continue processing if targetNamespaceEntry is nil, we may need to record start failure below.

	// workflow running or not, child started or not, parent close policy is abandon or not
	// 8 cases in total
	workflowRunning := mutableState.IsWorkflowExecutionRunning()
	childStarted := childInfo.StartedEventId != common.EmptyEventID
	if !workflowRunning && (!childStarted || childInfo.ParentClosePolicy != enumspb.PARENT_CLOSE_POLICY_ABANDON) {
		// three cases here:
		// case 1: workflow not running, child started, parent close policy is not abandon
		// case 2: workflow not running, child not started, parent close policy is not abandon
		// case 3: workflow not running, child not started, parent close policy is abandon
		//
		// NOTE: ideally for case 3, we should continue to start child. However, with current start child
		// and standby start child verification logic, we can't do that because:
		// 1. Once workflow is closed, we can't update mutable state or record child started event.
		// If the RPC call for scheduling first workflow task times out but the call actually succeeds on child workflow.
		// Then the child workflow can run, complete and another unrelated workflow can reuse this workflowID.
		// Now when the start child task retries, we can't rely on requestID to dedupe the start child call. (We can use runID instead of requestID to dedupe)
		// 2. No update to mutable state and child started event means we are not able to replicate the information
		// to the standby cluster, so standby start child logic won't be able to verify the child has started.
		// To resolve the issue above, we need to
		// 1. Start child workflow and schedule the first workflow task in one transaction. Use runID to perform deduplication
		// 2. Standby start child logic need to verify if child workflow actually started instead of relying on the information
		// in parent mutable state.
		return nil
	}

	// ChildExecution already started, just create WorkflowTask and complete transfer task
	// If parent already closed, since child workflow started event already written to history,
	// still schedule the workflowTask if the parent close policy is Abandon.
	// If parent close policy cancel or terminate, parent close policy will be applied in another
	// transfer task.
	// case 4, 5: workflow started, child started, parent close policy is or is not abandon
	// case 6: workflow closed, child started, parent close policy is abandon
	if childStarted {
		childExecution := &commonpb.WorkflowExecution{
			WorkflowId: childInfo.StartedWorkflowId,
			RunId:      childInfo.StartedRunId,
		}
		childClock := childInfo.Clock
		// NOTE: do not access anything related mutable state after this lock release
		// release the context lock since we no longer need mutable state and
		// the rest of logic is making RPC call, which takes time.
		release(nil)

		parentClock, err := t.shardContext.NewVectorClock()
		if err != nil {
			return err
		}

		if targetNamespaceEntry == nil {
			return serviceerror.NewNamespaceNotFound(childInfo.Namespace)
		}

		return t.createFirstWorkflowTask(ctx, targetNamespaceID.String(), childExecution, parentClock, childClock)
	}

	// remaining 2 cases:
	// case 7, 8: workflow running, child not started, parent close policy is or is not abandon

	initiatedEvent, err := mutableState.GetChildExecutionInitiatedEvent(ctx, task.InitiatedEventID)
	if err != nil {
		return err
	}
	attributes := initiatedEvent.GetStartChildWorkflowExecutionInitiatedEventAttributes()

	var parentNamespaceName namespace.Name
	if namespaceEntry, err := t.registry.GetNamespaceByID(namespace.ID(task.NamespaceID)); err != nil {
		if _, isNotFound := err.(*serviceerror.NamespaceNotFound); !isNotFound {
			return err
		}
		// It is possible that the parent namespace got deleted. Use namespaceID instead as this is only needed for the history event.
		parentNamespaceName = namespace.Name(task.NamespaceID)
	} else {
		parentNamespaceName = namespaceEntry.Name()
	}

	if targetNamespaceEntry == nil {
		return t.recordStartChildExecutionFailed(
			ctx,
			task,
			weContext,
			attributes,
			enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND,
		)
	}

	var sourceVersionStamp *commonpb.WorkerVersionStamp
	var inheritedBuildId string
	if attributes.InheritBuildId && mutableState.GetEffectiveVersioningBehavior() == enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED {
		// Do not set inheritedBuildId for v3 wfs.
		// setting inheritedBuildId of the child wf to the assignedBuildId of the parent
		inheritedBuildId = mutableState.GetAssignedBuildId()
		if inheritedBuildId == "" {
			// TODO: this is only needed for old versioning. get rid of StartWorkflowExecutionRequest.SourceVersionStamp
			// [cleanup-old-wv]
			// Copy version stamp to new workflow only if:
			// - command says to use compatible version
			// - using versioning
			sourceVersionStamp = worker_versioning.StampIfUsingVersioning(mutableState.GetMostRecentWorkerVersionStamp())
		}
	}

	// If there is a pinned override, then the effective version will be the same as the pinned override version.
	// If this is a cross-TQ child, we don't want to ask matching the same question twice, so we re-use the result from
	// the first matching task-queue-in-version check.
	newTQInPinnedVersion := false

	// Child of pinned parent will inherit the parent's version if the Child's Task Queue belongs to that version.
	var inheritedPinnedVersion *deploymentpb.WorkerDeploymentVersion
	if mutableState.GetEffectiveVersioningBehavior() == enumspb.VERSIONING_BEHAVIOR_PINNED {
		inheritedPinnedVersion = worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(mutableState.GetEffectiveDeployment())
		newTQ := attributes.GetTaskQueue().GetName()
		if attributes.GetNamespaceId() != mutableState.GetExecutionInfo().GetNamespaceId() { // don't inherit pinned version if child is in a different namespace
			inheritedPinnedVersion = nil
		} else if newTQ != mutableState.GetExecutionInfo().GetTaskQueue() {
			newTQInPinnedVersion, err = worker_versioning.GetIsWFTaskQueueInVersionDetector(t.matchingRawClient)(ctx, attributes.GetNamespaceId(), newTQ, inheritedPinnedVersion)
			if err != nil {
				return errors.New(fmt.Sprintf("error determining child task queue presence in inherited version: %s", err.Error()))
			}
			if !newTQInPinnedVersion {
				inheritedPinnedVersion = nil
			}
		}
	}

	// Pinned override is inherited if Task Queue of new run is compatible with the override version.
	var inheritedPinnedOverride *workflowpb.VersioningOverride
	if o := mutableState.GetExecutionInfo().GetVersioningInfo().GetVersioningOverride(); worker_versioning.OverrideIsPinned(o) {
		inheritedPinnedOverride = o
		newTQ := attributes.GetTaskQueue().GetName()
		if newTQ != mutableState.GetExecutionInfo().GetTaskQueue() && !newTQInPinnedVersion ||
			attributes.GetNamespaceId() != mutableState.GetExecutionInfo().GetNamespaceId() { // don't inherit pinned version if child is in a different namespace
			inheritedPinnedOverride = nil
		}
	}

	// Note: childStarted flag above is computed from the parent's history. When this is TRUE it's guaranteed that the child was succesfully started.
	// But if it's FALSE then the child *may or maynot* be started (ex: we failed to record ChildExecutionStarted event previously.)
	// Hence we need to check the child workflow ID and attempt to reconnect before proceeding to start a new instance of the child.
	// This path is usually taken when the parent is being reset and the reset point (i.e baseWorkflowInfo.LowestCommonAncestorEventId) is after the child was initiated.
	shouldTerminateAndStartChild := false
	resetChildID := fmt.Sprintf("%s:%s", attributes.GetWorkflowType().Name, attributes.GetWorkflowId())
	baseWorkflowInfo := mutableState.GetBaseWorkflowInfo()
	if mutableState.IsResetRun() && baseWorkflowInfo != nil && baseWorkflowInfo.LowestCommonAncestorEventId >= childInfo.InitiatedEventId { // child was started before the reset point.
		childRunID, childFirstRunID, err := t.verifyChildWorkflow(ctx, mutableState, targetNamespaceEntry, attributes.WorkflowId)
		if err != nil {
			return err
		}
		if childRunID != "" {
			childExecution := &commonpb.WorkflowExecution{
				WorkflowId: childInfo.StartedWorkflowId,
				RunId:      childRunID,
			}
			childClock := childInfo.Clock
			// Child execution is successfully started, record ChildExecutionStartedEvent in parent execution
			err = t.recordChildExecutionStarted(ctx, task, weContext, attributes, childFirstRunID, childClock)
			if err != nil {
				return err
			}
			// NOTE: do not access anything related mutable state after this lock release
			// release the context lock since we no longer need mutable state and
			// the rest of logic is making RPC call, which takes time.
			release(nil)

			parentClock, err := t.shardContext.NewVectorClock()
			if err != nil {
				return err
			}
			return t.createFirstWorkflowTask(ctx, targetNamespaceID.String(), childExecution, parentClock, childClock)
		}
		// now if there was no child found after reset then it could mean one of the following.
		// 1. The parent never got a chance to start the child. So we should go ahead and start one (below)
		// 2. The child was started, but may be terminated from someone external or timedout.
		// 3. There was a running workflow that is not related to the current run.
		// In all these cases it's ok to proceed to start a new instance of child (below) and accept the result of that operation.
	} else {
		// child was started after reset-point (or the parent is not in reset run). We need to first check if this child was recorded at the time of reset.
		if resetChildInfo, ok := mutableState.GetExecutionInfo().GetChildrenInitializedPostResetPoint()[resetChildID]; ok {
			shouldTerminateAndStartChild = resetChildInfo.ShouldTerminateAndStart
		}
	}

	executionInfo := mutableState.GetExecutionInfo()
	rootExecutionInfo := &workflowspb.RootExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: executionInfo.RootWorkflowId,
			RunId:      executionInfo.RootRunId,
		},
	}

	childRunID, childClock, err := t.startWorkflow(
		ctx,
		task,
		parentNamespaceName,
		targetNamespaceName,
		namespace.ID(targetNamespaceID),
		childInfo.CreateRequestId,
		attributes,
		sourceVersionStamp,
		rootExecutionInfo,
		inheritedBuildId,
		initiatedEvent.GetUserMetadata(),
		shouldTerminateAndStartChild,
		inheritedPinnedOverride,
		inheritedPinnedVersion,
		priorities.Merge(mutableState.GetExecutionInfo().Priority, attributes.Priority),
	)
	if err != nil {
		t.logger.Debug("Failed to start child workflow execution", tag.Error(err))
		if common.IsServiceTransientError(err) || common.IsContextDeadlineExceededErr(err) {
			// for retryable error just return
			return err
		}
		var failedCause enumspb.StartChildWorkflowExecutionFailedCause
		switch err.(type) {
		case *serviceerror.WorkflowExecutionAlreadyStarted:
			failedCause = enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS
		case *serviceerror.NamespaceNotFound:
			failedCause = enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND
		default:
			t.logger.Error("Unexpected error type returned from StartWorkflowExecution API call for child workflow.", tag.ServiceErrorType(err), tag.Error(err))
			return err
		}

		return t.recordStartChildExecutionFailed(
			ctx,
			task,
			weContext,
			attributes,
			failedCause,
		)
	}

	t.logger.Debug("Child Execution started successfully",
		tag.WorkflowID(attributes.WorkflowId), tag.WorkflowRunID(childRunID))

	// if shouldTerminateAndStartChild is true, it means the child was started after the reset point and we attempted to terminate it before starting a new one.
	// We should update the parent execution info to reflect that the child was potentially terminated and started.
	if shouldTerminateAndStartChild {
		childrenInitializedPostResetPoint := executionInfo.ChildrenInitializedPostResetPoint
		childrenInitializedPostResetPoint[resetChildID] = &persistencespb.ResetChildInfo{
			ShouldTerminateAndStart: false,
		}
		mutableState.SetChildrenInitializedPostResetPoint(childrenInitializedPostResetPoint)
	}

	// Child execution is successfully started, record ChildExecutionStartedEvent in parent execution
	err = t.recordChildExecutionStarted(ctx, task, weContext, attributes, childRunID, childClock)
	if err != nil {
		return err
	}

	// NOTE: do not access anything related mutable state after this lock is released.
	// Release the context lock since we no longer need mutable state and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	parentClock, err := t.shardContext.NewVectorClock()
	if err != nil {
		return err
	}
	return t.createFirstWorkflowTask(ctx, targetNamespaceID.String(), &commonpb.WorkflowExecution{
		WorkflowId: childInfo.StartedWorkflowId,
		RunId:      childRunID,
	}, parentClock, childClock)
}

// verifyChildWorkflow describes the childWorkflowID and identifies its parent. It then checks if the current run was derived from that parent by comparing the OriginalRunID value.
// It returns the child's runID if the checks pass. Empty runID is returned if the check doesn't pass.
func (t *transferQueueActiveTaskExecutor) verifyChildWorkflow(
	ctx context.Context,
	mutableState historyi.MutableState,
	childNamespace *namespace.Namespace,
	childWorkflowID string,
) (childID, firstRunID string, retError error) {
	childDescribeReq := &historyservice.DescribeWorkflowExecutionRequest{
		NamespaceId: childNamespace.ID().String(),
		Request: &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: childNamespace.Name().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: childWorkflowID,
			},
		},
	}
	response, err := t.historyRawClient.DescribeWorkflowExecution(ctx, childDescribeReq)
	if err != nil {
		// It's not an error if the child is not found. Return empty childID so that the child is created.
		if common.IsNotFoundError(err) {
			return "", "", nil
		}
		return "", "", err
	}

	if response.WorkflowExecutionInfo.ParentExecution == nil {
		// The child doesn't have a parent. Maybe it was started by some client.
		return "", "", nil
	}
	// Verify if the WorkflowIDs match first.
	if response.WorkflowExecutionInfo.ParentExecution.WorkflowId != mutableState.GetExecutionInfo().WorkflowId {
		return "", "", nil
	}

	childsParentRunID := response.WorkflowExecutionInfo.ParentExecution.RunId
	// Check if the child's parent was the base run for the current run.
	if childsParentRunID == mutableState.GetExecutionInfo().OriginalExecutionRunId {
		return response.WorkflowExecutionInfo.Execution.RunId, response.WorkflowExecutionInfo.FirstRunId, nil
	}

	// load the child's parent mutable state.
	wfKey := mutableState.GetWorkflowKey()
	wfKey.RunID = childsParentRunID
	wfContext, release, err := getWorkflowExecutionContext(
		ctx,
		t.shardContext,
		t.cache,
		wfKey,
		chasmworkflow.Archetype,
		locks.PriorityLow,
	)
	if err != nil {
		return "", "", err
	}
	defer func() { release(retError) }()

	childsParentMutableState, err := wfContext.LoadMutableState(ctx, t.shardContext)
	if err != nil {
		return "", "", err
	}

	// now check if the child's parent's original run id and the current run's original run ID are the same.
	if childsParentMutableState.GetExecutionInfo().OriginalExecutionRunId == mutableState.GetExecutionInfo().OriginalExecutionRunId {
		return response.WorkflowExecutionInfo.Execution.RunId, response.WorkflowExecutionInfo.FirstRunId, nil
	}

	return "", "", nil
}

func (t *transferQueueActiveTaskExecutor) processResetWorkflow(
	ctx context.Context,
	task *tasks.ResetWorkflowTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	currentContext, currentRelease, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { currentRelease(retError) }()

	currentMutableState, err := loadMutableStateForTransferTask(ctx, t.shardContext, currentContext, task, t.metricHandler, t.logger)
	if err != nil {
		return err
	}
	if currentMutableState == nil {
		return nil
	}

	logger := log.With(
		t.logger,
		tag.WorkflowNamespaceID(task.NamespaceID),
		tag.WorkflowID(task.WorkflowID),
		tag.WorkflowRunID(task.RunID),
	)

	if !currentMutableState.IsWorkflowExecutionRunning() {
		// it means this this might not be current anymore, we need to check
		var resp *persistence.GetCurrentExecutionResponse
		resp, err = t.shardContext.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
			ShardID:     t.shardContext.GetShardID(),
			NamespaceID: task.NamespaceID,
			WorkflowID:  task.WorkflowID,
		})
		if err != nil {
			return err
		}
		if resp.RunID != task.RunID {
			logger.Warn("Auto-Reset is skipped, because current run is stale.")
			return nil
		}
	}
	// TODO: current reset doesn't allow childWFs, in the future we will release this restriction
	if len(currentMutableState.GetPendingChildExecutionInfos()) > 0 {
		logger.Warn("Auto-Reset is skipped, because current run has pending child executions.")
		return nil
	}

	// TODO: why we are comparing task version to workflow start version here?
	// GenerateWorkflowResetTasks uses currentVersion
	currentStartVersion, err := currentMutableState.GetStartVersion()
	if err != nil {
		return err
	}

	err = CheckTaskVersion(t.shardContext, t.logger, currentMutableState.GetNamespaceEntry(), currentStartVersion, task.Version, task)
	if err != nil {
		return err
	}

	executionInfo := currentMutableState.GetExecutionInfo()
	executionState := currentMutableState.GetExecutionState()
	namespaceEntry, err := t.registry.GetNamespaceByID(namespace.ID(executionInfo.NamespaceId))
	if err != nil {
		return err
	}
	logger = log.With(logger, tag.WorkflowNamespace(namespaceEntry.Name().String()))

	reason, resetPoint := workflow.FindAutoResetPoint(t.shardContext.GetTimeSource(), namespaceEntry.VerifyBinaryChecksum, executionInfo.AutoResetPoints)
	if resetPoint == nil {
		logger.Warn("Auto-Reset is skipped, because reset point is not found.")
		return nil
	}
	logger = log.With(
		logger,
		tag.WorkflowResetBaseRunID(resetPoint.GetRunId()),
		tag.WorkflowBinaryChecksum(resetPoint.GetBinaryChecksum()),
		tag.WorkflowEventID(resetPoint.GetFirstWorkflowTaskCompletedId()),
	)

	var baseContext historyi.WorkflowContext
	var baseMutableState historyi.MutableState
	var baseRelease historyi.ReleaseWorkflowContextFunc
	if resetPoint.GetRunId() == executionState.RunId {
		baseContext = currentContext
		baseMutableState = currentMutableState
		baseRelease = currentRelease
	} else {
		baseContext, baseRelease, err = getWorkflowExecutionContext(
			ctx,
			t.shardContext,
			t.cache,
			definition.NewWorkflowKey(task.NamespaceID, task.WorkflowID, resetPoint.GetRunId()),
			chasmworkflow.Archetype,
			locks.PriorityLow,
		)
		if err != nil {
			return err
		}
		defer func() { baseRelease(retError) }()
		baseMutableState, err = loadMutableStateForTransferTask(ctx, t.shardContext, baseContext, task, t.metricHandler, t.logger)
		if err != nil {
			return err
		}
		if baseMutableState == nil {
			return nil
		}
	}

	// NOTE: reset need to go through history which may take a longer time,
	// so it's using its own timeout
	return t.resetWorkflow(
		ctx,
		task,
		reason,
		resetPoint,
		baseContext,
		baseMutableState,
		currentContext,
		currentMutableState,
		logger,
	)
}

func (t *transferQueueActiveTaskExecutor) recordChildExecutionStarted(
	ctx context.Context,
	task *tasks.StartChildExecutionTask,
	wfContext historyi.WorkflowContext,
	initiatedAttributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes,
	runID string,
	clock *clockspb.VectorClock,
) error {
	return t.updateWorkflowExecution(ctx, wfContext, true,
		func(mutableState historyi.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return consts.ErrWorkflowCompleted
			}

			ci, ok := mutableState.GetChildExecutionInfo(task.InitiatedEventID)
			if !ok || ci.StartedEventId != common.EmptyEventID {
				return serviceerror.NewNotFound("Pending child execution not found.")
			}

			_, err := mutableState.AddChildWorkflowExecutionStartedEvent(
				&commonpb.WorkflowExecution{
					WorkflowId: ci.StartedWorkflowId,
					RunId:      runID,
				},
				initiatedAttributes.WorkflowType,
				task.InitiatedEventID,
				initiatedAttributes.Header,
				clock,
			)

			return err
		})
}

func (t *transferQueueActiveTaskExecutor) recordStartChildExecutionFailed(
	ctx context.Context,
	task *tasks.StartChildExecutionTask,
	wfContext historyi.WorkflowContext,
	initiatedAttributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes,
	failedCause enumspb.StartChildWorkflowExecutionFailedCause,
) error {
	return t.updateWorkflowExecution(ctx, wfContext, true,
		func(mutableState historyi.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return consts.ErrWorkflowCompleted
			}

			ci, ok := mutableState.GetChildExecutionInfo(task.InitiatedEventID)
			if !ok || ci.StartedEventId != common.EmptyEventID {
				return serviceerror.NewNotFound("Pending child execution not found.")
			}

			_, err := mutableState.AddStartChildWorkflowExecutionFailedEvent(
				task.InitiatedEventID,
				failedCause,
				initiatedAttributes,
			)
			return err
		})
}

// createFirstWorkflowTask is used by StartChildExecution transfer task to create the first workflow task for
// child execution.
func (t *transferQueueActiveTaskExecutor) createFirstWorkflowTask(
	ctx context.Context,
	namespaceID string,
	execution *commonpb.WorkflowExecution,
	parentClock *clockspb.VectorClock,
	childClock *clockspb.VectorClock,
) error {
	_, err := t.historyRawClient.ScheduleWorkflowTask(ctx, &historyservice.ScheduleWorkflowTaskRequest{
		NamespaceId:         namespaceID,
		WorkflowExecution:   execution,
		IsFirstWorkflowTask: true,
		ParentClock:         parentClock,
		ChildClock:          childClock,
	})
	return err
}

func (t *transferQueueActiveTaskExecutor) requestCancelExternalExecutionCompleted(
	ctx context.Context,
	task *tasks.CancelExecutionTask,
	wfContext historyi.WorkflowContext,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	targetWorkflowID string,
	targetRunID string,
) error {
	return t.updateWorkflowExecution(ctx, wfContext, true,
		func(mutableState historyi.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return consts.ErrWorkflowCompleted
			}

			_, ok := mutableState.GetRequestCancelInfo(task.InitiatedEventID)
			if !ok {
				return workflow.ErrMissingRequestCancelInfo
			}

			_, err := mutableState.AddExternalWorkflowExecutionCancelRequested(
				task.InitiatedEventID,
				targetNamespace,
				targetNamespaceID,
				targetWorkflowID,
				targetRunID,
			)
			return err
		})
}

func (t *transferQueueActiveTaskExecutor) signalExternalExecutionCompleted(
	ctx context.Context,
	task *tasks.SignalExecutionTask,
	wfContext historyi.WorkflowContext,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	targetWorkflowID string,
	targetRunID string,
	control string,
) error {
	return t.updateWorkflowExecution(ctx, wfContext, true,
		func(mutableState historyi.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return consts.ErrWorkflowCompleted
			}

			_, ok := mutableState.GetSignalInfo(task.InitiatedEventID)
			if !ok {
				return workflow.ErrMissingSignalInfo
			}

			_, err := mutableState.AddExternalWorkflowExecutionSignaled(
				task.InitiatedEventID,
				targetNamespace,
				targetNamespaceID,
				targetWorkflowID,
				targetRunID,
				control,
			)
			return err
		})
}

func (t *transferQueueActiveTaskExecutor) requestCancelExternalExecutionFailed(
	ctx context.Context,
	task *tasks.CancelExecutionTask,
	wfContext historyi.WorkflowContext,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	targetWorkflowID string,
	targetRunID string,
	failedCause enumspb.CancelExternalWorkflowExecutionFailedCause,
) error {
	return t.updateWorkflowExecution(ctx, wfContext, true,
		func(mutableState historyi.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return consts.ErrWorkflowCompleted
			}

			_, ok := mutableState.GetRequestCancelInfo(task.InitiatedEventID)
			if !ok {
				return workflow.ErrMissingRequestCancelInfo
			}

			_, err := mutableState.AddRequestCancelExternalWorkflowExecutionFailedEvent(
				task.InitiatedEventID,
				targetNamespace,
				targetNamespaceID,
				targetWorkflowID,
				targetRunID,
				failedCause,
			)
			return err
		})
}

func (t *transferQueueActiveTaskExecutor) signalExternalExecutionFailed(
	ctx context.Context,
	task *tasks.SignalExecutionTask,
	wfContext historyi.WorkflowContext,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	targetWorkflowID string,
	targetRunID string,
	control string,
	failedCause enumspb.SignalExternalWorkflowExecutionFailedCause,
) error {
	return t.updateWorkflowExecution(ctx, wfContext, true,
		func(mutableState historyi.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return serviceerror.NewNotFound("Workflow is not running.")
			}

			_, ok := mutableState.GetSignalInfo(task.InitiatedEventID)
			if !ok {
				return workflow.ErrMissingSignalInfo
			}

			_, err := mutableState.AddSignalExternalWorkflowExecutionFailedEvent(
				task.InitiatedEventID,
				targetNamespace,
				targetNamespaceID,
				targetWorkflowID,
				targetRunID,
				control,
				failedCause,
			)
			return err
		})
}

func (t *transferQueueActiveTaskExecutor) updateWorkflowExecution(
	ctx context.Context,
	wfContext historyi.WorkflowContext,
	createWorkflowTask bool,
	action func(historyi.MutableState) error,
) error {
	mutableState, err := wfContext.LoadMutableState(ctx, t.shardContext)
	if err != nil {
		return err
	}

	if err := action(mutableState); err != nil {
		return err
	}

	if createWorkflowTask {
		// Create a transfer task to schedule a workflow task
		err := workflow.ScheduleWorkflowTask(mutableState)
		if err != nil {
			return err
		}
	}

	return wfContext.UpdateWorkflowExecutionAsActive(ctx, t.shardContext)
}

func (t *transferQueueActiveTaskExecutor) requestCancelExternalExecution(
	ctx context.Context,
	task *tasks.CancelExecutionTask,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	requestCancelInfo *persistencespb.RequestCancelInfo,
	attributes *historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes,
) error {
	request := &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: targetNamespaceID.String(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			Namespace:         targetNamespace.String(),
			WorkflowExecution: attributes.GetWorkflowExecution(),
			Identity:          consts.IdentityHistoryService,
			// Use the same request ID to dedupe RequestCancelWorkflowExecution calls
			RequestId: requestCancelInfo.GetCancelRequestId(),
			Reason:    attributes.Reason,
		},
		ExternalInitiatedEventId: task.InitiatedEventID,
		ExternalWorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      task.RunID,
		},
		ChildWorkflowOnly: attributes.GetChildWorkflowOnly(),
	}

	_, err := t.historyRawClient.RequestCancelWorkflowExecution(ctx, request)
	return err
}

func (t *transferQueueActiveTaskExecutor) signalExternalExecution(
	ctx context.Context,
	task *tasks.SignalExecutionTask,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	signalInfo *persistencespb.SignalInfo,
	attributes *historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes,
) error {
	request := &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: targetNamespaceID.String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         targetNamespace.String(),
			WorkflowExecution: attributes.GetWorkflowExecution(),
			Identity:          consts.IdentityHistoryService,
			SignalName:        attributes.SignalName,
			Input:             attributes.Input,
			// Use same request ID to deduplicate SignalWorkflowExecution calls
			RequestId: signalInfo.GetRequestId(),
			Control:   attributes.Control,
			Header:    attributes.Header,
		},
		ExternalWorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      task.RunID,
		},
		ChildWorkflowOnly: attributes.GetChildWorkflowOnly(),
	}

	_, err := t.historyRawClient.SignalWorkflowExecution(ctx, request)
	return err
}

func (t *transferQueueActiveTaskExecutor) startWorkflow(
	ctx context.Context,
	task *tasks.StartChildExecutionTask,
	namespace namespace.Name,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	childRequestID string,
	attributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes,
	sourceVersionStamp *commonpb.WorkerVersionStamp,
	rootExecutionInfo *workflowspb.RootExecutionInfo,
	inheritedBuildId string,
	userMetadata *sdkpb.UserMetadata,
	shouldTerminateAndStartChild bool,
	inheritedPinnedOverride *workflowpb.VersioningOverride,
	inheritedPinnedVersion *deploymentpb.WorkerDeploymentVersion,
	priority *commonpb.Priority,
) (string, *clockspb.VectorClock, error) {
	startRequest := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                targetNamespace.String(),
		WorkflowId:               attributes.WorkflowId,
		WorkflowType:             attributes.WorkflowType,
		TaskQueue:                attributes.TaskQueue,
		Input:                    attributes.Input,
		Header:                   attributes.Header,
		WorkflowExecutionTimeout: attributes.WorkflowExecutionTimeout,
		WorkflowRunTimeout:       attributes.WorkflowRunTimeout,
		WorkflowTaskTimeout:      attributes.WorkflowTaskTimeout,

		// Use the same request ID to dedupe StartWorkflowExecution calls
		RequestId:             childRequestID,
		WorkflowIdReusePolicy: attributes.WorkflowIdReusePolicy,
		RetryPolicy:           attributes.RetryPolicy,
		CronSchedule:          attributes.CronSchedule,
		Memo:                  attributes.Memo,
		SearchAttributes:      attributes.SearchAttributes,
		UserMetadata:          userMetadata,
		VersioningOverride:    inheritedPinnedOverride,
		Priority:              priority,
	}

	request := common.CreateHistoryStartWorkflowRequest(
		targetNamespaceID.String(),
		startRequest,
		&workflowspb.ParentExecutionInfo{
			NamespaceId: task.NamespaceID,
			Namespace:   namespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: task.WorkflowID,
				RunId:      task.RunID,
			},
			InitiatedId:      task.InitiatedEventID,
			InitiatedVersion: task.Version,
			Clock:            vclock.NewVectorClock(t.shardContext.GetClusterMetadata().GetClusterID(), t.shardContext.GetShardID(), task.TaskID),
		},
		rootExecutionInfo,
		t.shardContext.GetTimeSource().Now(),
	)

	request.SourceVersionStamp = sourceVersionStamp
	request.InheritedBuildId = inheritedBuildId
	request.InheritedPinnedVersion = inheritedPinnedVersion

	if shouldTerminateAndStartChild {
		request.StartRequest.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
		request.StartRequest.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING
		request.ChildWorkflowOnly = true
	}

	response, err := t.historyRawClient.StartWorkflowExecution(ctx, request)
	if err != nil {
		return "", nil, err
	}
	return response.GetRunId(), response.GetClock(), nil
}

func (t *transferQueueActiveTaskExecutor) resetWorkflow(
	ctx context.Context,
	task *tasks.ResetWorkflowTask,
	reason string,
	resetPoint *workflowpb.ResetPointInfo,
	baseContext historyi.WorkflowContext,
	baseMutableState historyi.MutableState,
	currentContext historyi.WorkflowContext,
	currentMutableState historyi.MutableState,
	logger log.Logger,
) error {
	// the actual reset operation needs to read history and may not be able to completed within
	// the original context timeout.
	// create a new context with a longer timeout, but retain all existing context values.
	resetWorkflowCtx, cancel := rpc.ResetContextTimeout(ctx, taskHistoryOpTimeout)
	defer cancel()

	namespaceID := namespace.ID(task.NamespaceID)
	workflowID := task.WorkflowID
	baseRunID := baseMutableState.GetExecutionState().GetRunId()

	resetRunID := uuid.New()
	baseRebuildLastEventID := resetPoint.GetFirstWorkflowTaskCompletedId() - 1
	baseVersionHistories := baseMutableState.GetExecutionInfo().GetVersionHistories()
	baseCurrentVersionHistory, err := versionhistory.GetCurrentVersionHistory(baseVersionHistories)
	if err != nil {
		return err
	}
	baseRebuildLastEventVersion, err := versionhistory.GetVersionHistoryEventVersion(baseCurrentVersionHistory, baseRebuildLastEventID)
	if err != nil {
		return err
	}
	baseCurrentBranchToken := baseCurrentVersionHistory.GetBranchToken()
	baseNextEventID := baseMutableState.GetNextEventID()

	namespaceName, err := t.shardContext.GetNamespaceRegistry().GetNamespaceName(namespaceID)
	if err != nil {
		return err
	}
	allowResetWithPendingChildren := t.shardContext.GetConfig().AllowResetWithPendingChildren(namespaceName.String())
	baseWorkflow := ndc.NewWorkflow(
		t.shardContext.GetClusterMetadata(),
		baseContext,
		baseMutableState,
		wcache.NoopReleaseFn, // this is fine since caller will defer on release
	)
	err = t.workflowResetter.ResetWorkflow(
		resetWorkflowCtx,
		namespaceID,
		workflowID,
		baseRunID,
		baseCurrentBranchToken,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		baseNextEventID,
		resetRunID,
		uuid.New(),
		baseWorkflow,
		ndc.NewWorkflow(
			t.shardContext.GetClusterMetadata(),
			currentContext,
			currentMutableState,
			wcache.NoopReleaseFn, // this is fine since caller will defer on release
		),
		reason,
		nil,
		nil,
		allowResetWithPendingChildren,
		nil,
	)

	switch err.(type) {
	case nil:
		return nil

	case *serviceerror.NotFound, *serviceerror.NamespaceNotFound:
		// This means the reset point is corrupted and not retry able.
		// There must be a bug in our system that we must fix.(for example, history is not the same in active/passive)
		metrics.AutoResetPointCorruptionCounter.With(t.metricHandler).Record(
			1,
			metrics.OperationTag(metrics.OperationTransferQueueProcessorScope),
		)
		logger.Error("Auto-Reset workflow failed and not retryable. The reset point is corrupted.", tag.Error(err))
		return nil

	default:
		// log this error and retry
		logger.Error("Auto-Reset workflow failed", tag.Error(err))
		return err
	}
}

func (t *transferQueueActiveTaskExecutor) processParentClosePolicy(
	ctx context.Context,
	parentNamespaceName string,
	parentExecution *commonpb.WorkflowExecution,
	childInfos map[int64]*persistencespb.ChildExecutionInfo,
) error {
	if len(childInfos) == 0 {
		return nil
	}

	scope := t.metricHandler.WithTags(metrics.OperationTag(metrics.TransferActiveTaskCloseExecutionScope))

	if t.shardContext.GetConfig().EnableParentClosePolicyWorker() &&
		len(childInfos) >= t.shardContext.GetConfig().ParentClosePolicyThreshold(parentNamespaceName) {

		executions := make([]parentclosepolicy.RequestDetail, 0, len(childInfos))
		for _, childInfo := range childInfos {
			if childInfo.ParentClosePolicy == enumspb.PARENT_CLOSE_POLICY_ABANDON {
				continue
			}

			childNamespaceID := namespace.ID(childInfo.GetNamespaceId())
			if childNamespaceID.IsEmpty() {
				// TODO (alex): Remove after childInfo.NamespaceId is back filled. Backward compatibility: old childInfo doesn't have NamespaceId set.
				// TODO (alex): consider reverse lookup of namespace name from ID but namespace name is not actually used.
				var err error
				childNamespaceID, err = t.registry.GetNamespaceID(namespace.Name(childInfo.GetNamespace()))
				switch err.(type) {
				case nil:
				case *serviceerror.NamespaceNotFound:
					// If child namespace is deleted there is nothing to close.
					continue
				default:
					return err
				}
			}

			executions = append(executions, parentclosepolicy.RequestDetail{
				Namespace:   childInfo.Namespace,
				NamespaceID: childNamespaceID.String(),
				WorkflowID:  childInfo.StartedWorkflowId,
				RunID:       childInfo.StartedRunId,
				Policy:      childInfo.ParentClosePolicy,
			})
		}

		if len(executions) == 0 {
			return nil
		}

		request := parentclosepolicy.Request{
			ParentExecution: parentExecution,
			Executions:      executions,
		}
		return t.parentClosePolicyClient.SendParentClosePolicyRequest(ctx, request)
	}

	for _, childInfo := range childInfos {
		err := t.applyParentClosePolicy(ctx, parentExecution, childInfo)
		switch err.(type) {
		case nil:
			metrics.ParentClosePolicyProcessorSuccess.With(scope).Record(1)
		case *serviceerror.NotFound:
			// If child execution is deleted there is nothing to close.
		case *serviceerror.NamespaceNotFound:
			// If child namespace is deleted there is nothing to close.
		default:
			metrics.ParentClosePolicyProcessorFailures.With(scope).Record(1)
			return err
		}
	}
	return nil
}

func (t *transferQueueActiveTaskExecutor) applyParentClosePolicy(
	ctx context.Context,
	parentExecution *commonpb.WorkflowExecution,
	childInfo *persistencespb.ChildExecutionInfo,
) error {
	switch childInfo.ParentClosePolicy {
	case enumspb.PARENT_CLOSE_POLICY_ABANDON:
		// noop
		return nil

	case enumspb.PARENT_CLOSE_POLICY_TERMINATE:
		childNamespaceID := namespace.ID(childInfo.GetNamespaceId())
		if childNamespaceID.IsEmpty() {
			// TODO (alex): Remove after childInfo.NamespaceId is back filled. Backward compatibility: old childInfo doesn't have NamespaceId set.
			// TODO (alex): consider reverse lookup of namespace name from ID but namespace name is not actually used.
			var err error
			childNamespaceID, err = t.registry.GetNamespaceID(namespace.Name(childInfo.GetNamespace()))
			if err != nil {
				return err
			}
		}
		_, err := t.historyRawClient.TerminateWorkflowExecution(ctx, &historyservice.TerminateWorkflowExecutionRequest{
			NamespaceId: childNamespaceID.String(),
			TerminateRequest: &workflowservice.TerminateWorkflowExecutionRequest{
				Namespace: childInfo.GetNamespace(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: childInfo.GetStartedWorkflowId(),
				},
				// Include StartedRunID as FirstExecutionRunID on the request to allow child to be terminated across runs.
				// If the child does continue as new it still propagates the RunID of first execution.
				FirstExecutionRunId: childInfo.GetStartedRunId(),
				Reason:              "by parent close policy",
				Identity:            consts.IdentityHistoryService,
			},
			ExternalWorkflowExecution: parentExecution,
			ChildWorkflowOnly:         true,
		})
		return err

	case enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL:
		childNamespaceID := namespace.ID(childInfo.GetNamespaceId())
		if childNamespaceID.IsEmpty() {
			// TODO (alex): Remove after childInfo.NamespaceId is back filled. Backward compatibility: old childInfo doesn't have NamespaceId set.
			// TODO (alex): consider reverse lookup of namespace name from ID but namespace name is not actually used.
			var err error
			childNamespaceID, err = t.registry.GetNamespaceID(namespace.Name(childInfo.GetNamespace()))
			if err != nil {
				return err
			}
		}

		_, err := t.historyRawClient.RequestCancelWorkflowExecution(ctx, &historyservice.RequestCancelWorkflowExecutionRequest{
			NamespaceId: childNamespaceID.String(),
			CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
				Namespace: childInfo.GetNamespace(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: childInfo.GetStartedWorkflowId(),
				},
				// Include StartedRunID as FirstExecutionRunID on the request to allow child to be canceled across runs.
				// If the child does continue as new it still propagates the RunID of first execution.
				FirstExecutionRunId: childInfo.GetStartedRunId(),
				Identity:            consts.IdentityHistoryService,
			},
			ExternalWorkflowExecution: parentExecution,
			ChildWorkflowOnly:         true,
		})
		return err

	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown parent close policy: %v", childInfo.ParentClosePolicy))
	}
}

func (t *transferQueueActiveTaskExecutor) targetNamespaceEntryHelper(
	targetNamespaceID namespace.ID,
	targetNamespaceName namespace.Name, // fallback if targetNamespaceID is not available.
) (*namespace.Namespace, error) {
	if targetNamespaceID == "" {
		// This is for backward compatibility.
		// Old mutable state / event may not have the target namespace ID set.

		targetNamespaceEntry, err := t.registry.GetNamespace(targetNamespaceName)
		if err != nil {
			if _, isNotFound := err.(*serviceerror.NamespaceNotFound); !isNotFound {
				return nil, err
			}

			t.logger.Debug("Target namespace is not found.", tag.WorkflowNamespace(targetNamespaceName.String()))
			return nil, nil
		}

		return targetNamespaceEntry, nil
	}

	targetNamespaceEntry, err := t.registry.GetNamespaceByID(targetNamespaceID)
	if err != nil {
		if _, isNotFound := err.(*serviceerror.NamespaceNotFound); !isNotFound {
			return nil, err
		}

		t.logger.Debug("Target namespace is not found.", tag.WorkflowNamespaceID(targetNamespaceID.String()))
		return nil, nil
	}
	return targetNamespaceEntry, nil
}

func copyChildWorkflowInfos(
	input map[int64]*persistencespb.ChildExecutionInfo,
) map[int64]*persistencespb.ChildExecutionInfo {
	result := make(map[int64]*persistencespb.ChildExecutionInfo)
	if input == nil {
		return result
	}

	for k, v := range input {
		result[k] = common.CloneProto(v)
	}
	return result
}
