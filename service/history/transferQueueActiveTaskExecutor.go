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
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	ns "go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/worker/parentclosepolicy"
)

type (
	transferQueueActiveTaskExecutor struct {
		*transferQueueTaskExecutorBase

		registry                namespace.Registry
		historyClient           historyservice.HistoryServiceClient
		parentClosePolicyClient parentclosepolicy.Client
	}
)

func newTransferQueueActiveTaskExecutor(
	shard shard.Context,
	historyEngine *historyEngineImpl,
	logger log.Logger,
	metricsClient metrics.Client,
	config *configs.Config,
	matchingClient matchingservice.MatchingServiceClient,
	registry namespace.Registry,
) queueTaskExecutor {
	return &transferQueueActiveTaskExecutor{
		transferQueueTaskExecutorBase: newTransferQueueTaskExecutorBase(
			shard,
			historyEngine,
			logger,
			metricsClient,
			config,
			matchingClient,
		),
		historyClient: shard.GetHistoryClient(),
		parentClosePolicyClient: parentclosepolicy.NewClient(
			shard.GetMetricsClient(),
			shard.GetLogger(),
			historyEngine.publicClient,
			config.NumParentClosePolicySystemWorkflows(),
		),
		registry: registry,
	}
}

func (t *transferQueueActiveTaskExecutor) execute(
	ctx context.Context,
	taskInfo tasks.Task,
	shouldProcessTask bool,
) error {

	if !shouldProcessTask {
		return nil
	}

	switch task := taskInfo.(type) {
	case *tasks.ActivityTask:
		return t.processActivityTask(ctx, task)
	case *tasks.WorkflowTask:
		return t.processWorkflowTask(ctx, task)
	case *tasks.CloseExecutionTask:
		return t.processCloseExecution(ctx, task)
	case *tasks.CancelExecutionTask:
		return t.processCancelExecution(ctx, task)
	case *tasks.SignalExecutionTask:
		return t.processSignalExecution(ctx, task)
	case *tasks.StartChildExecutionTask:
		return t.processStartChildExecution(ctx, task)
	case *tasks.ResetWorkflowTask:
		return t.processResetWorkflow(ctx, task)
	default:
		return errUnknownTransferTask
	}
}

func (t *transferQueueActiveTaskExecutor) processActivityTask(
	pctx context.Context,
	task *tasks.ActivityTask,
) (retError error) {

	ctx, cancel := context.WithTimeout(pctx, taskTimeout)
	defer cancel()
	namespaceID, execution := t.getNamespaceIDAndWorkflowExecution(task)
	context, release, err := t.cache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		workflow.CallerTypeTask,
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	ai, ok := mutableState.GetActivityInfo(task.ScheduleID)
	if !ok {
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, namespace.ID(task.NamespaceID), ai.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	timeout := timestamp.DurationValue(ai.ScheduleToStartTimeout)

	// NOTE: do not access anything related mutable state after this lock release
	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	return t.pushActivity(task, &timeout)
}

func (t *transferQueueActiveTaskExecutor) processWorkflowTask(
	pctx context.Context,
	task *tasks.WorkflowTask,
) (retError error) {

	ctx, cancel := context.WithTimeout(pctx, taskTimeout)
	defer cancel()
	namespaceID, execution := t.getNamespaceIDAndWorkflowExecution(task)
	context, release, err := t.cache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		workflow.CallerTypeTask,
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	workflowTask, found := mutableState.GetWorkflowTaskInfo(task.ScheduleID)
	if !found {
		return nil
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, namespace.ID(task.NamespaceID), workflowTask.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	executionInfo := mutableState.GetExecutionInfo()

	// NOTE: previously this section check whether mutable state has enabled
	// sticky workflowTask, if so convert the workflowTask to a sticky workflowTask.
	// that logic has a bug which timer task for that sticky workflowTask is not generated
	// the correct logic should check whether the workflow task is a sticky workflowTask
	// task or not.
	var taskQueue *taskqueuepb.TaskQueue
	var taskScheduleToStartTimeoutSeconds = int64(0)
	if mutableState.GetExecutionInfo().TaskQueue != task.TaskQueue {
		// this workflowTask is an sticky workflowTask
		// there shall already be an timer set
		taskQueue = &taskqueuepb.TaskQueue{
			Name: task.TaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_STICKY,
		}
		taskScheduleToStartTimeoutSeconds = int64(timestamp.DurationValue(executionInfo.StickyScheduleToStartTimeout).Seconds())
	} else {
		taskQueue = &taskqueuepb.TaskQueue{
			Name: task.TaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}
		workflowRunTimeout := timestamp.DurationValue(executionInfo.WorkflowRunTimeout)
		taskScheduleToStartTimeoutSeconds = int64(workflowRunTimeout.Round(time.Second).Seconds())
	}

	// NOTE: do not access anything related mutable state after this lock release
	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	return t.pushWorkflowTask(task, taskQueue, timestamp.DurationFromSeconds(taskScheduleToStartTimeoutSeconds))
}

func (t *transferQueueActiveTaskExecutor) processCloseExecution(
	pctx context.Context,
	task *tasks.CloseExecutionTask,
) (retError error) {

	ctx, cancel := context.WithTimeout(pctx, taskTimeout)
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

	mutableState, err := loadMutableStateForTransferTask(weContext, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	lastWriteVersion, err := mutableState.GetLastWriteVersion()
	if err != nil {
		return err
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, namespace.ID(task.NamespaceID), lastWriteVersion, task.Version, task)
	if err != nil || !ok {
		return err
	}

	executionInfo := mutableState.GetExecutionInfo()
	executionState := mutableState.GetExecutionState()
	replyToParentWorkflow := mutableState.HasParentExecution() && executionInfo.NewExecutionRunId == ""
	completionEvent, err := mutableState.GetCompletionEvent()
	if err != nil {
		return err
	}
	wfCloseTime := timestamp.TimeValue(completionEvent.GetEventTime())

	parentNamespaceID := executionInfo.ParentNamespaceId
	parentWorkflowID := executionInfo.ParentWorkflowId
	parentRunID := executionInfo.ParentRunId
	initiatedID := executionInfo.InitiatedId

	workflowTypeName := executionInfo.WorkflowTypeName
	workflowCloseTime := wfCloseTime
	workflowStatus := executionState.Status
	workflowHistoryLength := mutableState.GetNextEventID() - 1

	workflowStartTime := timestamp.TimeValue(mutableState.GetExecutionInfo().GetStartTime())
	workflowExecutionTime := timestamp.TimeValue(mutableState.GetExecutionInfo().GetExecutionTime())
	visibilityMemo := getWorkflowMemo(copyMemo(executionInfo.Memo))
	searchAttr := getSearchAttributes(copySearchAttributes(executionInfo.SearchAttributes))
	namespaceName := mutableState.GetNamespaceEntry().Name()
	children := copyChildWorkflowInfos(mutableState.GetPendingChildExecutionInfos())

	// NOTE: do not access anything related mutable state after this lock release
	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	err = t.recordWorkflowClosed(
		namespace.ID(task.NamespaceID),
		task.WorkflowID,
		task.RunID,
		workflowTypeName,
		workflowStartTime,
		workflowExecutionTime,
		workflowCloseTime,
		workflowStatus,
		workflowHistoryLength,
		visibilityMemo,
		searchAttr,
	)
	if err != nil {
		return err
	}

	// Communicate the result to parent execution if this is Child Workflow execution
	if replyToParentWorkflow {
		ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
		defer cancel()
		_, err := t.historyClient.RecordChildExecutionCompleted(ctx, &historyservice.RecordChildExecutionCompletedRequest{
			NamespaceId: parentNamespaceID,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: parentWorkflowID,
				RunId:      parentRunID,
			},
			InitiatedId: initiatedID,
			CompletedExecution: &commonpb.WorkflowExecution{
				WorkflowId: task.WorkflowID,
				RunId:      task.RunID,
			},
			CompletionEvent: completionEvent,
		})
		switch err.(type) {
		case nil:
			// noop
		case *serviceerror.NotFound:
			// parent gone, noop
		default:
			return err
		}
	}

	return t.processParentClosePolicy(namespaceID.String(), namespaceName.String(), children)
}

func (t *transferQueueActiveTaskExecutor) processCancelExecution(
	pctx context.Context,
	task *tasks.CancelExecutionTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(pctx, taskTimeout)
	defer cancel()

	namespaceID, execution := t.getNamespaceIDAndWorkflowExecution(task)
	executionContext, release, err := t.cache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		workflow.CallerTypeTask,
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(executionContext, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	requestCancelInfo, ok := mutableState.GetRequestCancelInfo(task.InitiatedID)
	if !ok {
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, namespace.ID(task.NamespaceID), requestCancelInfo.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	targetNamespaceEntry, err := t.shard.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(task.TargetNamespaceID))
	if err != nil {
		return err
	}
	targetNamespace := targetNamespaceEntry.Name()

	// handle workflow cancel itself
	if task.NamespaceID == task.TargetNamespaceID && task.WorkflowID == task.TargetWorkflowID {
		// it does not matter if the run ID is a mismatch
		err = t.requestCancelExternalExecutionFailed(task, executionContext, targetNamespace, task.TargetWorkflowID, task.TargetRunID)
		if _, ok := err.(*serviceerror.NotFound); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil
		}
		return err
	}

	if err = t.requestCancelExternalExecutionWithRetry(
		task,
		targetNamespace,
		requestCancelInfo,
	); err != nil {
		t.logger.Debug(fmt.Sprintf("Failed to cancel external workflow execution. Error: %v", err))

		// Check to see if the error is non-transient, in which case add RequestCancelFailed
		// event and complete transfer task by setting the err = nil
		if common.IsServiceTransientError(err) || common.IsContextDeadlineExceededErr(err) {
			// for retryable error just return
			return err
		}
		return t.requestCancelExternalExecutionFailed(
			task,
			executionContext,
			targetNamespace,
			task.TargetWorkflowID,
			task.TargetRunID,
		)
	}

	// Record ExternalWorkflowExecutionCancelRequested in source execution
	return t.requestCancelExternalExecutionCompleted(
		task,
		executionContext,
		targetNamespace,
		task.TargetWorkflowID,
		task.TargetRunID,
	)
}

func (t *transferQueueActiveTaskExecutor) processSignalExecution(
	pctx context.Context,
	task *tasks.SignalExecutionTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(pctx, taskTimeout)
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

	mutableState, err := loadMutableStateForTransferTask(weContext, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	signalInfo, ok := mutableState.GetSignalInfo(task.InitiatedID)
	if !ok {
		// TODO: here we should also RemoveSignalMutableState from target workflow
		// Otherwise, target SignalRequestID still can leak if shard restart after signalExternalExecutionCompleted
		// To do that, probably need to add the SignalRequestID in transfer task.
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, namespace.ID(task.NamespaceID), signalInfo.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	initiatedEvent, err := mutableState.GetSignalExternalInitiatedEvent(task.InitiatedID)
	if err != nil {
		return err
	}
	attributes := initiatedEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes()

	targetNamespaceEntry, err := t.shard.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(task.TargetNamespaceID))
	if err != nil {
		return err
	}
	targetNamespace := targetNamespaceEntry.Name()

	// handle workflow signal itself
	if task.NamespaceID == task.TargetNamespaceID && task.WorkflowID == task.TargetWorkflowID {
		// it does not matter if the run ID is a mismatch
		return t.signalExternalExecutionFailed(
			task,
			weContext,
			targetNamespace,
			task.TargetWorkflowID,
			task.TargetRunID,
			attributes.Control,
		)
	}

	if err = t.signalExternalExecutionWithRetry(
		task,
		targetNamespace,
		signalInfo,
		attributes,
	); err != nil {
		t.logger.Debug("Failed to signal external workflow execution", tag.Error(err))

		// Check to see if the error is non-transient, in which case add SignalFailed
		// event and complete transfer task by setting the err = nil
		if common.IsServiceTransientError(err) || common.IsContextDeadlineExceededErr(err) {
			// for retryable error just return
			return err
		}
		return t.signalExternalExecutionFailed(
			task,
			weContext,
			targetNamespace,
			task.TargetWorkflowID,
			task.TargetRunID,
			attributes.Control,
		)
	}

	err = t.signalExternalExecutionCompleted(
		task,
		weContext,
		targetNamespace,
		task.TargetWorkflowID,
		task.TargetRunID,
		attributes.Control,
	)
	if err != nil {
		return err
	}

	signalRequestID := signalInfo.GetRequestId()

	// release the weContext lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(retError)
	// remove signalRequestedID from target workflow, after Signal detail is removed from source workflow
	ctx, cancel = context.WithTimeout(context.Background(), taskTimeout)
	defer cancel()
	_, err = t.historyClient.RemoveSignalMutableState(ctx, &historyservice.RemoveSignalMutableStateRequest{
		NamespaceId: task.TargetNamespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: task.TargetWorkflowID,
			RunId:      task.TargetRunID,
		},
		RequestId: signalRequestID,
	})
	return err
}

func (t *transferQueueActiveTaskExecutor) processStartChildExecution(
	pctx context.Context,
	task *tasks.StartChildExecutionTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(pctx, taskTimeout)
	defer cancel()

	namespaceID, execution := t.getNamespaceIDAndWorkflowExecution(task)
	context, release, err := t.cache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		workflow.CallerTypeTask,
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	// Get parent namespace name
	var parentNamespaceName namespace.Name
	if namespaceEntry, err := t.shard.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(task.NamespaceID)); err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			return err
		}
		// it is possible that the namespace got deleted. Use namespaceID instead as this is only needed for the history event
		parentNamespaceName = namespace.Name(task.NamespaceID)
	} else {
		parentNamespaceName = namespaceEntry.Name()
	}

	// Get target namespace name
	var targetNamespaceName namespace.Name
	if namespaceEntry, err := t.shard.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(task.TargetNamespaceID)); err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			return err
		}
		// it is possible that the namespace got deleted. Use namespaceID instead as this is only needed for the history event
		targetNamespaceName = namespace.Name(task.TargetNamespaceID)
	} else {
		targetNamespaceName = namespaceEntry.Name()
	}

	childInfo, ok := mutableState.GetChildExecutionInfo(task.InitiatedID)
	if !ok {
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, namespace.ID(task.NamespaceID), childInfo.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	initiatedEvent, err := mutableState.GetChildExecutionInitiatedEvent(task.InitiatedID)
	if err != nil {
		return err
	}
	attributes := initiatedEvent.GetStartChildWorkflowExecutionInitiatedEventAttributes()

	// ChildExecution already started, just create WorkflowTask and complete transfer task
	if childInfo.StartedId != common.EmptyEventID {
		childExecution := &commonpb.WorkflowExecution{
			WorkflowId: childInfo.StartedWorkflowId,
			RunId:      childInfo.StartedRunId,
		}
		// NOTE: do not access anything related mutable state after this lock release
		// release the context lock since we no longer need mutable state builder and
		// the rest of logic is making RPC call, which takes time.
		release(nil)
		return t.createFirstWorkflowTask(task.TargetNamespaceID, childExecution)
	}

	childRunID, err := t.startWorkflowWithRetry(
		task,
		parentNamespaceName,
		targetNamespaceName,
		childInfo,
		attributes,
	)
	if err != nil {
		t.logger.Debug("Failed to start child workflow execution", tag.Error(err))

		// Check to see if the error is non-transient, in which case add StartChildWorkflowExecutionFailed
		// event and complete transfer task by setting the err = nil
		if _, ok := err.(*serviceerror.WorkflowExecutionAlreadyStarted); ok {
			err = t.recordStartChildExecutionFailed(task, context, attributes)
		}

		return err
	}

	t.logger.Debug("Child Execution started successfully",
		tag.WorkflowID(attributes.WorkflowId), tag.WorkflowRunID(childRunID))

	// Child execution is successfully started, record ChildExecutionStartedEvent in parent execution
	err = t.recordChildExecutionStarted(task, context, attributes, childRunID)
	if err != nil {
		return err
	}

	// NOTE: do not access anything related mutable state after this lock release
	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	return t.createFirstWorkflowTask(task.TargetNamespaceID, &commonpb.WorkflowExecution{
		WorkflowId: task.TargetWorkflowID,
		RunId:      childRunID,
	})
}

func (t *transferQueueActiveTaskExecutor) processResetWorkflow(
	pctx context.Context,
	task *tasks.ResetWorkflowTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(pctx, taskTimeout)
	defer cancel()

	namespaceID, execution := t.getNamespaceIDAndWorkflowExecution(task)
	currentContext, currentRelease, err := t.cache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		workflow.CallerTypeTask,
	)
	if err != nil {
		return err
	}
	defer func() { currentRelease(retError) }()

	currentMutableState, err := loadMutableStateForTransferTask(currentContext, task, t.metricsClient, t.logger)
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
		resp, err = t.shard.GetExecutionManager().GetCurrentExecution(&persistence.GetCurrentExecutionRequest{
			ShardID:     t.shard.GetShardID(),
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

	currentStartVersion, err := currentMutableState.GetStartVersion()
	if err != nil {
		return err
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, namespace.ID(task.NamespaceID), currentStartVersion, task.Version, task)
	if err != nil || !ok {
		return err
	}

	executionInfo := currentMutableState.GetExecutionInfo()
	executionState := currentMutableState.GetExecutionState()
	namespaceEntry, err := t.shard.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(executionInfo.NamespaceId))
	if err != nil {
		return err
	}
	logger = log.With(logger, tag.WorkflowNamespace(namespaceEntry.Name().String()))

	reason, resetPoint := workflow.FindAutoResetPoint(t.shard.GetTimeSource(), namespaceEntry.VerifyBinaryChecksum, executionInfo.AutoResetPoints)
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

	var baseContext workflow.Context
	var baseMutableState workflow.MutableState
	var baseRelease workflow.ReleaseCacheFunc
	if resetPoint.GetRunId() == executionState.RunId {
		baseContext = currentContext
		baseMutableState = currentMutableState
		baseRelease = currentRelease
	} else {
		baseExecution := &commonpb.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      resetPoint.GetRunId(),
		}
		ctx, cancel := context.WithTimeout(context.Background(), taskTimeout)
		defer cancel()
		baseContext, baseRelease, err = t.cache.GetOrCreateWorkflowExecution(
			ctx,
			namespace.ID(task.NamespaceID),
			*baseExecution,
			workflow.CallerTypeTask,
		)
		if err != nil {
			return err
		}
		defer func() { baseRelease(retError) }()
		baseMutableState, err = loadMutableStateForTransferTask(baseContext, task, t.metricsClient, t.logger)
		if err != nil {
			return err
		}
		if baseMutableState == nil {
			return nil
		}
	}

	if err := t.resetWorkflow(
		task,
		reason,
		resetPoint,
		baseMutableState,
		currentContext,
		currentMutableState,
		logger,
	); err != nil {
		return err
	}
	return nil
}

func (t *transferQueueActiveTaskExecutor) recordChildExecutionStarted(
	task *tasks.StartChildExecutionTask,
	context workflow.Context,
	initiatedAttributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes,
	runID string,
) error {

	return t.updateWorkflowExecution(context, true,
		func(mutableState workflow.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return serviceerror.NewNotFound("Workflow execution already completed.")
			}

			namespace := namespace.Name(initiatedAttributes.Namespace)
			ci, ok := mutableState.GetChildExecutionInfo(task.InitiatedID)
			if !ok || ci.StartedId != common.EmptyEventID {
				return serviceerror.NewNotFound("Pending child execution not found.")
			}

			_, err := mutableState.AddChildWorkflowExecutionStartedEvent(
				namespace,
				&commonpb.WorkflowExecution{
					WorkflowId: task.TargetWorkflowID,
					RunId:      runID,
				},
				initiatedAttributes.WorkflowType,
				task.InitiatedID,
				initiatedAttributes.Header,
			)

			return err
		})
}

func (t *transferQueueActiveTaskExecutor) recordStartChildExecutionFailed(
	task *tasks.StartChildExecutionTask,
	context workflow.Context,
	initiatedAttributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes,
) error {

	return t.updateWorkflowExecution(context, true,
		func(mutableState workflow.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return serviceerror.NewNotFound("Workflow execution already completed.")
			}

			ci, ok := mutableState.GetChildExecutionInfo(task.InitiatedID)
			if !ok || ci.StartedId != common.EmptyEventID {
				return serviceerror.NewNotFound("Pending child execution not found.")
			}

			_, err := mutableState.AddStartChildWorkflowExecutionFailedEvent(
				task.InitiatedID,
				enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS,
				initiatedAttributes,
			)
			return err
		})
}

// createFirstWorkflowTask is used by StartChildExecution transfer task to create the first workflow task for
// child execution.
func (t *transferQueueActiveTaskExecutor) createFirstWorkflowTask(
	namespaceID string,
	execution *commonpb.WorkflowExecution,
) error {

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()
	_, err := t.historyClient.ScheduleWorkflowTask(ctx, &historyservice.ScheduleWorkflowTaskRequest{
		NamespaceId:         namespaceID,
		WorkflowExecution:   execution,
		IsFirstWorkflowTask: true,
	})

	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			// Maybe child workflow execution already timed out or terminated
			// Safe to discard the error and complete this transfer task
			return nil
		}
	}

	return err
}

func (t *transferQueueActiveTaskExecutor) requestCancelExternalExecutionCompleted(
	task *tasks.CancelExecutionTask,
	context workflow.Context,
	targetNamespace namespace.Name,
	targetWorkflowID string,
	targetRunID string,
) error {

	err := t.updateWorkflowExecution(context, true,
		func(mutableState workflow.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return &serviceerror.NotFound{Message: "Workflow execution already completed."}
			}

			_, ok := mutableState.GetRequestCancelInfo(task.InitiatedID)
			if !ok {
				return workflow.ErrMissingRequestCancelInfo
			}

			_, err := mutableState.AddExternalWorkflowExecutionCancelRequested(
				task.InitiatedID,
				targetNamespace,
				targetWorkflowID,
				targetRunID,
			)
			return err
		})

	if _, ok := err.(*serviceerror.NotFound); ok {
		// this could happen if this is a duplicate processing of the task,
		// or the execution has already completed.
		return nil
	}
	return err
}

func (t *transferQueueActiveTaskExecutor) signalExternalExecutionCompleted(
	task *tasks.SignalExecutionTask,
	context workflow.Context,
	targetNamespace namespace.Name,
	targetWorkflowID string,
	targetRunID string,
	control string,
) error {

	err := t.updateWorkflowExecution(context, true,
		func(mutableState workflow.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return &serviceerror.NotFound{Message: "Workflow execution already completed."}
			}

			_, ok := mutableState.GetSignalInfo(task.InitiatedID)
			if !ok {
				return workflow.ErrMissingSignalInfo
			}

			_, err := mutableState.AddExternalWorkflowExecutionSignaled(
				task.InitiatedID,
				targetNamespace,
				targetWorkflowID,
				targetRunID,
				control,
			)
			return err
		})

	if _, ok := err.(*serviceerror.NotFound); ok {
		// this could happen if this is a duplicate processing of the task,
		// or the execution has already completed.
		return nil
	}
	return err
}

func (t *transferQueueActiveTaskExecutor) requestCancelExternalExecutionFailed(
	task *tasks.CancelExecutionTask,
	context workflow.Context,
	targetNamespace namespace.Name,
	targetWorkflowID string,
	targetRunID string,
) error {

	err := t.updateWorkflowExecution(context, true,
		func(mutableState workflow.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return &serviceerror.NotFound{Message: "Workflow execution already completed."}
			}

			_, ok := mutableState.GetRequestCancelInfo(task.InitiatedID)
			if !ok {
				return workflow.ErrMissingRequestCancelInfo
			}

			_, err := mutableState.AddRequestCancelExternalWorkflowExecutionFailedEvent(
				task.InitiatedID,
				targetNamespace,
				targetWorkflowID,
				targetRunID,
				enumspb.CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
			)
			return err
		})

	if _, ok := err.(*serviceerror.NotFound); ok {
		// this could happen if this is a duplicate processing of the task,
		// or the execution has already completed.
		return nil
	}
	return err
}

func (t *transferQueueActiveTaskExecutor) signalExternalExecutionFailed(
	task *tasks.SignalExecutionTask,
	context workflow.Context,
	targetNamespace namespace.Name,
	targetWorkflowID string,
	targetRunID string,
	control string,
) error {

	err := t.updateWorkflowExecution(context, true,
		func(mutableState workflow.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return &serviceerror.NotFound{Message: "Workflow is not running."}
			}

			_, ok := mutableState.GetSignalInfo(task.InitiatedID)
			if !ok {
				return workflow.ErrMissingSignalInfo
			}

			_, err := mutableState.AddSignalExternalWorkflowExecutionFailedEvent(
				task.InitiatedID,
				targetNamespace,
				targetWorkflowID,
				targetRunID,
				control,
				enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
			)
			return err
		})

	if _, ok := err.(*serviceerror.NotFound); ok {
		// this could happen if this is a duplicate processing of the task,
		// or the execution has already completed.
		return nil
	}
	return err
}

func (t *transferQueueActiveTaskExecutor) updateWorkflowExecution(
	context workflow.Context,
	createWorkflowTask bool,
	action func(builder workflow.MutableState) error,
) error {

	mutableState, err := context.LoadWorkflowExecution()
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

	return context.UpdateWorkflowExecutionAsActive(t.shard.GetTimeSource().Now())
}

func (t *transferQueueActiveTaskExecutor) requestCancelExternalExecutionWithRetry(
	task *tasks.CancelExecutionTask,
	targetNamespace namespace.Name,
	requestCancelInfo *persistencespb.RequestCancelInfo,
) error {

	request := &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: task.TargetNamespaceID,
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			Namespace: targetNamespace.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: task.TargetWorkflowID,
				RunId:      task.TargetRunID,
			},
			Identity: consts.IdentityHistoryService,
			// Use the same request ID to dedupe RequestCancelWorkflowExecution calls
			RequestId: requestCancelInfo.GetCancelRequestId(),
		},
		ExternalInitiatedEventId: task.InitiatedID,
		ExternalWorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      task.RunID,
		},
		ChildWorkflowOnly: task.TargetChildWorkflowOnly,
	}

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()
	op := func() error {
		_, err := t.historyClient.RequestCancelWorkflowExecution(ctx, request)
		return err
	}

	err := backoff.Retry(op, workflow.PersistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	return err
}

func (t *transferQueueActiveTaskExecutor) signalExternalExecutionWithRetry(
	task *tasks.SignalExecutionTask,
	targetNamespace namespace.Name,
	signalInfo *persistencespb.SignalInfo,
	attributes *historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes,
) error {

	request := &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: task.TargetNamespaceID,
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace: targetNamespace.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: task.TargetWorkflowID,
				RunId:      task.TargetRunID,
			},
			Identity:   consts.IdentityHistoryService,
			SignalName: attributes.SignalName,
			Input:      attributes.Input,
			// Use same request ID to deduplicate SignalWorkflowExecution calls
			RequestId: signalInfo.GetRequestId(),
			Control:   attributes.Control,
			Header:    attributes.Header,
		},
		ExternalWorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      task.RunID,
		},
		ChildWorkflowOnly: task.TargetChildWorkflowOnly,
	}

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()
	op := func() error {
		_, err := t.historyClient.SignalWorkflowExecution(ctx, request)
		return err
	}

	return backoff.Retry(op, workflow.PersistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (t *transferQueueActiveTaskExecutor) startWorkflowWithRetry(
	task *tasks.StartChildExecutionTask,
	namespace namespace.Name,
	targetNamespace namespace.Name,
	childInfo *persistencespb.ChildExecutionInfo,
	attributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes,
) (string, error) {
	request := common.CreateHistoryStartWorkflowRequest(
		task.TargetNamespaceID,
		&workflowservice.StartWorkflowExecutionRequest{
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
			RequestId:             childInfo.CreateRequestId,
			WorkflowIdReusePolicy: attributes.WorkflowIdReusePolicy,
			RetryPolicy:           attributes.RetryPolicy,
			CronSchedule:          attributes.CronSchedule,
			Memo:                  attributes.Memo,
			SearchAttributes:      attributes.SearchAttributes,
		},
		&workflowspb.ParentExecutionInfo{
			NamespaceId: task.NamespaceID,
			Namespace:   namespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: task.WorkflowID,
				RunId:      task.RunID,
			},
			InitiatedId: task.InitiatedID,
		},
		t.shard.GetTimeSource().Now(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()
	var response *historyservice.StartWorkflowExecutionResponse
	var err error
	op := func() error {
		response, err = t.historyClient.StartWorkflowExecution(ctx, request)
		return err
	}

	err = backoff.Retry(op, workflow.PersistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return "", err
	}
	return response.GetRunId(), nil
}

func (t *transferQueueActiveTaskExecutor) resetWorkflow(
	task *tasks.ResetWorkflowTask,
	reason string,
	resetPoint *workflowpb.ResetPointInfo,
	baseMutableState workflow.MutableState,
	currentContext workflow.Context,
	currentMutableState workflow.MutableState,
	logger log.Logger,
) error {

	var err error
	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
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

	err = t.historyService.workflowResetter.resetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		baseRunID,
		baseCurrentBranchToken,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		baseNextEventID,
		resetRunID,
		uuid.New(),
		newNDCWorkflow(
			ctx,
			t.shard.GetNamespaceRegistry(),
			t.shard.GetClusterMetadata(),
			currentContext,
			currentMutableState,
			workflow.NoopReleaseFn, // this is fine since caller will defer on release
		),
		reason,
		nil,
		enumspb.RESET_REAPPLY_TYPE_SIGNAL,
	)

	switch err.(type) {
	case nil:
		return nil

	case *serviceerror.NotFound:
		// This means the reset point is corrupted and not retry able.
		// There must be a bug in our system that we must fix.(for example, history is not the same in active/passive)
		t.metricsClient.IncCounter(metrics.TransferQueueProcessorScope, metrics.AutoResetPointCorruptionCounter)
		logger.Error("Auto-Reset workflow failed and not retryable. The reset point is corrupted.", tag.Error(err))
		return nil

	default:
		// log this error and retry
		logger.Error("Auto-Reset workflow failed", tag.Error(err))
		return err
	}
}

func (t *transferQueueActiveTaskExecutor) processParentClosePolicy(
	namespaceID string,
	namespace string,
	childInfos map[int64]*persistencespb.ChildExecutionInfo,
) error {

	if len(childInfos) == 0 {
		return nil
	}

	scope := t.metricsClient.Scope(metrics.TransferActiveTaskCloseExecutionScope)

	if t.shard.GetConfig().EnableParentClosePolicyWorker() &&
		len(childInfos) >= t.shard.GetConfig().ParentClosePolicyThreshold(namespace) {

		executions := make([]parentclosepolicy.RequestDetail, 0, len(childInfos))
		for _, childInfo := range childInfos {
			if childInfo.ParentClosePolicy == enumspb.PARENT_CLOSE_POLICY_ABANDON {
				continue
			}

			childNamespaceId, err := t.registry.GetNamespaceID(ns.Name(childInfo.GetNamespace()))
			if err != nil {
				return err
			}

			executions = append(executions, parentclosepolicy.RequestDetail{
				Namespace:   childInfo.Namespace,
				NamespaceID: string(childNamespaceId),
				WorkflowID:  childInfo.StartedWorkflowId,
				RunID:       childInfo.StartedRunId,
				Policy:      childInfo.ParentClosePolicy,
			})
		}

		if len(executions) == 0 {
			return nil
		}

		request := parentclosepolicy.Request{
			Namespace:   namespace,
			NamespaceID: namespaceID,
			Executions:  executions,
		}
		return t.parentClosePolicyClient.SendParentClosePolicyRequest(request)
	}

	for _, childInfo := range childInfos {
		if err := t.applyParentClosePolicy(
			childInfo,
		); err != nil {
			if _, ok := err.(*serviceerror.NotFound); !ok {
				scope.IncCounter(metrics.ParentClosePolicyProcessorFailures)
				return err
			}
		}
		scope.IncCounter(metrics.ParentClosePolicyProcessorSuccess)
	}
	return nil
}

func (t *transferQueueActiveTaskExecutor) applyParentClosePolicy(
	childInfo *persistencespb.ChildExecutionInfo,
) error {

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()

	switch childInfo.ParentClosePolicy {
	case enumspb.PARENT_CLOSE_POLICY_ABANDON:
		// noop
		return nil

	case enumspb.PARENT_CLOSE_POLICY_TERMINATE:
		childNamespaceId, err := t.registry.GetNamespaceID(ns.Name(childInfo.GetNamespace()))
		if err != nil {
			return err
		}
		_, err = t.historyClient.TerminateWorkflowExecution(ctx, &historyservice.TerminateWorkflowExecutionRequest{
			NamespaceId: string(childNamespaceId),
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
		})
		return err

	case enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL:
		nsId, err := t.registry.GetNamespaceID(ns.Name(childInfo.GetNamespace()))
		if err != nil {
			return err
		}

		_, err = t.historyClient.RequestCancelWorkflowExecution(ctx, &historyservice.RequestCancelWorkflowExecutionRequest{
			NamespaceId: string(nsId),
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
		})
		return err

	default:
		return serviceerror.NewInternal(
			fmt.Sprintf("unknown parent close policy: %v", childInfo.ParentClosePolicy),
		)
	}
}

func copyChildWorkflowInfos(
	input map[int64]*persistencespb.ChildExecutionInfo,
) map[int64]*persistencespb.ChildExecutionInfo {

	result := make(map[int64]*persistencespb.ChildExecutionInfo)
	if input == nil {
		return result
	}

	for k, v := range input {
		result[k] = proto.Clone(v).(*persistencespb.ChildExecutionInfo)
	}
	return result
}
