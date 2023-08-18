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
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	visibilityQueueTaskExecutor struct {
		shard          shard.Context
		cache          wcache.Cache
		logger         log.Logger
		metricProvider metrics.Handler
		visibilityMgr  manager.VisibilityManager

		ensureCloseBeforeDelete    dynamicconfig.BoolPropertyFn
		enableCloseWorkflowCleanup dynamicconfig.BoolPropertyFnWithNamespaceFilter
	}
)

var errUnknownVisibilityTask = serviceerror.NewInternal("unknown visibility task")

func newVisibilityQueueTaskExecutor(
	shard shard.Context,
	workflowCache wcache.Cache,
	visibilityMgr manager.VisibilityManager,
	logger log.Logger,
	metricProvider metrics.Handler,
	ensureCloseBeforeDelete dynamicconfig.BoolPropertyFn,
	enableCloseWorkflowCleanup dynamicconfig.BoolPropertyFnWithNamespaceFilter,
) queues.Executor {
	return &visibilityQueueTaskExecutor{
		shard:          shard,
		cache:          workflowCache,
		logger:         logger,
		metricProvider: metricProvider,
		visibilityMgr:  visibilityMgr,

		ensureCloseBeforeDelete:    ensureCloseBeforeDelete,
		enableCloseWorkflowCleanup: enableCloseWorkflowCleanup,
	}
}

func (t *visibilityQueueTaskExecutor) Execute(
	ctx context.Context,
	executable queues.Executable,
) ([]metrics.Tag, bool, error) {
	task := executable.GetTask()
	taskType := queues.GetVisibilityTaskTypeTagValue(task)
	namespaceTag, replicationState := getNamespaceTagAndReplicationStateByID(
		t.shard.GetNamespaceRegistry(),
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
		// Visibility tasks should all be safe, but close execution task
		// might do a setWorkflowExecution to clean up memo and search attributes, which
		// will be blocked by shard context during ns handover
		// TODO: move this logic to queues.Executable when metrics tag doesn't need to
		// be returned from task executor
		return metricsTags, true, consts.ErrNamespaceHandover
	}

	var err error
	switch task := task.(type) {
	case *tasks.StartExecutionVisibilityTask:
		err = t.processStartExecution(ctx, task)
	case *tasks.UpsertExecutionVisibilityTask:
		err = t.processUpsertExecution(ctx, task)
	case *tasks.CloseExecutionVisibilityTask:
		err = t.processCloseExecution(ctx, task)
	case *tasks.DeleteExecutionVisibilityTask:
		err = t.processDeleteExecution(ctx, task)
	default:
		err = errUnknownVisibilityTask
	}

	return metricsTags, true, err
}

func (t *visibilityQueueTaskExecutor) processStartExecution(
	ctx context.Context,
	task *tasks.StartExecutionVisibilityTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := weContext.LoadMutableState(ctx)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	// verify task version for RecordWorkflowStarted.
	// upsert doesn't require verifyTask, because it is just a sync of mutableState.
	startVersion, err := mutableState.GetStartVersion()
	if err != nil {
		return err
	}
	err = CheckTaskVersion(t.shard, t.logger, mutableState.GetNamespaceEntry(), startVersion, task.Version, task)
	if err != nil {
		return err
	}

	executionInfo := mutableState.GetExecutionInfo()
	executionState := mutableState.GetExecutionState()
	wfTypeName := executionInfo.WorkflowTypeName

	workflowStartTime := timestamp.TimeValue(mutableState.GetExecutionInfo().GetStartTime())
	workflowExecutionTime := timestamp.TimeValue(mutableState.GetExecutionInfo().GetExecutionTime())
	visibilityMemo := getWorkflowMemo(copyMemo(executionInfo.Memo))
	searchAttr := getSearchAttributes(copySearchAttributes(executionInfo.SearchAttributes))
	executionStatus := executionState.GetStatus()
	taskQueue := executionInfo.TaskQueue
	stateTransitionCount := executionInfo.GetStateTransitionCount()

	// NOTE: do not access anything related mutable state after this lock release
	// release the context lock since we no longer need mutable state and
	// the rest of logic is making RPC call, which takes time.
	release(nil)

	return t.recordStartExecution(
		ctx,
		namespace.ID(task.GetNamespaceID()),
		task.GetWorkflowID(),
		task.GetRunID(),
		wfTypeName,
		workflowStartTime,
		workflowExecutionTime,
		stateTransitionCount,
		task.GetTaskID(),
		executionStatus,
		taskQueue,
		visibilityMemo,
		searchAttr,
	)
}

func (t *visibilityQueueTaskExecutor) processUpsertExecution(
	ctx context.Context,
	task *tasks.UpsertExecutionVisibilityTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := weContext.LoadMutableState(ctx)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	executionInfo := mutableState.GetExecutionInfo()
	executionState := mutableState.GetExecutionState()
	wfTypeName := executionInfo.WorkflowTypeName

	workflowStartTime := timestamp.TimeValue(mutableState.GetExecutionInfo().GetStartTime())
	workflowExecutionTime := timestamp.TimeValue(mutableState.GetExecutionInfo().GetExecutionTime())
	visibilityMemo := getWorkflowMemo(copyMemo(executionInfo.Memo))
	searchAttr := getSearchAttributes(copySearchAttributes(executionInfo.SearchAttributes))
	executionStatus := executionState.GetStatus()
	taskQueue := executionInfo.TaskQueue
	stateTransitionCount := executionInfo.GetStateTransitionCount()

	// NOTE: do not access anything related mutable state after this lock release
	// release the context lock since we no longer need mutable state and
	// the rest of logic is making RPC call, which takes time.
	release(nil)

	return t.upsertExecution(
		ctx,
		namespace.ID(task.GetNamespaceID()),
		task.GetWorkflowID(),
		task.GetRunID(),
		wfTypeName,
		workflowStartTime,
		workflowExecutionTime,
		stateTransitionCount,
		task.GetTaskID(),
		executionStatus,
		taskQueue,
		visibilityMemo,
		searchAttr,
	)
}

func (t *visibilityQueueTaskExecutor) recordStartExecution(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	workflowTypeName string,
	startTime time.Time,
	executionTime time.Time,
	stateTransitionCount int64,
	taskID int64,
	status enumspb.WorkflowExecutionStatus,
	taskQueue string,
	visibilityMemo *commonpb.Memo,
	searchAttributes *commonpb.SearchAttributes,
) error {
	namespaceEntry, err := t.shard.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}

	request := &manager.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &manager.VisibilityRequestBase{
			NamespaceID: namespaceID,
			Namespace:   namespaceEntry.Name(),
			Execution: commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			WorkflowTypeName:     workflowTypeName,
			StartTime:            startTime,
			ExecutionTime:        executionTime,
			StateTransitionCount: stateTransitionCount, TaskID: taskID,
			Status:           status,
			ShardID:          t.shard.GetShardID(),
			Memo:             visibilityMemo,
			TaskQueue:        taskQueue,
			SearchAttributes: searchAttributes,
		},
	}
	return t.visibilityMgr.RecordWorkflowExecutionStarted(ctx, request)
}

func (t *visibilityQueueTaskExecutor) upsertExecution(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	workflowTypeName string,
	startTime time.Time,
	executionTime time.Time,
	stateTransitionCount int64,
	taskID int64,
	status enumspb.WorkflowExecutionStatus,
	taskQueue string,
	visibilityMemo *commonpb.Memo,
	searchAttributes *commonpb.SearchAttributes,
) error {
	namespaceEntry, err := t.shard.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}

	request := &manager.UpsertWorkflowExecutionRequest{
		VisibilityRequestBase: &manager.VisibilityRequestBase{
			NamespaceID: namespaceID,
			Namespace:   namespaceEntry.Name(),
			Execution: commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			WorkflowTypeName:     workflowTypeName,
			StartTime:            startTime,
			ExecutionTime:        executionTime,
			StateTransitionCount: stateTransitionCount, TaskID: taskID,
			ShardID:          t.shard.GetShardID(),
			Status:           status,
			Memo:             visibilityMemo,
			TaskQueue:        taskQueue,
			SearchAttributes: searchAttributes,
		},
	}

	return t.visibilityMgr.UpsertWorkflowExecution(ctx, request)
}

func (t *visibilityQueueTaskExecutor) processCloseExecution(
	parentCtx context.Context,
	task *tasks.CloseExecutionVisibilityTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(parentCtx, taskTimeout)
	defer cancel()

	namespaceEntry, err := t.shard.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(task.GetNamespaceID()))
	if err != nil {
		return err
	}

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := weContext.LoadMutableState(ctx)
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
	err = CheckTaskVersion(t.shard, t.logger, mutableState.GetNamespaceEntry(), lastWriteVersion, task.Version, task)
	if err != nil {
		return err
	}

	executionInfo := mutableState.GetExecutionInfo()
	executionState := mutableState.GetExecutionState()
	wfCloseTime, err := mutableState.GetWorkflowCloseTime(ctx)
	if err != nil {
		return err
	}
	workflowTypeName := executionInfo.WorkflowTypeName
	workflowStatus := executionState.Status
	workflowHistoryLength := mutableState.GetNextEventID() - 1
	workflowStartTime := timestamp.TimeValue(mutableState.GetExecutionInfo().GetStartTime())
	workflowExecutionTime := timestamp.TimeValue(mutableState.GetExecutionInfo().GetExecutionTime())
	visibilityMemo := getWorkflowMemo(copyMemo(executionInfo.Memo))
	searchAttr := getSearchAttributes(copySearchAttributes(executionInfo.SearchAttributes))
	taskQueue := executionInfo.TaskQueue
	stateTransitionCount := executionInfo.GetStateTransitionCount()
	historySizeBytes := executionInfo.GetExecutionStats().GetHistorySize()

	// NOTE: do not access anything related mutable state after this lock release
	// release the context lock since we no longer need mutable state and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	err = t.recordCloseExecution(
		ctx,
		namespaceEntry,
		task.GetWorkflowID(),
		task.GetRunID(),
		workflowTypeName,
		workflowStartTime,
		workflowExecutionTime,
		timestamp.TimeValue(wfCloseTime),
		workflowStatus,
		stateTransitionCount,
		workflowHistoryLength,
		task.GetTaskID(),
		visibilityMemo,
		taskQueue,
		searchAttr,
		historySizeBytes,
	)
	if err != nil {
		return err
	}

	// Elasticsearch bulk processor doesn't respect context timeout
	// because under heavy load bulk flush might take longer than taskTimeout.
	// Therefore, ctx timeout might be already expired
	// and parentCtx (which doesn't have timeout) must be used everywhere bellow.

	if t.enableCloseWorkflowCleanup(namespaceEntry.Name().String()) {
		return t.cleanupExecutionInfo(parentCtx, task)
	}
	return nil
}

func (t *visibilityQueueTaskExecutor) recordCloseExecution(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	workflowID string,
	runID string,
	workflowTypeName string,
	startTime time.Time,
	executionTime time.Time,
	endTime time.Time,
	status enumspb.WorkflowExecutionStatus,
	stateTransitionCount int64,
	historyLength int64,
	taskID int64,
	visibilityMemo *commonpb.Memo,
	taskQueue string,
	searchAttributes *commonpb.SearchAttributes,
	historySizeBytes int64,
) error {
	return t.visibilityMgr.RecordWorkflowExecutionClosed(ctx, &manager.RecordWorkflowExecutionClosedRequest{
		VisibilityRequestBase: &manager.VisibilityRequestBase{
			NamespaceID: namespaceEntry.ID(),
			Namespace:   namespaceEntry.Name(),
			Execution: commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			WorkflowTypeName:     workflowTypeName,
			StartTime:            startTime,
			ExecutionTime:        executionTime,
			StateTransitionCount: stateTransitionCount,
			Status:               status,
			TaskID:               taskID,
			ShardID:              t.shard.GetShardID(),
			Memo:                 visibilityMemo,
			TaskQueue:            taskQueue,
			SearchAttributes:     searchAttributes,
		},
		CloseTime:        endTime,
		HistoryLength:    historyLength,
		HistorySizeBytes: historySizeBytes,
	})
}

func (t *visibilityQueueTaskExecutor) processDeleteExecution(
	ctx context.Context,
	task *tasks.DeleteExecutionVisibilityTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	request := &manager.VisibilityDeleteWorkflowExecutionRequest{
		NamespaceID: namespace.ID(task.NamespaceID),
		WorkflowID:  task.WorkflowID,
		RunID:       task.RunID,
		TaskID:      task.TaskID,
		StartTime:   task.StartTime,
		CloseTime:   task.CloseTime,
	}
	if t.ensureCloseBeforeDelete() {
		// If visibility delete task is executed before visibility close task then visibility close task
		// (which change workflow execution status by uploading new visibility record) will resurrect visibility record.
		//
		// Queue states/ack levels are updated with delay (default 30s). Therefore, this check could return false
		// if the workflow was closed and then deleted within this delay period.
		if t.isCloseExecutionVisibilityTaskPending(task) {
			// Return retryable error for task processor to retry the operation later.
			return consts.ErrDependencyTaskNotCompleted
		}
	}
	return t.visibilityMgr.DeleteWorkflowExecution(ctx, request)
}

func (t *visibilityQueueTaskExecutor) isCloseExecutionVisibilityTaskPending(task *tasks.DeleteExecutionVisibilityTask) bool {
	CloseExecutionVisibilityTaskID := task.CloseExecutionVisibilityTaskID
	// taskID == 0 if workflow still running in passive cluster or closed before this field was added (v1.17).
	if CloseExecutionVisibilityTaskID == 0 {
		return false
	}
	// check if close execution visibility task is completed
	visibilityQueueState, ok := t.shard.GetQueueState(tasks.CategoryVisibility)
	if !ok {
		return true
	}
	queryTask := &tasks.CloseExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(task.GetNamespaceID(), task.GetWorkflowID(), task.GetRunID()),
		TaskID:      CloseExecutionVisibilityTaskID,
	}
	return !queues.IsTaskAcked(queryTask, visibilityQueueState)
}

// cleanupExecutionInfo cleans up workflow execution info after visibility close
// task has been processed and acked by visibility store.
func (t *visibilityQueueTaskExecutor) cleanupExecutionInfo(
	ctx context.Context,
	task *tasks.CloseExecutionVisibilityTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := weContext.LoadMutableState(ctx)
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
	err = CheckTaskVersion(t.shard, t.logger, mutableState.GetNamespaceEntry(), lastWriteVersion, task.Version, task)
	if err != nil {
		return err
	}

	executionInfo := mutableState.GetExecutionInfo()
	executionInfo.Memo = nil
	executionInfo.SearchAttributes = nil
	executionInfo.CloseVisibilityTaskCompleted = true
	return weContext.SetWorkflowExecution(ctx)
}

func getWorkflowMemo(
	memoFields map[string]*commonpb.Payload,
) *commonpb.Memo {
	if memoFields == nil {
		return nil
	}
	return &commonpb.Memo{Fields: memoFields}
}

func copyMemo(
	memoFields map[string]*commonpb.Payload,
) map[string]*commonpb.Payload {
	if memoFields == nil {
		return nil
	}

	result := make(map[string]*commonpb.Payload)
	for k, v := range memoFields {
		result[k] = common.CloneProto(v)
	}
	return result
}

func getSearchAttributes(
	indexedFields map[string]*commonpb.Payload,
) *commonpb.SearchAttributes {
	if indexedFields == nil {
		return nil
	}
	return &commonpb.SearchAttributes{IndexedFields: indexedFields}
}

func copySearchAttributes(
	input map[string]*commonpb.Payload,
) map[string]*commonpb.Payload {
	if input == nil {
		return nil
	}

	result := make(map[string]*commonpb.Payload)
	for k, v := range input {
		result[k] = common.CloneProto(v)
	}
	return result
}
