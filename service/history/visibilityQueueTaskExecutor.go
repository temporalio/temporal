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
	"errors"
	"time"

	"github.com/gogo/protobuf/proto"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/manager"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/worker/parentclosepolicy"
)

type (
	visibilityQueueTaskExecutor struct {
		shard                   shard.Context
		historyService          *historyEngineImpl
		cache                   workflow.Cache
		logger                  log.Logger
		metricsClient           metrics.Client
		matchingClient          matchingservice.MatchingServiceClient
		visibilityMgr           manager.VisibilityManager
		config                  *configs.Config
		historyClient           historyservice.HistoryServiceClient
		parentClosePolicyClient parentclosepolicy.Client
	}
)

var (
	errUnknownVisibilityTask = errors.New("unknown visibility task")
)

func newVisibilityQueueTaskExecutor(
	shard shard.Context,
	historyService *historyEngineImpl,
	visibilityMgr manager.VisibilityManager,
	logger log.Logger,
	metricsClient metrics.Client,
	config *configs.Config,
) *visibilityQueueTaskExecutor {
	return &visibilityQueueTaskExecutor{
		shard:          shard,
		historyService: historyService,
		cache:          historyService.historyCache,
		logger:         logger,
		metricsClient:  metricsClient,
		matchingClient: shard.GetService().GetMatchingClient(),
		visibilityMgr:  visibilityMgr,
		config:         config,
		historyClient:  shard.GetService().GetHistoryClient(),
		parentClosePolicyClient: parentclosepolicy.NewClient(
			shard.GetMetricsClient(),
			shard.GetLogger(),
			historyService.publicClient,
			config.NumParentClosePolicySystemWorkflows(),
		),
	}
}

func (t *visibilityQueueTaskExecutor) execute(
	ctx context.Context,
	taskInfo queueTaskInfo,
	shouldProcessTask bool,
) error {

	task, ok := taskInfo.(*persistencespb.VisibilityTaskInfo)
	if !ok {
		return errUnexpectedQueueTask
	}

	if !shouldProcessTask {
		return nil
	}

	switch task.GetTaskType() {
	case enumsspb.TASK_TYPE_VISIBILITY_START_EXECUTION:
		return t.processStartOrUpsertExecution(ctx, task, true)
	case enumsspb.TASK_TYPE_VISIBILITY_UPSERT_EXECUTION:
		return t.processStartOrUpsertExecution(ctx, task, false)
	case enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION:
		return t.processCloseExecution(ctx, task)
	case enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION:
		return t.processDeleteExecution(task)
	default:
		return errUnknownVisibilityTask
	}
}

func (t *visibilityQueueTaskExecutor) processStartOrUpsertExecution(
	ctx context.Context,
	task *persistencespb.VisibilityTaskInfo,
	isStartExecution bool,
) (retError error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, taskTimeout)

	defer cancel()
	weContext, release, err := t.cache.GetOrCreateWorkflowExecution(
		ctx,
		task.GetNamespaceId(),
		commonpb.WorkflowExecution{
			WorkflowId: task.GetWorkflowId(),
			RunId:      task.GetRunId(),
		},
		workflow.CallerTypeTask,
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := weContext.LoadWorkflowExecution()
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	// verify task version for RecordWorkflowStarted.
	// upsert doesn't require verifyTask, because it is just a sync of mutableState.
	if isStartExecution {
		startVersion, err := mutableState.GetStartVersion()
		if err != nil {
			return err
		}
		ok, err := verifyTaskVersion(t.shard, t.logger, task.GetNamespaceId(), startVersion, task.Version, task)
		if err != nil || !ok {
			return err
		}
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
	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)

	if isStartExecution {
		return t.recordStartExecution(
			task.GetNamespaceId(),
			task.GetWorkflowId(),
			task.GetRunId(),
			wfTypeName,
			workflowStartTime,
			workflowExecutionTime,
			stateTransitionCount,
			task.GetTaskId(),
			executionStatus,
			taskQueue,
			visibilityMemo,
			searchAttr,
		)
	}
	return t.upsertExecution(
		task.GetNamespaceId(),
		task.GetWorkflowId(),
		task.GetRunId(),
		wfTypeName,
		workflowStartTime,
		workflowExecutionTime,
		stateTransitionCount,
		task.GetTaskId(),
		executionStatus,
		taskQueue,
		visibilityMemo,
		searchAttr,
	)
}
func (t *visibilityQueueTaskExecutor) recordStartExecution(
	namespaceID string,
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

	namespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}

	// if sampled for longer retention is enabled, only record those sampled events
	if namespaceEntry.IsSampledForLongerRetentionEnabled(workflowID) && !namespaceEntry.IsSampledForLongerRetention(workflowID) {
		return nil
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
	return t.visibilityMgr.RecordWorkflowExecutionStarted(request)
}

func (t *visibilityQueueTaskExecutor) upsertExecution(
	namespaceID string,
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

	namespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(namespaceID)
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

	return t.visibilityMgr.UpsertWorkflowExecution(request)
}

func (t *visibilityQueueTaskExecutor) processCloseExecution(
	ctx context.Context,
	task *persistencespb.VisibilityTaskInfo,
) (retError error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, taskTimeout)

	defer cancel()
	weContext, release, err := t.cache.GetOrCreateWorkflowExecution(
		ctx,
		task.GetNamespaceId(),
		commonpb.WorkflowExecution{
			WorkflowId: task.GetWorkflowId(),
			RunId:      task.GetRunId(),
		},
		workflow.CallerTypeTask,
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := weContext.LoadWorkflowExecution()
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
	ok, err := verifyTaskVersion(t.shard, t.logger, task.GetNamespaceId(), lastWriteVersion, task.Version, task)
	if err != nil || !ok {
		return err
	}

	executionInfo := mutableState.GetExecutionInfo()
	executionState := mutableState.GetExecutionState()
	completionEvent, err := mutableState.GetCompletionEvent()
	if err != nil {
		return err
	}
	wfCloseTime := timestamp.TimeValue(completionEvent.GetEventTime())

	workflowTypeName := executionInfo.WorkflowTypeName
	workflowCloseTime := wfCloseTime
	workflowStatus := executionState.Status
	workflowHistoryLength := mutableState.GetNextEventID() - 1

	workflowStartTime := timestamp.TimeValue(mutableState.GetExecutionInfo().GetStartTime())
	workflowExecutionTime := timestamp.TimeValue(mutableState.GetExecutionInfo().GetExecutionTime())
	visibilityMemo := getWorkflowMemo(copyMemo(executionInfo.Memo))
	searchAttr := getSearchAttributes(copySearchAttributes(executionInfo.SearchAttributes))
	taskQueue := executionInfo.TaskQueue
	stateTransitionCount := executionInfo.GetStateTransitionCount()

	// NOTE: do not access anything related mutable state after this lock release
	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	return t.recordCloseExecution(
		task.GetNamespaceId(),
		task.GetWorkflowId(),
		task.GetRunId(),
		workflowTypeName,
		workflowStartTime,
		workflowExecutionTime,
		workflowCloseTime,
		workflowStatus,
		stateTransitionCount,
		workflowHistoryLength,
		task.GetTaskId(),
		visibilityMemo,
		taskQueue,
		searchAttr,
	)
}

func (t *visibilityQueueTaskExecutor) recordCloseExecution(
	namespaceID string,
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
) error {

	namespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}

	recordWorkflowClose := true

	retention := namespaceEntry.Retention(workflowID)
	// if sampled for longer retention is enabled, only record those sampled events
	if namespaceEntry.IsSampledForLongerRetentionEnabled(workflowID) &&
		!namespaceEntry.IsSampledForLongerRetention(workflowID) {
		recordWorkflowClose = false
	}

	if recordWorkflowClose {
		return t.visibilityMgr.RecordWorkflowExecutionClosed(&manager.RecordWorkflowExecutionClosedRequest{
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
				StateTransitionCount: stateTransitionCount, Status: status,
				TaskID:           taskID,
				ShardID:          t.shard.GetShardID(),
				Memo:             visibilityMemo,
				TaskQueue:        taskQueue,
				SearchAttributes: searchAttributes,
			},
			CloseTime:     endTime,
			HistoryLength: historyLength,
			Retention:     &retention,
		})
	}

	return nil
}

func (t *visibilityQueueTaskExecutor) processDeleteExecution(
	task *persistencespb.VisibilityTaskInfo,
) (retError error) {
	request := &manager.VisibilityDeleteWorkflowExecutionRequest{
		NamespaceID: task.GetNamespaceId(),
		WorkflowID:  task.GetWorkflowId(),
		RunID:       task.GetRunId(),
		TaskID:      task.GetTaskId(),
	}
	return t.visibilityMgr.DeleteWorkflowExecution(request)
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
		result[k] = proto.Clone(v).(*commonpb.Payload)
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
		result[k] = proto.Clone(v).(*commonpb.Payload)
	}
	return result
}
