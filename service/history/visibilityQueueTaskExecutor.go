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
	"errors"
	"time"

	"github.com/gogo/protobuf/proto"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/client/matching"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/worker/parentclosepolicy"
)

type (
	visibilityQueueTaskExecutor struct {
		shard                   shard.Context
		historyService          *historyEngineImpl
		cache                   *historyCache
		logger                  log.Logger
		metricsClient           metrics.Client
		matchingClient          matching.Client
		visibilityMgr           persistence.VisibilityManager
		config                  *configs.Config
		historyClient           history.Client
		parentClosePolicyClient parentclosepolicy.Client
	}
)

var (
	errUnknownVisibilityTask = errors.New("unknown visibility task")
)

func newVisibilityQueueTaskExecutor(
	shard shard.Context,
	historyService *historyEngineImpl,
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
		visibilityMgr:  shard.GetService().GetVisibilityManager(),
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
		return t.processStartOrUpsertExecution(task, true)
	case enumsspb.TASK_TYPE_VISIBILITY_UPSERT_EXECUTION:
		return t.processStartOrUpsertExecution(task, false)
	case enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION:
		return t.processCloseExecution(task)
	case enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION:
		return t.processDeleteExecution(task)
	default:
		return errUnknownVisibilityTask
	}
}

func (t *visibilityQueueTaskExecutor) processStartOrUpsertExecution(
	task *persistencespb.VisibilityTaskInfo,
	isStartExecution bool,
) (retError error) {

	weContext, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		task.GetNamespaceId(),
		commonpb.WorkflowExecution{
			WorkflowId: task.GetWorkflowId(),
			RunId:      task.GetRunId(),
		},
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := weContext.loadWorkflowExecution()
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
	runTimeout := executionInfo.WorkflowRunTimeout
	wfTypeName := executionInfo.WorkflowTypeName

	startEvent, err := mutableState.GetStartEvent()
	if err != nil {
		return err
	}
	startTimestamp := timestamp.TimeValue(startEvent.GetEventTime())
	executionTimestamp := getWorkflowExecutionTime(mutableState, startEvent)
	visibilityMemo := getWorkflowMemo(executionInfo.Memo)
	// TODO (alex): remove copy?
	searchAttr := copySearchAttributes(executionInfo.SearchAttributes)

	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)

	if isStartExecution {
		return t.recordStartExecution(
			task.GetNamespaceId(),
			task.GetWorkflowId(),
			task.GetRunId(),
			wfTypeName,
			startTimestamp.UnixNano(),
			executionTimestamp.UnixNano(),
			runTimeout,
			task.GetTaskId(),
			executionState.GetStatus(),
			executionInfo.TaskQueue,
			visibilityMemo,
			searchAttr,
		)
	}
	return t.upsertExecution(
		task.GetNamespaceId(),
		task.GetWorkflowId(),
		task.GetRunId(),
		wfTypeName,
		startTimestamp.UnixNano(),
		executionTimestamp.UnixNano(),
		runTimeout,
		task.GetTaskId(),
		executionState.GetStatus(),
		executionInfo.TaskQueue,
		visibilityMemo,
		searchAttr,
	)
}
func (t *visibilityQueueTaskExecutor) recordStartExecution(
	namespaceID string,
	workflowID string,
	runID string,
	workflowTypeName string,
	startTimeUnixNano int64,
	executionTimeUnixNano int64,
	runTimeout *time.Duration,
	taskID int64,
	status enumspb.WorkflowExecutionStatus,
	taskQueue string,
	visibilityMemo *commonpb.Memo,
	searchAttributes map[string]*commonpb.Payload,
) error {

	namespace := defaultNamespace

	if namespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(namespaceID); err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			return err
		}
	} else {
		namespace = namespaceEntry.GetInfo().Name
		// if sampled for longer retention is enabled, only record those sampled events
		if namespaceEntry.IsSampledForLongerRetentionEnabled(workflowID) &&
			!namespaceEntry.IsSampledForLongerRetention(workflowID) {
			return nil
		}
	}

	request := &persistence.RecordWorkflowExecutionStartedRequest{
		VisibilityRequestBase: &persistence.VisibilityRequestBase{
			NamespaceID: namespaceID,
			Namespace:   namespace,
			Execution: commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			WorkflowTypeName:   workflowTypeName,
			StartTimestamp:     startTimeUnixNano,
			ExecutionTimestamp: executionTimeUnixNano,
			TaskID:             taskID,
			Status:             status,
			ShardID:            t.shard.GetShardID(),
			Memo:               visibilityMemo,
			TaskQueue:          taskQueue,
			SearchAttributes:   searchAttributes,
		},
		RunTimeout: int64(timestamp.DurationValue(runTimeout).Round(time.Second).Seconds()),
	}
	return t.visibilityMgr.RecordWorkflowExecutionStartedV2(request)
}

func (t *visibilityQueueTaskExecutor) upsertExecution(
	namespaceID string,
	workflowID string,
	runID string,
	workflowTypeName string,
	startTimeUnixNano int64,
	executionTimeUnixNano int64,
	workflowTimeout *time.Duration,
	taskID int64,
	status enumspb.WorkflowExecutionStatus,
	taskQueue string,
	visibilityMemo *commonpb.Memo,
	searchAttributes map[string]*commonpb.Payload,
) error {

	namespace := defaultNamespace
	namespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(namespaceID)
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			return err
		}
	} else {
		namespace = namespaceEntry.GetInfo().Name
	}

	request := &persistence.UpsertWorkflowExecutionRequest{
		VisibilityRequestBase: &persistence.VisibilityRequestBase{
			NamespaceID: namespaceID,
			Namespace:   namespace,
			Execution: commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			WorkflowTypeName:   workflowTypeName,
			StartTimestamp:     startTimeUnixNano,
			ExecutionTimestamp: executionTimeUnixNano,
			TaskID:             taskID,
			ShardID:            t.shard.GetShardID(),
			Status:             status,
			Memo:               visibilityMemo,
			TaskQueue:          taskQueue,
			SearchAttributes:   searchAttributes,
		},
		WorkflowTimeout: int64(timestamp.DurationValue(workflowTimeout).Round(time.Second).Seconds()),
	}

	return t.visibilityMgr.UpsertWorkflowExecutionV2(request)
}

func (t *visibilityQueueTaskExecutor) processCloseExecution(
	task *persistencespb.VisibilityTaskInfo,
) (retError error) {

	weContext, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		task.GetNamespaceId(),
		commonpb.WorkflowExecution{
			WorkflowId: task.GetWorkflowId(),
			RunId:      task.GetRunId(),
		},
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := weContext.loadWorkflowExecution()
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

	startEvent, err := mutableState.GetStartEvent()
	if err != nil {
		return err
	}
	workflowStartTime := timestamp.TimeValue(startEvent.GetEventTime())
	workflowExecutionTime := getWorkflowExecutionTime(mutableState, startEvent)
	visibilityMemo := getWorkflowMemo(executionInfo.Memo)
	searchAttr := executionInfo.SearchAttributes

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
		workflowHistoryLength,
		task.GetTaskId(),
		visibilityMemo,
		executionInfo.TaskQueue,
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
	historyLength int64,
	taskID int64,
	visibilityMemo *commonpb.Memo,
	taskQueue string,
	searchAttributes map[string]*commonpb.Payload,
) error {

	// Record closing in visibility store
	retentionSeconds := int64(0)
	namespace := defaultNamespace
	recordWorkflowClose := true

	namespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(namespaceID)
	if err != nil && !isWorkflowNotExistError(err) {
		return err
	}

	if err == nil {
		// retention in namespace config is in days, convert to seconds
		retentionSeconds = int64(timestamp.DurationFromDays(namespaceEntry.GetRetentionDays(workflowID)).Seconds())
		namespace = namespaceEntry.GetInfo().Name
		// if sampled for longer retention is enabled, only record those sampled events
		if namespaceEntry.IsSampledForLongerRetentionEnabled(workflowID) &&
			!namespaceEntry.IsSampledForLongerRetention(workflowID) {
			recordWorkflowClose = false
		}
	}

	if recordWorkflowClose {
		return t.visibilityMgr.RecordWorkflowExecutionClosedV2(&persistence.RecordWorkflowExecutionClosedRequest{
			VisibilityRequestBase: &persistence.VisibilityRequestBase{
				NamespaceID: namespaceID,
				Namespace:   namespace,
				Execution: commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				WorkflowTypeName:   workflowTypeName,
				StartTimestamp:     startTime.UnixNano(),
				ExecutionTimestamp: executionTime.UnixNano(),
				Status:             status,
				TaskID:             taskID,
				ShardID:            t.shard.GetShardID(),
				Memo:               visibilityMemo,
				TaskQueue:          taskQueue,
				SearchAttributes:   searchAttributes,
			},
			CloseTimestamp:   endTime.UnixNano(),
			HistoryLength:    historyLength,
			RetentionSeconds: retentionSeconds,
		})
	}

	return nil
}

func (t *visibilityQueueTaskExecutor) processDeleteExecution(
	task *persistencespb.VisibilityTaskInfo,
) (retError error) {
	op := func() error {
		request := &persistence.VisibilityDeleteWorkflowExecutionRequest{
			NamespaceID: task.GetNamespaceId(),
			WorkflowID:  task.GetWorkflowId(),
			RunID:       task.GetRunId(),
			TaskID:      task.GetTaskId(),
		}
		// TODO: expose GetVisibilityManager method on shardContext interface
		return t.shard.GetService().GetVisibilityManager().DeleteWorkflowExecutionV2(request) // delete from db
	}
	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

// Argument startEvent is to save additional call of msBuilder.GetStartEvent
func getWorkflowExecutionTime(
	msBuilder mutableState,
	startEvent *historypb.HistoryEvent,
) time.Time {
	// Use value 0 to represent workflows that don't need backoff. Since ES doesn't support
	// comparison between two field, we need a value to differentiate them from cron workflows
	// or later runs of a workflow that needs retry.
	executionTimestamp := time.Unix(0, 0).UTC()
	if startEvent == nil {
		return executionTimestamp
	}

	if backoffDuration := timestamp.DurationValue(startEvent.GetWorkflowExecutionStartedEventAttributes().GetFirstWorkflowTaskBackoff()); backoffDuration != 0 {
		startTime := timestamp.TimeValue(startEvent.GetEventTime())
		executionTimestamp = startTime.Add(backoffDuration)
	}
	return executionTimestamp
}

func getWorkflowMemo(
	memoFields map[string]*commonpb.Payload,
) *commonpb.Memo {

	if memoFields == nil {
		return nil
	}
	return &commonpb.Memo{Fields: memoFields}
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
