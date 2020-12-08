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
	case enumsspb.TASK_TYPE_VISIBILITY_UPSERT_EXECUTION:
		return t.processUpsertExecution(task)
	case enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION:
		return t.processDeleteExecution(task)
	default:
		return errUnknownVisibilityTask
	}
}

// processUpsertExecution combines former
// 		processCloseExecution
//		processRecordWorkflowStarted
//		processUpsertWorkflowSearchAttributes
func (t *visibilityQueueTaskExecutor) processUpsertExecution(
	task *persistencespb.VisibilityTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getNamespaceIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	ms, err := context.loadWorkflowExecution()
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil
		}
		return err
	}

	// verify task version for RecordWorkflowStarted.
	// upsert doesn't require verifyTask, because it is just a sync of mutableState.
	startVersion, err := ms.GetStartVersion()
	if err != nil {
		return err
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, task.GetNamespaceId(), startVersion, task.Version, task)
	if err != nil || !ok {
		return err
	}

	executionInfo := ms.GetExecutionInfo()
	startEvent, err := ms.GetStartEvent()
	if err != nil {
		return err
	}

	executionStatus := ms.GetExecutionState().GetStatus()
	closeTime := time.Unix(0, 0).UTC()
	if executionStatus != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		completionEvent, err := ms.GetCompletionEvent()
		if err != nil {
			return err
		}
		closeTime = timestamp.TimeValue(completionEvent.GetEventTime())
	}

	workflowHistoryLength := ms.GetNextEventID() - 1

	executionTime := getWorkflowExecutionTime(ms, startEvent)
	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)

	namespace := defaultNamespace
	retentionSeconds := int64(0)
	namespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(task.GetNamespaceId())
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			return err
		}
	} else {
		namespace = namespaceEntry.GetInfo().GetName()
		retentionSeconds = int64(timestamp.DurationFromDays(namespaceEntry.GetRetentionDays(task.GetWorkflowId())).Seconds())
	}

	request := &persistence.UpsertWorkflowExecutionRequestV2{
		NamespaceID: task.GetNamespaceId(),
		Namespace:   namespace,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: task.GetWorkflowId(),
			RunId:      task.GetRunId(),
		},
		WorkflowTypeName:   executionInfo.GetWorkflowTypeName(),
		StartTimestamp:     timestamp.TimeValue(startEvent.GetEventTime()).UnixNano(),
		ExecutionTimestamp: executionTime.UnixNano(),
		RunTimeout:         int64(timestamp.DurationValue(executionInfo.GetWorkflowRunTimeout()).Round(time.Second).Seconds()),
		ShardID:            t.shard.GetShardID(),
		TaskID:             task.GetTaskId(),
		Status:             executionStatus,
		Memo:               getWorkflowMemo(executionInfo.GetMemo()),
		TaskQueue:          executionInfo.TaskQueue,
		// TODO (alex): remove copy?
		SearchAttributes: copySearchAttributes(executionInfo.GetSearchAttributes()),
		CloseTimestamp:   closeTime.UnixNano(),
		HistoryLength:    workflowHistoryLength,
		RetentionSeconds: retentionSeconds,
	}

	return t.visibilityMgr.UpsertWorkflowExecutionV2(request)
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

func (t *visibilityQueueTaskExecutor) getNamespaceIDAndWorkflowExecution(
	task *persistencespb.VisibilityTaskInfo,
) (string, commonpb.WorkflowExecution) {

	return task.GetNamespaceId(), commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      task.GetRunId(),
	}
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
