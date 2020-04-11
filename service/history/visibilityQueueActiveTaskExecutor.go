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
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/client/history"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
	"github.com/temporalio/temporal/service/worker/parentclosepolicy"
)

type (
	visibilityQueueActiveTaskExecutor struct {
		*visibilityQueueTaskExecutorBase

		historyClient           history.Client
		parentClosePolicyClient parentclosepolicy.Client
	}
)

func newVisibilityQueueActiveTaskExecutor(
	shard ShardContext,
	historyService *historyEngineImpl,
	logger log.Logger,
	metricsClient metrics.Client,
	config *Config,
) queueTaskExecutor {
	return &visibilityQueueActiveTaskExecutor{
		visibilityQueueTaskExecutorBase: newVisibilityQueueTaskExecutorBase(
			shard,
			historyService,
			logger,
			metricsClient,
			config,
		),
		historyClient: shard.GetService().GetHistoryClient(),
		parentClosePolicyClient: parentclosepolicy.NewClient(
			shard.GetMetricsClient(),
			shard.GetLogger(),
			historyService.publicClient,
			config.NumParentClosePolicySystemWorkflows(),
		),
	}
}

func (t *visibilityQueueActiveTaskExecutor) execute(
	taskInfo queueTaskInfo,
	shouldProcessTask bool,
) error {

	task, ok := taskInfo.(*persistenceblobs.VisibilityTaskInfo)
	if !ok {
		return errUnexpectedQueueTask
	}

	if !shouldProcessTask {
		return nil
	}

	switch task.TaskType {
	case persistence.VisibilityTaskTypeRecordCloseExecution:
		return t.processRecordCloseExecution(task)
	case persistence.VisibilityTaskTypeRecordWorkflowStarted:
		return t.processRecordWorkflowStarted(task)
	case persistence.VisibilityTaskTypeUpsertWorkflowSearchAttributes:
		return t.processUpsertWorkflowSearchAttributes(task)
	default:
		return errUnknownVisibilityTask
	}
}

func (t *visibilityQueueActiveTaskExecutor) processRecordCloseExecution(
	task *persistenceblobs.VisibilityTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForVisibilityTask(context, task, t.metricsClient, t.logger)
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
	ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, lastWriteVersion, task.Version, task)
	if err != nil || !ok {
		return err
	}

	executionInfo := mutableState.GetExecutionInfo()
	completionEvent, err := mutableState.GetCompletionEvent()
	if err != nil {
		return err
	}
	wfCloseTime := completionEvent.GetTimestamp()

	workflowTypeName := executionInfo.WorkflowTypeName
	workflowCloseTimestamp := wfCloseTime
	workflowCloseStatus := executionInfo.CloseStatus
	workflowHistoryLength := mutableState.GetNextEventID() - 1

	startEvent, err := mutableState.GetStartEvent()
	if err != nil {
		return err
	}
	workflowStartTimestamp := startEvent.GetTimestamp()
	workflowExecutionTimestamp := getWorkflowExecutionTimestamp(mutableState, startEvent)
	visibilityMemo := getWorkflowMemo(executionInfo.Memo)
	searchAttr := executionInfo.SearchAttributes

	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	err = t.recordWorkflowClosed(
		primitives.UUIDString(task.DomainID),
		task.WorkflowID,
		primitives.UUIDString(task.RunID),
		workflowTypeName,
		workflowStartTimestamp,
		workflowExecutionTimestamp.UnixNano(),
		workflowCloseTimestamp,
		workflowCloseStatus,
		workflowHistoryLength,
		task.GetTaskID(),
		visibilityMemo,
		searchAttr,
	)
	if err != nil {
		return err
	}

	return nil
}

func (t *visibilityQueueActiveTaskExecutor) processRecordWorkflowStarted(
	task *persistenceblobs.VisibilityTaskInfo,
) (retError error) {

	return t.processRecordWorkflowStartedOrUpsertHelper(task, true)
}

func (t *visibilityQueueActiveTaskExecutor) processUpsertWorkflowSearchAttributes(
	task *persistenceblobs.VisibilityTaskInfo,
) (retError error) {

	return t.processRecordWorkflowStartedOrUpsertHelper(task, false)
}

func (t *visibilityQueueActiveTaskExecutor) processRecordWorkflowStartedOrUpsertHelper(
	task *persistenceblobs.VisibilityTaskInfo,
	recordStart bool,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForVisibilityTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	// verify task version for RecordWorkflowStarted.
	// upsert doesn't require verifyTask, because it is just a sync of mutableState.
	if recordStart {
		startVersion, err := mutableState.GetStartVersion()
		if err != nil {
			return err
		}
		ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, startVersion, task.Version, task)
		if err != nil || !ok {
			return err
		}
	}

	executionInfo := mutableState.GetExecutionInfo()
	workflowTimeout := executionInfo.WorkflowTimeout
	wfTypeName := executionInfo.WorkflowTypeName
	startEvent, err := mutableState.GetStartEvent()
	if err != nil {
		return err
	}
	startTimestamp := startEvent.GetTimestamp()
	executionTimestamp := getWorkflowExecutionTimestamp(mutableState, startEvent)
	visibilityMemo := getWorkflowMemo(executionInfo.Memo)
	searchAttr := copySearchAttributes(executionInfo.SearchAttributes)

	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)

	if recordStart {
		return t.recordWorkflowStarted(
			primitives.UUID(task.DomainID).String(),
			task.WorkflowID,
			primitives.UUID(task.RunID).String(),
			wfTypeName,
			startTimestamp,
			executionTimestamp.UnixNano(),
			workflowTimeout,
			task.GetTaskID(),
			visibilityMemo,
			searchAttr,
		)
	}
	return t.upsertWorkflowExecution(
		primitives.UUID(task.DomainID).String(),
		task.WorkflowID,
		primitives.UUID(task.RunID).String(),
		wfTypeName,
		startTimestamp,
		executionTimestamp.UnixNano(),
		workflowTimeout,
		task.GetTaskID(),
		visibilityMemo,
		searchAttr,
	)
}
