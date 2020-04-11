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
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
	"github.com/temporalio/temporal/common/xdc"
)

type (
	visibilityQueueStandbyTaskExecutor struct {
		*visibilityQueueTaskExecutorBase

		clusterName         string
		historyRereplicator xdc.HistoryRereplicator
		nDCHistoryResender  xdc.NDCHistoryResender
	}
)

func newVisibilityQueueStandbyTaskExecutor(
	shard ShardContext,
	historyService *historyEngineImpl,
	historyRereplicator xdc.HistoryRereplicator,
	nDCHistoryResender xdc.NDCHistoryResender,
	logger log.Logger,
	metricsClient metrics.Client,
	clusterName string,
	config *Config,
) queueTaskExecutor {
	return &visibilityQueueStandbyTaskExecutor{
		visibilityQueueTaskExecutorBase: newVisibilityQueueTaskExecutorBase(
			shard,
			historyService,
			logger,
			metricsClient,
			config,
		),
		clusterName:         clusterName,
		historyRereplicator: historyRereplicator,
		nDCHistoryResender:  nDCHistoryResender,
	}
}

func (t *visibilityQueueStandbyTaskExecutor) execute(
	taskInfo queueTaskInfo,
	shouldProcessTask bool,
) error {

	visibilityTask, ok := taskInfo.(*persistenceblobs.VisibilityTaskInfo)
	if !ok {
		return errUnexpectedQueueTask
	}

	if !shouldProcessTask &&
		visibilityTask.TaskType != persistence.VisibilityTaskTypeRecordCloseExecution {
		// guarantee the processing of workflow execution close
		return nil
	}

	switch visibilityTask.TaskType {
	case persistence.VisibilityTaskTypeRecordCloseExecution:
		return t.processRecordCloseExecution(visibilityTask)
	case persistence.VisibilityTaskTypeRecordWorkflowStarted:
		return t.processRecordWorkflowStarted(visibilityTask)
	case persistence.VisibilityTaskTypeUpsertWorkflowSearchAttributes:
		return t.processUpsertWorkflowSearchAttributes(visibilityTask)
	default:
		return errUnknownVisibilityTask
	}
}

func (t *visibilityQueueStandbyTaskExecutor) processRecordCloseExecution(
	visibilityTask *persistenceblobs.VisibilityTaskInfo,
) error {

	processTaskIfClosed := true
	actionFn := func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {

		if mutableState.IsWorkflowExecutionRunning() {
			// this can happen if workflow is reset.
			return nil, nil
		}

		completionEvent, err := mutableState.GetCompletionEvent()
		if err != nil {
			return nil, err
		}
		wfCloseTime := completionEvent.GetTimestamp()

		executionInfo := mutableState.GetExecutionInfo()
		workflowTypeName := executionInfo.WorkflowTypeName
		workflowCloseTimestamp := wfCloseTime
		workflowCloseStatus := executionInfo.CloseStatus
		workflowHistoryLength := mutableState.GetNextEventID() - 1
		startEvent, err := mutableState.GetStartEvent()
		if err != nil {
			return nil, err
		}
		workflowStartTimestamp := startEvent.GetTimestamp()
		workflowExecutionTimestamp := getWorkflowExecutionTimestamp(mutableState, startEvent)
		visibilityMemo := getWorkflowMemo(executionInfo.Memo)
		searchAttr := executionInfo.SearchAttributes

		lastWriteVersion, err := mutableState.GetLastWriteVersion()
		if err != nil {
			return nil, err
		}
		ok, err := verifyTaskVersion(t.shard, t.logger, visibilityTask.DomainID, lastWriteVersion, visibilityTask.Version, visibilityTask)
		if err != nil || !ok {
			return nil, err
		}

		// DO NOT REPLY TO PARENT
		// since event replication should be done by active cluster
		return nil, t.recordWorkflowClosed(
			primitives.UUIDString(visibilityTask.DomainID),
			visibilityTask.WorkflowID,
			primitives.UUIDString(visibilityTask.RunID),
			workflowTypeName,
			workflowStartTimestamp,
			workflowExecutionTimestamp.UnixNano(),
			workflowCloseTimestamp,
			workflowCloseStatus,
			workflowHistoryLength,
			visibilityTask.GetTaskID(),
			visibilityMemo,
			searchAttr,
		)
	}

	return t.processVisibility(
		processTaskIfClosed,
		visibilityTask,
		actionFn,
		standbyTaskPostActionNoOp,
	) // no op post action, since the entire workflow is finished
}

func (t *visibilityQueueStandbyTaskExecutor) processRecordWorkflowStarted(
	visibilityTask *persistenceblobs.VisibilityTaskInfo,
) error {

	// TODO becker can we just process these anyways?
	processTaskIfClosed := false
	return t.processVisibility(
		processTaskIfClosed,
		visibilityTask,
		func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {
			return nil, t.processRecordWorkflowStartedOrUpsertHelper(visibilityTask, mutableState, true)
		},
		standbyTaskPostActionNoOp,
	)
}

func (t *visibilityQueueStandbyTaskExecutor) processUpsertWorkflowSearchAttributes(
	visibilityTask *persistenceblobs.VisibilityTaskInfo,
) error {

	// TODO becker can we just process these anyways?
	processTaskIfClosed := false
	return t.processVisibility(
		processTaskIfClosed,
		visibilityTask,
		func(context workflowExecutionContext, mutableState mutableState) (interface{}, error) {
			return nil, t.processRecordWorkflowStartedOrUpsertHelper(visibilityTask, mutableState, false)
		},
		standbyTaskPostActionNoOp,
	)
}

func (t *visibilityQueueStandbyTaskExecutor) processRecordWorkflowStartedOrUpsertHelper(
	visibilityTask *persistenceblobs.VisibilityTaskInfo,
	mutableState mutableState,
	isRecordStart bool,
) error {

	// verify task version for RecordWorkflowStarted.
	// upsert doesn't require verifyTask, because it is just a sync of mutableState.
	if isRecordStart {
		startVersion, err := mutableState.GetStartVersion()
		if err != nil {
			return err
		}
		ok, err := verifyTaskVersion(t.shard, t.logger, visibilityTask.DomainID, startVersion, visibilityTask.Version, visibilityTask)
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

	if isRecordStart {
		return t.recordWorkflowStarted(
			primitives.UUIDString(visibilityTask.DomainID),
			visibilityTask.WorkflowID,
			primitives.UUIDString(visibilityTask.RunID),
			wfTypeName,
			startTimestamp,
			executionTimestamp.UnixNano(),
			workflowTimeout,
			visibilityTask.GetTaskID(),
			visibilityMemo,
			searchAttr,
		)
	}
	return t.upsertWorkflowExecution(
		primitives.UUIDString(visibilityTask.DomainID),
		visibilityTask.WorkflowID,
		primitives.UUIDString(visibilityTask.RunID),
		wfTypeName,
		startTimestamp,
		executionTimestamp.UnixNano(),
		workflowTimeout,
		visibilityTask.GetTaskID(),
		visibilityMemo,
		searchAttr,
	)

}

func (t *visibilityQueueStandbyTaskExecutor) processVisibility(
	processTaskIfClosed bool,
	taskInfo queueTaskInfo,
	actionFn standbyActionFn,
	postActionFn standbyPostActionFn,
) (retError error) {

	visibilityTask := taskInfo.(*persistenceblobs.VisibilityTaskInfo)
	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(visibilityTask),
	)
	if err != nil {
		return err
	}
	defer func() {
		if retError == ErrTaskRetry {
			release(nil)
		} else {
			release(retError)
		}
	}()

	mutableState, err := loadMutableStateForVisibilityTask(context, visibilityTask, t.metricsClient, t.logger)
	if err != nil || mutableState == nil {
		return err
	}

	if !mutableState.IsWorkflowExecutionRunning() && !processTaskIfClosed {
		// workflow already finished, no need to process the timer
		return nil
	}

	historyResendInfo, err := actionFn(context, mutableState)
	if err != nil {
		return err
	}

	release(nil)
	return postActionFn(taskInfo, historyResendInfo, t.logger)
}
