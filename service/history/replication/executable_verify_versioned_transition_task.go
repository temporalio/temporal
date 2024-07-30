// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package replication

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/workflow"

	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	ExecutableVerifyVersionedTransitionTask struct {
		ProcessToolBox

		definition.WorkflowKey
		ExecutableTask

		taskAttr *replicationspb.VerifyVersionedTransitionTaskAttributes
	}
)

var _ ctasks.Task = (*ExecutableVerifyVersionedTransitionTask)(nil)
var _ TrackableExecutableTask = (*ExecutableVerifyVersionedTransitionTask)(nil)

func NewExecutableVerifyVersionedTransitionTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	sourceClusterName string,
	replicationTask *replicationspb.ReplicationTask,
) *ExecutableVerifyVersionedTransitionTask {
	task := replicationTask.GetVerifyVersionedTransitionTaskAttributes()
	return &ExecutableVerifyVersionedTransitionTask{
		ProcessToolBox: processToolBox,

		WorkflowKey: definition.NewWorkflowKey(task.NamespaceId, task.WorkflowId, task.RunId),
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.VerifyVersionedTransitionTaskScope,
			taskCreationTime,
			time.Now().UTC(),
			sourceClusterName,
			replicationTask.Priority,
			replicationTask,
		),
		taskAttr: task,
	}
}

func (e *ExecutableVerifyVersionedTransitionTask) QueueID() interface{} {
	return e.WorkflowKey
}

func (e *ExecutableVerifyVersionedTransitionTask) Execute() error {
	if e.TerminalState() {
		return nil
	}

	namespaceName, apply, nsError := e.GetNamespaceInfo(headers.SetCallerInfo(
		context.Background(),
		headers.SystemPreemptableCallerInfo,
	), e.NamespaceID)
	if nsError != nil {
		return nsError
	} else if !apply {
		e.Logger.Warn("Skipping the replication task",
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.WorkflowID),
			tag.WorkflowRunID(e.RunID),
			tag.TaskID(e.ExecutableTask.TaskID()),
		)
		metrics.ReplicationTasksSkipped.With(e.MetricsHandler).Record(
			1,
			metrics.OperationTag(metrics.VerifyVersionedTransitionTaskScope),
			metrics.NamespaceTag(namespaceName),
		)
		return nil
	}

	ctx, cancel := newTaskContext(namespaceName, e.Config.ReplicationTaskApplyTimeout())
	defer cancel()

	ms, err := e.getMutableState(ctx, e.RunID)
	if err != nil {
		switch err.(type) {
		case *serviceerror.NotFound:
			return serviceerrors.NewSyncState(
				"missing mutable state, resend",
				e.NamespaceID,
				e.WorkflowID,
				e.RunID,
				e.ReplicationTask().VersionedTransition,
			)
		default:
			return err
		}
	}

	transitionHistory := ms.GetExecutionInfo().TransitionHistory
	err = workflow.TransitionHistoryStalenessCheck(transitionHistory, e.ReplicationTask().VersionedTransition)

	// case 1: versioned transition up to date on current mutable state
	if err == nil {
		if ms.GetNextEventID() < e.taskAttr.NextEventId {
			return serviceerror.NewDataLoss(fmt.Sprintf("Workflow event missed. NamespaceId: %v, workflowId: %v, runId: %v, nextEventId: %v, versionedTransition: %v",
				e.NamespaceID, e.WorkflowID, e.RunID, e.taskAttr.NextEventId, e.ReplicationTask().VersionedTransition))
		}
		return e.verifyNewRunExist(ctx)
	}

	nextEventId := e.taskAttr.NextEventId
	eventVersion := e.ReplicationTask().VersionedTransition.NamespaceFailoverVersion
	_, err = versionhistory.FindFirstVersionHistoryIndexByVersionHistoryItem(ms.GetExecutionInfo().VersionHistories, &historyspb.VersionHistoryItem{
		EventId: nextEventId - 1,
		Version: eventVersion,
	})
	// case 2: events are to date on non-current branch
	if err == nil {
		return e.verifyNewRunExist(ctx)
	}

	// case 3: verify task has newer versioned transition, need to sync state
	if workflow.CompareVersionedTransition(e.ReplicationTask().VersionedTransition, transitionHistory[len(transitionHistory)-1]) > 0 {
		return serviceerrors.NewSyncState(
			"mutable state not up to date",
			e.NamespaceID,
			e.WorkflowID,
			e.RunID,
			e.ReplicationTask().VersionedTransition,
		)
	}
	// case 4: verify task is an older versioned transition, and events are not up-to-date, resend the events
	return e.getRetryReplication(ctx, ms)
}

func (e *ExecutableVerifyVersionedTransitionTask) getRetryReplication(ctx context.Context, ms workflow.MutableState) error {
	sourceAdmin, err := e.ClientBean.GetRemoteAdminClient(e.SourceClusterName())
	if err != nil {
		return err
	}
	res, err := sourceAdmin.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: e.NamespaceID,
		Execution: &common.WorkflowExecution{
			WorkflowId: e.WorkflowID,
			RunId:      e.RunID,
		},
	})
	if err != nil {
		return err
	}
	// calculate the events diff for resend
	sourceHistories := res.DatabaseMutableState.GetExecutionInfo().VersionHistories
	index, err := versionhistory.FindFirstVersionHistoryIndexByVersionHistoryItem(sourceHistories, e.taskAttr.LastVersionHistoryItem)
	if err != nil {
		return err
	}
	sourceHistory := sourceHistories.Histories[index]
	item, _, err := versionhistory.FindLCAVersionHistoryItemAndIndex(ms.GetExecutionInfo().VersionHistories, sourceHistory)
	if err != nil {
		return err
	}
	return serviceerrors.NewRetryReplication(
		"retry replication",
		e.NamespaceID,
		e.WorkflowID,
		e.RunID,
		item.EventId,
		item.Version,
		e.taskAttr.NextEventId,
		e.ReplicationTask().VersionedTransition.NamespaceFailoverVersion,
	)
}

func (e *ExecutableVerifyVersionedTransitionTask) verifyNewRunExist(ctx context.Context) error {
	if len(e.taskAttr.NewRunId) == 0 {
		return nil
	}
	_, err := e.getMutableState(ctx, e.taskAttr.NewRunId)
	switch err.(type) {
	case nil:
		return nil
	case *serviceerror.NotFound:
		return serviceerror.NewDataLoss(fmt.Sprintf("workflow new run not found. NamespaceId: %v, workflowId: %v, runId: %v, newRunId: %v",
			e.NamespaceID, e.WorkflowID, e.RunID, e.taskAttr.NewRunId))
	default:
		return err
	}
}

func (e *ExecutableVerifyVersionedTransitionTask) getMutableState(ctx context.Context, runId string) (_ workflow.MutableState, retError error) {
	shardContext, err := e.ShardController.GetShardByNamespaceWorkflow(
		namespace.ID(e.NamespaceID),
		e.WorkflowID,
	)
	wfContext, release, err := e.WorkflowCache.GetOrCreateWorkflowExecution(
		ctx,
		shardContext,
		namespace.ID(e.NamespaceID),
		&common.WorkflowExecution{
			WorkflowId: e.WorkflowID,
			RunId:      runId,
		},
		locks.PriorityLow,
	)
	if err != nil {
		return nil, err
	}
	defer func() { release(retError) }()
	return wfContext.LoadMutableState(ctx, shardContext)
}

func (e *ExecutableVerifyVersionedTransitionTask) HandleErr(err error) error {
	switch taskErr := err.(type) {
	case *serviceerrors.SyncState:
		namespaceName, _, nsError := e.GetNamespaceInfo(headers.SetCallerInfo(
			context.Background(),
			headers.SystemPreemptableCallerInfo,
		), e.NamespaceID)
		if nsError != nil {
			return err
		}
		ctx, cancel := newTaskContext(namespaceName, e.Config.ReplicationTaskApplyTimeout())
		defer cancel()

		if doContinue, syncStateErr := e.SyncState(
			ctx,
			e.ExecutableTask.SourceClusterName(),
			taskErr,
			ResendAttempt,
		); syncStateErr != nil || !doContinue {
			return err
		}
		return e.Execute()
	case *serviceerrors.RetryReplication:
		namespaceName, _, nsError := e.GetNamespaceInfo(headers.SetCallerInfo(
			context.Background(),
			headers.SystemPreemptableCallerInfo,
		), e.NamespaceID)
		if nsError != nil {
			return err
		}
		ctx, cancel := newTaskContext(namespaceName, e.Config.ReplicationTaskApplyTimeout())
		defer cancel()

		if doContinue, resendErr := e.Resend(
			ctx,
			e.ExecutableTask.SourceClusterName(),
			taskErr,
			ResendAttempt,
		); resendErr != nil || !doContinue {
			return err
		}
		return e.Execute()
	default:
		e.Logger.Error("VerifyVersionedTransition replication task encountered error",
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.WorkflowID),
			tag.WorkflowRunID(e.RunID),
			tag.TaskID(e.ExecutableTask.TaskID()),
			tag.Error(err),
		)
		return err
	}
}
