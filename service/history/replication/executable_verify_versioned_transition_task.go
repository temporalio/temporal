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
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	common2 "go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/workflow"
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
	sourceShardKey ClusterShardKey,
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
			sourceShardKey,
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
				nil,
				nil,
			)
		default:
			return err
		}
	}

	transitionHistory := ms.GetExecutionInfo().TransitionHistory
	if len(transitionHistory) == 0 {
		return nil
	}
	err = workflow.TransitionHistoryStalenessCheck(transitionHistory, e.ReplicationTask().VersionedTransition)

	// case 1: VersionedTransition is up-to-date on current mutable state
	if err == nil {
		if ms.GetNextEventId() < e.taskAttr.NextEventId {
			return serviceerror.NewDataLoss(fmt.Sprintf("Workflow event missed. NamespaceId: %v, workflowId: %v, runId: %v, expected last eventId: %v, versionedTransition: %v",
				e.NamespaceID, e.WorkflowID, e.RunID, e.taskAttr.NextEventId-1, e.ReplicationTask().VersionedTransition))
		}
		return e.verifyNewRunExist(ctx)
	}

	// case 2: verify task has newer VersionedTransition, need to sync state
	if workflow.CompareVersionedTransition(e.ReplicationTask().VersionedTransition, transitionHistory[len(transitionHistory)-1]) > 0 {
		return serviceerrors.NewSyncState(
			"mutable state not up to date",
			e.NamespaceID,
			e.WorkflowID,
			e.RunID,
			transitionHistory[len(transitionHistory)-1],
			ms.GetExecutionInfo().VersionHistories,
		)
	}
	// case 3: state transition is on non-current branch, but no event to verify
	if e.taskAttr.NextEventId == common2.EmptyEventID {
		return e.verifyNewRunExist(ctx)
	}

	targetHistory := &historyspb.VersionHistory{
		Items: e.taskAttr.EventVersionHistory,
	}

	lcaItem, _, err := versionhistory.FindLCAVersionHistoryItemAndIndex(ms.GetExecutionInfo().VersionHistories, targetHistory)
	if err != nil {
		return err
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(targetHistory)
	if err != nil {
		return err
	}
	// case 4: event on non-current branch are up-to-date
	if versionhistory.IsEqualVersionHistoryItem(lcaItem, lastItem) {
		return e.verifyNewRunExist(ctx)
	}
	// case 5: event on non-current branch are not up-to-date, we need to backfill events
	startEventVersion, err := versionhistory.GetVersionHistoryEventVersion(targetHistory, lcaItem.EventId+1)
	if err != nil {
		return err
	}
	return e.BackFillEvents(
		ctx,
		e.ExecutableTask.SourceClusterName(),
		e.WorkflowKey,
		lcaItem.EventId+1,
		startEventVersion,
		lastItem.EventId,
		lastItem.Version,
		e.taskAttr.NewRunId,
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

func (e *ExecutableVerifyVersionedTransitionTask) getMutableState(ctx context.Context, runId string) (_ *persistencespb.WorkflowMutableState, retError error) {
	shardContext, err := e.ShardController.GetShardByNamespaceWorkflow(
		namespace.ID(e.NamespaceID),
		e.WorkflowID,
	)
	if err != nil {
		return nil, err
	}
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
	ms, err := wfContext.LoadMutableState(ctx, shardContext)
	if err != nil {
		return nil, err
	}

	return ms.CloneToProto(), nil
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
			taskErr,
			ResendAttempt,
		); syncStateErr != nil || !doContinue {
			if syncStateErr != nil {
				e.Logger.Error("VerifyVersionedTransition replication task encountered error during sync state",
					tag.WorkflowNamespaceID(e.NamespaceID),
					tag.WorkflowID(e.WorkflowID),
					tag.WorkflowRunID(e.RunID),
					tag.TaskID(e.ExecutableTask.TaskID()),
					tag.Error(syncStateErr),
				)
			}
			// return original task processing error
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
