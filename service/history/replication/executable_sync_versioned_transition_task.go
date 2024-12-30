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

	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	ExecutableSyncVersionedTransitionTask struct {
		ProcessToolBox

		definition.WorkflowKey
		ExecutableTask

		taskAttr *replicationspb.SyncVersionedTransitionTaskAttributes
	}
)

var _ ctasks.Task = (*ExecutableSyncVersionedTransitionTask)(nil)
var _ TrackableExecutableTask = (*ExecutableSyncVersionedTransitionTask)(nil)

func NewExecutableSyncVersionedTransitionTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	sourceClusterName string,
	sourceShardKey ClusterShardKey,
	replicationTask *replicationspb.ReplicationTask,
) *ExecutableSyncVersionedTransitionTask {
	task := replicationTask.GetSyncVersionedTransitionTaskAttributes()
	return &ExecutableSyncVersionedTransitionTask{
		ProcessToolBox: processToolBox,

		WorkflowKey: definition.NewWorkflowKey(task.NamespaceId, task.WorkflowId, task.RunId),
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.SyncVersionedTransitionTaskScope,
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

func (e *ExecutableSyncVersionedTransitionTask) QueueID() interface{} {
	return e.WorkflowKey
}

func (e *ExecutableSyncVersionedTransitionTask) Execute() error {
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
			metrics.OperationTag(metrics.SyncVersionedTransitionTaskScope),
			metrics.NamespaceTag(namespaceName),
		)
		return nil
	}

	ctx, cancel := newTaskContext(namespaceName, e.Config.ReplicationTaskApplyTimeout())
	defer cancel()
	shardContext, err := e.ShardController.GetShardByNamespaceWorkflow(
		namespace.ID(e.NamespaceID),
		e.WorkflowID,
	)
	if err != nil {
		return err
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}
	return engine.ReplicateVersionedTransition(ctx, e.taskAttr.VersionedTransitionArtifact, e.SourceClusterName())
}

func (e *ExecutableSyncVersionedTransitionTask) HandleErr(err error) error {
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
		var mutation *replicationspb.SyncWorkflowStateMutationAttributes
		var snapshot *replicationspb.SyncWorkflowStateSnapshotAttributes
		switch artifactType := e.taskAttr.VersionedTransitionArtifact.StateAttributes.(type) {
		case *replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes:
			snapshot = e.taskAttr.VersionedTransitionArtifact.GetSyncWorkflowStateSnapshotAttributes()
		case *replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes:
			mutation = e.taskAttr.VersionedTransitionArtifact.GetSyncWorkflowStateMutationAttributes()
		default:
			return serviceerror.NewInvalidArgument(fmt.Sprintf("unknown artifact type %T", artifactType))
		}
		versionHistories := func() *historyspb.VersionHistories {
			if snapshot != nil {
				return snapshot.State.ExecutionInfo.VersionHistories
			}
			return mutation.StateMutation.ExecutionInfo.VersionHistories
		}()
		history, err := versionhistory.GetCurrentVersionHistory(versionHistories)
		if err != nil {
			return err
		}

		startEvent := taskErr.StartEventId + 1
		endEvent := taskErr.EndEventId - 1
		startEventVersion, err := versionhistory.GetVersionHistoryEventVersion(history, startEvent)
		if err != nil {
			return err
		}
		endEventVersion, err := versionhistory.GetVersionHistoryEventVersion(history, endEvent)
		if err != nil {
			return err
		}
		if resendErr := e.BackFillEvents(
			ctx,
			e.ExecutableTask.SourceClusterName(),
			definition.NewWorkflowKey(e.NamespaceID, e.WorkflowID, e.RunID),
			startEvent,
			startEventVersion,
			endEvent,
			endEventVersion,
			"",
		); resendErr != nil {
			return err
		}
		return e.Execute()
	default:
		e.Logger.Error("Sync Versioned Transition replication task encountered error",
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.WorkflowID),
			tag.WorkflowRunID(e.RunID),
			tag.TaskID(e.ExecutableTask.TaskID()),
			tag.Error(err),
		)
		return err
	}
}
