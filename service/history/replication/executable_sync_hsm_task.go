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
	"time"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/shard"
)

// This is mostly copied from ExecutableActivityStateTask
// The 4 replication executable task implemenatations are quite similar
// we may want to do some refactoring later.

type (
	ExecutableSyncHSMTask struct {
		ProcessToolBox

		definition.WorkflowKey
		ExecutableTask

		taskAttr *replicationspb.SyncHSMAttributes

		markPoisonPillAttempts int
	}
)

var _ ctasks.Task = (*ExecutableSyncHSMTask)(nil)
var _ TrackableExecutableTask = (*ExecutableSyncHSMTask)(nil)

// var _ BatchableTask = (*ExecutableSyncHSMTask)(nil)

func NewExecutableSyncHSMTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	task *replicationspb.SyncHSMAttributes,
	sourceClusterName string,
	priority enumsspb.TaskPriority,
) *ExecutableSyncHSMTask {
	return &ExecutableSyncHSMTask{
		ProcessToolBox: processToolBox,

		WorkflowKey: definition.NewWorkflowKey(task.NamespaceId, task.WorkflowId, task.RunId),
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.SyncHSMTaskScope,
			taskCreationTime,
			time.Now().UTC(),
			sourceClusterName,
			priority,
		),
		taskAttr:               task,
		markPoisonPillAttempts: 0,
	}
}

func (e *ExecutableSyncHSMTask) QueueID() interface{} {
	return e.WorkflowKey
}

func (e *ExecutableSyncHSMTask) Execute() error {
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
			metrics.OperationTag(metrics.SyncHSMTaskScope),
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
	return engine.SyncHSM(ctx, &shard.SyncHSMRequest{
		WorkflowKey:         e.WorkflowKey,
		StateMachineNode:    e.taskAttr.StateMachineNode,
		EventVersionHistory: e.taskAttr.VersionHistory,
	})

}

func (e *ExecutableSyncHSMTask) HandleErr(err error) error {
	switch retryErr := err.(type) {
	case nil, *serviceerror.NotFound:
		return nil
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
			retryErr,
			ResendAttempt,
		); resendErr != nil || !doContinue {
			return err
		}
		return e.Execute()
	default:
		e.Logger.Error("Sync HSM replication task encountered error",
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.WorkflowID),
			tag.WorkflowRunID(e.RunID),
			tag.TaskID(e.ExecutableTask.TaskID()),
			tag.Error(err),
		)
		return err
	}
}

func (e *ExecutableSyncHSMTask) MarkPoisonPill() error {

	replicationTaskInfo := e.toReplicationTaskInfo()

	if e.markPoisonPillAttempts >= MarkPoisonPillMaxAttempts {
		e.Logger.Error("MarkPoisonPill reached max attempts",
			tag.SourceCluster(e.SourceClusterName()),
			tag.ReplicationTask(replicationTaskInfo),
			tag.NewAnyTag("sync-hsm-task-attr", e.taskAttr),
		)
		return nil
	}
	e.markPoisonPillAttempts++

	shardContext, err := e.ShardController.GetShardByNamespaceWorkflow(
		namespace.ID(e.NamespaceID),
		e.WorkflowID,
	)
	if err != nil {
		return err
	}

	e.Logger.Error("enqueue sync HSM replication task to DLQ",
		tag.ShardID(shardContext.GetShardID()),
		tag.WorkflowNamespaceID(e.NamespaceID),
		tag.WorkflowID(e.WorkflowID),
		tag.WorkflowRunID(e.RunID),
		tag.TaskID(e.ExecutableTask.TaskID()),
	)

	ctx, cancel := newTaskContext(e.NamespaceID, e.Config.ReplicationTaskApplyTimeout())
	defer cancel()

	// TODO: GetShardID will break GetDLQReplicationMessages we need to handle DLQ for cross shard replication.
	return writeTaskToDLQ(ctx, e.DLQWriter, shardContext, e.SourceClusterName(), replicationTaskInfo)
}

func (e *ExecutableSyncHSMTask) toReplicationTaskInfo() *persistencespb.ReplicationTaskInfo {
	return &persistencespb.ReplicationTaskInfo{
		NamespaceId:    e.NamespaceID,
		WorkflowId:     e.WorkflowID,
		RunId:          e.RunID,
		TaskType:       enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM,
		TaskId:         e.ExecutableTask.TaskID(),
		VisibilityTime: timestamppb.New(e.TaskCreationTime()),
	}
}

// TODO: implement the following methods to batch syncHSM task if needed
// if not implemented the task will be treated as unbatchable

// func (e *ExecutableSyncHSMTask) BatchWith(incomingTask BatchableTask) (TrackableExecutableTask, bool) {
// 	panic("not implemented")
// }

// func (e *ExecutableSyncHSMTask) CanBatch() bool {
// 	panic("not implemented")
// }

// func (e *ExecutableSyncHSMTask) MarkUnbatchable() {
// 	panic("not implemented")
// }
