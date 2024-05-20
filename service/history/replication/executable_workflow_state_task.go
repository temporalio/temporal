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

package replication

import (
	"context"
	"time"

	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
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
	ExecutableWorkflowStateTask struct {
		ProcessToolBox

		definition.WorkflowKey
		ExecutableTask
		req *historyservice.ReplicateWorkflowStateRequest
	}
)

var _ ctasks.Task = (*ExecutableWorkflowStateTask)(nil)
var _ TrackableExecutableTask = (*ExecutableWorkflowStateTask)(nil)

// TODO should workflow task be batched?

func NewExecutableWorkflowStateTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	task *replicationspb.SyncWorkflowStateTaskAttributes,
	sourceClusterName string,
) *ExecutableWorkflowStateTask {
	namespaceID := task.GetWorkflowState().ExecutionInfo.NamespaceId
	workflowID := task.GetWorkflowState().ExecutionInfo.WorkflowId
	runID := task.GetWorkflowState().ExecutionState.RunId
	return &ExecutableWorkflowStateTask{
		ProcessToolBox: processToolBox,

		WorkflowKey: definition.NewWorkflowKey(namespaceID, workflowID, runID),
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.SyncWorkflowStateTaskScope,
			taskCreationTime,
			time.Now().UTC(),
			sourceClusterName,
		),
		req: &historyservice.ReplicateWorkflowStateRequest{
			NamespaceId:   namespaceID,
			WorkflowState: task.GetWorkflowState(),
			RemoteCluster: sourceClusterName,
		},
	}
}

func (e *ExecutableWorkflowStateTask) QueueID() interface{} {
	return e.WorkflowKey
}

func (e *ExecutableWorkflowStateTask) Execute() error {
	if e.TerminalState() {
		return nil
	}

	namespaceName, apply, err := e.GetNamespaceInfo(headers.SetCallerInfo(
		context.Background(),
		headers.SystemPreemptableCallerInfo,
	), e.NamespaceID)
	if err != nil {
		return err
	} else if !apply {
		e.Logger.Warn("Skipping the replication task",
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.WorkflowID),
			tag.WorkflowRunID(e.RunID),
			tag.TaskID(e.ExecutableTask.TaskID()),
		)
		metrics.ReplicationTasksSkipped.With(e.MetricsHandler).Record(
			1,
			metrics.OperationTag(metrics.SyncWorkflowStateTaskScope),
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
	return engine.ReplicateWorkflowState(ctx, e.req)
}

func (e *ExecutableWorkflowStateTask) HandleErr(err error) error {
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
		e.Logger.Error("workflow state replication task encountered error",
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.WorkflowID),
			tag.WorkflowRunID(e.RunID),
			tag.TaskID(e.ExecutableTask.TaskID()),
			tag.Error(err),
		)
		return err
	}
}

func (e *ExecutableWorkflowStateTask) MarkPoisonPill() error {
	shardContext, err := e.ShardController.GetShardByNamespaceWorkflow(
		namespace.ID(e.NamespaceID),
		e.WorkflowID,
	)
	if err != nil {
		return err
	}

	// TODO: GetShardID will break GetDLQReplicationMessages we need to handle DLQ for cross shard replication.
	taskInfo := &persistencespb.ReplicationTaskInfo{
		NamespaceId: e.NamespaceID,
		WorkflowId:  e.WorkflowID,
		RunId:       e.RunID,
		TaskId:      e.ExecutableTask.TaskID(),
		TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE,
	}

	e.Logger.Error("enqueue workflow state replication task to DLQ",
		tag.ShardID(shardContext.GetShardID()),
		tag.WorkflowNamespaceID(e.NamespaceID),
		tag.WorkflowID(e.WorkflowID),
		tag.WorkflowRunID(e.RunID),
		tag.TaskID(e.ExecutableTask.TaskID()),
	)

	ctx, cancel := newTaskContext(e.NamespaceID, e.Config.ReplicationTaskApplyTimeout())
	defer cancel()

	return writeTaskToDLQ(ctx, e.DLQWriter, shardContext, e.SourceClusterName(), taskInfo)
}
