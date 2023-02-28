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
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	ExecutableActivityTask struct {
		ProcessToolBox

		definition.WorkflowKey
		ExecutableTask
		req *historyservice.SyncActivityRequest

		// variables to be perhaps removed (not essential to logic)
		sourceClusterName string
	}
)

var _ ctasks.Task = (*ExecutableActivityTask)(nil)
var _ TrackableExecutableTask = (*ExecutableActivityTask)(nil)

func NewExecutableActivityTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	task *replicationspb.SyncActivityTaskAttributes,
	sourceClusterName string,
) *ExecutableActivityTask {
	return &ExecutableActivityTask{
		ProcessToolBox: processToolBox,

		WorkflowKey: definition.NewWorkflowKey(task.NamespaceId, task.WorkflowId, task.RunId),
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.SyncActivityTaskScope,
			taskCreationTime,
			time.Now().UTC(),
		),
		req: &historyservice.SyncActivityRequest{
			NamespaceId:        task.NamespaceId,
			WorkflowId:         task.WorkflowId,
			RunId:              task.RunId,
			Version:            task.Version,
			ScheduledEventId:   task.ScheduledEventId,
			ScheduledTime:      task.ScheduledTime,
			StartedEventId:     task.StartedEventId,
			StartedTime:        task.StartedTime,
			LastHeartbeatTime:  task.LastHeartbeatTime,
			Details:            task.Details,
			Attempt:            task.Attempt,
			LastFailure:        task.LastFailure,
			LastWorkerIdentity: task.LastWorkerIdentity,
			VersionHistory:     task.GetVersionHistory(),
		},

		sourceClusterName: sourceClusterName,
	}
}

func (e *ExecutableActivityTask) Execute() error {
	namespaceName, apply, nsError := e.GetNamespaceInfo(e.NamespaceID)
	if nsError != nil {
		return nsError
	} else if !apply {
		return nil
	}
	ctx, cancel := newTaskContext(namespaceName)
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
	return engine.SyncActivity(ctx, e.req)
}

func (e *ExecutableActivityTask) HandleErr(err error) error {
	switch retryErr := err.(type) {
	case nil, *serviceerror.NotFound:
		return nil
	case *serviceerrors.RetryReplication:
		namespaceName, _, nsError := e.GetNamespaceInfo(e.NamespaceID)
		if nsError != nil {
			return err
		}
		ctx, cancel := newTaskContext(namespaceName)
		defer cancel()

		if resendErr := e.Resend(
			ctx,
			e.sourceClusterName,
			retryErr,
		); resendErr != nil {
			return err
		}
		return e.Execute()
	default:
		return err
	}
}
