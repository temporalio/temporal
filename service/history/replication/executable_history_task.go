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

	commonpb "go.temporal.io/api/common/v1"
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
	ExecutableHistoryTask struct {
		ProcessToolBox

		definition.WorkflowKey
		ExecutableTask
		req *historyservice.ReplicateEventsV2Request

		// variables to be perhaps removed (not essential to logic)
		sourceClusterName string
	}
)

var _ ctasks.Task = (*ExecutableHistoryTask)(nil)
var _ TrackableExecutableTask = (*ExecutableHistoryTask)(nil)

func NewExecutableHistoryTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	task *replicationspb.HistoryTaskAttributes,
	sourceClusterName string,
) *ExecutableHistoryTask {
	return &ExecutableHistoryTask{
		ProcessToolBox: processToolBox,

		WorkflowKey: definition.NewWorkflowKey(task.NamespaceId, task.WorkflowId, task.RunId),
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.HistoryReplicationTaskScope,
			taskCreationTime,
			time.Now().UTC(),
		),
		req: &historyservice.ReplicateEventsV2Request{
			NamespaceId: task.NamespaceId,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: task.WorkflowId,
				RunId:      task.RunId,
			},
			VersionHistoryItems: task.VersionHistoryItems,
			Events:              task.Events,
			// new run events does not need version history since there is no prior events
			NewRunEvents: task.NewRunEvents,
		},

		sourceClusterName: sourceClusterName,
	}
}

func (e *ExecutableHistoryTask) Execute() error {
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
	return engine.ReplicateEventsV2(ctx, e.req)
}

func (e *ExecutableHistoryTask) HandleErr(err error) error {
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
