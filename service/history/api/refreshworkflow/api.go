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

package refreshworkflow

import (
	"context"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	shard shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (retError error) {
	err := api.ValidateNamespaceUUID(namespace.ID(workflowKey.NamespaceID))
	if err != nil {
		return err
	}

	workflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		api.BypassMutableStateConsistencyPredicate,
		workflowKey,
		workflow.LockPriorityLow,
	)
	if err != nil {
		return err
	}
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	mutableState := workflowLease.GetMutableState()
	mutableStateTaskRefresher := workflow.NewTaskRefresher(
		shard,
		shard.GetConfig(),
		shard.GetNamespaceRegistry(),
		shard.GetLogger(),
	)

	err = mutableStateTaskRefresher.RefreshTasks(ctx, mutableState)
	if err != nil {
		return err
	}

	return shard.AddTasks(ctx, &persistence.AddHistoryTasksRequest{
		ShardID: shard.GetShardID(),
		// RangeID is set by shard
		NamespaceID: workflowKey.NamespaceID,
		WorkflowID:  workflowKey.WorkflowID,
		Tasks:       mutableState.PopTasks(),
	})
}
