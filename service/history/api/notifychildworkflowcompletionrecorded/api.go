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

package notifychildworkflowcompletionrecorded

import (
	"context"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	request *historyservice.NotifyChildExecutionCompletionRecordedRequest,
	shard shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.NotifyChildExecutionCompletionRecordedResponse, retError error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if err := api.ValidateNamespaceUUID(namespaceID); err != nil {
		return nil, err
	}

	workflowContext, err := workflowConsistencyChecker.GetWorkflowContext(
		ctx,
		nil,
		// it's ok we have stale state during notification,
		// if notification is lost, close workflow verification will contact parent workflow.
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			request.NamespaceId,
			request.ChildExecution.WorkflowId,
			request.ChildExecution.RunId,
		),
		workflow.LockPriorityLow,
	)
	if err != nil {
		return nil, err
	}
	defer func() { workflowContext.GetReleaseFn()(retError) }()

	mutableState := workflowContext.GetMutableState()
	executionInfo := mutableState.GetExecutionInfo()
	if executionInfo.ParentInitiatedId != request.ParentInitiatedId ||
		executionInfo.ParentInitiatedVersion != request.ParentInitiatedVersion {
		shard.GetLogger().Info("child workflow's parent event ID / version mismatch",
			tag.ShardID(shard.GetShardID()),
			tag.WorkflowNamespaceID(mutableState.GetWorkflowKey().NamespaceID),
			tag.WorkflowID(mutableState.GetWorkflowKey().WorkflowID),
			tag.WorkflowRunID(mutableState.GetWorkflowKey().RunID))
		return nil, serviceerror.NewNotFound("child workflow's parent event ID / version mismatch")
	}

	mutableState.GetEphemeralMessages().Set(workflow.NewEphemeralMessageChildCompletionKey(
		request.ParentInitiatedId,
		request.ParentInitiatedVersion,
	), struct{}{})
	shard.GetLogger().Info("receive notification",
		tag.ShardID(shard.GetShardID()),
		tag.WorkflowNamespaceID(mutableState.GetWorkflowKey().NamespaceID),
		tag.WorkflowID(mutableState.GetWorkflowKey().WorkflowID),
		tag.WorkflowRunID(mutableState.GetWorkflowKey().RunID))
	return &historyservice.NotifyChildExecutionCompletionRecordedResponse{}, nil
}
