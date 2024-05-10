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

package verifychildworkflowcompletionrecorded

import (
	"context"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	request *historyservice.VerifyChildExecutionCompletionRecordedRequest,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.VerifyChildExecutionCompletionRecordedResponse, retError error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if err := api.ValidateNamespaceUUID(namespaceID); err != nil {
		return nil, err
	}

	workflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		request.Clock,
		// it's ok we have stale state when doing verification,
		// the logic will return WorkflowNotReady error and the caller will retry
		// this can prevent keep reloading mutable state when there's a replication lag
		// in parent shard.
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			request.NamespaceId,
			request.ParentExecution.WorkflowId,
			request.ParentExecution.RunId,
		),
		workflow.LockPriorityLow,
	)
	if err != nil {
		return nil, err
	}
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	mutableState := workflowLease.GetMutableState()
	if !mutableState.IsWorkflowExecutionRunning() &&
		mutableState.GetExecutionState().State != enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		// parent has already completed and can't be blocked after failover.
		return &historyservice.VerifyChildExecutionCompletionRecordedResponse{}, nil
	}

	onCurrentBranch, err := api.IsHistoryEventOnCurrentBranch(mutableState, request.ParentInitiatedId, request.ParentInitiatedVersion)
	if err != nil {
		// initiated event not found on any branch
		return nil, consts.ErrWorkflowNotReady
	}

	if !onCurrentBranch {
		// due to conflict resolution, the initiated event may on a different branch of the workflow.
		// we don't have to do anything and can simply return not found error. Standby logic
		// after seeing this error will give up verification.
		return nil, consts.ErrChildExecutionNotFound
	}

	ci, isRunning := mutableState.GetChildExecutionInfo(request.ParentInitiatedId)
	if isRunning {
		if ci.StartedEventId != common.EmptyEventID &&
			ci.GetStartedWorkflowId() != request.ChildExecution.GetWorkflowId() {
			// this can happen since we may not have the initiated version
			return nil, consts.ErrChildExecutionNotFound
		}

		return nil, consts.ErrWorkflowNotReady
	}

	return &historyservice.VerifyChildExecutionCompletionRecordedResponse{}, nil
}
