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

package terminateworkflow

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	req *historyservice.TerminateWorkflowExecutionRequest,
	shard shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.TerminateWorkflowExecutionResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	request := req.TerminateRequest
	parentExecution := req.ExternalWorkflowExecution
	childWorkflowOnly := req.ChildWorkflowOnly
	workflowID := request.WorkflowExecution.WorkflowId
	runID := request.WorkflowExecution.RunId
	firstExecutionRunID := request.FirstExecutionRunId
	if len(request.FirstExecutionRunId) != 0 {
		runID = ""
	}

	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			runID,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			// There is a workflow execution currently running with the WorkflowID.
			// If user passed in a FirstExecutionRunID with the request to allow terminate to work across runs then
			// let's compare the FirstExecutionRunID on the request to make sure we terminate the correct workflow
			// execution.
			executionInfo := mutableState.GetExecutionInfo()
			if len(firstExecutionRunID) > 0 && executionInfo.FirstExecutionRunId != firstExecutionRunID {
				return nil, consts.ErrWorkflowExecutionNotFound
			}

			if childWorkflowOnly {
				if parentExecution.GetWorkflowId() != executionInfo.ParentWorkflowId ||
					parentExecution.GetRunId() != executionInfo.ParentRunId {
					return nil, consts.ErrWorkflowParent
				}
			}

			return api.UpdateWorkflowTerminate, workflow.TerminateWorkflow(
				mutableState,
				request.GetReason(),
				request.GetDetails(),
				request.GetIdentity(),
				false,
			)
		},
		nil,
		shard,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.TerminateWorkflowExecutionResponse{}, nil
}
