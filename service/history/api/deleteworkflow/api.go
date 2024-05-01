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

package deleteworkflow

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	request *historyservice.DeleteWorkflowExecutionRequest,
	shard shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	workflowDeleteManager deletemanager.DeleteManager,
) (_ *historyservice.DeleteWorkflowExecutionResponse, retError error) {
	workflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			request.NamespaceId,
			request.WorkflowExecution.WorkflowId,
			request.WorkflowExecution.RunId,
		),
		workflow.LockPriorityLow,
	)
	if err != nil {
		return nil, err
	}
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	// Open and Close workflow executions are deleted differently.
	// Open workflow execution is deleted by terminating with special flag `deleteAfterTerminate` set to true.
	// This flag will be carried over with CloseExecutionTask and workflow will be deleted as the last step while processing the task.
	//
	// Close workflow execution is deleted using DeleteExecutionTask.
	//
	// DeleteWorkflowExecution is not replicated automatically. Workflow executions must be deleted separately in each cluster.
	// Although running workflows in active cluster are terminated first and the termination event might be replicated.
	// In passive cluster, workflow executions are just deleted in regardless of its state.

	if workflowLease.GetMutableState().IsWorkflowExecutionRunning() {
		if request.GetClosedWorkflowOnly() {
			// skip delete open workflow
			return &historyservice.DeleteWorkflowExecutionResponse{}, nil
		}
		ns, err := shard.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(request.GetNamespaceId()))
		if err != nil {
			return nil, err
		}
		if ns.ActiveInCluster(shard.GetClusterMetadata().GetCurrentClusterName()) {
			// If workflow execution is running and in active cluster.
			if err := api.UpdateWorkflowWithNew(
				shard,
				ctx,
				workflowLease,
				func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
					mutableState := workflowLease.GetMutableState()

					return api.UpdateWorkflowTerminate, workflow.TerminateWorkflow(
						mutableState,
						"Delete workflow execution",
						nil,
						consts.IdentityHistoryService,
						true,
					)
				},
				nil,
			); err != nil {
				return nil, err
			}
			return &historyservice.DeleteWorkflowExecutionResponse{}, nil
		}
	}

	// If workflow execution is closed or in passive cluster.
	if err := workflowDeleteManager.AddDeleteWorkflowExecutionTask(
		ctx,
		namespace.ID(request.GetNamespaceId()),
		&commonpb.WorkflowExecution{
			WorkflowId: request.GetWorkflowExecution().GetWorkflowId(),
			RunId:      request.GetWorkflowExecution().GetRunId(),
		},
		workflowLease.GetMutableState(),
	); err != nil {
		return nil, err
	}
	return &historyservice.DeleteWorkflowExecutionResponse{}, nil
}
