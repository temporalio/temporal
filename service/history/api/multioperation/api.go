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

package multioperation

import (
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/startworkflow"
	"go.temporal.io/server/service/history/api/updateworkflow"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	req *historyservice.ExecuteMultiOperationRequest,
	shardContext shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	tokenSerializer common.TaskTokenSerializer,
	visibilityManager manager.VisibilityManager,
	matchingClient matchingservice.MatchingServiceClient,
) (_ *historyservice.ExecuteMultiOperationResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(shardContext, namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	if len(req.Operations) != 2 {
		return nil, serviceerror.NewInternal("expected exactly 2 operations")
	}
	startReq := req.Operations[0].GetStartWorkflow()
	if startReq == nil {
		return nil, serviceerror.NewInternal("expected first operation to be Start Workflow")
	}
	updateReq := req.Operations[1].GetUpdateWorkflow()
	if updateReq == nil {
		return nil, serviceerror.NewInternal("expected second operation to be Update Workflow")
	}

	// grab current Workflow lease, if there is one
	currentWorkflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			string(namespaceID),
			startReq.StartRequest.WorkflowId,
			"",
		),
		workflow.LockPriorityHigh,
	)
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		currentWorkflowLease = nil
	} else if err != nil {
		return nil, err
	} else {
		defer func() { currentWorkflowLease.GetReleaseFn()(retError) }()
	}

	if currentWorkflowLease != nil {
		// TODO: support for already-running Workflow
		return nil, serviceerror.NewUnimplemented("not implemented yet")
	}

	starter, err := startworkflow.NewStarter(
		shardContext,
		workflowConsistencyChecker,
		tokenSerializer,
		visibilityManager,
		startReq,
	)
	if err != nil {
		// TODO: send per-operation error details
		return nil, fmt.Errorf("failed create workflow starter: %w", err)
	}

	var workflowCtx workflow.Context
	var updateErr error
	updater := updateworkflow.NewUpdater(
		shardContext,
		workflowConsistencyChecker,
		matchingClient,
		updateReq,
	)
	startResp, err := starter.Invoke(
		ctx,
		func(lease api.WorkflowLease) error {
			workflowCtx = lease.GetContext()
			_, updateErr = updater.ApplyRequest(ctx, lease)
			return updateErr
		},
	)
	if err != nil {
		// TODO: send per-operation error details
		return nil, fmt.Errorf("failed to start workflow: %w", err)
	}
	startOpResp := &historyservice.ExecuteMultiOperationResponse_Response{
		Response: &historyservice.ExecuteMultiOperationResponse_Response_StartWorkflow{
			StartWorkflow: startResp,
		},
	}

	if updateErr != nil {
		updateResp := updater.OnError(updateErr)
		return &historyservice.ExecuteMultiOperationResponse{
			Responses: []*historyservice.ExecuteMultiOperationResponse_Response{
				startOpResp,
				{
					Response: &historyservice.ExecuteMultiOperationResponse_Response_UpdateWorkflow{
						UpdateWorkflow: updateResp,
					},
				},
			},
		}, nil
	}

	// Without this, there's no Update registry on the call from Matching back to History.
	// TODO: eventually, we'll want to put the MS into the cache as well
	if _, err = workflowConsistencyChecker.GetWorkflowCache().Put(
		shardContext,
		namespaceID,
		&commonpb.WorkflowExecution{WorkflowId: req.WorkflowId, RunId: startResp.RunId},
		workflowCtx,
		shardContext.GetMetricsHandler(),
	); err != nil {
		return nil, err
	}

	updateResp, err := updater.OnSuccess(ctx)
	if err != nil {
		// TODO: send Update outcome failure instead
		return nil, fmt.Errorf("failed to complete Workflow Update: %w", err)
	}
	return &historyservice.ExecuteMultiOperationResponse{
		Responses: []*historyservice.ExecuteMultiOperationResponse_Response{
			startOpResp,
			{
				Response: &historyservice.ExecuteMultiOperationResponse_Response_UpdateWorkflow{
					UpdateWorkflow: updateResp,
				},
			},
		},
	}, nil
}
