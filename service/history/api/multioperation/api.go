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
	enumspb "go.temporal.io/api/enums/v1"
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

var (
	multiOpAbortedErr = serviceerror.NewMultiOperationAborted("Operation was aborted.")
)

func Invoke(
	ctx context.Context,
	req *historyservice.ExecuteMultiOperationRequest,
	shardContext shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	tokenSerializer common.TaskTokenSerializer,
	visibilityManager manager.VisibilityManager,
	matchingClient matchingservice.MatchingServiceClient,
) (*historyservice.ExecuteMultiOperationResponse, error) {
	if len(req.Operations) != 2 {
		return nil, serviceerror.NewInternal("expected exactly 2 operations")
	}

	startReq := req.Operations[0].GetStartWorkflow()
	if startReq == nil {
		return nil, serviceerror.NewInternal("expected first operation to be Start Workflow")
	}
	starter, err := startworkflow.NewStarter(
		shardContext,
		workflowConsistencyChecker,
		tokenSerializer,
		visibilityManager,
		startReq,
	)
	if err != nil {
		return nil, newMultiOpError(err, multiOpAbortedErr)
	}

	updateReq := req.Operations[1].GetUpdateWorkflow()
	if updateReq == nil {
		return nil, serviceerror.NewInternal("expected second operation to be Update Workflow")
	}
	updater := updateworkflow.NewUpdater(
		shardContext,
		workflowConsistencyChecker,
		matchingClient,
		updateReq,
	)

	// If the workflow id conflict policy is to terminate, it doesn't matter if the workflow is actually running or not:
	// always attempt to start and update. The start will take care of terminating the workflow, if necessary.
	if startReq.StartRequest.WorkflowIdConflictPolicy == enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING {
		return startAndUpdateWorkflow(ctx, shardContext, workflowConsistencyChecker, starter, updater)
	}

	currentWorkflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(req.NamespaceId, startReq.StartRequest.WorkflowId, ""),
		workflow.LockPriorityHigh,
	)
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		currentWorkflowLease = nil
	} else if err != nil {
		return nil, newMultiOpError(err, multiOpAbortedErr)
	}

	// workflow was already started, ...
	if currentWorkflowLease != nil {
		switch startReq.StartRequest.WorkflowIdConflictPolicy {
		// ... abort the entire operation
		case enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL:
			currentWorkflowLease.GetReleaseFn()(nil) // nil since nothing was modified

			wfKey := currentWorkflowLease.GetContext().GetWorkflowKey()
			err = serviceerror.NewWorkflowExecutionAlreadyStarted(
				fmt.Sprintf("Workflow execution is already running. WorkflowId: %v, RunId: %v.", wfKey.WorkflowID, wfKey.RunID),
				startReq.StartRequest.RequestId,
				wfKey.RunID,
			)
			return nil, newMultiOpError(err, multiOpAbortedErr)

		// ... skip the start and only send the update
		case enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING:
			// NOTE: currentWorkflowLease will be released by the function
			return updateWorkflow(ctx, shardContext, currentWorkflowLease, updater)

		// ... fail since this policy should have been taken care of earlier already
		case enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING:
			currentWorkflowLease.GetReleaseFn()(nil) // nil since nothing was modified

			return nil, serviceerror.NewInternal("unhandled workflow id conflict policy: terminate-existing")

		// ... fail since this policy is invalid
		case enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED:
			currentWorkflowLease.GetReleaseFn()(nil) // nil since nothing was modified

			return nil, serviceerror.NewInternal("unhandled workflow id conflict policy: unspecified")
		}
	}

	// workflow hasn't been started yet: start and then apply update
	return startAndUpdateWorkflow(
		ctx,
		shardContext,
		workflowConsistencyChecker,
		starter,
		updater,
	)
}

func updateWorkflow(
	ctx context.Context,
	shardContext shard.Context,
	currentWorkflowLease api.WorkflowLease,
	updater *updateworkflow.Updater,
) (*historyservice.ExecuteMultiOperationResponse, error) {
	startOpResp := &historyservice.ExecuteMultiOperationResponse_Response{
		Response: &historyservice.ExecuteMultiOperationResponse_Response_StartWorkflow{
			StartWorkflow: &historyservice.StartWorkflowExecutionResponse{
				RunId:   currentWorkflowLease.GetContext().GetWorkflowKey().RunID,
				Started: false,
			},
		},
	}

	// apply update to workflow
	err := api.UpdateWorkflowWithNew(
		shardContext,
		ctx,
		currentWorkflowLease,
		func(lease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			return updater.ApplyRequest(ctx, lease)
		},
		nil,
	)

	// release lock since all changes to workflow have been completed now
	currentWorkflowLease.GetReleaseFn()(err)

	if err != nil {
		return onUpdateError(err, updater, startOpResp)
	}

	// wait for the update to complete
	updateResp, err := updater.OnSuccess(ctx)
	if err != nil {
		return nil, newMultiOpError(multiOpAbortedErr, err)
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

func startAndUpdateWorkflow(
	ctx context.Context,
	shardContext shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	starter *startworkflow.Starter,
	updater *updateworkflow.Updater,
) (*historyservice.ExecuteMultiOperationResponse, error) {
	var workflowKey definition.WorkflowKey
	var workflowCtx workflow.Context
	var namespaceID namespace.ID
	var updateErr error

	// hook to apply workflow update before the workflow is being persisted
	hook := func(lease api.WorkflowLease) error {
		workflowCtx = lease.GetContext()
		workflowKey = workflowCtx.GetWorkflowKey()
		namespaceID = lease.GetMutableState().GetNamespaceEntry().ID()
		_, updateErr = updater.ApplyRequest(ctx, lease) // ignoring UpdateWorkflowAction return since Start will create WFT
		return updateErr
	}

	// start workflow and apply update via `hook`
	startResp, err := starter.Invoke(ctx, hook)
	startOpResp := &historyservice.ExecuteMultiOperationResponse_Response{
		Response: &historyservice.ExecuteMultiOperationResponse_Response_StartWorkflow{
			StartWorkflow: startResp,
		},
	}
	if err != nil {
		// an update error occurred
		if updateErr != nil {
			return onUpdateError(updateErr, updater, startOpResp)
		}

		// a start error occurred
		return nil, newMultiOpError(err, multiOpAbortedErr)
	}

	if !startResp.Started {
		// The workflow was meant to be started - but was actually not started since it's already running.
		// The best way forward is to exit and retry from the top.
		// By returning an Unavailable service error, the entire MultiOperation will be retried.
		return nil, serviceerror.NewUnavailable("Workflow could not be started as it is already running")
	}

	// without this, there's no Update registry on the call from Matching back to History
	// TODO: eventually, we'll want to put the MS into the cache as well
	workflowCache := workflowConsistencyChecker.GetWorkflowCache()
	if _, err = workflowCache.Put(
		shardContext,
		namespaceID,
		&commonpb.WorkflowExecution{WorkflowId: workflowKey.WorkflowID, RunId: workflowKey.RunID},
		workflowCtx,
		shardContext.GetMetricsHandler(),
	); err != nil {
		return nil, err
	}

	// wait for the update to complete
	updateResp, err := updater.OnSuccess(ctx)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Errorf("failed to complete Workflow Update: %w", err).Error())
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

func onUpdateError(
	updateErr error,
	updater *updateworkflow.Updater,
	startOpResp *historyservice.ExecuteMultiOperationResponse_Response,
) (*historyservice.ExecuteMultiOperationResponse, error) {
	updateResp := updater.OnError(updateErr)
	if updateResp != nil {
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
	return nil, newMultiOpError(multiOpAbortedErr, updateErr)
}

func newMultiOpError(errs ...error) error {
	return serviceerror.NewMultiOperationExecution("MultiOperation could not be executed.", errs)
}
