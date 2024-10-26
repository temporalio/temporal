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
	"go.temporal.io/server/common/locks"
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

	// TODO
	if startReq.StartRequest.WorkflowIdConflictPolicy == enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING {
		return nil, serviceerror.NewInvalidArgument("workflow id conflict policy terminate-existing is not supported yet")
	}

	currentWorkflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		definition.NewWorkflowKey(req.NamespaceId, startReq.StartRequest.WorkflowId, ""),
		locks.PriorityHigh,
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
		case enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING:
			// ... skip the start and only send the update
			// NOTE: currentWorkflowLease will be released by the function
			return updateWorkflow(ctx, shardContext, currentWorkflowLease, updater)

		case enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL:
			// ... if same request ID, just send update
			// NOTE: currentWorkflowLease will be released by the function
			if dedup(startReq, currentWorkflowLease) {
				return updateWorkflow(ctx, shardContext, currentWorkflowLease, updater)
			}

			// ... otherwise, abort the entire operation
			currentWorkflowLease.GetReleaseFn()(nil) // nil since nothing was modified
			wfKey := currentWorkflowLease.GetContext().GetWorkflowKey()
			err = serviceerror.NewWorkflowExecutionAlreadyStarted(
				fmt.Sprintf("Workflow execution is already running. WorkflowId: %v, RunId: %v.", wfKey.WorkflowID, wfKey.RunID),
				startReq.StartRequest.RequestId,
				wfKey.RunID,
			)
			return nil, newMultiOpError(err, multiOpAbortedErr)

		case enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING:
			currentWorkflowLease.GetReleaseFn()(nil) // nil since nothing was modified

			return nil, serviceerror.NewInternal("unhandled workflow id conflict policy: terminate-existing")

		case enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED:
			// ... fail since this policy is invalid
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
			ms := lease.GetMutableState()
			updateReg := lease.GetContext().UpdateRegistry(ctx, ms)
			return updater.ApplyRequest(ctx, updateReg, ms)
		},
		nil,
	)

	// release lock since all changes to workflow have been completed now
	currentWorkflowLease.GetReleaseFn()(err)

	if err != nil {
		return nil, newMultiOpError(multiOpAbortedErr, err)
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
	var updateErr error

	// hook is invoked before workflow is persisted
	applyUpdateFunc := func(lease api.WorkflowLease) error {
		// workflowCtx is the response from the cache write: either it's the context from the currently held lease
		// OR the already-existing, previously cached context (this happens when the workflow is being terminated;
		// it re-uses the same context).
		var workflowCtx workflow.Context

		// It is crucial to put the Update registry (inside the workflow context) into the cache, as it needs to
		// exist on the Matching call back to History when delivering a workflow task to a worker.
		//
		// The cache write needs to happen *before* the persistence write because a failed cache write means an
		// early error response that aborts the entire MultiOperation request. And it allows for a simple retry, too -
		// whereas if the cache write happened and failed *after* a successful persistence write,
		// it would leave behind a started workflow that will never receive the update.
		ms := lease.GetMutableState()
		wfContext := lease.GetContext()
		// if MutableState isn't set, the next request for it will load it from the database
		// - but receive a new instance that is inconsistent with this one
		wfContext.(*workflow.ContextImpl).MutableState = ms
		workflowKey := wfContext.GetWorkflowKey()
		workflowCtx, updateErr = workflowConsistencyChecker.GetWorkflowCache().Put(
			shardContext,
			ms.GetNamespaceEntry().ID(),
			&commonpb.WorkflowExecution{WorkflowId: workflowKey.WorkflowID, RunId: workflowKey.RunID},
			wfContext,
			shardContext.GetMetricsHandler(),
		)
		if updateErr == nil {
			// UpdateWorkflowAction return value is ignored since Start will always create WFT
			updateReg := workflowCtx.UpdateRegistry(ctx, ms)
			_, updateErr = updater.ApplyRequest(ctx, updateReg, ms)
		}
		return updateErr
	}

	// start workflow, using the hook to apply the update operation
	startResp, err := starter.Invoke(ctx, applyUpdateFunc)
	if err != nil {
		// an update error occurred
		if updateErr != nil {
			return nil, newMultiOpError(multiOpAbortedErr, updateErr)
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

	// wait for the update to complete
	updateResp, err := updater.OnSuccess(ctx)
	if err != nil {
		return nil, newMultiOpError(nil, err) // `nil` for start since it succeeded
	}

	return &historyservice.ExecuteMultiOperationResponse{
		Responses: []*historyservice.ExecuteMultiOperationResponse_Response{
			&historyservice.ExecuteMultiOperationResponse_Response{
				Response: &historyservice.ExecuteMultiOperationResponse_Response_StartWorkflow{
					StartWorkflow: startResp,
				},
			},
			{
				Response: &historyservice.ExecuteMultiOperationResponse_Response_UpdateWorkflow{
					UpdateWorkflow: updateResp,
				},
			},
		},
	}, nil
}

func newMultiOpError(errs ...error) error {
	return serviceerror.NewMultiOperationExecution("MultiOperation could not be executed.", errs)
}

func dedup(startReq *historyservice.StartWorkflowExecutionRequest, currentWorkflowLease api.WorkflowLease) bool {
	return startReq.StartRequest.RequestId == currentWorkflowLease.GetMutableState().GetExecutionState().GetCreateRequestId()
}
