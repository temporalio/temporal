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

type (
	// updateError is a wrapper to distinguish an update error from a start error.
	updateError struct{ error }
	// noStartError is a sentinel error that indicates no workflow start occurred.
	noStartError struct{ startworkflow.StartOutcome }
)

type (
	multiOp struct {
		shardContext       shard.Context
		namespaceId        namespace.ID
		consistencyChecker api.WorkflowConsistencyChecker

		updateReq *historyservice.UpdateWorkflowExecutionRequest
		startReq  *historyservice.StartWorkflowExecutionRequest

		updater *updateworkflow.Updater
		starter *startworkflow.Starter
	}
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
		return nil, serviceerror.NewInvalidArgument("expected exactly 2 operations")
	}

	updateReq := req.Operations[1].GetUpdateWorkflow()
	if updateReq == nil {
		return nil, serviceerror.NewInvalidArgument("expected second operation to be Update Workflow")
	}

	startReq := req.Operations[0].GetStartWorkflow()
	if startReq == nil {
		return nil, serviceerror.NewInvalidArgument("expected first operation to be Start Workflow")
	}

	mo := &multiOp{
		shardContext:       shardContext,
		namespaceId:        namespace.ID(req.NamespaceId),
		consistencyChecker: workflowConsistencyChecker,
		updateReq:          updateReq,
		startReq:           startReq,
	}

	var err error
	mo.starter, err = startworkflow.NewStarter(
		shardContext,
		workflowConsistencyChecker,
		tokenSerializer,
		visibilityManager,
		startReq,
		mo.workflowLeaseCallback(ctx),
	)
	if err != nil {
		return nil, newMultiOpError(err, multiOpAbortedErr)
	}

	mo.updater = updateworkflow.NewUpdater(
		mo.shardContext,
		mo.consistencyChecker,
		matchingClient,
		mo.updateReq,
	)

	return mo.Invoke(ctx)
}

func (mo *multiOp) Invoke(ctx context.Context) (*historyservice.ExecuteMultiOperationResponse, error) {
	// For workflow id conflict policy terminate-existing, always attempt a start
	// since that works when the workflow is already running *and* when it's not running.
	conflictPolicy := mo.startReq.StartRequest.WorkflowIdConflictPolicy
	if conflictPolicy == enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING {
		resp, err := mo.startAndUpdateWorkflow(ctx)
		var noStartErr *noStartError
		switch {
		case errors.As(err, &noStartErr):
			// The start request was deduped, no termination is needed.
			// Continue below by only sending the update.
		case err != nil:
			return nil, err
		default:
			return resp, nil
		}
	}

	runningWorkflowLease, err := mo.getRunningWorkflowLease(ctx)
	if err != nil {
		return nil, err
	}

	// Workflow was already started ...
	if runningWorkflowLease != nil {
		if err = mo.allowUpdateWorkflow(ctx, runningWorkflowLease, conflictPolicy); err != nil {
			runningWorkflowLease.GetReleaseFn()(nil) // nil since nothing was modified
			return nil, err
		}
		return mo.updateWorkflow(ctx, runningWorkflowLease)
	}

	// Workflow hasn't been started yet ...
	resp, err := mo.startAndUpdateWorkflow(ctx)
	var noStartErr *noStartError
	if errors.As(err, &noStartErr) {
		// The workflow was meant to be started - but was actually *not* started.
		// The problem is that the update has not been applied.
		//
		// This can happen when there's a race: another workflow start occurred right after the check for a
		// running workflow above - but before the new workflow could be created (and locked).
		// TODO: Consider a refactoring of the startworkflow.Starter to make this case impossible.
		//
		// The best way forward is to exit and retry from the top.
		// By returning an Unavailable service error, the entire MultiOperation will be retried.
		return nil, newMultiOpError(serviceerror.NewUnavailable(err.Error()), multiOpAbortedErr)
	}
	return resp, err
}

func (mo *multiOp) workflowLeaseCallback(
	ctx context.Context,
) api.CreateOrUpdateLeaseFunc {
	return func(
		existingLease api.WorkflowLease,
		shardContext shard.Context,
		ms workflow.MutableState,
	) (api.WorkflowLease, error) {
		var res api.WorkflowLease

		if existingLease == nil {
			// Create a new *locked* workflow context. This is important since without the lock, task processing
			// would try to modify the mutable state concurrently. Once the Starter completes, it will release the lock.
			//
			// The cache write needs to happen *before* the persistence write because a failed cache write means an
			// early error response that aborts the entire MultiOperation request. And it allows for a simple retry, too -
			// whereas if the cache write happened and failed *after* a successful persistence write,
			// it would leave behind a started workflow that will never receive the update.
			workflowContext, releaseFunc, err := mo.consistencyChecker.GetWorkflowCache().GetOrCreateWorkflowExecution(
				ctx,
				shardContext,
				ms.GetNamespaceEntry().ID(),
				&commonpb.WorkflowExecution{WorkflowId: ms.GetExecutionInfo().WorkflowId, RunId: ms.GetExecutionState().RunId},
				locks.PriorityHigh,
			)
			if err != nil {
				return nil, err
			}
			res = api.NewWorkflowLease(workflowContext, releaseFunc, ms)
		} else {
			// TODO(stephanos): remove this hack
			// If the lease already exists, but the update needs to be re-applied since it was aborted due to a conflict.
			res = existingLease
			ms = existingLease.GetMutableState()
		}

		// If MutableState isn't set here, the next request for it will load it from the database
		// - but receive a new instance that won't have the in-memory Update registry.
		res.GetContext().(*workflow.ContextImpl).MutableState = ms

		// Add the Update.
		// NOTE: UpdateWorkflowAction return value is ignored since ther Starter will always create a WFT.
		updateReg := res.GetContext().UpdateRegistry(ctx)
		if _, err := mo.updater.ApplyRequest(ctx, updateReg, ms); err != nil {
			// Wrapping the error so Update and Start errors can be distinguished later.
			return nil, updateError{err}
		}
		return res, nil
	}
}

func (mo *multiOp) getRunningWorkflowLease(ctx context.Context) (api.WorkflowLease, error) {
	runningWorkflowLease, err := mo.consistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		definition.NewWorkflowKey(mo.namespaceId.String(), mo.startReq.StartRequest.WorkflowId, ""),
		locks.PriorityHigh,
	)
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return nil, nil
	}
	if err != nil {
		return nil, newMultiOpError(err, multiOpAbortedErr)
	}

	if runningWorkflowLease == nil {
		return nil, nil
	}

	if !runningWorkflowLease.GetMutableState().IsWorkflowExecutionRunning() {
		runningWorkflowLease.GetReleaseFn()(nil)
		return nil, nil
	}

	return runningWorkflowLease, nil
}

func (mo *multiOp) allowUpdateWorkflow(
	ctx context.Context,
	currentWorkflowLease api.WorkflowLease,
	conflictPolicy enumspb.WorkflowIdConflictPolicy,
) error {
	switch conflictPolicy {
	case enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING:
		// Allow sending the update.
		return nil

	case enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL:
		// If it's the same request ID, allow sending the update.
		if canDedup(mo.startReq, currentWorkflowLease) {
			return nil
		}

		// Otherwise, don't allow sending the update.
		wfKey := currentWorkflowLease.GetContext().GetWorkflowKey()
		err := serviceerror.NewWorkflowExecutionAlreadyStarted(
			fmt.Sprintf("Workflow execution is already running. WorkflowId: %v, RunId: %v.", wfKey.WorkflowID, wfKey.RunID),
			mo.startReq.StartRequest.RequestId,
			wfKey.RunID,
		)
		return newMultiOpError(err, multiOpAbortedErr)

	case enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING:
		// Allow sending the update since the termination was deduped earlier.
		return nil

	case enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED:
		// Don't allow sending the update as the policy is invalid.
		// This should never happen as it should be validated by the frontend.
		return serviceerror.NewInvalidArgument("unhandled workflow id conflict policy: unspecified")

	default:
		// Don't allow sending the update as the policy is invalid.
		// This should never happen as it should be validated by the frontend.
		return serviceerror.NewInternal("unhandled workflow id conflict policy")
	}
}

func (mo *multiOp) updateWorkflow(
	ctx context.Context,
	currentWorkflowLease api.WorkflowLease,
) (*historyservice.ExecuteMultiOperationResponse, error) {
	// Apply the update to the workflow.
	err := api.UpdateWorkflowWithNew(
		mo.shardContext,
		ctx,
		currentWorkflowLease,
		func(lease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			ms := lease.GetMutableState()
			updateReg := lease.GetContext().UpdateRegistry(ctx)
			return mo.updater.ApplyRequest(ctx, updateReg, ms)
		},
		nil,
	)

	// Release lock here since all changes to the workflow have been completed now.
	currentWorkflowLease.GetReleaseFn()(err)

	if err != nil {
		return nil, newMultiOpError(multiOpAbortedErr, err)
	}

	// Wait for the update to complete.
	updateResp, err := mo.updater.OnSuccess(ctx)
	if err != nil {
		return nil, newMultiOpError(multiOpAbortedErr, err)
	}

	startResp := &historyservice.StartWorkflowExecutionResponse{
		RunId:   currentWorkflowLease.GetContext().GetWorkflowKey().RunID,
		Started: false, // set explicitly for emphasis
	}

	return makeResponse(startResp, updateResp), nil
}

func (mo *multiOp) startAndUpdateWorkflow(ctx context.Context) (*historyservice.ExecuteMultiOperationResponse, error) {
	startResp, startOutcome, err := mo.starter.Invoke(ctx)
	if err != nil {
		// An update error occurred.
		if errors.As(err, &updateError{}) {
			return nil, newMultiOpError(multiOpAbortedErr, err)
		}
		// A start error occurred.
		return nil, newMultiOpError(err, multiOpAbortedErr)
	}
	if startOutcome != startworkflow.StartNew {
		// The workflow was not started.
		// Aborting since the update has not been applied.
		return nil, &noStartError{startOutcome}
	}

	// Wait for the update to complete.
	updateResp, err := mo.updater.OnSuccess(ctx)
	if err != nil {
		return nil, newMultiOpError(nil, err) // `nil` for start since it succeeded
	}

	return makeResponse(startResp, updateResp), nil
}

func makeResponse(
	startResp *historyservice.StartWorkflowExecutionResponse,
	updateResp *historyservice.UpdateWorkflowExecutionResponse,
) *historyservice.ExecuteMultiOperationResponse {
	return &historyservice.ExecuteMultiOperationResponse{
		Responses: []*historyservice.ExecuteMultiOperationResponse_Response{
			{
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
	}
}

func newMultiOpError(startErr, updateErr error) error {
	var message string
	switch {
	case startErr != nil && !errors.Is(startErr, multiOpAbortedErr):
		message = fmt.Sprintf("Start failed: %v", startErr)
	case updateErr != nil && !errors.Is(updateErr, multiOpAbortedErr):
		message = fmt.Sprintf("Update failed: %v", updateErr)
	default:
		message = "Reason unknown"
	}
	return serviceerror.NewMultiOperationExecution(
		fmt.Sprintf("MultiOperation could not be executed: %v", message),
		[]error{startErr, updateErr})
}

func canDedup(startReq *historyservice.StartWorkflowExecutionRequest, currentWorkflowLease api.WorkflowLease) bool {
	return startReq.StartRequest.RequestId == currentWorkflowLease.GetMutableState().GetExecutionState().GetCreateRequestId()
}

func (e *noStartError) Error() string {
	return fmt.Sprintf("Workflow was not started: %v", e.StartOutcome)
}
