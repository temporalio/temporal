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

type (
	// updateError is a wrapper to distinguish an update error from a start error.
	updateError struct{ error }
	// noStartError is a sentinel error that indicates no workflow start occurred.
	noStartError struct{ startworkflow.StartOutcome }
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
	updater := updateworkflow.NewUpdater(
		shardContext,
		workflowConsistencyChecker,
		matchingClient,
		updateReq,
	)

	startReq := req.Operations[0].GetStartWorkflow()
	if startReq == nil {
		return nil, serviceerror.NewInvalidArgument("expected first operation to be Start Workflow")
	}
	starter, err := startworkflow.NewStarter(
		shardContext,
		workflowConsistencyChecker,
		tokenSerializer,
		visibilityManager,
		startReq,
		func(
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
				workflowContext, releaseFunc, err := workflowConsistencyChecker.GetWorkflowCache().GetOrCreateWorkflowExecution(
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
			if _, err := updater.ApplyRequest(ctx, updateReg, ms); err != nil {
				// Wrapping the error so Update and Start errors can be distinguished later.
				return nil, updateError{err}
			}
			return res, nil
		},
	)
	if err != nil {
		return nil, newMultiOpError(err, multiOpAbortedErr)
	}

	// For workflow id conflict policy terminate-existing, always attempt a start
	// since that works when the workflow is already running *and* when it's not running.
	conflictPolicy := startReq.StartRequest.WorkflowIdConflictPolicy
	if conflictPolicy == enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING {
		resp, err := startAndUpdateWorkflow(ctx, starter, updater)
		var noStartErr *noStartError
		switch {
		case errors.As(err, &noStartErr):
			// The start request was deduped. Continue below and only send the update.
		case err != nil:
			return nil, err
		default:
			return resp, nil
		}
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
	if currentWorkflowLease != nil && currentWorkflowLease.GetMutableState().IsWorkflowExecutionRunning() {
		switch conflictPolicy {
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
			return updateWorkflow(ctx, shardContext, currentWorkflowLease, updater)

		case enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED:
			// ... fail since this policy is invalid
			currentWorkflowLease.GetReleaseFn()(nil) // nil since nothing was modified
			return nil, serviceerror.NewInvalidArgument("unhandled workflow id conflict policy: unspecified")
		}
	}

	// workflow hasn't been started yet: start and then apply update
	resp, err := startAndUpdateWorkflow(ctx, starter, updater)
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

func updateWorkflow(
	ctx context.Context,
	shardContext shard.Context,
	currentWorkflowLease api.WorkflowLease,
	updater *updateworkflow.Updater,
) (*historyservice.ExecuteMultiOperationResponse, error) {
	// apply update to workflow
	err := api.UpdateWorkflowWithNew(
		shardContext,
		ctx,
		currentWorkflowLease,
		func(lease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			ms := lease.GetMutableState()
			updateReg := lease.GetContext().UpdateRegistry(ctx)
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
			{
				Response: &historyservice.ExecuteMultiOperationResponse_Response_StartWorkflow{
					StartWorkflow: &historyservice.StartWorkflowExecutionResponse{
						RunId:   currentWorkflowLease.GetContext().GetWorkflowKey().RunID,
						Started: false, // set explicitly for emphasis
					},
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

// NOTE: Returns a `noStartError` if the start was unexpectedly reused/deduped.
func startAndUpdateWorkflow(
	ctx context.Context,
	starter *startworkflow.Starter,
	updater *updateworkflow.Updater,
) (*historyservice.ExecuteMultiOperationResponse, error) {
	startResp, startOutcome, err := starter.Invoke(ctx)
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

	// wait for the update to complete
	updateResp, err := updater.OnSuccess(ctx)
	if err != nil {
		return nil, newMultiOpError(nil, err) // `nil` for start since it succeeded
	}

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
	}, nil
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

func dedup(startReq *historyservice.StartWorkflowExecutionRequest, currentWorkflowLease api.WorkflowLease) bool {
	return startReq.StartRequest.RequestId == currentWorkflowLease.GetMutableState().GetExecutionState().GetCreateRequestId()
}

func (e *noStartError) Error() string {
	return fmt.Sprintf("Workflow was not started: %v", e.StartOutcome)
}
