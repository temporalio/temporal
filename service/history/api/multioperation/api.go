package multioperation

import (
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/startworkflow"
	"go.temporal.io/server/service/history/api/updateworkflow"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
)

var multiOpAbortedErr = serviceerror.NewMultiOperationAborted("Operation was aborted.")

type (
	// updateError is a wrapper to distinguish an update error from a start error.
	updateError struct{ error }
)

// Unwrap returns the wrapped error to support errors.As() and errors.Is()
func (e updateError) Unwrap() error {
	return e.error
}

type (
	updateWithStart struct {
		shardContext       historyi.ShardContext
		namespaceId        namespace.ID
		consistencyChecker api.WorkflowConsistencyChecker
		testHooks          testhooks.TestHooks

		updateReq *historyservice.UpdateWorkflowExecutionRequest
		startReq  *historyservice.StartWorkflowExecutionRequest

		updater            *updateworkflow.Updater
		starter            *startworkflow.Starter
		workflowWasStarted bool // indicates whether the workflow was started or not
	}
)

func Invoke(
	ctx context.Context,
	req *historyservice.ExecuteMultiOperationRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	tokenSerializer *tasktoken.Serializer,
	matchingClient matchingservice.MatchingServiceClient,
	versionCache worker_versioning.VersionMembershipAndReactivationStatusCache,
	reactivationSignaler api.VersionReactivationSignalerFn,
	testHooks testhooks.TestHooks,
) (*historyservice.ExecuteMultiOperationResponse, error) {
	namespaceEntry, err := api.GetActiveNamespace(shardContext, namespace.ID(req.GetNamespaceId()), req.WorkflowId)
	if err != nil {
		return nil, err
	}
	ns := namespaceEntry.Name().String()

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

	newUpdateWithStart := func() (*updateWithStart, error) {
		uws := &updateWithStart{
			shardContext:       shardContext,
			namespaceId:        namespace.ID(req.NamespaceId),
			consistencyChecker: workflowConsistencyChecker,
			testHooks:          testHooks,
			updateReq:          updateReq,
			startReq:           startReq,
		}

		var err error
		uws.starter, err = startworkflow.NewStarter(
			shardContext,
			workflowConsistencyChecker,
			tokenSerializer,
			startReq,
			matchingClient,
			versionCache,
			reactivationSignaler,
			uws.workflowLeaseCallback(ctx),
			// allowDerivedRunID: false. The held lease would self-deadlock on terminate-existing with a derived run ID.
			// TODO: support run-ID dedup for Update-with-Start and drop this parameter.
			false,
		)
		if err != nil {
			return nil, newMultiOpError(err, multiOpAbortedErr)
		}

		uws.updater = updateworkflow.NewUpdater(
			uws.shardContext,
			uws.consistencyChecker,
			matchingClient,
			uws.updateReq,
		)
		return uws, nil
	}

	uws, err := newUpdateWithStart()
	if err != nil {
		return nil, err
	}

	res, err := uws.Invoke(ctx)
	if err != nil {
		// When an Update is admitted but not yet accepted, it can be aborted by a closing workflow.
		// Since - compared to an Update - Update-with-Start has the ability to start a new workflow, the
		// server will retry the Update-with-Start operation (but only once to keep latency and resource usage low).
		// TODO(stephan): remove dynamic config again
		allowServerSideRetry := shardContext.GetConfig().EnableUpdateWithStartRetryOnClosedWorkflowAbort(ns)
		if !allowServerSideRetry || !uws.updateOnlyWasAbortedByClosingWorkflow(err) {
			return nil, err
		}

		// Re-create to ensure the state is clean.
		uws, err = newUpdateWithStart()
		if err != nil {
			return nil, err
		}

		testhooks.Call(uws.testHooks, testhooks.UpdateWithStartOnClosingWorkflowRetry, uws.namespaceId)

		res, err = uws.Invoke(ctx)
		if err != nil {
			// If the Update-with-Start encountered the same error of a closing workflow again, it will convert
			// the error to Aborted (which is a retryable error) to allow the client to retry the operation.
			// TODO(stephan): remove dynamic config again
			allowClientSideRetry := shardContext.GetConfig().EnableUpdateWithStartRetryableErrorOnClosedWorkflowAbort(ns)
			if !allowClientSideRetry || !uws.updateOnlyWasAbortedByClosingWorkflow(err) {
				return nil, err
			}

			var multiOpsErr *serviceerror.MultiOperationExecution
			errors.As(err, &multiOpsErr)
			return nil, serviceerror.NewMultiOperationExecution(multiOpsErr.Error(), []error{
				multiOpsErr.OperationErrors()[0],
				serviceerror.NewAborted(multiOpsErr.OperationErrors()[1].Error()), // changed from NotFound to Aborted!
			})
		}
	}

	return res, nil
}

func (uws *updateWithStart) updateOnlyWasAbortedByClosingWorkflow(err error) bool {
	if uws.workflowWasStarted {
		return false
	}
	var multiOpsErr *serviceerror.MultiOperationExecution
	if ok := errors.As(err, &multiOpsErr); !ok {
		return false
	}
	if len(multiOpsErr.OperationErrors()) != 2 {
		return false
	}
	if !errors.Is(multiOpsErr.OperationErrors()[1], update.AbortedByWorkflowClosingErr) {
		return false
	}
	return true
}

func (uws *updateWithStart) Invoke(ctx context.Context) (*historyservice.ExecuteMultiOperationResponse, error) {
	workflowLease, err := uws.getWorkflowLease(ctx)
	if err != nil {
		return nil, err
	}

	// Workflow already exists.
	if workflowLease != nil {
		updateID := uws.updateReq.Request.Request.Meta.GetUpdateId()

		// If Update is complete, return it.
		if completedUpdate, err := workflowLease.GetMutableState().GetCompletedUpdate(ctx, updateID); err == nil {
			workflowKey := workflowLease.GetContext().GetWorkflowKey()
			// Capture response state before releasing the lock to avoid data races with concurrent goroutines.
			ms := workflowLease.GetMutableState()
			executionState := ms.GetExecutionState()
			status := executionState.Status
			updateNexusCtx := ms.GetUpdateNexusSerializationContext(updateID)
			nexusSerializationContext := common.CloneProto(updateNexusCtx)
			updateResponse := uws.updater.CreateCompletedResponse(
				ms,
				workflowKey,
				completedUpdate.GetOutcome(),
				completedUpdate.GetAcceptedEventId(),
				nexusSerializationContext,
			)
			workflowLease.GetReleaseFn()(nil)
			return makeResponse(
				&historyservice.StartWorkflowExecutionResponse{
					RunId:   workflowKey.RunID,
					Started: false, // set explicitly for emphasis
					Status:  status,
					Link:    generateAttachedRequestIDLink(uws.startReq, workflowKey, executionState),
				},
				updateResponse,
			), nil
		}

		// If Workflow is running ...
		if workflowLease.GetMutableState().IsWorkflowExecutionRunning() {
			// If Start is deduped, apply new/attach to update.
			if canDedup(uws.startReq, workflowLease) {
				return uws.updateWorkflow(ctx, workflowLease, false) // lease released inside
			}

			// If Update exists, attach to update.
			if upd := workflowLease.GetContext().UpdateRegistry(ctx).Find(ctx, updateID); upd != nil {
				return uws.updateWorkflow(ctx, workflowLease, false) // lease released inside
			}

			// If conflict policy allows re-using the workflow, apply the update.
			if uws.startReq.StartRequest.WorkflowIdConflictPolicy == enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING {
				return uws.reuseWorkflow(ctx, workflowLease) // lease released inside
			}
		}

		// Release before starting workflow further down.
		workflowLease.GetReleaseFn()(nil)
	}

	testhooks.Call(uws.testHooks, testhooks.UpdateWithStartInBetweenLockAndStart, uws.namespaceId)

	// Workflow does not exist or requires a new run - start and update it!
	return uws.startAndUpdateWorkflow(ctx)
}

func (uws *updateWithStart) workflowLeaseCallback(
	ctx context.Context,
) api.CreateOrUpdateLeaseFunc {
	return func(
		existingLease api.WorkflowLease,
		shardContext historyi.ShardContext,
		ms historyi.MutableState,
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
			workflowContext, releaseFunc, err := uws.consistencyChecker.GetWorkflowCache().GetOrCreateWorkflowExecution(
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
		if _, err := uws.updater.ApplyRequest(ctx, updateReg, ms); err != nil {
			// Wrapping the error so Update and Start errors can be distinguished later.
			return nil, updateError{err}
		}
		return res, nil
	}
}

func (uws *updateWithStart) getWorkflowLease(ctx context.Context) (api.WorkflowLease, error) {
	runningWorkflowLease, err := uws.consistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		definition.NewWorkflowKey(uws.namespaceId.String(), uws.startReq.StartRequest.WorkflowId, ""),
		locks.PriorityHigh,
	)
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return nil, nil
	}
	if err != nil {
		return nil, newMultiOpError(err, multiOpAbortedErr)
	}
	return runningWorkflowLease, nil
}

func (uws *updateWithStart) reuseWorkflow(
	ctx context.Context,
	currentWorkflowLease api.WorkflowLease,
) (*historyservice.ExecuteMultiOperationResponse, error) {
	return uws.updateWorkflow(ctx, currentWorkflowLease, true) // lease released inside
}

func (uws *updateWithStart) updateWorkflow(
	ctx context.Context,
	currentWorkflowLease api.WorkflowLease,
	applyConflictOptions bool,
) (*historyservice.ExecuteMultiOperationResponse, error) {
	var responseLink *commonpb.Link
	var nexusContextMatch metrics.ReasonString
	var conflictOptionsErr error

	// Apply the update to the workflow.
	err := api.UpdateWorkflowWithNew(
		uws.shardContext,
		ctx,
		currentWorkflowLease,
		func(lease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			ms := lease.GetMutableState()
			if applyConflictOptions {
				responseLink, nexusContextMatch, conflictOptionsErr = startworkflow.ApplyOnConflictOptions(ctx, lease, uws.startReq.StartRequest)
				if conflictOptionsErr == nil && uws.startReq.StartRequest.GetOnConflictOptions() != nil &&
					!ms.HasPendingWorkflowTask() && !ms.IsWorkflowExecutionStatusPaused() {
					_, conflictOptionsErr = ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
				}
				if conflictOptionsErr != nil {
					return nil, conflictOptionsErr
				}
			}
			updateReg := lease.GetContext().UpdateRegistry(ctx)
			action, err := uws.updater.ApplyRequest(ctx, updateReg, ms)
			if err != nil {
				return nil, err
			}
			if applyConflictOptions && uws.startReq.StartRequest.GetOnConflictOptions() != nil {
				action.Noop = false
			}
			if responseLink == nil {
				wfKey := lease.GetContext().GetWorkflowKey()
				responseLink = api.GenerateStartedEventRefLink(uws.startReq.StartRequest.GetNamespace(), wfKey.WorkflowID, wfKey.RunID)
				if uws.startReq.StartRequest.GetOnConflictOptions().GetAttachRequestId() {
					if link := generateAttachedRequestIDLink(uws.startReq, wfKey, ms.GetExecutionState()); link != nil {
						responseLink = link
					}
				}
			}
			return action, nil
		},
		nil,
	)

	// Release lock here since all changes to the workflow have been completed now.
	currentWorkflowLease.GetReleaseFn()(err)

	if err != nil {
		if conflictOptionsErr != nil {
			return nil, newMultiOpError(conflictOptionsErr, multiOpAbortedErr)
		}
		return nil, newMultiOpError(multiOpAbortedErr, err)
	}
	if nexusContextMatch != "" {
		metrics.NexusWorkflowUseExisting.With(uws.shardContext.GetMetricsHandler()).Record(1, metrics.NexusSerializationContextMatchTag(nexusContextMatch))
	}

	// Wait for the update to complete.
	updateResp, err := uws.updater.OnSuccess(ctx)
	if err != nil {
		return nil, newMultiOpError(multiOpAbortedErr, err)
	}

	wfKey := currentWorkflowLease.GetContext().GetWorkflowKey()
	startResp := &historyservice.StartWorkflowExecutionResponse{
		RunId:   wfKey.RunID,
		Started: false, // set explicitly for emphasis
		Status:  enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		Link:    responseLink,
	}

	return makeResponse(startResp, updateResp), nil
}

func (uws *updateWithStart) startAndUpdateWorkflow(ctx context.Context) (*historyservice.ExecuteMultiOperationResponse, error) {
	startResp, startOutcome, err := uws.starter.Invoke(ctx)
	if err != nil {
		// An update error occurred.
		if errors.As(err, &updateError{}) {
			return nil, newMultiOpError(multiOpAbortedErr, err)
		}
		// A start error occurred.
		return nil, newMultiOpError(err, multiOpAbortedErr)
	}
	if startOutcome != startworkflow.StartNew {
		// The workflow was meant to be started - but was actually *not* started.
		// The problem is that the update has not been applied.
		//
		// This can happen when there's a race: another workflow start occurred right after the check for a
		// running workflow above - but before the new workflow could be created (and locked).
		// TODO: Consider a refactoring of the startworkflow.Starter to make this case impossible.
		//
		// The best way forward is to exit and retry from the top.
		// By returning an Unavailable service error, the entire MultiOperation will be retried.
		return nil, newMultiOpError(
			serviceerror.NewUnavailablef("Workflow was not started: %v", startOutcome),
			multiOpAbortedErr)
	}
	uws.workflowWasStarted = true

	// Wait for the update to complete.
	updateResp, err := uws.updater.OnSuccess(ctx)
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
	// Unwrap updateError wrapper if present to allow downstream error inspection
	var ue updateError
	if errors.As(updateErr, &ue) && ue.error != nil {
		updateErr = ue.error
	}

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

func generateAttachedRequestIDLink(
	startReq *historyservice.StartWorkflowExecutionRequest,
	wfKey definition.WorkflowKey,
	executionState *persistencespb.WorkflowExecutionState,
) *commonpb.Link {
	requestID := startReq.StartRequest.GetRequestId()
	if !startReq.StartRequest.GetOnConflictOptions().GetAttachRequestId() ||
		executionState.GetRequestIds()[requestID].GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
		return nil
	}
	return api.GenerateRequestIDRefLink(
		startReq.StartRequest.GetNamespace(),
		wfKey.WorkflowID,
		wfKey.RunID,
		requestID,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
	)
}

func canDedup(startReq *historyservice.StartWorkflowExecutionRequest, currentWorkflowLease api.WorkflowLease) bool {
	requestID := startReq.StartRequest.RequestId
	executionState := currentWorkflowLease.GetMutableState().GetExecutionState()
	if requestID == executionState.GetCreateRequestId() {
		return true
	}
	if requestID == "" {
		return false
	}
	_, ok := executionState.GetRequestIds()[requestID]
	return ok
}
