package verifychildworkflowcompletionrecorded

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/common/wideevents"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/workflowresend"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func verifyChildExecution(
	ctx context.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	request *historyservice.VerifyChildExecutionCompletionRecordedRequest,
) (versionedTransition *persistencespb.VersionedTransition,
	versionHistories *historyspb.VersionHistories,
	parentWorkflowState string,
	retError error,
) {
	workflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		request.Clock,
		// it's ok we have stale state when doing verification,
		// the logic will return WorkflowNotReady error and the caller will retry
		// this can prevent keep reloading mutable state when there's a replication lag
		// in parent shard.
		definition.NewWorkflowKey(
			request.NamespaceId,
			request.ParentExecution.WorkflowId,
			request.ParentExecution.RunId,
		),
		locks.PriorityLow,
	)
	if err != nil {
		return nil, nil, "", err
	}
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	mutableState := workflowLease.GetMutableState()
	parentWorkflowState = mutableState.GetExecutionState().GetState().String()
	if !mutableState.IsWorkflowExecutionRunning() &&
		mutableState.GetExecutionState().State != enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		// parent has already completed and can't be blocked after failover.
		return nil, nil, parentWorkflowState, nil
	}

	onCurrentBranch, err := api.IsHistoryEventOnCurrentBranch(mutableState, request.ParentInitiatedId, request.ParentInitiatedVersion)
	if err != nil {
		// initiated event not found on any branch
		return nil, nil, parentWorkflowState, consts.ErrWorkflowNotReady
	}

	if !onCurrentBranch {
		// due to conflict resolution, the initiated event may on a different branch of the workflow.
		// we don't have to do anything and can simply return not found error. Standby logic
		// after seeing this error will give up verification.
		return nil, nil, parentWorkflowState, consts.ErrChildExecutionNotFound
	}

	ci, isRunning := mutableState.GetChildExecutionInfo(request.ParentInitiatedId)
	if isRunning {
		if ci.StartedEventId != common.EmptyEventID &&
			ci.GetStartedWorkflowId() != request.ChildExecution.GetWorkflowId() {
			// this can happen since we may not have the initiated version
			return nil, nil, parentWorkflowState, consts.ErrChildExecutionNotFound
		}

		return nil, nil, parentWorkflowState, consts.ErrWorkflowNotReady
	}

	versionedTransition = transitionhistory.CopyVersionedTransition(transitionhistory.LastVersionedTransition(mutableState.GetExecutionInfo().TransitionHistory))
	versionHistories = versionhistory.CopyVersionHistories(mutableState.GetExecutionInfo().VersionHistories)
	return versionedTransition, versionHistories, parentWorkflowState, nil
}

func Invoke(
	ctx context.Context,
	request *historyservice.VerifyChildExecutionCompletionRecordedRequest,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	shardContext historyi.ShardContext,
	resendScheduler workflowresend.Scheduler,
) (*historyservice.VerifyChildExecutionCompletionRecordedResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if err := api.ValidateNamespaceUUID(namespaceID); err != nil {
		return nil, err
	}

	resendParent := false
	versionedTransition, versionHistories, parentWorkflowState, errVerify := verifyChildExecution(ctx, workflowConsistencyChecker, request)
	switch errVerify.(type) {
	case nil:
		return &historyservice.VerifyChildExecutionCompletionRecordedResponse{}, nil
	case *serviceerror.NotFound, *serviceerror.WorkflowNotReady:
		resendParent = request.GetResendParent()
	}
	if !resendParent {
		return nil, errVerify
	}

	metricsHandler := shardContext.GetMetricsHandler()
	emitLifecycle := shardContext.GetConfig().EmitReplicationLifecycleEvents()

	resend := func(ctx context.Context) error {
		metrics.ParentWorkflowResendAttempts.With(metricsHandler).Record(1)
		startTime := time.Now().UTC()
		err := resendParentAndVerify(
			ctx,
			request,
			workflowConsistencyChecker,
			shardContext,
			namespaceID,
			versionedTransition,
			versionHistories,
			errVerify,
			parentWorkflowState,
			emitLifecycle,
		)
		metrics.ParentWorkflowResendLatency.With(metricsHandler).Record(time.Since(startTime))
		if err != nil && !isExpectedResendError(err) {
			metrics.ParentWorkflowResendFailures.With(metricsHandler).Record(1)
		}
		logResendFailure(shardContext, request, err)
		return err
	}

	if resendScheduler == nil || !shardContext.GetConfig().EnableAsyncParentWorkflowResend() {
		if err := resend(ctx); err != nil {
			return nil, err
		}
		return &historyservice.VerifyChildExecutionCompletionRecordedResponse{}, nil
	}

	// The resend can take minutes while the calling standby task's deadline is short, so run it in
	// the background and let that task retry until the parent lands.
	//
	// The host scheduler deduplicates by workflow and applies an aggregate limit across shards.
	parentKey := definition.NewWorkflowKey(request.NamespaceId, request.ParentExecution.WorkflowId, request.ParentExecution.RunId)

	// The context is detached from the request, which gRPC cancels when this handler returns, and
	// rooted at the shard lifecycle so the work stops with the shard.
	resendCtx := rpc.CopyContextValues(shardContext.GetLifecycleContext(), ctx)
	submitResult := resendScheduler.TrySubmit(
		resendCtx,
		parentKey,
		shardContext.GetConfig().ReplicationTaskApplyTimeout(),
		func(ctx context.Context) {
			if emitLifecycle {
				emitParentResendLifecycleEvent(
					shardContext,
					request,
					parentWorkflowState,
					wideevents.ParentChildOutcomeScheduled,
					nil,
					parentResendEventDetails(errVerify),
				)
			}
			_ = resend(ctx)
		},
	)
	switch submitResult {
	case workflowresend.SubmitResultAccepted:
		// Accepted work records its attempt when execution starts.
	case workflowresend.SubmitResultDuplicate:
		metrics.ParentWorkflowResendSkipped.With(metricsHandler).Record(1)
		if emitLifecycle {
			emitParentResendLifecycleEvent(
				shardContext,
				request,
				parentWorkflowState,
				wideevents.ParentChildOutcomeDeduplicated,
				nil,
				parentResendEventDetails(errVerify),
			)
		}
	case workflowresend.SubmitResultAtCapacity:
		metrics.ParentWorkflowResendLimited.With(metricsHandler).Record(1)
		if emitLifecycle {
			details := parentResendEventDetails(errVerify)
			details["max_in_flight"] = shardContext.GetConfig().WorkflowResendHostMaxInFlight()
			emitParentResendLifecycleEvent(
				shardContext,
				request,
				parentWorkflowState,
				wideevents.ParentChildOutcomeLimited,
				nil,
				details,
			)
		}
	default:
		metrics.ParentWorkflowResendFailures.With(metricsHandler).Record(1)
	}
	// The submission result is intentionally used only for metrics. Preserve the verification error
	// so the durable standby task retries regardless of the admission outcome.
	return nil, errVerify
}

func logResendFailure(
	shardContext historyi.ShardContext,
	request *historyservice.VerifyChildExecutionCompletionRecordedRequest,
	err error,
) {
	if isExpectedResendError(err) {
		return
	}
	shardContext.GetThrottledLogger().Error(
		"Failed to resend parent workflow for child completion verification",
		tag.WorkflowNamespaceID(request.GetNamespaceId()),
		tag.NewStringTag("parent-workflow-id", request.ParentExecution.GetWorkflowId()),
		tag.NewStringTag("parent-run-id", request.ParentExecution.GetRunId()),
		tag.NewStringTag("child-workflow-id", request.ChildExecution.GetWorkflowId()),
		tag.NewStringTag("child-run-id", request.ChildExecution.GetRunId()),
		tag.Error(err),
	)
}

func isExpectedResendError(err error) bool {
	if err == nil || common.IsContextCanceledErr(err) {
		return true
	}
	var notFoundErr *serviceerror.NotFound
	var workflowNotReadyErr *serviceerror.WorkflowNotReady
	var namespaceNotFoundErr *serviceerror.NamespaceNotFound
	return errors.As(err, &notFoundErr) ||
		errors.As(err, &workflowNotReadyErr) ||
		errors.As(err, &namespaceNotFoundErr)
}

// resendParentAndVerify pulls the parent workflow's state from the source cluster, applies it, and
// re-checks the child's completion. Separate from Invoke so the async path can run it detached.
func resendParentAndVerify(
	ctx context.Context,
	request *historyservice.VerifyChildExecutionCompletionRecordedRequest,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	versionedTransition *persistencespb.VersionedTransition,
	versionHistories *historyspb.VersionHistories,
	errVerify error,
	parentWorkflowState string,
	emitLifecycle bool,
) error {
	activeClusterName := ""
	emitResult := func(outcome string, eventErr error, stage string) {
		if !emitLifecycle {
			return
		}
		details := parentResendEventDetails(errVerify)
		if activeClusterName != "" {
			details["source_cluster"] = activeClusterName
		}
		if stage != "" {
			details["stage"] = stage
		}
		emitParentResendLifecycleEvent(shardContext, request, parentWorkflowState, outcome, eventErr, details)
	}
	result, err := workflowresend.SyncWorkflowStateFromSource(
		ctx,
		shardContext,
		namespaceID,
		request.ParentExecution,
		versionedTransition,
		versionHistories,
		func(sourceCluster string) {
			activeClusterName = sourceCluster
			emitResult(wideevents.ParentChildOutcomeStarted, nil, "sync_workflow_state")
		},
	)
	if err != nil {
		emitResult(wideevents.ParentChildOutcomeFailed, err, "sync_workflow_state")
		return err
	}
	switch result {
	case workflowresend.SyncWorkflowStateResultSourceNotFound:
		// TODO: add parent workflow to workflowNotFoundCache
		emitResult(
			wideevents.ParentChildOutcomeSourceNotFound,
			serviceerror.NewNotFound("parent workflow not found on source cluster"),
			"sync_workflow_state",
		)
		return nil
	case workflowresend.SyncWorkflowStateResultSkipped:
		return errVerify
	case workflowresend.SyncWorkflowStateResultApplied:
	default:
		return fmt.Errorf("unknown workflow state sync result: %d", result)
	}

	_, _, observedParentWorkflowState, err := verifyChildExecution(ctx, workflowConsistencyChecker, request)
	if observedParentWorkflowState != "" {
		parentWorkflowState = observedParentWorkflowState
	}
	if err != nil {
		emitResult(wideevents.ParentChildOutcomeFailed, err, "verify_after_resend")
		return err
	}
	emitResult(wideevents.ParentChildOutcomeSucceeded, nil, "")
	return nil
}

func parentResendEventDetails(initialError error) map[string]any {
	return map[string]any{
		"initial_error_type": util.ErrorType(initialError),
	}
}

func emitParentResendLifecycleEvent(
	shardContext historyi.ShardContext,
	request *historyservice.VerifyChildExecutionCompletionRecordedRequest,
	parentWorkflowState string,
	outcome string,
	err error,
	details map[string]any,
) {
	eventDetails := make(map[string]any, len(details)+11)
	maps.Copy(eventDetails, details)
	eventDetails["event_type"] = wideevents.ParentChildLifecycleEventType
	eventDetails["phase"] = wideevents.ParentChildPhaseParentResend
	eventDetails["outcome"] = outcome
	eventDetails["local_cluster"] = shardContext.GetClusterMetadata().GetCurrentClusterName()
	eventDetails["parent_namespace_id"] = request.GetNamespaceId()
	eventDetails["parent_workflow_id"] = request.GetParentExecution().GetWorkflowId()
	eventDetails["parent_run_id"] = request.GetParentExecution().GetRunId()
	eventDetails["parent_workflow_state"] = parentWorkflowState
	eventDetails["child_workflow_id"] = request.GetChildExecution().GetWorkflowId()
	eventDetails["child_run_id"] = request.GetChildExecution().GetRunId()
	eventDetails["parent_initiated_id"] = request.GetParentInitiatedId()
	eventDetails["parent_initiated_version"] = request.GetParentInitiatedVersion()

	namespaceName := ""
	if entry, namespaceErr := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(request.GetNamespaceId())); namespaceErr == nil {
		namespaceName = entry.Name().String()
	}
	sourceCluster, ok := eventDetails["source_cluster"].(string)
	if !ok {
		sourceCluster = ""
	}
	delete(eventDetails, "source_cluster")
	if err != nil {
		eventDetails["error"] = err.Error()
		eventDetails["error_type"] = util.ErrorType(err)
	}
	payload := wideevents.ReplicationLifecyclePayload{
		TaskType:      wideevents.ReplTaskSyncWorkflowState,
		Shard:         shardContext.GetShardID(),
		Namespace:     namespaceName,
		NamespaceID:   request.GetNamespaceId(),
		WorkflowID:    request.GetParentExecution().GetWorkflowId(),
		RunID:         request.GetParentExecution().GetRunId(),
		SourceCluster: sourceCluster,
	}
	switch outcome {
	case wideevents.ParentChildOutcomeScheduled,
		wideevents.ParentChildOutcomeStarted,
		wideevents.ParentChildOutcomeDeduplicated:
		eventDetails["operation"] = wideevents.ReplOperationStandbyVerificationSyncState
		eventDetails["message"] = "Parent workflow resend checkpoint"
		payload.Phase = wideevents.ReplicationExecuting
		payload.Details = eventDetails
		wideevents.Emit(shardContext.GetEventLogger(), payload)
	case wideevents.ParentChildOutcomeSucceeded, wideevents.ParentChildOutcomeSourceNotFound:
		eventDetails["operation"] = wideevents.ReplOperationStandbyVerificationSyncState
		eventDetails["message"] = "Parent workflow resend checkpoint"
		payload.Phase = wideevents.ReplicationApplied
		payload.Outcome = wideevents.ParentChildOutcomeVerified
		payload.Details = eventDetails
		wideevents.Emit(shardContext.GetEventLogger(), payload)
	default:
		wideevents.EmitReplicationError(
			shardContext.GetEventLogger(),
			payload,
			wideevents.ReplOperationStandbyVerificationSyncState,
			"Parent workflow resend checkpoint",
			err,
			eventDetails,
		)
	}
}
