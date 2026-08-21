package verifychildworkflowcompletionrecorded

import (
	"context"
	"errors"
	"maps"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
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
	inFlightResends *workflowresend.InFlightResends,
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
	asyncResend := shardContext.GetConfig().EnableAsyncParentWorkflowResend()

	// The measured resend, run either inline or in the background.
	resend := func(ctx context.Context) (*historyservice.VerifyChildExecutionCompletionRecordedResponse, error) {
		metrics.ParentWorkflowResendAttempts.With(metricsHandler).Record(1)
		startTime := time.Now().UTC()
		resp, err := resendParentAndVerify(ctx, request, workflowConsistencyChecker, shardContext, namespaceID, versionedTransition, versionHistories, errVerify, parentWorkflowState, emitLifecycle)
		metrics.ParentWorkflowResendLatency.With(metricsHandler).Record(time.Since(startTime))
		if err != nil {
			recordResendFailure(shardContext, metricsHandler, request, err)
		}
		return resp, err
	}

	if !asyncResend {
		return resend(ctx)
	}

	// The resend can take minutes while the calling standby task's deadline is short, so run it in
	// the background and let that task retry until the parent lands.
	//
	// At most one resend per parent, and at most ParentWorkflowResendMaxInFlight per shard: callers
	// retry while an earlier resend runs, so without these a stale parent, or a namespace with many
	// of them, would spawn goroutines without bound.
	parentKey := definition.NewWorkflowKey(request.NamespaceId, request.ParentExecution.WorkflowId, request.ParentExecution.RunId)
	maxInFlight := shardContext.GetConfig().ParentWorkflowResendMaxInFlight()
	claimed, atCapacity := inFlightResends.TryClaim(parentKey, maxInFlight)
	if atCapacity {
		metrics.ParentWorkflowResendLimited.With(metricsHandler).Record(1)
		if emitLifecycle {
			details := parentResendEventDetails(errVerify)
			details["max_in_flight"] = maxInFlight
			emitParentResendLifecycleEvent(shardContext, request, parentWorkflowState, wideevents.ParentChildOutcomeLimited, nil, details)
		}
		shardContext.GetLogger().Warn("Dropped parent workflow resend, shard is at its in-flight limit",
			tag.WorkflowNamespaceID(request.GetNamespaceId()),
			tag.NewStringTag("parent-workflow-id", request.ParentExecution.GetWorkflowId()),
			tag.NewStringTag("parent-run-id", request.ParentExecution.GetRunId()),
			tag.NewStringTag("child-workflow-id", request.ChildExecution.GetWorkflowId()),
			tag.NewStringTag("child-run-id", request.ChildExecution.GetRunId()),
			tag.NewInt("max-in-flight", shardContext.GetConfig().ParentWorkflowResendMaxInFlight()),
		)
		return nil, errVerify
	}
	if !claimed {
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
		return nil, errVerify
	}
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

	// The context is detached from the request, which gRPC cancels when this handler returns, and
	// rooted at the shard lifecycle so the work stops with the shard.
	resendCtx := rpc.CopyContextValues(shardContext.GetLifecycleContext(), ctx)
	resendCtx, cancel := context.WithTimeout(resendCtx, shardContext.GetConfig().ReplicationTaskApplyTimeout())
	go func() {
		defer cancel()
		defer inFlightResends.Release(parentKey)
		defer func() {
			var panicErr error
			log.CapturePanic(shardContext.GetLogger(), &panicErr)
			if panicErr != nil {
				metrics.ParentWorkflowResendFailures.With(metricsHandler).Record(1)
			}
		}()
		_, _ = resend(resendCtx)
	}()
	return nil, errVerify
}

// recordResendFailure reports a failed parent resend. On the asynchronous path no caller receives
// the error, so these are its only signals.
func recordResendFailure(
	shardContext historyi.ShardContext,
	metricsHandler metrics.Handler,
	request *historyservice.VerifyChildExecutionCompletionRecordedRequest,
	err error,
) {
	metrics.ParentWorkflowResendFailures.With(metricsHandler).Record(1)
	shardContext.GetLogger().Error("Failed to resend parent workflow for child completion verification",
		tag.WorkflowNamespaceID(request.GetNamespaceId()),
		tag.NewStringTag("parent-workflow-id", request.ParentExecution.GetWorkflowId()),
		tag.NewStringTag("parent-run-id", request.ParentExecution.GetRunId()),
		tag.NewStringTag("child-workflow-id", request.ChildExecution.GetWorkflowId()),
		tag.NewStringTag("child-run-id", request.ChildExecution.GetRunId()),
		tag.Error(err),
	)
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
) (*historyservice.VerifyChildExecutionCompletionRecordedResponse, error) {
	// Resend parent workflow from source cluster

	clusterMetadata := shardContext.GetClusterMetadata()
	targetClusterInfo := clusterMetadata.GetAllClusterInfo()[clusterMetadata.GetCurrentClusterName()]
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

	namespaceEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		emitResult(wideevents.ParentChildOutcomeFailed, err, "resolve_namespace")
		return nil, err
	}

	activeClusterName = namespaceEntry.ActiveClusterName(namespace.RoutingKey{ID: request.ParentExecution.WorkflowId})
	if activeClusterName == clusterMetadata.GetCurrentClusterName() {
		err = errors.New("namespace becomes active when processing task as standby")
		emitResult(wideevents.ParentChildOutcomeFailed, err, "resolve_source_cluster")
		return nil, err
	}

	remoteAdminClient, err := shardContext.GetRemoteAdminClient(activeClusterName)
	if err != nil {
		emitResult(wideevents.ParentChildOutcomeFailed, err, "resolve_remote_client")
		return nil, err
	}
	emitResult(wideevents.ParentChildOutcomeStarted, nil, "sync_workflow_state")

	resp, err := remoteAdminClient.SyncWorkflowState(ctx, &adminservice.SyncWorkflowStateRequest{
		NamespaceId: request.NamespaceId,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: request.ParentExecution.WorkflowId,
			RunId:      request.ParentExecution.RunId,
		},
		ArchetypeId:         chasm.WorkflowArchetypeID,
		VersionedTransition: versionedTransition,
		VersionHistories:    versionHistories,
		TargetClusterId:     int32(targetClusterInfo.InitialFailoverVersion),
	})

	if err != nil {
		if common.IsNotFoundError(err) {
			// parent workflow is not found on source cluster,
			// we can return empty response to indicate that verification is done
			// TODO: add parent workflow to workflowNotFoundCache
			emitResult(wideevents.ParentChildOutcomeSourceNotFound, err, "sync_workflow_state")
			return &historyservice.VerifyChildExecutionCompletionRecordedResponse{}, nil
		}
		if _, ok := errors.AsType[*serviceerror.FailedPrecondition](err); ok {
			// Unable to perform sync state. Transition history maybe disabled in source cluster.
			emitResult(wideevents.ParentChildOutcomeFailed, err, "sync_workflow_state")
			return nil, errVerify
		}
		emitResult(wideevents.ParentChildOutcomeFailed, err, "sync_workflow_state")
		return nil, err
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		emitResult(wideevents.ParentChildOutcomeFailed, err, "get_engine")
		return nil, err
	}
	err = engine.ReplicateVersionedTransition(ctx, chasm.WorkflowArchetypeID, resp.VersionedTransitionArtifact, activeClusterName)
	if err != nil {
		if !errors.Is(err, consts.ErrDuplicate) {
			emitResult(wideevents.ParentChildOutcomeFailed, err, "replicate_versioned_transition")
			return nil, err
		}
	}

	// Verify child execution again after resending parent workflow
	_, _, observedParentWorkflowState, err := verifyChildExecution(ctx, workflowConsistencyChecker, request)
	if observedParentWorkflowState != "" {
		parentWorkflowState = observedParentWorkflowState
	}
	if err != nil {
		emitResult(wideevents.ParentChildOutcomeFailed, err, "verify_after_resend")
		return nil, err
	}
	emitResult(wideevents.ParentChildOutcomeSucceeded, nil, "")
	return &historyservice.VerifyChildExecutionCompletionRecordedResponse{}, nil
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
