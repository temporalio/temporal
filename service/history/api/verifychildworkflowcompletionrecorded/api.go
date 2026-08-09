package verifychildworkflowcompletionrecorded

import (
	"context"
	"errors"
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
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func verifyChildExecution(
	ctx context.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	request *historyservice.VerifyChildExecutionCompletionRecordedRequest,
) (versionedTransition *persistencespb.VersionedTransition,
	versionHistories *historyspb.VersionHistories, retError error) {
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
		return nil, nil, err
	}
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	mutableState := workflowLease.GetMutableState()
	if !mutableState.IsWorkflowExecutionRunning() &&
		mutableState.GetExecutionState().State != enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		// parent has already completed and can't be blocked after failover.
		return nil, nil, nil
	}

	onCurrentBranch, err := api.IsHistoryEventOnCurrentBranch(mutableState, request.ParentInitiatedId, request.ParentInitiatedVersion)
	if err != nil {
		// initiated event not found on any branch
		return nil, nil, consts.ErrWorkflowNotReady
	}

	if !onCurrentBranch {
		// due to conflict resolution, the initiated event may on a different branch of the workflow.
		// we don't have to do anything and can simply return not found error. Standby logic
		// after seeing this error will give up verification.
		return nil, nil, consts.ErrChildExecutionNotFound
	}

	ci, isRunning := mutableState.GetChildExecutionInfo(request.ParentInitiatedId)
	if isRunning {
		if ci.StartedEventId != common.EmptyEventID &&
			ci.GetStartedWorkflowId() != request.ChildExecution.GetWorkflowId() {
			// this can happen since we may not have the initiated version
			return nil, nil, consts.ErrChildExecutionNotFound
		}

		return nil, nil, consts.ErrWorkflowNotReady
	}

	versionedTransition = transitionhistory.CopyVersionedTransition(transitionhistory.LastVersionedTransition(mutableState.GetExecutionInfo().TransitionHistory))
	versionHistories = versionhistory.CopyVersionHistories(mutableState.GetExecutionInfo().VersionHistories)
	return versionedTransition, versionHistories, nil
}

func Invoke(
	ctx context.Context,
	request *historyservice.VerifyChildExecutionCompletionRecordedRequest,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	shardContext historyi.ShardContext,
	inFlightResends *InFlightResends,
) (*historyservice.VerifyChildExecutionCompletionRecordedResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if err := api.ValidateNamespaceUUID(namespaceID); err != nil {
		return nil, err
	}

	resendParent := false
	versionedTransition, versionHistories, errVerify := verifyChildExecution(ctx, workflowConsistencyChecker, request)
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

	// The measured resend, run either inline or in the background.
	resend := func(ctx context.Context) (*historyservice.VerifyChildExecutionCompletionRecordedResponse, error) {
		metrics.ParentWorkflowResendAttempts.With(metricsHandler).Record(1)
		startTime := time.Now().UTC()
		resp, err := resendParentAndVerify(ctx, request, workflowConsistencyChecker, shardContext, namespaceID, versionedTransition, versionHistories, errVerify)
		metrics.ParentWorkflowResendLatency.With(metricsHandler).Record(time.Since(startTime))
		if err != nil {
			recordResendFailure(shardContext, metricsHandler, request, err)
		}
		return resp, err
	}

	if !shardContext.GetConfig().EnableAsyncParentWorkflowResend() {
		return resend(ctx)
	}

	// The resend can take minutes while the calling standby task's deadline is short, so run it in
	// the background and let that task retry until the parent lands.
	//
	// At most one resend per parent, and at most ParentWorkflowResendMaxInFlight per shard: callers
	// retry while an earlier resend runs, so without these a stale parent, or a namespace with many
	// of them, would spawn goroutines without bound.
	parentKey := definition.NewWorkflowKey(request.NamespaceId, request.ParentExecution.WorkflowId, request.ParentExecution.RunId)
	claimed, atCapacity := inFlightResends.tryClaim(parentKey, shardContext.GetConfig().ParentWorkflowResendMaxInFlight())
	if atCapacity {
		metrics.ParentWorkflowResendLimited.With(metricsHandler).Record(1)
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
		return nil, errVerify
	}

	// The context is detached from the request, which gRPC cancels when this handler returns, and
	// rooted at the shard lifecycle so the work stops with the shard.
	resendCtx := rpc.CopyContextValues(shardContext.GetLifecycleContext(), ctx)
	resendCtx, cancel := context.WithTimeout(resendCtx, shardContext.GetConfig().ReplicationTaskApplyTimeout())
	go func() {
		defer cancel()
		defer inFlightResends.release(parentKey)
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
) (*historyservice.VerifyChildExecutionCompletionRecordedResponse, error) {
	// Resend parent workflow from source cluster

	clusterMetadata := shardContext.GetClusterMetadata()
	targetClusterInfo := clusterMetadata.GetAllClusterInfo()[clusterMetadata.GetCurrentClusterName()]

	namespaceEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	activeClusterName := namespaceEntry.ActiveClusterName(namespace.RoutingKey{ID: request.ParentExecution.WorkflowId})
	if activeClusterName == clusterMetadata.GetCurrentClusterName() {
		return nil, errors.New("namespace becomes active when processing task as standby")
	}

	remoteAdminClient, err := shardContext.GetRemoteAdminClient(activeClusterName)
	if err != nil {
		return nil, err
	}

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
			return &historyservice.VerifyChildExecutionCompletionRecordedResponse{}, nil
		}
		var failedPreconditionErr *serviceerror.FailedPrecondition
		if errors.As(err, &failedPreconditionErr) {
			// Unable to perform sync state. Transition history maybe disabled in source cluster.
			return nil, errVerify
		}
		return nil, err
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return nil, err
	}
	err = engine.ReplicateVersionedTransition(ctx, chasm.WorkflowArchetypeID, resp.VersionedTransitionArtifact, activeClusterName)
	if err != nil {
		if !errors.Is(err, consts.ErrDuplicate) {
			return nil, err
		}
	}

	// Verify child execution again after resending parent workflow
	_, _, err = verifyChildExecution(ctx, workflowConsistencyChecker, request)
	if err != nil {
		return nil, err
	}
	return &historyservice.VerifyChildExecutionCompletionRecordedResponse{}, nil
}
