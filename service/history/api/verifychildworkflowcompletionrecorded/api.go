package verifychildworkflowcompletionrecorded

import (
	"context"
	"errors"

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
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
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

	// Resend parent workflow from source cluster

	clusterMetadata := shardContext.GetClusterMetadata()
	targetClusterInfo := clusterMetadata.GetAllClusterInfo()[clusterMetadata.GetCurrentClusterName()]

	namespaceEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(namespaceID))
	if err != nil {
		return nil, err
	}

	activeClusterName := namespaceEntry.ActiveClusterName(request.ParentExecution.WorkflowId)
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
		return nil, err
	}

	// Verify child execution again after resending parent workflow
	_, _, err = verifyChildExecution(ctx, workflowConsistencyChecker, request)
	if err != nil {
		return nil, err
	}
	return &historyservice.VerifyChildExecutionCompletionRecordedResponse{}, nil
}
