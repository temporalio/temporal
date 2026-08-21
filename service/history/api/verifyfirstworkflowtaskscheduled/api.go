package verifyfirstworkflowtaskscheduled

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
	"go.temporal.io/server/service/history/api/workflowresend"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func Invoke(
	ctx context.Context,
	req *historyservice.VerifyFirstWorkflowTaskScheduledRequest,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	shardContext historyi.ShardContext,
	inFlightResends *workflowresend.InFlightResends,
) error {
	namespaceID := namespace.ID(req.GetNamespaceId())
	if err := api.ValidateNamespaceUUID(namespaceID); err != nil {
		return err
	}

	versionedTransition, versionHistories, errVerify := verifyFirstWorkflowTaskScheduled(ctx, req, workflowConsistencyChecker)
	if errVerify == nil {
		return nil
	}
	switch errVerify.(type) {
	case *serviceerror.NotFound, *serviceerror.WorkflowNotReady:
		if !req.GetResendChild() || !shardContext.GetConfig().EnableChildWorkflowResend() {
			return errVerify
		}
	default:
		return errVerify
	}

	metricsHandler := shardContext.GetMetricsHandler()
	resend := func(ctx context.Context) error {
		metrics.ChildWorkflowResendAttempts.With(metricsHandler).Record(1)
		startTime := time.Now().UTC()
		err := resendChildAndVerify(
			ctx,
			req,
			workflowConsistencyChecker,
			shardContext,
			namespaceID,
			versionedTransition,
			versionHistories,
			errVerify,
		)
		metrics.ChildWorkflowResendLatency.With(metricsHandler).Record(time.Since(startTime))
		if err != nil {
			recordResendFailure(shardContext, metricsHandler, req, err)
		}
		return err
	}

	childKey := definition.NewWorkflowKey(req.NamespaceId, req.WorkflowExecution.WorkflowId, req.WorkflowExecution.RunId)
	claimed, atCapacity := inFlightResends.TryClaim(childKey, shardContext.GetConfig().ChildWorkflowResendMaxInFlight())
	if atCapacity {
		metrics.ChildWorkflowResendLimited.With(metricsHandler).Record(1)
		shardContext.GetLogger().Warn("Dropped child workflow resend, shard is at its in-flight limit",
			tag.WorkflowNamespaceID(req.GetNamespaceId()),
			tag.WorkflowID(req.WorkflowExecution.GetWorkflowId()),
			tag.WorkflowRunID(req.WorkflowExecution.GetRunId()),
			tag.NewInt("max-in-flight", shardContext.GetConfig().ChildWorkflowResendMaxInFlight()),
		)
		return errVerify
	}
	if !claimed {
		metrics.ChildWorkflowResendSkipped.With(metricsHandler).Record(1)
		return errVerify
	}

	resendCtx := rpc.CopyContextValues(shardContext.GetLifecycleContext(), ctx)
	resendCtx, cancel := context.WithTimeout(resendCtx, shardContext.GetConfig().ReplicationTaskApplyTimeout())
	go func() {
		defer cancel()
		defer inFlightResends.Release(childKey)
		defer func() {
			var panicErr error
			log.CapturePanic(shardContext.GetLogger(), &panicErr)
			if panicErr != nil {
				metrics.ChildWorkflowResendFailures.With(metricsHandler).Record(1)
			}
		}()
		_ = resend(resendCtx)
	}()
	return errVerify
}

func verifyFirstWorkflowTaskScheduled(
	ctx context.Context,
	req *historyservice.VerifyFirstWorkflowTaskScheduledRequest,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (versionedTransition *persistencespb.VersionedTransition, versionHistories *historyspb.VersionHistories, retError error) {
	workflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		req.Clock,
		definition.NewWorkflowKey(
			req.NamespaceId,
			req.WorkflowExecution.WorkflowId,
			req.WorkflowExecution.RunId,
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
		return nil, nil, nil
	}

	if !mutableState.HadOrHasWorkflowTask() {
		executionInfo := mutableState.GetExecutionInfo()
		versionedTransition = transitionhistory.CopyVersionedTransition(
			transitionhistory.LastVersionedTransition(executionInfo.TransitionHistory),
		)
		versionHistories = versionhistory.CopyVersionHistories(executionInfo.VersionHistories)
		return versionedTransition, versionHistories, consts.ErrWorkflowNotReady
	}

	return nil, nil, nil
}

func recordResendFailure(
	shardContext historyi.ShardContext,
	metricsHandler metrics.Handler,
	req *historyservice.VerifyFirstWorkflowTaskScheduledRequest,
	err error,
) {
	metrics.ChildWorkflowResendFailures.With(metricsHandler).Record(1)
	shardContext.GetLogger().Error("Failed to resend child workflow for first workflow task verification",
		tag.WorkflowNamespaceID(req.GetNamespaceId()),
		tag.WorkflowID(req.WorkflowExecution.GetWorkflowId()),
		tag.WorkflowRunID(req.WorkflowExecution.GetRunId()),
		tag.Error(err),
	)
}

func resendChildAndVerify(
	ctx context.Context,
	req *historyservice.VerifyFirstWorkflowTaskScheduledRequest,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	versionedTransition *persistencespb.VersionedTransition,
	versionHistories *historyspb.VersionHistories,
	errVerify error,
) error {
	clusterMetadata := shardContext.GetClusterMetadata()
	currentClusterName := clusterMetadata.GetCurrentClusterName()

	namespaceEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}
	if !namespaceEntry.IsOnCluster(currentClusterName) {
		return nil
	}
	targetClusterInfo := clusterMetadata.GetAllClusterInfo()[currentClusterName]

	activeClusterName := namespaceEntry.ActiveClusterName(namespace.RoutingKey{ID: req.WorkflowExecution.WorkflowId})
	if activeClusterName == currentClusterName {
		return errors.New("namespace becomes active when processing task as standby")
	}

	remoteAdminClient, err := shardContext.GetRemoteAdminClient(activeClusterName)
	if err != nil {
		return err
	}

	resp, err := remoteAdminClient.SyncWorkflowState(ctx, &adminservice.SyncWorkflowStateRequest{
		NamespaceId: req.NamespaceId,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: req.WorkflowExecution.WorkflowId,
			RunId:      req.WorkflowExecution.RunId,
		},
		ArchetypeId:         chasm.WorkflowArchetypeID,
		VersionedTransition: versionedTransition,
		VersionHistories:    versionHistories,
		TargetClusterId:     int32(targetClusterInfo.InitialFailoverVersion),
	})
	if err != nil {
		if common.IsNotFoundError(err) {
			return nil
		}
		var failedPreconditionErr *serviceerror.FailedPrecondition
		if errors.As(err, &failedPreconditionErr) {
			return errVerify
		}
		return err
	}

	namespaceEntry, err = shardContext.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}
	if !namespaceEntry.IsOnCluster(currentClusterName) {
		return nil
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}
	if err := engine.ReplicateVersionedTransition(
		ctx,
		chasm.WorkflowArchetypeID,
		resp.VersionedTransitionArtifact,
		activeClusterName,
	); err != nil && !errors.Is(err, consts.ErrDuplicate) {
		return err
	}

	_, _, err = verifyFirstWorkflowTaskScheduled(ctx, req, workflowConsistencyChecker)
	return err
}
