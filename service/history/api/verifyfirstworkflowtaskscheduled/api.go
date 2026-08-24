package verifyfirstworkflowtaskscheduled

import (
	"context"
	"errors"
	"fmt"
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
	resendScheduler workflowresend.Scheduler,
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
		if err != nil && !isExpectedResendError(err) {
			metrics.ChildWorkflowResendFailures.With(metricsHandler).Record(1)
		}
		logResendFailure(shardContext, req, err)
		return err
	}

	childKey := definition.NewWorkflowKey(req.NamespaceId, req.WorkflowExecution.WorkflowId, req.WorkflowExecution.RunId)
	resendCtx := rpc.CopyContextValues(shardContext.GetLifecycleContext(), ctx)
	submitResult := resendScheduler.TrySubmit(
		resendCtx,
		childKey,
		shardContext.GetConfig().ReplicationTaskApplyTimeout(),
		func(ctx context.Context) {
			_ = resend(ctx)
		},
	)
	switch submitResult {
	case workflowresend.SubmitResultAccepted:
	case workflowresend.SubmitResultDuplicate:
		metrics.ChildWorkflowResendSkipped.With(metricsHandler).Record(1)
	case workflowresend.SubmitResultAtCapacity:
		metrics.ChildWorkflowResendLimited.With(metricsHandler).Record(1)
	default:
		// SubmitResultFailed and unknown values are admission failures.
		metrics.ChildWorkflowResendFailures.With(metricsHandler).Record(1)
	}
	// The submission result is intentionally used only for metrics. Preserve the verification error
	// so the durable standby task retries regardless of the admission outcome.
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

func logResendFailure(
	shardContext historyi.ShardContext,
	req *historyservice.VerifyFirstWorkflowTaskScheduledRequest,
	err error,
) {
	if isExpectedResendError(err) {
		return
	}
	shardContext.GetThrottledLogger().Error(
		"Failed to resend child workflow for first workflow task verification",
		tag.WorkflowNamespaceID(req.GetNamespaceId()),
		tag.WorkflowID(req.WorkflowExecution.GetWorkflowId()),
		tag.WorkflowRunID(req.WorkflowExecution.GetRunId()),
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
	result, err := workflowresend.SyncWorkflowStateFromSource(
		ctx,
		shardContext,
		namespaceID,
		req.WorkflowExecution,
		versionedTransition,
		versionHistories,
		nil,
	)
	if err != nil {
		return err
	}
	switch result {
	case workflowresend.SyncWorkflowStateResultSourceNotFound:
		return nil
	case workflowresend.SyncWorkflowStateResultSkipped:
		return errVerify
	case workflowresend.SyncWorkflowStateResultApplied:
		_, _, err = verifyFirstWorkflowTaskScheduled(ctx, req, workflowConsistencyChecker)
		return err
	default:
		return fmt.Errorf("unknown workflow state sync result: %d", result)
	}
}
