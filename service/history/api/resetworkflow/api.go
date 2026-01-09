package resetworkflow

import (
	"context"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/ndc"
)

func Invoke(
	ctx context.Context,
	resetRequest *historyservice.ResetWorkflowExecutionRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	matchingClient matchingservice.MatchingServiceClient,
	versionMembershipCache worker_versioning.VersionMembershipCache,
) (_ *historyservice.ResetWorkflowExecutionResponse, retError error) {
	namespaceID := namespace.ID(resetRequest.GetNamespaceId())
	err := api.ValidateNamespaceUUID(namespaceID)
	if err != nil {
		return nil, err
	}

	request := resetRequest.ResetRequest
	workflowID := request.WorkflowExecution.GetWorkflowId()
	baseRunID := request.WorkflowExecution.GetRunId()

	baseWorkflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			baseRunID,
		),
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	defer func() { baseWorkflowLease.GetReleaseFn()(retError) }()

	baseMutableState := baseWorkflowLease.GetMutableState()
	if request.GetWorkflowTaskFinishEventId() <= common.FirstEventID ||
		request.GetWorkflowTaskFinishEventId() >= baseMutableState.GetNextEventID() {
		return nil, serviceerror.NewInvalidArgument("Workflow task finish ID must be > 1 && <= workflow last event ID.")
	}

	// Validate versioning override, if any.
	err = validatePostResetOperationInputs(ctx, request.GetPostResetOperations(), matchingClient, versionMembershipCache,
		baseMutableState.GetExecutionInfo().GetTaskQueue(), namespaceID.String())
	if err != nil {
		return nil, err
	}

	// also load the current run of the workflow, it can be different from the base runID
	currentRunID, err := workflowConsistencyChecker.GetCurrentWorkflowRunID(
		ctx,
		namespaceID.String(),
		request.WorkflowExecution.GetWorkflowId(),
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	if baseRunID == "" {
		baseRunID = currentRunID
	}

	var currentWorkflowLease api.WorkflowLease
	if currentRunID == baseRunID {
		currentWorkflowLease = baseWorkflowLease
	} else {
		currentWorkflowLease, err = workflowConsistencyChecker.GetWorkflowLease(
			ctx,
			nil,
			definition.NewWorkflowKey(
				namespaceID.String(),
				workflowID,
				currentRunID,
			),
			locks.PriorityHigh,
		)
		if err != nil {
			return nil, err
		}
		defer func() { currentWorkflowLease.GetReleaseFn()(retError) }()
	}

	// dedup by requestID
	if currentWorkflowLease.GetMutableState().GetExecutionState().CreateRequestId == request.GetRequestId() {
		shardContext.GetLogger().Info("Duplicated reset request",
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(currentRunID),
			tag.WorkflowNamespaceID(namespaceID.String()))
		return &historyservice.ResetWorkflowExecutionResponse{
			RunId: currentRunID,
		}, nil
	}

	resetRunID := uuid.New().String()
	baseRebuildLastEventID := request.GetWorkflowTaskFinishEventId() - 1
	baseVersionHistories := baseMutableState.GetExecutionInfo().GetVersionHistories()
	baseCurrentVersionHistory, err := versionhistory.GetCurrentVersionHistory(baseVersionHistories)
	if err != nil {
		return nil, err
	}
	baseRebuildLastEventVersion, err := versionhistory.GetVersionHistoryEventVersion(baseCurrentVersionHistory, baseRebuildLastEventID)
	if err != nil {
		return nil, err
	}
	baseCurrentBranchToken := baseCurrentVersionHistory.GetBranchToken()
	baseNextEventID := baseMutableState.GetNextEventID()
	baseWorkflow := ndc.NewWorkflow(
		shardContext.GetClusterMetadata(),
		baseWorkflowLease.GetContext(),
		baseWorkflowLease.GetMutableState(),
		baseWorkflowLease.GetReleaseFn(),
	)

	namespaceEntry, err := api.GetActiveNamespace(shardContext, namespaceID, workflowID)
	if err != nil {
		return nil, err
	}

	metrics.WorkflowResetCount.With(
		shardContext.GetMetricsHandler().WithTags(
			metrics.NamespaceTag(namespaceEntry.Name().String()),
			metrics.OperationTag(metrics.HistoryResetWorkflowScope),
			metrics.VersioningBehaviorTag(baseMutableState.GetEffectiveVersioningBehavior()),
		),
	).Record(1)

	allowResetWithPendingChildren := shardContext.GetConfig().AllowResetWithPendingChildren(namespaceEntry.Name().String())
	if err := ndc.NewWorkflowResetter(
		shardContext,
		workflowConsistencyChecker.GetWorkflowCache(),
		shardContext.GetLogger(),
	).ResetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		baseRunID,
		baseCurrentBranchToken,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		baseNextEventID,
		resetRunID,
		request.GetRequestId(),
		baseWorkflow,
		ndc.NewWorkflow(
			shardContext.GetClusterMetadata(),
			currentWorkflowLease.GetContext(),
			currentWorkflowLease.GetMutableState(),
			currentWorkflowLease.GetReleaseFn(),
		),
		request.GetReason(),
		nil,
		GetResetReapplyExcludeTypes(request.GetResetReapplyExcludeTypes(), request.GetResetReapplyType()),
		allowResetWithPendingChildren,
		resetRequest.ResetRequest.PostResetOperations,
	); err != nil {
		return nil, err
	}
	return &historyservice.ResetWorkflowExecutionResponse{
		RunId: resetRunID,
	}, nil
}

// GetResetReapplyExcludeTypes computes the set of requested exclude types. It
// uses the reset_reapply_exclude_types request field (a set of event types to
// exclude from reapply), as well as the deprecated reset_reapply_type request
// field (a specification of what to include).
func GetResetReapplyExcludeTypes(
	excludeTypes []enumspb.ResetReapplyExcludeType,
	includeType enumspb.ResetReapplyType,
) map[enumspb.ResetReapplyExcludeType]struct{} {
	// A client who wishes to have reapplication of all supported event types should omit the deprecated
	// reset_reapply_type field (since its default value is RESET_REAPPLY_TYPE_ALL_ELIGIBLE).
	exclude := map[enumspb.ResetReapplyExcludeType]struct{}{}
	switch includeType {
	case enumspb.RESET_REAPPLY_TYPE_SIGNAL:
		// A client sending this value of the deprecated reset_reapply_type field will not have any events other than
		// signal reapplied.
		exclude[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE] = struct{}{}
	case enumspb.RESET_REAPPLY_TYPE_NONE:
		exclude[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL] = struct{}{}
		exclude[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE] = struct{}{}
	case enumspb.RESET_REAPPLY_TYPE_UNSPECIFIED, enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE:
		// Do nothing.
	}
	for _, e := range excludeTypes {
		exclude[e] = struct{}{}
	}
	return exclude
}

// validatePostResetOperationInputs validates the optional post reset operation inputs.
func validatePostResetOperationInputs(ctx context.Context,
	postResetOperations []*workflowpb.PostResetOperation,
	matchingClient matchingservice.MatchingServiceClient,
	versionMembershipCache worker_versioning.VersionMembershipCache,
	taskQueue string,
	namespaceID string) error {
	for _, operation := range postResetOperations {
		switch op := operation.GetVariant().(type) {
		case *workflowpb.PostResetOperation_UpdateWorkflowOptions_:
			opts := op.UpdateWorkflowOptions.GetWorkflowExecutionOptions()
			if err := worker_versioning.ValidateVersioningOverride(ctx, opts.GetVersioningOverride(), matchingClient, versionMembershipCache, taskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, namespaceID); err != nil {
				return err
			}
		default:
			return serviceerror.NewInvalidArgumentf("unsupported post reset operation: %T", op)
		}
	}
	return nil
}
