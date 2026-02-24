package reapplyevents

import (
	"context"

	"github.com/google/uuid"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/ndc"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

func Invoke(
	ctx context.Context,
	namespaceUUID namespace.ID,
	workflowID string,
	runID string,
	reapplyEvents []*historypb.HistoryEvent,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	workflowResetter ndc.WorkflowResetter,
	eventsReapplier ndc.EventsReapplier,
) error {
	if shardContext.GetConfig().SkipReapplicationByNamespaceID(namespaceUUID) {
		return nil
	}

	namespaceEntry, err := api.GetActiveNamespace(shardContext, namespace.ID(namespaceUUID.String()), workflowID)
	if err != nil {
		return err
	}
	namespaceID := namespaceEntry.ID()
	isGlobalNamespace := namespaceEntry.IsGlobalNamespace()

	return api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			"",
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			context := workflowLease.GetContext()
			mutableState := workflowLease.GetMutableState()
			// Filter out reapply event from the same cluster
			toReapplyEvents := make([]*historypb.HistoryEvent, 0, len(reapplyEvents))

			clusterMetadata := shardContext.GetClusterMetadata()
			currentCluster := clusterMetadata.GetCurrentClusterName()

			for _, event := range reapplyEvents {
				if clusterMetadata.ClusterNameForFailoverVersion(
					isGlobalNamespace,
					event.GetVersion(),
				) == currentCluster {
					// The reapply is from the same cluster. Ignoring.
					continue
				}
				dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
				if mutableState.IsResourceDuplicated(dedupResource) {
					// already apply the signal
					continue
				}

				toReapplyEvents = append(toReapplyEvents, event)
			}
			if len(toReapplyEvents) == 0 {
				return &api.UpdateWorkflowAction{
					Noop:               true,
					CreateWorkflowTask: false,
				}, nil
			}

			if !mutableState.IsWorkflowExecutionRunning() {
				// need to reset target workflow (which is also the current workflow)
				// to accept events to be reapplied
				baseRunID := mutableState.GetExecutionState().GetRunId()
				resetRunID := uuid.New()
				baseRebuildLastEventID := mutableState.GetLastCompletedWorkflowTaskStartedEventId()

				// TODO when https://github.com/uber/cadence/issues/2420 is finished, remove this block,
				//  since cannot reapply event to a finished workflow which had no workflow tasks started
				if baseRebuildLastEventID == common.EmptyEventID {
					shardContext.GetLogger().Warn("cannot reapply event to a finished workflow with no workflow task",
						tag.WorkflowNamespaceID(namespaceID.String()),
						tag.WorkflowID(workflowID),
					)
					metrics.EventReapplySkippedCount.With(shardContext.GetMetricsHandler()).Record(
						1,
						metrics.OperationTag(metrics.HistoryReapplyEventsScope))
					return &api.UpdateWorkflowAction{
						Noop:               true,
						CreateWorkflowTask: false,
					}, nil
				}

				baseVersionHistories := mutableState.GetExecutionInfo().GetVersionHistories()
				baseCurrentVersionHistory, err := versionhistory.GetCurrentVersionHistory(baseVersionHistories)
				if err != nil {
					return nil, err
				}
				baseRebuildLastEventVersion, err := versionhistory.GetVersionHistoryEventVersion(baseCurrentVersionHistory, baseRebuildLastEventID)
				if err != nil {
					return nil, err
				}
				baseCurrentBranchToken := baseCurrentVersionHistory.GetBranchToken()
				baseNextEventID := mutableState.GetNextEventID()
				baseWorkflow := ndc.NewWorkflow(
					shardContext.GetClusterMetadata(),
					context,
					mutableState,
					wcache.NoopReleaseFn,
				)

				err = workflowResetter.ResetWorkflow(
					ctx,
					namespaceID,
					workflowID,
					baseRunID,
					baseCurrentBranchToken,
					baseRebuildLastEventID,
					baseRebuildLastEventVersion,
					baseNextEventID,
					resetRunID.String(),
					uuid.New().String(),
					baseWorkflow,
					baseWorkflow,
					ndc.EventsReapplicationResetWorkflowReason,
					toReapplyEvents,
					nil,
					false, // allowResetWithPendingChildren
					nil,
				)
				switch err.(type) {
				case *serviceerror.InvalidArgument:
					// no-op. Usually this is due to reset workflow with pending child workflows
					shardContext.GetLogger().Warn("Cannot reset workflow. Ignoring reapply events.", tag.Error(err))
				case nil:
					// no-op
				default:
					return nil, err
				}
				return &api.UpdateWorkflowAction{
					Noop:               true,
					CreateWorkflowTask: false,
				}, nil
			}

			reappliedEvents, err := eventsReapplier.ReapplyEvents(
				ctx,
				mutableState,
				context.UpdateRegistry(ctx),
				toReapplyEvents,
				runID,
			)
			if err != nil {
				shardContext.GetLogger().Error("failed to re-apply stale events", tag.Error(err))
				return nil, err
			}
			if len(reappliedEvents) == 0 {
				return &api.UpdateWorkflowAction{
					Noop:               true,
					CreateWorkflowTask: false,
				}, nil
			}
			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: false,
			}, nil
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)
}
