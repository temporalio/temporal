package eventhandler

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/shard"
)

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination history_events_handler_mock.go

// Local vs Remote
// Local and Remote are introduced to handle the case:
//
// wf_1 was originally started in cluster_A, and was migrated to cluster_B.
// Then wf_1 data was deleted in cluster_A. Then we want to migrate the wf_1 data back to cluster_A.
// in this case, local events are the event 1 to the last events that are generated in cluster_A (event_version % failover_incremental == cluster_A's initial_failover_version)
// remote events are events thereafter.
// For local events, we should use ImportWorkflowExecution API to process and for remote events, we should use ReplicateHistoryEvents API to process.
type (
	// HistoryEventsHandler is to handle all cases that add history events from remote cluster to current cluster.
	// so it can be used by:
	// 1. ExecutableHistoryTask to replicate events
	// 2. When HistoryResender trying to add events (currently triggered by RetryReplication error, which still need to handle import case)
	HistoryEventsHandler interface {
		HandleHistoryEvents(
			ctx context.Context,
			sourceClusterName string,
			workflowKey definition.WorkflowKey,
			baseExecutionInfo *workflowspb.BaseExecutionInfo,
			versionHistoryItems []*historyspb.VersionHistoryItem,
			historyEvents [][]*historypb.HistoryEvent,
			newEvents []*historypb.HistoryEvent,
			newRunID string,
		) error
	}

	historyEventsHandlerImpl struct {
		clusterMetadata cluster.Metadata
		eventImporter   EventImporter
		shardController shard.Controller
		logger          log.Logger
	}
)

func NewHistoryEventsHandler(
	clusterMetadata cluster.Metadata,
	eventImporter EventImporter,
	shardController shard.Controller,
	logger log.Logger,
) HistoryEventsHandler {
	return &historyEventsHandlerImpl{
		clusterMetadata: clusterMetadata,
		eventImporter:   eventImporter,
		shardController: shardController,
		logger:          logger,
	}
}

func (h *historyEventsHandlerImpl) HandleHistoryEvents(
	ctx context.Context,
	sourceClusterName string,
	workflowKey definition.WorkflowKey,
	baseExecutionInfo *workflowspb.BaseExecutionInfo,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	historyEvents [][]*historypb.HistoryEvent,
	newEvents []*historypb.HistoryEvent,
	newRunID string,
) error {
	if len(historyEvents) == 0 {
		return serviceerror.NewInvalidArgument("Empty batches")
	}
	localEvents, remoteEvents, err := h.splitBatchesToLocalAndRemote(historyEvents, versionHistoryItems)
	if err != nil {
		return err
	}

	if len(localEvents) != 0 {
		if err := h.handleLocalGeneratedEvent(
			ctx,
			sourceClusterName,
			workflowKey,
			versionHistoryItems,
		); err != nil {
			return err
		}
	}
	if len(remoteEvents) != 0 {
		if err := h.handleRemoteGeneratedHistoryEvents(
			ctx,
			workflowKey,
			baseExecutionInfo,
			versionHistoryItems,
			remoteEvents,
			newEvents,
			newRunID,
		); err != nil {
			return err
		}
	}
	return nil
}

func (h *historyEventsHandlerImpl) splitBatchesToLocalAndRemote(
	eventsBatches [][]*historypb.HistoryEvent,
	versionHistoryItems []*historyspb.VersionHistoryItem,
) (local [][]*historypb.HistoryEvent, remote [][]*historypb.HistoryEvent, err error) {
	for _, batch := range eventsBatches {
		if len(batch) == 0 {
			return nil, nil, serviceerror.NewInvalidArgument("Empty batch")
		}
	}
	localVersionHistory, _ := versionhistory.SplitVersionHistoryByLastLocalGeneratedItem(versionHistoryItems, h.clusterMetadata.GetClusterID(), h.clusterMetadata.GetFailoverVersionIncrement())
	if len(localVersionHistory) == 0 {
		return nil, eventsBatches, nil
	}
	lastLocalEventId := localVersionHistory[len(localVersionHistory)-1].EventId
	firstBatch := eventsBatches[0]
	lastBatch := eventsBatches[len(eventsBatches)-1]

	if lastBatch[len(lastBatch)-1].EventId <= lastLocalEventId {
		return eventsBatches, nil, nil
	}
	if firstBatch[0].EventId > lastLocalEventId {
		return nil, eventsBatches, nil
	}
	lastLocalBatchIndex := -1
	for index, batch := range eventsBatches {
		if batch[len(batch)-1].EventId == lastLocalEventId {
			lastLocalBatchIndex = index
			break
		}
	}
	if lastLocalBatchIndex == -1 {
		return nil, nil, serviceerror.NewInternal("No boundary events found") // if this happens, means the events are not consecutive and we have bug somewhere
	}
	return eventsBatches[:lastLocalBatchIndex+1], eventsBatches[lastLocalBatchIndex+1:], nil
}

func (h *historyEventsHandlerImpl) handleLocalGeneratedEvent(
	ctx context.Context,
	sourceClusterName string,
	workflowKey definition.WorkflowKey,
	versionHistoryItems []*historyspb.VersionHistoryItem,
) error {
	if len(versionHistoryItems) == 0 {
		return serviceerror.NewInvalidArgument("local generated version history items is empty")
	}
	localVersionHistory, _ := versionhistory.SplitVersionHistoryByLastLocalGeneratedItem(versionHistoryItems, h.clusterMetadata.GetClusterID(), h.clusterMetadata.GetFailoverVersionIncrement())
	lastVersionHistoryItem := localVersionHistory[len(localVersionHistory)-1]
	shardContext, err := h.shardController.GetShardByNamespaceWorkflow(namespace.ID(workflowKey.NamespaceID), workflowKey.WorkflowID)
	if err != nil {
		return err
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}
	mu, err := engine.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId: workflowKey.NamespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
	})

	switch err.(type) {
	case nil:
		_, err = versionhistory.FindFirstVersionHistoryIndexByVersionHistoryItem(mu.GetVersionHistories(), lastVersionHistoryItem)
		// if mutable state is found, we expect it should have at least events to the last local generated event, otherwise it is a data lose
		if err != nil {
			return serviceerror.NewInvalidArgumentf("Encountered data lose issue when handling local generated events, expected event: %v, version : %v", lastVersionHistoryItem.EventId, lastVersionHistoryItem.Version)
		}
		return nil
	case *serviceerror.NotFound:
		// if mutable state not found, we import from beginning
		return h.eventImporter.ImportHistoryEventsFromBeginning(
			ctx,
			sourceClusterName,
			workflowKey,
			lastVersionHistoryItem.EventId,
			lastVersionHistoryItem.Version,
		)
	default:
		return err
	}
}

func (h *historyEventsHandlerImpl) handleRemoteGeneratedHistoryEvents(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	baseExecutionInfo *workflowspb.BaseExecutionInfo,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	historyEvents [][]*historypb.HistoryEvent,
	newEvents []*historypb.HistoryEvent,
	newRunID string,
) error {
	shardContext, err := h.shardController.GetShardByNamespaceWorkflow(
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
	)
	if err != nil {
		return err
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}
	return engine.ReplicateHistoryEvents(
		ctx,
		workflowKey,
		baseExecutionInfo,
		versionHistoryItems,
		historyEvents,
		newEvents,
		newRunID,
	)
}
