package replication

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/history/replication"
)

func SyncWorkflowState(
	ctx context.Context,
	request *historyservice.SyncWorkflowStateRequest,
	replicationProgressCache replication.ProgressCache,
	syncStateRetriever replication.SyncStateRetriever,
	logger log.Logger,
) (_ *historyservice.SyncWorkflowStateResponse, retError error) {
	archetypeID := request.GetArchetypeId()
	if archetypeID == chasm.UnspecifiedArchetypeID {
		archetypeID = chasm.WorkflowArchetypeID
	}
	result, err := syncStateRetriever.GetSyncWorkflowStateArtifact(
		ctx,
		request.GetNamespaceId(),
		request.GetExecution(),
		archetypeID,
		request.GetVersionedTransition(),
		request.GetVersionHistories(),
	)
	if err != nil {
		logger.Error("SyncWorkflowState failed to retrieve sync state artifact", tag.WorkflowNamespaceID(request.GetNamespaceId()),
			tag.WorkflowID(request.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(request.GetExecution().GetRunId()),
			tag.Error(err))
		return nil, err
	}

	err = replicationProgressCache.Update(request.GetExecution().GetRunId(), request.GetTargetClusterId(), result.VersionedTransitionHistory, result.SyncedVersionHistory.GetItems())
	if err != nil {
		logger.Error("SyncWorkflowState failed to update progress cache",
			tag.WorkflowNamespaceID(request.GetNamespaceId()),
			tag.WorkflowID(request.GetExecution().GetWorkflowId()),
			tag.WorkflowRunID(request.GetExecution().GetRunId()),
			tag.Error(err))
	}

	return historyservice.SyncWorkflowStateResponse_builder{
		VersionedTransitionArtifact: result.VersionedTransitionArtifact,
	}.Build(), nil
}
