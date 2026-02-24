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
		request.Execution,
		archetypeID,
		request.VersionedTransition,
		request.VersionHistories,
	)
	if err != nil {
		logger.Error("SyncWorkflowState failed to retrieve sync state artifact", tag.WorkflowNamespaceID(request.NamespaceId),
			tag.WorkflowID(request.Execution.WorkflowId),
			tag.WorkflowRunID(request.Execution.RunId),
			tag.Error(err))
		return nil, err
	}

	err = replicationProgressCache.Update(request.Execution.RunId, request.TargetClusterId, result.VersionedTransitionHistory, result.SyncedVersionHistory.Items)
	if err != nil {
		logger.Error("SyncWorkflowState failed to update progress cache",
			tag.WorkflowNamespaceID(request.NamespaceId),
			tag.WorkflowID(request.Execution.WorkflowId),
			tag.WorkflowRunID(request.Execution.RunId),
			tag.Error(err))
	}

	return &historyservice.SyncWorkflowStateResponse{
		VersionedTransitionArtifact: result.VersionedTransitionArtifact,
	}, nil
}
