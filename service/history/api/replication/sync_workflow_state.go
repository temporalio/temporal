package replication

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	replicationpb "go.temporal.io/server/api/replication/v1"
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
	result, err := syncStateRetriever.GetSyncWorkflowStateArtifact(ctx, request.GetNamespaceId(), request.Execution, request.VersionedTransition, request.VersionHistories)
	response := &historyservice.SyncWorkflowStateResponse{}
	switch result.Type {
	case replication.Mutation:
		if result.Mutation == nil {
			return nil, serviceerror.NewInvalidArgument("SyncWorkflowState failed to retrieve mutation")
		}
		response.Attributes = &historyservice.SyncWorkflowStateResponse_Mutation{
			Mutation: &replicationpb.SyncWorkflowStateMutationAttributes{
				StateMutation:                     result.Mutation,
				ExclusiveStartVersionedTransition: request.VersionedTransition,
			},
		}
	case replication.Snapshot:
		if result.Snapshot == nil {
			return nil, serviceerror.NewInvalidArgument("SyncWorkflowState failed to retrieve snapshot")
		}
		response.Attributes = &historyservice.SyncWorkflowStateResponse_Snapshot{
			Snapshot: &replicationpb.SyncWorkflowStateSnapshotAttributes{
				State: result.Snapshot,
			},
		}
	default:
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Unknown sync state artifact type: %v", result.Type))
	}
	if result.LastVersionHistory != nil && result.VersionedTransitionHistory != nil {
		err = replicationProgressCache.Update(request.Execution.RunId, request.TargetClusterId, result.VersionedTransitionHistory, result.LastVersionHistory.Items)
		if err != nil {
			logger.Error("SyncWorkflowState failed to update progress cache",
				tag.WorkflowNamespaceID(request.NamespaceId),
				tag.WorkflowID(request.Execution.WorkflowId),
				tag.WorkflowRunID(request.Execution.RunId),
				tag.Error(err))
		}
	}
	return response, nil
}
