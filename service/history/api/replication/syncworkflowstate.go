package replication

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	replicationpb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const (
	defaultPageSize = 32
)

type (
	LastUpdatedStateTransitionGetter interface {
		GetLastUpdateVersionedTransition() *persistencepb.VersionedTransition
	}
)

func SyncWorkflowState(
	ctx context.Context,
	shardContext shard.Context,
	request *historyservice.SyncWorkflowStateRequest,
	workflowCache wcache.Cache,
	logger log.Logger,
) (_ *historyservice.SyncWorkflowStateResponse, retError error) {
	wfCtx, releaseFunc, err := workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		shardContext,
		namespace.ID(request.GetNamespaceId()),
		request.Execution,
		locks.PriorityLow,
	)
	defer func() {
		if releaseFunc != nil {
			releaseFunc(retError)
		}
	}()

	if err != nil {
		return nil, err
	}
	mu, err := wfCtx.LoadMutableState(ctx, shardContext)
	if err != nil {
		return nil, err
	}
	response := &historyservice.SyncWorkflowStateResponse{}
	isSameBranch := workflow.TransitionHistoryStalenessCheck(mu.GetExecutionInfo().TransitionHistory, request.VersionedTransition) == nil
	tombstoneBatch := mu.GetExecutionInfo().SubStateMachineTombstoneBatches
	if isSameBranch &&
		len(tombstoneBatch) > 0 && workflow.CompareVersionedTransition(tombstoneBatch[0].VersionedTransition, request.VersionedTransition) < 0 {
		mutation, err := getMutation(mu, request.VersionedTransition)
		if err != nil {
			return nil, err
		}
		response.Attributes = &historyservice.SyncWorkflowStateResponse_Mutation{
			Mutation: &replicationpb.SyncWorkflowStateMutationAttributes{
				StateMutation:                     mutation,
				InclusiveStartVersionedTransition: request.VersionedTransition,
			},
		}
	} else {
		snapshot, err := getSnapshot(mu)
		if err != nil {
			return nil, err
		}
		response.Attributes = &historyservice.SyncWorkflowStateResponse_State{
			State: snapshot,
		}
	}

	newRunId := mu.GetExecutionInfo().NewExecutionRunId

	releaseFunc(nil)
	releaseFunc = nil

	if len(newRunId) > 0 {
		newRunInfo, err := getNewRunInfo(ctx, shardContext, workflowCache, namespace.ID(request.GetNamespaceId()), request.Execution.GetWorkflowId(), newRunId, logger)
		if err != nil {
			return nil, err
		}
		response.NewRunInfo = newRunInfo
	}

	events, err := getSyncStateEvents(ctx, shardContext, request.VersionHistories, mu.GetExecutionInfo().GetVersionHistories())
	if err != nil {
		return nil, err
	}
	response.EventBatches = events

	return response, nil
}

func getNewRunInfo(ctx context.Context, shardContext shard.Context, workflowCache wcache.Cache, namespaceId namespace.ID, workflowId string, runId string, logger log.Logger) (_ *replicationpb.NewRunInfo, retError error) {
	wfCtx, releaseFunc, err := workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		shardContext,
		namespaceId,
		&commonpb.WorkflowExecution{
			WorkflowId: workflowId,
			RunId:      runId,
		},
		locks.PriorityLow,
	)
	defer func() {
		if releaseFunc != nil {
			releaseFunc(retError)
		}
	}()

	if err != nil {
		return nil, err
	}
	mu, err := wfCtx.LoadMutableState(ctx, shardContext)
	switch err.(type) {
	case nil:
	case *serviceerror.NotFound:
		logger.Info("new run not found", tag.WorkflowNamespaceID(namespaceId.String()), tag.WorkflowID(workflowId), tag.WorkflowRunID(runId))
		return nil, nil
	default:
		return nil, err
	}
	versionHistory, err := versionhistory.GetCurrentVersionHistory(mu.GetExecutionInfo().VersionHistories)
	if err != nil {
		return nil, err
	}
	releaseFunc(nil)
	releaseFunc = nil
	newRunEvents, err := getEventsBlob(ctx, shardContext, versionHistory.BranchToken, common.FirstEventID, common.FirstEventID+1)
	switch err.(type) {
	case nil:
	case *serviceerror.NotFound:
		logger.Info("new run event not found", tag.WorkflowNamespaceID(namespaceId.String()), tag.WorkflowID(workflowId), tag.WorkflowRunID(runId))
		return nil, nil
	default:
		return nil, err
	}
	if len(newRunEvents) == 0 {
		logger.Info("new run event is empty", tag.WorkflowNamespaceID(namespaceId.String()), tag.WorkflowID(workflowId), tag.WorkflowRunID(runId))
		return nil, nil
	}
	return &replicationpb.NewRunInfo{
		RunId:      runId,
		EventBatch: newRunEvents[0],
	}, nil
}

func getMutation(mutableState workflow.MutableState, versionedTransition *persistencepb.VersionedTransition) (*persistencepb.WorkflowMutableStateMutation, error) {
	rootNode := mutableState.HSM()
	updatedStateMachine, err := getUpdatedSubStateMachine(rootNode, versionedTransition)
	if err != nil {
		return nil, err
	}
	tombstoneBatch := mutableState.GetExecutionInfo().SubStateMachineTombstoneBatches
	var tombstones []*persistencepb.StateMachineTombstoneBatch
	for i, tombstone := range tombstoneBatch {
		if workflow.CompareVersionedTransition(tombstone.VersionedTransition, versionedTransition) >= 0 {
			tombstones = tombstoneBatch[i:]
			break
		}
	}
	executionInfo := mutableState.GetExecutionInfo()
	executionInfo.UpdateInfos = nil
	executionInfo.SubStateMachinesByType = nil
	return &persistencepb.WorkflowMutableStateMutation{
		UpdatedActivityInfos:            getUpdatedInfo(mutableState.GetPendingActivityInfos(), versionedTransition),
		UpdatedTimerInfos:               getUpdatedInfo(mutableState.GetPendingTimerInfos(), versionedTransition),
		UpdatedChildExecutionInfos:      getUpdatedInfo(mutableState.GetPendingChildExecutionInfos(), versionedTransition),
		UpdatedRequestCancelInfos:       getUpdatedInfo(mutableState.GetPendingRequestCancelExternalInfos(), versionedTransition),
		UpdatedSignalInfos:              getUpdatedInfo(mutableState.GetPendingSignalExternalInfos(), versionedTransition),
		UpdatedUpdateInfos:              getUpdatedInfo(mutableState.GetExecutionInfo().UpdateInfos, versionedTransition),
		UpdatedSubStateMachines:         updatedStateMachine,
		SubStateMachineTombstoneBatches: tombstones,
		SignalRequestedIds:              mutableState.GetPendingSignalRequestedIds(),
		ExecutionInfo:                   executionInfo,
	}, nil
}

func getSnapshot(mutableState workflow.MutableState) (*replicationpb.SyncWorkflowStateSnapshotAttributes, error) {
	mutableStateProto := mutableState.CloneToProto()
	err := workflow.SanitizeMutableState(mutableStateProto)
	if err != nil {
		return nil, err
	}
	return &replicationpb.SyncWorkflowStateSnapshotAttributes{
		State: mutableStateProto,
	}, nil
}

func getEventsBlob(ctx context.Context, shardContext shard.Context, branchToken []byte, startEventId int64, endEventId int64) ([]*commonpb.DataBlob, error) {
	rawHistoryResponse, err := shardContext.GetExecutionManager().ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		BranchToken: branchToken,
		MinEventID:  startEventId,
		MaxEventID:  endEventId,
		PageSize:    defaultPageSize,
		ShardID:     shardContext.GetShardID(),
	})
	if err != nil {
		return nil, err
	}
	return rawHistoryResponse.HistoryEventBlobs, nil
}

func getSyncStateEvents(ctx context.Context, shardContext shard.Context, targetVersionHistories *history.VersionHistories, sourceVersionHistories *history.VersionHistories) ([]*commonpb.DataBlob, error) {
	startEventId := common.EndEventID
	sourceHistory, err := versionhistory.GetCurrentVersionHistory(sourceVersionHistories)
	if err != nil {
		return nil, err
	}
	lcaItem, _, err := versionhistory.FindLCAVersionHistoryItemAndIndex(targetVersionHistories, sourceHistory)
	if err != nil {
		return nil, err
	}
	startEventId = lcaItem.GetEventId() + 1
	sourceLastItem, err := versionhistory.GetLastVersionHistoryItem(sourceHistory)
	if err != nil {
		return nil, err
	}
	return getEventsBlob(ctx, shardContext, sourceHistory.BranchToken, startEventId, sourceLastItem.GetEventId()+1)
}

func isInfoUpdated(subStateMachine LastUpdatedStateTransitionGetter, versionedTransition *persistencepb.VersionedTransition) bool {
	if subStateMachine == nil {
		return false
	}
	lastUpdate := subStateMachine.GetLastUpdateVersionedTransition()
	return workflow.CompareVersionedTransition(lastUpdate, versionedTransition) > 0
}

func getUpdatedInfo[K comparable, V LastUpdatedStateTransitionGetter](subStateMachine map[K]V, versionedTransition *persistencepb.VersionedTransition) map[K]V {
	result := make(map[K]V)
	for k, v := range subStateMachine {
		if isInfoUpdated(v, versionedTransition) {
			result[k] = v
		}
	}
	return result
}

func getUpdatedSubStateMachine(n *hsm.Node, versionedTransition *persistencepb.VersionedTransition) ([]*persistencepb.WorkflowMutableStateMutation_StateMachineNodeMutation, error) {
	var updatedStateMachines []*persistencepb.WorkflowMutableStateMutation_StateMachineNodeMutation
	walkFn := func(node *hsm.Node) error {
		if node == nil {
			return serviceerror.NewInvalidArgument("Nil node is not expected")
		}
		if node.Parent == nil {
			return nil
		}
		convertKey := func(ori []hsm.Key) *persistencepb.StateMachinePath {
			var path []*persistencepb.StateMachineKey
			for _, k := range ori {
				path = append(path, &persistencepb.StateMachineKey{
					Type: k.Type,
					Id:   k.ID,
				})
			}
			return &persistencepb.StateMachinePath{
				Path: path,
			}
		}
		if isInfoUpdated(node.InternalRepr(), versionedTransition) {
			subStateMachine := node.InternalRepr()
			workflow.SanitizeStateMachineNode(subStateMachine)
			updatedStateMachines = append(updatedStateMachines, &persistencepb.WorkflowMutableStateMutation_StateMachineNodeMutation{
				Path:                          convertKey(node.Path()),
				Data:                          subStateMachine.Data,
				InitialVersionedTransition:    subStateMachine.InitialVersionedTransition,
				LastUpdateVersionedTransition: subStateMachine.LastUpdateVersionedTransition,
			})
		}
		return nil
	}
	err := n.Walk(walkFn)
	if err != nil {
		return nil, err
	}
	return updatedStateMachines, nil
}
