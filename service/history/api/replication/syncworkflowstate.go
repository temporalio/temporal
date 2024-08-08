package replication

import (
	"context"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	replicationpb "go.temporal.io/server/api/replication/v1"
	common2 "go.temporal.io/server/common"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
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
) (_ *historyservice.SyncWorkflowStateResponse, retError error) {
	mu, err := getMutableState(ctx, shardContext, namespace.ID(request.GetNamespaceId()), request.Execution, workflowCache)
	if err != nil {
		return nil, err
	}
	isSameBranch := workflow.TransitionHistoryStalenessCheck(mu.GetExecutionInfo().TransitionHistory, request.VersionedTransition) == nil
	isTombstoneIncluded := false
	tombstoneBatch := mu.GetExecutionInfo().SubStateMachineTombstoneBatches
	for _, tombstone := range tombstoneBatch {
		if workflow.CompareVersionedTransition(tombstone.VersionedTransition, request.VersionedTransition) < 0 { // tombstone is older than the request
			isTombstoneIncluded = true
			break
		}
	}
	if isSameBranch && isTombstoneIncluded { // return mutation
		rootNode := mu.HSM()
		updateStateMachine, err := getUpdatedSubStateMachine(rootNode, request.VersionedTransition)
		if err != nil {
			return nil, err
		}
		var deletedActivities, deletedChildExecutionInfos, deletedRequestCancelInfos, deletedSignalInfos []int64
		var deletedTimers, deletedUpdateInfos []string

		var deletedSubStateMachine []*persistencepb.StateMachinePath
		addTombstone := func(tombstone *persistencepb.StateMachineTombstone) error {
			switch x := tombstone.StateMachineKey.(type) {
			case *persistencepb.StateMachineTombstone_ActivityScheduledEventId:
				deletedActivities = append(deletedActivities, x.ActivityScheduledEventId)
			case *persistencepb.StateMachineTombstone_TimerId:
				deletedTimers = append(deletedTimers, x.TimerId)
			case *persistencepb.StateMachineTombstone_ChildExecutionInitiatedEventId:
				deletedChildExecutionInfos = append(deletedChildExecutionInfos, x.ChildExecutionInitiatedEventId)
			case *persistencepb.StateMachineTombstone_RequestCancelInitiatedEventId:
				deletedRequestCancelInfos = append(deletedRequestCancelInfos, x.RequestCancelInitiatedEventId)
			case *persistencepb.StateMachineTombstone_SignalExternalInitiatedEventId:
				deletedSignalInfos = append(deletedSignalInfos, x.SignalExternalInitiatedEventId)
			case *persistencepb.StateMachineTombstone_UpdateId:
				deletedUpdateInfos = append(deletedUpdateInfos, x.UpdateId)
			case *persistencepb.StateMachineTombstone_StateMachinePath:
				deletedSubStateMachine = append(deletedSubStateMachine, x.StateMachinePath)
			default:
				return serviceerror.NewInvalidArgument("unknown tombstone type")
			}
			return nil
		}
		for _, tombstone := range tombstoneBatch {
			for _, t := range tombstone.StateMachineTombstones {
				if err := addTombstone(t); err != nil {
					return nil, err
				}
			}
		}
		executionInfo := mu.GetExecutionInfo()
		executionInfo.UpdateInfos = nil
		executionInfo.SubStateMachinesByType = nil
		mutation := &persistencepb.WorkflowMutableStateMutation{
			UpdatedActivityInfos:       getUpdatedInfo(mu.GetPendingActivityInfos(), request.VersionedTransition),
			UpdatedTimerInfos:          getUpdatedInfo(mu.GetPendingTimerInfos(), request.VersionedTransition),
			UpdatedChildExecutionInfos: getUpdatedInfo(mu.GetPendingChildExecutionInfos(), request.VersionedTransition),
			UpdatedRequestCancelInfos:  getUpdatedInfo(mu.GetPendingRequestCancelExternalInfos(), request.VersionedTransition),
			UpdatedSignalInfos:         getUpdatedInfo(mu.GetPendingSignalExternalInfos(), request.VersionedTransition),
			UpdatedUpdateInfos:         getUpdatedInfo(mu.GetExecutionInfo().UpdateInfos, request.VersionedTransition),
			UpdatedSubStateMachines:    updateStateMachine,
			DeletedActivities:          deletedActivities,
			DeletedTimers:              deletedTimers,
			DeletedChildExecutionInfos: deletedChildExecutionInfos,
			DeletedRequestCancelInfos:  deletedRequestCancelInfos,
			DeletedSignalInfos:         deletedSignalInfos,
			DeletedUpdateInfos:         deletedUpdateInfos,
			DeletedSubStateMachines:    deletedSubStateMachine,
			SignalRequestedIds:         mu.GetPendingSignalRequestedIds(),
			ExecutionInfo:              executionInfo,
		}

		events, err := getSyncStateEvents(ctx, shardContext, request.VersionHistories, mu.GetExecutionInfo().GetVersionHistories(), true)
		if err != nil {
			return nil, err
		}
		newRunInfo, err := getNewRunInfo(ctx, shardContext, workflowCache, mu)
		if err != nil {
			return nil, err
		}
		return &historyservice.SyncWorkflowStateResponse{
			EventBatches: events,
			Attributes: &historyservice.SyncWorkflowStateResponse_Mutation{
				Mutation: &replicationpb.SyncWorkflowStateMutationAttributes{
					StateMutation:                     mutation,
					InclusiveStartVersionedTransition: request.VersionedTransition,
				},
			},
			NewRunInfo: newRunInfo,
		}, nil
	}

	events, err := getSyncStateEvents(ctx, shardContext, request.VersionHistories, mu.GetExecutionInfo().GetVersionHistories(), isSameBranch)
	if err != nil {
		return nil, err
	}

	newRunInfo, err := getNewRunInfo(ctx, shardContext, workflowCache, mu)
	if err != nil {
		return nil, err
	}

	return &historyservice.SyncWorkflowStateResponse{
		EventBatches: events,
		Attributes: &historyservice.SyncWorkflowStateResponse_State{
			State: &replicationpb.SyncWorkflowStateSnapshotAttributes{
				State: mu.CloneToProto(),
			},
		},
		NewRunInfo: newRunInfo,
	}, nil
}

func getNewRunInfo(ctx context.Context, shardContext shard.Context, workflowCache wcache.Cache, mutableState workflow.MutableState) (*replicationpb.NewRunInfo, error) {
	newRunId := mutableState.GetExecutionInfo().GetNewExecutionRunId()
	if len(newRunId) == 0 {
		return nil, nil
	}
	mu, err := getMutableState(ctx, shardContext, namespace.ID(mutableState.GetExecutionInfo().NamespaceId), &common.WorkflowExecution{
		WorkflowId: mutableState.GetExecutionInfo().WorkflowId,
		RunId:      newRunId,
	}, workflowCache)
	if err != nil {
		return nil, err
	}
	versionHistory, err := versionhistory.GetCurrentVersionHistory(mu.GetExecutionInfo().VersionHistories)
	if err != nil {
		return nil, err
	}
	eventsBlob, err := getEventsBlob(ctx, shardContext, versionHistory.BranchToken, common2.FirstEventID, common2.FirstEventID+1)
	if err != nil {
		return nil, err
	}
	if len(eventsBlob) == 0 {
		return nil, serviceerror.NewInvalidArgument("no events found")
	}
	return &replicationpb.NewRunInfo{
		RunId:      newRunId,
		EventBatch: eventsBlob[0], // we only send first batch for new run
	}, nil
}

func getMutableState(ctx context.Context, shardContext shard.Context, namespaceID namespace.ID, execution *common.WorkflowExecution, workflowCache wcache.Cache) (_ workflow.MutableState, retError error) {
	wfCtx, releaseFunc, err := workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		shardContext,
		namespaceID,
		execution,
		locks.PriorityLow,
	)
	defer releaseFunc(retError)
	if err != nil {
		return nil, err
	}
	return wfCtx.LoadMutableState(ctx, shardContext)
}

func getEventsBlob(ctx context.Context, shardContext shard.Context, branchToken []byte, startEventId int64, endEventId int64) ([]*common.DataBlob, error) {
	rawHistoryResponse, err := shardContext.GetExecutionManager().ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		BranchToken: branchToken,
		MinEventID:  startEventId,
		MaxEventID:  endEventId,
		PageSize:    100,
		ShardID:     shardContext.GetShardID(),
	})
	if err != nil {
		return nil, err
	}
	return rawHistoryResponse.HistoryEventBlobs, nil
}

func getSyncStateEvents(ctx context.Context, shardContext shard.Context, targetVersionHistories *history.VersionHistories, sourceVersionHistories *history.VersionHistories, isSameBranch bool) ([]*common.DataBlob, error) {
	startEventId := common2.EndEventID
	sourceHistory, err := versionhistory.GetCurrentVersionHistory(sourceVersionHistories)
	if err != nil {
		return nil, err
	}
	if isSameBranch {
		targetHistory, err := versionhistory.GetCurrentVersionHistory(targetVersionHistories)
		result, err := versionhistory.CompareVersionHistory(targetHistory, sourceHistory)
		if err != nil {
			return nil, err
		}
		if result > 0 {
			return nil, serviceerror.NewInvalidArgument("target version history greater than source version history")
		}
		if result == 0 {
			return nil, nil
		}
		targetLastItem, err := versionhistory.GetLastVersionHistoryItem(targetHistory)
		if err != nil {
			return nil, err
		}
		startEventId = targetLastItem.GetEventId() + 1
	} else {
		lcaItem, _, err := versionhistory.FindLCAVersionHistoryItemAndIndex(targetVersionHistories, sourceHistory)
		if err != nil {
			return nil, err
		}
		startEventId = lcaItem.GetEventId() + 1
	}

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
		if isInfoUpdated(node.GetStateMachineNode(), versionedTransition) {
			updatedStateMachines = append(updatedStateMachines, &persistencepb.WorkflowMutableStateMutation_StateMachineNodeMutation{
				Path:                          convertKey(node.Path()),
				Data:                          node.GetStateMachineNode().Data,
				InitialVersionedTransition:    node.GetStateMachineNode().InitialVersionedTransition,
				LastUpdateVersionedTransition: node.GetStateMachineNode().LastUpdateVersionedTransition,
			})
		}
		return nil
	}
	childNodes := n.GetChildNodes()
	for _, child := range childNodes {
		if err := walkFn(child); err != nil {
			return nil, err
		}
	}
	return updatedStateMachines, nil
}
