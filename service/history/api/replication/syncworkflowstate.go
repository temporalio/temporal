package replication

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
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
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (_ *historyservice.SyncWorkflowStateResponse, retError error) {
	workflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		definition.NewWorkflowKey(request.NamespaceId, request.Execution.WorkflowId, request.Execution.RunId),
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	defer func() { workflowLease.GetReleaseFn()(retError) }()
	mu, err := workflowLease.GetContext().LoadMutableState(ctx, shardContext)
	mu.CloneToProto()
	//mu := mutableState.GetExecutionInfo()
	if err = workflow.TransitionHistoryStalenessCheck(mu.GetExecutionInfo().TransitionHistory, request.VersionedTransition); err == nil {
		// decide sync or mutation
		tombstoneIncluded := false
		tombstoneBatch := mu.GetExecutionInfo().SubStateMachineTombstoneBatches
		for _, tombstone := range tombstoneBatch {
			if workflow.CompareVersionedTransition(tombstone.VersionedTransition, request.VersionedTransition) < 0 { // tombstone is older than the request
				tombstoneIncluded = true
				break
			}
		}
		if tombstoneIncluded { // return mutation
			rootNode := mu.HSM()
			updateStateMachine, err := getUpdatedSubStateMachine(rootNode, request.VersionedTransition)
			if err != nil {
				return nil, err
			}
			deletedActivities := []int64{}
			deletedTimers := []string{}
			deletedChildExecutionInfos := []int64{}
			deletedRequestCancelInfos := []int64{}
			deletedSingalInfos := []int64{}
			deletedUpdateInfos := []string{}
			deletedSubStateMachine := []*persistencepb.StateMachinePath{}
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
					deletedSingalInfos = append(deletedSingalInfos, x.SignalExternalInitiatedEventId)
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
				DeletedSignalInfos:         deletedSingalInfos,
				DeletedUpdateInfos:         deletedUpdateInfos,
				DeletedSubStateMachines:    deletedSubStateMachine,
				SignalRequestedIds:         convert.StringSetToSlice(mu.pending),
				ExecutionInfo:              mu.ExecutionInfo,
			}
		}
	}
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
