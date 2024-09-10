// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination sync_state_retriever_mock.go

package replication

import (
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/history/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	replicationpb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const (
	Mutation ResultType = iota
	Snapshot
	defaultPageSize = 32
)

type (
	ResultType      int
	SyncStateResult struct {
		Type                       ResultType
		Mutation                   *persistencepb.WorkflowMutableStateMutation
		Snapshot                   *persistencepb.WorkflowMutableState
		EventBlobs                 []*commonpb.DataBlob
		NewRunInfo                 *replicationpb.NewRunInfo
		VersionedTransitionHistory []*persistencepb.VersionedTransition
		LastVersionHistory         *history.VersionHistory
	}
	SyncStateRetriever interface {
		GetSyncWorkflowStateArtifact(
			ctx context.Context,
			namespaceID string,
			execution *commonpb.WorkflowExecution,
			versionedTransition *persistencepb.VersionedTransition,
			versionHistories *history.VersionHistories,
		) (*SyncStateResult, error)
	}

	SyncStateRetrieverImpl struct {
		shardContext  shard.Context
		workflowCache wcache.Cache
		logger        log.Logger
	}
	lastUpdatedStateTransitionGetter interface {
		GetLastUpdateVersionedTransition() *persistencepb.VersionedTransition
	}
)

func NewSyncStateRetriever(
	shardContext shard.Context,
	workflowCache wcache.Cache,
	logger log.Logger,
) *SyncStateRetrieverImpl {
	return &SyncStateRetrieverImpl{
		shardContext:  shardContext,
		workflowCache: workflowCache,
		logger:        logger,
	}
}

func (s *SyncStateRetrieverImpl) GetSyncWorkflowStateArtifact(
	ctx context.Context,
	namespaceID string,
	execution *commonpb.WorkflowExecution,
	versionedTransition *persistencepb.VersionedTransition,
	versionHistories *history.VersionHistories,
) (_ *SyncStateResult, retError error) {
	wfCtx, releaseFunc, err := s.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		s.shardContext,
		namespace.ID(namespaceID),
		execution,
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
	mu, err := wfCtx.LoadMutableState(ctx, s.shardContext)
	if err != nil {
		return nil, err
	}
	isSameBranch := false
	if versionedTransition != nil {
		err = workflow.TransitionHistoryStalenessCheck(mu.GetExecutionInfo().TransitionHistory, versionedTransition)
		switch {
		case err == nil:
			isSameBranch = true
		case errors.Is(err, consts.ErrStaleState):
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(
				"stale version for workflow, request version transition: %v, mutable state transition history: %v",
				versionedTransition,
				mu.GetExecutionInfo().TransitionHistory,
			))
		default:
		}
	}

	result := &SyncStateResult{}
	tombstoneBatch := mu.GetExecutionInfo().SubStateMachineTombstoneBatches
	if isSameBranch &&
		(len(tombstoneBatch) == 0 ||
			(workflow.CompareVersionedTransition(tombstoneBatch[0].VersionedTransition, versionedTransition) <= 0 ||
				versionedTransition.TransitionCount+1 == tombstoneBatch[0].VersionedTransition.TransitionCount)) {
		mutation, err := s.getMutation(mu, versionedTransition)
		if err != nil {
			return nil, err
		}
		result.Type = Mutation
		result.Mutation = mutation
	} else {
		snapshot, err := s.getSnapshot(mu)
		if err != nil {
			return nil, err
		}
		result.Type = Snapshot
		result.Snapshot = snapshot
	}

	newRunId := mu.GetExecutionInfo().NewExecutionRunId
	sourceVersionHistories := versionhistory.CopyVersionHistories(mu.GetExecutionInfo().VersionHistories)
	sourceTransitionHistory := workflow.CopyVersionedTransitions(mu.GetExecutionInfo().TransitionHistory)
	releaseFunc(nil)
	releaseFunc = nil

	if len(newRunId) > 0 {
		newRunInfo, err := s.getNewRunInfo(ctx, namespace.ID(namespaceID), execution, newRunId)
		if err != nil {
			return nil, err
		}
		result.NewRunInfo = newRunInfo
	}

	events, err := s.getSyncStateEvents(ctx, versionHistories, sourceVersionHistories)
	if err != nil {
		return nil, err
	}
	result.EventBlobs = events
	result.VersionedTransitionHistory = sourceTransitionHistory
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(sourceVersionHistories)
	if err != nil {
		s.logger.Error("SyncWorkflowState failed to get current version history and update progress cache",
			tag.WorkflowNamespaceID(namespaceID),
			tag.WorkflowID(execution.WorkflowId),
			tag.WorkflowRunID(execution.RunId),
			tag.Error(err))
		return result, nil
	}
	result.LastVersionHistory = currentVersionHistory
	return result, nil
}

func (s *SyncStateRetrieverImpl) getNewRunInfo(ctx context.Context, namespaceId namespace.ID, execution *commonpb.WorkflowExecution, newRunId string) (_ *replicationpb.NewRunInfo, retError error) {
	wfCtx, releaseFunc, err := s.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		s.shardContext,
		namespaceId,
		&commonpb.WorkflowExecution{
			WorkflowId: execution.WorkflowId,
			RunId:      newRunId,
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
	mu, err := wfCtx.LoadMutableState(ctx, s.shardContext)
	switch err.(type) {
	case nil:
	case *serviceerror.NotFound:
		s.logger.Info(fmt.Sprintf("SyncWorkflowState new run not found, newRunId: %v", newRunId),
			tag.WorkflowNamespaceID(namespaceId.String()),
			tag.WorkflowID(execution.WorkflowId),
			tag.WorkflowRunID(execution.RunId))
		return nil, nil
	default:
		return nil, err
	}
	versionHistory, err := versionhistory.GetCurrentVersionHistory(mu.GetExecutionInfo().VersionHistories)
	if err != nil {
		return nil, err
	}
	versionHistory = versionhistory.CopyVersionHistory(versionHistory)
	releaseFunc(nil)
	releaseFunc = nil
	newRunEvents, err := s.getEventsBlob(ctx, versionHistory.BranchToken, common.FirstEventID, common.FirstEventID+1)
	switch err.(type) {
	case nil:
	case *serviceerror.NotFound:
		s.logger.Info(fmt.Sprintf("SyncWorkflowState new run event not found, newRunId: %v", newRunId),
			tag.WorkflowNamespaceID(namespaceId.String()),
			tag.WorkflowID(execution.WorkflowId),
			tag.WorkflowRunID(execution.RunId))
		return nil, nil
	default:
		return nil, err
	}
	if len(newRunEvents) == 0 {
		s.logger.Info(fmt.Sprintf("SyncWorkflowState new run event is empty, newRunId: %v", newRunId),
			tag.WorkflowNamespaceID(namespaceId.String()),
			tag.WorkflowID(execution.WorkflowId),
			tag.WorkflowRunID(execution.RunId))
		return nil, nil
	}
	return &replicationpb.NewRunInfo{
		RunId:      newRunId,
		EventBatch: newRunEvents[0],
	}, nil
}

func (s *SyncStateRetrieverImpl) getMutation(mutableState workflow.MutableState, versionedTransition *persistencepb.VersionedTransition) (*persistencepb.WorkflowMutableStateMutation, error) {
	rootNode := mutableState.HSM()
	updatedStateMachine, err := s.getUpdatedSubStateMachine(rootNode, versionedTransition)
	if err != nil {
		return nil, err
	}
	mutableStateClone := mutableState.CloneToProto()
	err = workflow.SanitizeMutableState(mutableStateClone)
	if err != nil {
		return nil, err
	}
	if err := common.DiscardUnknownProto(mutableStateClone); err != nil {
		return nil, err
	}
	tombstoneBatch := mutableStateClone.GetExecutionInfo().SubStateMachineTombstoneBatches
	var tombstones []*persistencepb.StateMachineTombstoneBatch
	for i, tombstone := range tombstoneBatch {
		if workflow.CompareVersionedTransition(tombstone.VersionedTransition, versionedTransition) > 0 {
			tombstones = tombstoneBatch[i:]
			break
		}
	}
	mutableStateClone.ExecutionInfo.UpdateInfos = nil
	mutableStateClone.ExecutionInfo.SubStateMachinesByType = nil
	mutableStateClone.ExecutionInfo.SubStateMachineTombstoneBatches = nil
	var signalRequestedIds []string
	if workflow.CompareVersionedTransition(mutableStateClone.ExecutionInfo.SignalRequestIdsLastUpdateVersionedTransition, versionedTransition) > 0 {
		signalRequestedIds = mutableStateClone.SignalRequestedIds
	}
	return &persistencepb.WorkflowMutableStateMutation{
		UpdatedActivityInfos:            getUpdatedInfo(mutableStateClone.ActivityInfos, versionedTransition),
		UpdatedTimerInfos:               getUpdatedInfo(mutableStateClone.TimerInfos, versionedTransition),
		UpdatedChildExecutionInfos:      getUpdatedInfo(mutableStateClone.ChildExecutionInfos, versionedTransition),
		UpdatedRequestCancelInfos:       getUpdatedInfo(mutableStateClone.RequestCancelInfos, versionedTransition),
		UpdatedSignalInfos:              getUpdatedInfo(mutableStateClone.SignalInfos, versionedTransition),
		UpdatedUpdateInfos:              getUpdatedInfo(mutableStateClone.ExecutionInfo.UpdateInfos, versionedTransition),
		UpdatedSubStateMachines:         updatedStateMachine,
		SubStateMachineTombstoneBatches: tombstones,
		SignalRequestedIds:              signalRequestedIds,
		ExecutionInfo:                   mutableStateClone.ExecutionInfo,
	}, nil
}

func (s *SyncStateRetrieverImpl) getSnapshot(mutableState workflow.MutableState) (*persistencepb.WorkflowMutableState, error) {
	mutableStateProto := mutableState.CloneToProto()
	err := workflow.SanitizeMutableState(mutableStateProto)
	if err != nil {
		return nil, err
	}
	if err := common.DiscardUnknownProto(mutableStateProto); err != nil {
		return nil, err
	}
	return mutableStateProto, nil
}

func (s *SyncStateRetrieverImpl) getEventsBlob(ctx context.Context, branchToken []byte, startEventId int64, endEventId int64) ([]*commonpb.DataBlob, error) {
	rawHistoryResponse, err := s.shardContext.GetExecutionManager().ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		BranchToken: branchToken,
		MinEventID:  startEventId,
		MaxEventID:  endEventId,
		PageSize:    defaultPageSize,
		ShardID:     s.shardContext.GetShardID(),
	})
	if err != nil {
		return nil, err
	}
	return rawHistoryResponse.HistoryEventBlobs, nil
}

func (s *SyncStateRetrieverImpl) getSyncStateEvents(ctx context.Context, targetVersionHistories *history.VersionHistories, sourceVersionHistories *history.VersionHistories) ([]*commonpb.DataBlob, error) {
	sourceHistory, err := versionhistory.GetCurrentVersionHistory(sourceVersionHistories)
	if err != nil {
		return nil, err
	}
	lcaItem, _, err := versionhistory.FindLCAVersionHistoryItemAndIndex(targetVersionHistories, sourceHistory)
	if err != nil {
		return nil, err
	}
	sourceLastItem, err := versionhistory.GetLastVersionHistoryItem(sourceHistory)
	if err != nil {
		return nil, err
	}
	if versionhistory.IsEqualVersionHistoryItem(lcaItem, sourceLastItem) {
		return nil, nil
	}
	startEventId := lcaItem.GetEventId() + 1

	return s.getEventsBlob(ctx, sourceHistory.BranchToken, startEventId, sourceLastItem.GetEventId()+1)
}

func isInfoUpdated(subStateMachine lastUpdatedStateTransitionGetter, versionedTransition *persistencepb.VersionedTransition) bool {
	if subStateMachine == nil {
		return false
	}
	lastUpdate := subStateMachine.GetLastUpdateVersionedTransition()
	return workflow.CompareVersionedTransition(lastUpdate, versionedTransition) > 0
}

func getUpdatedInfo[K comparable, V lastUpdatedStateTransitionGetter](subStateMachine map[K]V, versionedTransition *persistencepb.VersionedTransition) map[K]V {
	result := make(map[K]V)
	for k, v := range subStateMachine {
		if isInfoUpdated(v, versionedTransition) {
			result[k] = v
		}
	}
	return result
}

func (s *SyncStateRetrieverImpl) getUpdatedSubStateMachine(n *hsm.Node, versionedTransition *persistencepb.VersionedTransition) ([]*persistencepb.WorkflowMutableStateMutation_StateMachineNodeMutation, error) {
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
