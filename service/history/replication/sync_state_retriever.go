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
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const (
	defaultPageSize = 32
)

type (
	ResultType      int
	SyncStateResult struct {
		VersionedTransitionArtifact *replicationpb.VersionedTransitionArtifact
		VersionedTransitionHistory  []*persistencepb.VersionedTransition
		SyncedVersionHistory        *history.VersionHistory
	}
	SyncStateRetriever interface {
		GetSyncWorkflowStateArtifact(
			ctx context.Context,
			namespaceID string,
			execution *commonpb.WorkflowExecution,
			targetVersionedTransition *persistencepb.VersionedTransition,
			targetVersionHistories *history.VersionHistories,
		) (*SyncStateResult, error)
		GetSyncWorkflowStateArtifactFromMutableState(
			ctx context.Context,
			namespaceID string,
			execution *commonpb.WorkflowExecution,
			mutableState workflow.MutableState,
			targetVersionedTransition *persistencepb.VersionedTransition,
			targetVersionHistories [][]*history.VersionHistoryItem,
			releaseFunc wcache.ReleaseCacheFunc,
		) (*SyncStateResult, error)
	}

	SyncStateRetrieverImpl struct {
		shardContext               shard.Context
		workflowCache              wcache.Cache
		workflowConsistencyChecker api.WorkflowConsistencyChecker
		eventBlobCache             persistence.XDCCache
		logger                     log.Logger
	}
	lastUpdatedStateTransitionGetter interface {
		GetLastUpdateVersionedTransition() *persistencepb.VersionedTransition
	}
)

func NewSyncStateRetriever(
	shardContext shard.Context,
	workflowCache wcache.Cache,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	eventBlobCache persistence.XDCCache,
	logger log.Logger,
) *SyncStateRetrieverImpl {
	return &SyncStateRetrieverImpl{
		shardContext:               shardContext,
		workflowCache:              workflowCache,
		workflowConsistencyChecker: workflowConsistencyChecker,
		eventBlobCache:             eventBlobCache,
		logger:                     logger,
	}
}

func (s *SyncStateRetrieverImpl) GetSyncWorkflowStateArtifact(
	ctx context.Context,
	namespaceID string,
	execution *commonpb.WorkflowExecution,
	targetCurrentVersionedTransition *persistencepb.VersionedTransition,
	targetVersionHistories *history.VersionHistories,
) (_ *SyncStateResult, retError error) {
	wfLease, err := s.workflowConsistencyChecker.GetWorkflowLeaseWithConsistencyCheck(
		ctx,
		nil,
		func(mutableState workflow.MutableState) bool {
			if targetCurrentVersionedTransition == nil {
				return true
			}
			return !errors.Is(workflow.TransitionHistoryStalenessCheck(mutableState.GetExecutionInfo().TransitionHistory, targetCurrentVersionedTransition), consts.ErrStaleState)
		},
		definition.WorkflowKey{
			NamespaceID: namespaceID,
			WorkflowID:  execution.WorkflowId,
			RunID:       execution.RunId,
		},
		locks.PriorityLow,
	)
	if err != nil {
		return nil, err
	}
	mutableState := wfLease.GetMutableState()
	releaseFunc := wfLease.GetReleaseFn()

	defer func() {
		if releaseFunc != nil {
			releaseFunc(retError)
		}
	}()
	if mutableState.HasBufferedEvents() {
		return nil, serviceerror.NewWorkflowNotReady("workflow has buffered events")
	}

	if len(mutableState.GetExecutionInfo().TransitionHistory) == 0 {
		// workflow essentially in an unknown state
		// e.g. an event-based replication task got applied to the workflow after
		// a syncVersionedTransition task is converted and streamed to target.
		return nil, consts.ErrTransitionHistoryDisabled
	}

	var versionHistoriesItems [][]*history.VersionHistoryItem
	if targetVersionHistories != nil {
		for _, versionHistory := range targetVersionHistories.Histories {
			versionHistoriesItems = append(versionHistoriesItems, versionHistory.Items)
		}
	}

	return s.getSyncStateResult(ctx, namespaceID, execution, mutableState, targetCurrentVersionedTransition, versionHistoriesItems, releaseFunc)
}

func (s *SyncStateRetrieverImpl) GetSyncWorkflowStateArtifactFromMutableState(
	ctx context.Context,
	namespaceID string,
	execution *commonpb.WorkflowExecution,
	mu workflow.MutableState,
	targetCurrentVersionedTransition *persistencepb.VersionedTransition,
	targetVersionHistories [][]*history.VersionHistoryItem,
	releaseFunc wcache.ReleaseCacheFunc,
) (_ *SyncStateResult, retError error) {
	return s.getSyncStateResult(ctx, namespaceID, execution, mu, targetCurrentVersionedTransition, targetVersionHistories, releaseFunc)
}

func (s *SyncStateRetrieverImpl) getSyncStateResult(
	ctx context.Context,
	namespaceID string,
	execution *commonpb.WorkflowExecution,
	mutableState workflow.MutableState,
	targetCurrentVersionedTransition *persistencepb.VersionedTransition,
	targetVersionHistories [][]*history.VersionHistoryItem,
	cacheReleaseFunc wcache.ReleaseCacheFunc,
) (_ *SyncStateResult, retError error) {
	shouldReturnMutation := func() bool {
		if targetCurrentVersionedTransition == nil {
			return false
		}
		// not on the same branch
		if workflow.TransitionHistoryStalenessCheck(mutableState.GetExecutionInfo().TransitionHistory, targetCurrentVersionedTransition) != nil {
			return false
		}
		tombstoneBatch := mutableState.GetExecutionInfo().SubStateMachineTombstoneBatches
		if len(tombstoneBatch) == 0 {
			return false
		}
		if mutableState.GetExecutionInfo().LastTransitionHistoryBreakPoint != nil &&
			// the target transition falls into the previous break point, need to send snapshot
			workflow.CompareVersionedTransition(mutableState.GetExecutionInfo().LastTransitionHistoryBreakPoint, targetCurrentVersionedTransition) >= 0 {
			return false
		}
		if workflow.CompareVersionedTransition(tombstoneBatch[0].VersionedTransition, targetCurrentVersionedTransition) <= 0 {
			return true
		}

		if targetCurrentVersionedTransition.TransitionCount+1 == tombstoneBatch[0].VersionedTransition.TransitionCount &&
			targetCurrentVersionedTransition.NamespaceFailoverVersion == tombstoneBatch[0].VersionedTransition.NamespaceFailoverVersion {
			return true
		}
		return false
	}

	versionedTransitionArtifact := &replicationpb.VersionedTransitionArtifact{}
	if shouldReturnMutation() {
		mutation, err := s.getMutation(mutableState, targetCurrentVersionedTransition)
		if err != nil {
			return nil, err
		}
		versionedTransitionArtifact.StateAttributes = &replicationpb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes{
			SyncWorkflowStateMutationAttributes: &replicationpb.SyncWorkflowStateMutationAttributes{
				StateMutation:                     mutation,
				ExclusiveStartVersionedTransition: targetCurrentVersionedTransition,
			},
		}
	} else {
		snapshot, err := s.getSnapshot(mutableState)
		if err != nil {
			return nil, err
		}
		versionedTransitionArtifact.StateAttributes = &replicationpb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
			SyncWorkflowStateSnapshotAttributes: &replicationpb.SyncWorkflowStateSnapshotAttributes{
				State: snapshot,
			},
		}
	}

	newRunId := mutableState.GetExecutionInfo().NewExecutionRunId
	sourceVersionHistories := versionhistory.CopyVersionHistories(mutableState.GetExecutionInfo().VersionHistories)
	sourceTransitionHistory := workflow.CopyVersionedTransitions(mutableState.GetExecutionInfo().TransitionHistory)
	if cacheReleaseFunc != nil {
		cacheReleaseFunc(nil)
	}

	if len(newRunId) > 0 {
		newRunInfo, err := s.getNewRunInfo(ctx, namespace.ID(namespaceID), execution, newRunId)
		if err != nil {
			return nil, err
		}
		versionedTransitionArtifact.NewRunInfo = newRunInfo
	}

	wfKey := definition.WorkflowKey{
		NamespaceID: namespaceID,
		WorkflowID:  execution.WorkflowId,
		RunID:       execution.RunId,
	}
	events, err := s.getSyncStateEvents(ctx, wfKey, targetVersionHistories, sourceVersionHistories)
	if err != nil {
		return nil, err
	}
	versionedTransitionArtifact.EventBatches = events
	result := &SyncStateResult{
		VersionedTransitionArtifact: versionedTransitionArtifact,
	}
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
	result.SyncedVersionHistory = currentVersionHistory
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
	mutableState, err := wfCtx.LoadMutableState(ctx, s.shardContext)
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
	versionHistory, err := versionhistory.GetCurrentVersionHistory(mutableState.GetExecutionInfo().VersionHistories)
	if err != nil {
		return nil, err
	}
	versionHistory = versionhistory.CopyVersionHistory(versionHistory)
	releaseFunc(nil)
	releaseFunc = nil
	wfKey := definition.WorkflowKey{
		NamespaceID: namespaceId.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       newRunId,
	}
	newRunEvents, err := s.getEventsBlob(ctx, wfKey, versionHistory, common.FirstEventID, common.FirstEventID+1, true)
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

	var signalRequestedIds []string
	if workflow.CompareVersionedTransition(mutableStateClone.ExecutionInfo.SignalRequestIdsLastUpdateVersionedTransition, versionedTransition) > 0 {
		signalRequestedIds = mutableStateClone.SignalRequestedIds
	}
	mutation := &persistencepb.WorkflowMutableStateMutation{
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
		ExecutionState:                  mutableStateClone.ExecutionState,
	}
	mutableStateClone.ExecutionInfo.UpdateInfos = nil
	mutableStateClone.ExecutionInfo.SubStateMachinesByType = nil
	mutableStateClone.ExecutionInfo.SubStateMachineTombstoneBatches = nil
	return mutation, nil
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

func (s *SyncStateRetrieverImpl) getEventsBlob(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	versionHistory *history.VersionHistory,
	startEventId int64,
	endEventId int64,
	isNewRun bool,
) ([]*commonpb.DataBlob, error) {
	var eventBlobs []*commonpb.DataBlob

	if s.eventBlobCache != nil {
		for {
			eventVersion, err := versionhistory.GetVersionHistoryEventVersion(versionHistory, startEventId)
			if err != nil {
				return nil, err
			}
			xdcCacheValue, ok := s.eventBlobCache.Get(persistence.NewXDCCacheKey(workflowKey, startEventId, eventVersion))
			if !ok {
				break
			}
			left := endEventId - startEventId
			if !isNewRun && int64(len(xdcCacheValue.EventBlobs)) > left {
				s.logger.Error(
					fmt.Sprintf("xdc cached events are truncated, want [%d, %d), got [%d, %d) from cache",
						startEventId, endEventId, startEventId, xdcCacheValue.NextEventID),
					tag.FirstEventVersion(eventVersion),
					tag.WorkflowNamespaceID(workflowKey.NamespaceID),
					tag.WorkflowID(workflowKey.WorkflowID),
					tag.WorkflowRunID(workflowKey.RunID),
				)
				eventBlobs = append(eventBlobs, xdcCacheValue.EventBlobs[:left]...)
				return eventBlobs, nil
			}
			eventBlobs = append(eventBlobs, xdcCacheValue.EventBlobs...)
			startEventId = xdcCacheValue.NextEventID
			if startEventId >= endEventId {
				return eventBlobs, nil
			}
		}
	}

	rawHistoryResponse, err := s.shardContext.GetExecutionManager().ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		BranchToken: versionHistory.BranchToken,
		MinEventID:  startEventId,
		MaxEventID:  endEventId,
		PageSize:    defaultPageSize,
		ShardID:     s.shardContext.GetShardID(),
	})
	if err != nil {
		return nil, err
	}

	eventBlobs = append(eventBlobs, rawHistoryResponse.HistoryEventBlobs...)
	return eventBlobs, nil
}

func (s *SyncStateRetrieverImpl) getSyncStateEvents(ctx context.Context, workflowKey definition.WorkflowKey, targetVersionHistories [][]*history.VersionHistoryItem, sourceVersionHistories *history.VersionHistories) ([]*commonpb.DataBlob, error) {
	if targetVersionHistories == nil {
		// return nil, so target will retrieve the missing events from source
		return nil, nil
	}
	sourceHistory, err := versionhistory.GetCurrentVersionHistory(sourceVersionHistories)
	if err != nil {
		return nil, err
	}
	lcaItem, _, err := versionhistory.FindLCAVersionHistoryItemFromItems(targetVersionHistories, sourceHistory.Items)
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

	return s.getEventsBlob(ctx, workflowKey, sourceHistory, startEventId, sourceLastItem.GetEventId()+1, false)
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
	// Source cluster uses Walk() to generate node mutations.
	// Walk() uses pre-order DFS. Updated parent nodes will be added before children.
	err := n.Walk(walkFn)
	if err != nil {
		return nil, err
	}
	return updatedStateMachines, nil
}
