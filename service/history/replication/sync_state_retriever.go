//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination sync_state_retriever_mock.go

package replication

import (
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const (
	defaultPageSize = 32
)

type (
	ResultType      int
	SyncStateResult struct {
		VersionedTransitionArtifact *replicationspb.VersionedTransitionArtifact
		VersionedTransitionHistory  []*persistencespb.VersionedTransition
		SyncedVersionHistory        *historyspb.VersionHistory
	}
	SyncStateRetriever interface {
		GetSyncWorkflowStateArtifact(
			ctx context.Context,
			namespaceID string,
			execution *commonpb.WorkflowExecution,
			targetVersionedTransition *persistencespb.VersionedTransition,
			targetVersionHistories *historyspb.VersionHistories,
		) (*SyncStateResult, error)
		GetSyncWorkflowStateArtifactFromMutableState(
			ctx context.Context,
			namespaceID string,
			execution *commonpb.WorkflowExecution,
			mutableState historyi.MutableState,
			targetVersionedTransition *persistencespb.VersionedTransition,
			targetVersionHistories [][]*historyspb.VersionHistoryItem,
			releaseFunc historyi.ReleaseWorkflowContextFunc,
		) (*SyncStateResult, error)
		GetSyncWorkflowStateArtifactFromMutableStateForNewWorkflow(
			ctx context.Context,
			namespaceID string,
			execution *commonpb.WorkflowExecution,
			mutableState historyi.MutableState,
			releaseFunc historyi.ReleaseWorkflowContextFunc,
			taskVersionedTransition *persistencespb.VersionedTransition,
		) (*SyncStateResult, error)
	}

	SyncStateRetrieverImpl struct {
		shardContext               historyi.ShardContext
		workflowCache              wcache.Cache
		workflowConsistencyChecker api.WorkflowConsistencyChecker
		eventBlobCache             persistence.XDCCache
		logger                     log.Logger
	}
	lastUpdatedStateTransitionGetter interface {
		GetLastUpdateVersionedTransition() *persistencespb.VersionedTransition
	}
)

func NewSyncStateRetriever(
	shardContext historyi.ShardContext,
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
	targetCurrentVersionedTransition *persistencespb.VersionedTransition,
	targetVersionHistories *historyspb.VersionHistories,
) (_ *SyncStateResult, retError error) {
	wfLease, err := s.workflowConsistencyChecker.GetChasmLeaseWithConsistencyCheck(
		ctx,
		nil,
		func(mutableState historyi.MutableState) bool {
			if targetCurrentVersionedTransition == nil {
				return true
			}
			return !errors.Is(transitionhistory.StalenessCheck(mutableState.GetExecutionInfo().TransitionHistory, targetCurrentVersionedTransition), consts.ErrStaleState)
		},
		definition.WorkflowKey{
			NamespaceID: namespaceID,
			WorkflowID:  execution.WorkflowId,
			RunID:       execution.RunId,
		},
		chasm.ArchetypeAny, // SyncWorkflowState API works on all archetypes
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

	var versionHistoriesItems [][]*historyspb.VersionHistoryItem
	if targetVersionHistories != nil {
		for _, versionHistory := range targetVersionHistories.Histories {
			versionHistoriesItems = append(versionHistoriesItems, versionHistory.Items)
		}
	}

	return s.getSyncStateResult(ctx, namespaceID, execution, mutableState, targetCurrentVersionedTransition, versionHistoriesItems, releaseFunc, false)
}

func (s *SyncStateRetrieverImpl) GetSyncWorkflowStateArtifactFromMutableState(
	ctx context.Context,
	namespaceID string,
	execution *commonpb.WorkflowExecution,
	mu historyi.MutableState,
	targetCurrentVersionedTransition *persistencespb.VersionedTransition,
	targetVersionHistories [][]*historyspb.VersionHistoryItem,
	releaseFunc historyi.ReleaseWorkflowContextFunc,
) (_ *SyncStateResult, retError error) {
	return s.getSyncStateResult(ctx, namespaceID, execution, mu, targetCurrentVersionedTransition, targetVersionHistories, releaseFunc, false)
}

func (s *SyncStateRetrieverImpl) GetSyncWorkflowStateArtifactFromMutableStateForNewWorkflow(
	ctx context.Context,
	namespaceID string,
	execution *commonpb.WorkflowExecution,
	mu historyi.MutableState,
	releaseFunc historyi.ReleaseWorkflowContextFunc,
	taskVersionedTransition *persistencespb.VersionedTransition,
) (_ *SyncStateResult, retError error) {
	targetVersionedTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: taskVersionedTransition.NamespaceFailoverVersion,
		TransitionCount:          0,
	}
	return s.getSyncStateResult(ctx, namespaceID, execution, mu, targetVersionedTransition, nil, releaseFunc, true)
}

func (s *SyncStateRetrieverImpl) getSyncStateResult(
	ctx context.Context,
	namespaceID string,
	execution *commonpb.WorkflowExecution,
	mutableState historyi.MutableState,
	targetCurrentVersionedTransition *persistencespb.VersionedTransition,
	targetVersionHistories [][]*historyspb.VersionHistoryItem,
	cacheReleaseFunc historyi.ReleaseWorkflowContextFunc,
	isNewWorkflow bool,
) (_ *SyncStateResult, retError error) {
	shouldReturnMutation := func() bool {
		if targetCurrentVersionedTransition == nil {
			return false
		}
		tombstoneBatch := mutableState.GetExecutionInfo().SubStateMachineTombstoneBatches
		if isNewWorkflow && len(tombstoneBatch) != 0 && tombstoneBatch[0].VersionedTransition.TransitionCount == 1 {
			return true
		}
		// not on the same branch
		if transitionhistory.StalenessCheck(mutableState.GetExecutionInfo().TransitionHistory, targetCurrentVersionedTransition) != nil {
			return false
		}
		if len(tombstoneBatch) == 0 {
			return false
		}
		if mutableState.GetExecutionInfo().LastTransitionHistoryBreakPoint != nil &&
			// the target transition falls into the previous break point, need to send snapshot
			transitionhistory.Compare(mutableState.GetExecutionInfo().LastTransitionHistoryBreakPoint, targetCurrentVersionedTransition) >= 0 {
			return false
		}
		if transitionhistory.Compare(tombstoneBatch[0].VersionedTransition, targetCurrentVersionedTransition) <= 0 {
			return true
		}

		if targetCurrentVersionedTransition.TransitionCount+1 == tombstoneBatch[0].VersionedTransition.TransitionCount &&
			targetCurrentVersionedTransition.NamespaceFailoverVersion == tombstoneBatch[0].VersionedTransition.NamespaceFailoverVersion {
			return true
		}
		return false
	}

	versionedTransitionArtifact := &replicationspb.VersionedTransitionArtifact{}
	if shouldReturnMutation() {
		mutation, err := s.getMutation(mutableState, targetCurrentVersionedTransition)
		if err != nil {
			return nil, err
		}
		versionedTransitionArtifact.StateAttributes = &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes{
			SyncWorkflowStateMutationAttributes: &replicationspb.SyncWorkflowStateMutationAttributes{
				StateMutation:                     mutation,
				ExclusiveStartVersionedTransition: targetCurrentVersionedTransition,
			},
		}
	} else {
		snapshot, err := s.getSnapshot(mutableState)
		if err != nil {
			return nil, err
		}
		versionedTransitionArtifact.StateAttributes = &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
			SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
				State: snapshot,
			},
		}
	}
	versionedTransitionArtifact.IsFirstSync = isNewWorkflow

	newRunId := mutableState.GetExecutionInfo().SuccessorRunId
	sourceVersionHistories := versionhistory.CopyVersionHistories(mutableState.GetExecutionInfo().VersionHistories)
	sourceTransitionHistory := transitionhistory.CopyVersionedTransitions(mutableState.GetExecutionInfo().TransitionHistory)
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

	events, err := s.getSyncStateEvents(
		ctx, definition.WorkflowKey{
			NamespaceID: namespaceID,
			WorkflowID:  execution.WorkflowId,
			RunID:       execution.RunId,
		},
		targetVersionHistories,
		sourceVersionHistories,
		isNewWorkflow,
	)
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

func (s *SyncStateRetrieverImpl) getNewRunInfo(ctx context.Context, namespaceId namespace.ID, execution *commonpb.WorkflowExecution, newRunId string) (_ *replicationspb.NewRunInfo, retError error) {
	// CHASM runs don't have new run, so can continue to use GetOrCreateWorkflowExecution here.
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
		s.logger.Info("SyncWorkflowState new run not found",
			tag.WorkflowNewRunID(newRunId),
			tag.WorkflowNamespaceID(namespaceId.String()),
			tag.WorkflowID(execution.WorkflowId),
			tag.WorkflowRunID(execution.RunId))
		return nil, nil
	default:
		return nil, err
	}

	// if new run is not started by current cluster, it means the new run transaction is not happened at current cluster
	// so when sending replication task, we should not include new run info
	startVersion, err := mutableState.GetStartVersion()
	if err != nil {
		return nil, err
	}
	clusterMetadata := s.shardContext.GetClusterMetadata()
	if !clusterMetadata.IsVersionFromSameCluster(startVersion, clusterMetadata.GetClusterID()) {
		return nil, nil
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
		s.logger.Info("SyncWorkflowState new run event not found",
			tag.WorkflowNewRunID(newRunId),
			tag.WorkflowNamespaceID(namespaceId.String()),
			tag.WorkflowID(execution.WorkflowId),
			tag.WorkflowRunID(execution.RunId))
		return nil, nil
	default:
		return nil, err
	}
	if len(newRunEvents) == 0 {
		s.logger.Info("SyncWorkflowState new run event is empty",
			tag.WorkflowNewRunID(newRunId),
			tag.WorkflowNamespaceID(namespaceId.String()),
			tag.WorkflowID(execution.WorkflowId),
			tag.WorkflowRunID(execution.RunId))
		return nil, nil
	}
	return &replicationspb.NewRunInfo{
		RunId:      newRunId,
		EventBatch: newRunEvents[0],
	}, nil
}

func (s *SyncStateRetrieverImpl) getMutation(
	mutableState historyi.MutableState,
	exclusiveMinVT *persistencespb.VersionedTransition,
) (*persistencespb.WorkflowMutableStateMutation, error) {
	rootNode := mutableState.HSM()
	updatedStateMachine, err := s.getUpdatedSubStateMachine(rootNode, exclusiveMinVT)
	if err != nil {
		return nil, err
	}

	executionInfo := mutableState.GetExecutionInfo()
	tombstoneBatch := executionInfo.SubStateMachineTombstoneBatches
	var tombstones []*persistencespb.StateMachineTombstoneBatch
	for i, tombstone := range tombstoneBatch {
		if transitionhistory.Compare(tombstone.VersionedTransition, exclusiveMinVT) > 0 {
			tombstones = tombstoneBatch[i:]
			break
		}
	}

	var signalRequestedIds []string
	if transitionhistory.Compare(executionInfo.SignalRequestIdsLastUpdateVersionedTransition, exclusiveMinVT) > 0 {
		signalRequestedIds = mutableState.GetPendingSignalRequestedIds()
	}

	mutation := &persistencespb.WorkflowMutableStateMutation{
		UpdatedActivityInfos:            getUpdatedInfo(mutableState.GetPendingActivityInfos(), exclusiveMinVT),
		UpdatedTimerInfos:               getUpdatedInfo(mutableState.GetPendingTimerInfos(), exclusiveMinVT),
		UpdatedChildExecutionInfos:      getUpdatedInfo(mutableState.GetPendingChildExecutionInfos(), exclusiveMinVT),
		UpdatedRequestCancelInfos:       getUpdatedInfo(mutableState.GetPendingRequestCancelExternalInfos(), exclusiveMinVT),
		UpdatedSignalInfos:              getUpdatedInfo(mutableState.GetPendingSignalExternalInfos(), exclusiveMinVT),
		UpdatedUpdateInfos:              getUpdatedInfo(executionInfo.UpdateInfos, exclusiveMinVT),
		UpdatedChasmNodes:               mutableState.ChasmTree().Snapshot(exclusiveMinVT).Nodes,
		UpdatedSubStateMachines:         updatedStateMachine,
		SubStateMachineTombstoneBatches: tombstones,
		SignalRequestedIds:              signalRequestedIds,
		ExecutionInfo:                   executionInfo,
		ExecutionState:                  mutableState.GetExecutionState(),
	}

	mutation = common.CloneProto(mutation)
	workflow.SanitizeMutableStateMutation(mutation)
	if err := common.DiscardUnknownProto(mutation); err != nil {
		return nil, err
	}

	mutation.ExecutionInfo.UpdateInfos = nil
	mutation.ExecutionInfo.SubStateMachinesByType = nil
	mutation.ExecutionInfo.SubStateMachineTombstoneBatches = nil
	return mutation, nil
}

func (s *SyncStateRetrieverImpl) getSnapshot(mutableState historyi.MutableState) (*persistencespb.WorkflowMutableState, error) {
	mutableStateProto := mutableState.CloneToProto()
	workflow.SanitizeMutableState(mutableStateProto)
	if err := common.DiscardUnknownProto(mutableStateProto); err != nil {
		return nil, err
	}
	return mutableStateProto, nil
}

func (s *SyncStateRetrieverImpl) getEventsBlob(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	versionHistory *historyspb.VersionHistory,
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

func (s *SyncStateRetrieverImpl) getSyncStateEvents(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	targetVersionHistories [][]*historyspb.VersionHistoryItem,
	sourceVersionHistories *historyspb.VersionHistories,
	isNewWorkflow bool,
) ([]*commonpb.DataBlob, error) {
	if sourceVersionHistories == nil {
		// This should never happen for new workflows.
		return nil, nil
	}
	sourceHistory, err := versionhistory.GetCurrentVersionHistory(sourceVersionHistories)
	if err != nil {
		return nil, err
	}

	if versionhistory.IsEmptyVersionHistory(sourceHistory) {
		return nil, nil
	}

	if isNewWorkflow {
		sourceLastItem, err := versionhistory.GetLastVersionHistoryItem(sourceHistory)
		if err != nil {
			return nil, err
		}
		return s.getEventsBlob(ctx, workflowKey, sourceHistory, 1, sourceLastItem.GetEventId()+1, false)
	}

	if targetVersionHistories == nil {
		// return nil, so target will retrieve the missing events from source
		return nil, nil
	}

	// TODO: we may need to handle the case where mutable state only starts to generate events during middle of its execution.
	// In that case targetVersionHistories maybe empty and sourceVersionHistories is not empty.
	// The LCA logic doesn't work in that case today.
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

func isInfoUpdated(subStateMachine lastUpdatedStateTransitionGetter, versionedTransition *persistencespb.VersionedTransition) bool {
	if subStateMachine == nil {
		return false
	}
	lastUpdate := subStateMachine.GetLastUpdateVersionedTransition()
	return transitionhistory.Compare(lastUpdate, versionedTransition) > 0
}

func getUpdatedInfo[K comparable, V lastUpdatedStateTransitionGetter](subStateMachine map[K]V, versionedTransition *persistencespb.VersionedTransition) map[K]V {
	result := make(map[K]V)
	for k, v := range subStateMachine {
		if isInfoUpdated(v, versionedTransition) {
			result[k] = v
		}
	}
	return result
}

func (s *SyncStateRetrieverImpl) getUpdatedSubStateMachine(n *hsm.Node, versionedTransition *persistencespb.VersionedTransition) ([]*persistencespb.WorkflowMutableStateMutation_StateMachineNodeMutation, error) {
	var updatedStateMachines []*persistencespb.WorkflowMutableStateMutation_StateMachineNodeMutation
	walkFn := func(node *hsm.Node) error {
		if node == nil {
			return serviceerror.NewInvalidArgument("Nil node is not expected")
		}
		if node.Parent == nil {
			return nil
		}
		convertKey := func(ori []hsm.Key) *persistencespb.StateMachinePath {
			var path []*persistencespb.StateMachineKey
			for _, k := range ori {
				path = append(path, &persistencespb.StateMachineKey{
					Type: k.Type,
					Id:   k.ID,
				})
			}
			return &persistencespb.StateMachinePath{
				Path: path,
			}
		}
		if isInfoUpdated(node.InternalRepr(), versionedTransition) {
			subStateMachine := node.InternalRepr()
			workflow.SanitizeStateMachineNode(subStateMachine)
			updatedStateMachines = append(updatedStateMachines, &persistencespb.WorkflowMutableStateMutation_StateMachineNodeMutation{
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
