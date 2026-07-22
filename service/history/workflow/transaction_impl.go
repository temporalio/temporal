package workflow

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/notification"
)

type (
	completionMetric struct {
		shouldRecord     bool
		isWorkflow       bool
		taskQueue        string
		namespaceState   string
		workflowTypeName string
		status           enumspb.WorkflowExecutionStatus
		startTime        *timestamppb.Timestamp
		closeTime        *timestamppb.Timestamp
	}
	TransactionImpl struct {
		shard  historyi.ShardContext
		logger log.Logger
	}
)

var _ Transaction = (*TransactionImpl)(nil)

func NewTransaction(
	shardContext historyi.ShardContext,
) *TransactionImpl {
	return &TransactionImpl{
		shard:  shardContext,
		logger: shardContext.GetLogger(),
	}
}

func (t *TransactionImpl) CreateWorkflowExecution(
	ctx context.Context,
	createMode persistence.CreateWorkflowMode,
	archetypeID chasm.ArchetypeID,
	newWorkflowFailoverVersion int64,
	newWorkflowSnapshot *persistence.WorkflowSnapshot,
	newWorkflowEventsSeq []*persistence.WorkflowEvents,
	isWorkflow bool,
) (int64, error) {

	engine, err := t.shard.GetEngine(ctx)
	if err != nil {
		return 0, err
	}

	resp, err := createWorkflowExecution(
		ctx,
		t.shard,
		newWorkflowFailoverVersion,
		&persistence.CreateWorkflowExecutionRequest{
			ShardID: t.shard.GetShardID(),
			// RangeID , this is set by shard context
			Mode:                createMode,
			ArchetypeID:         archetypeID,
			NewWorkflowSnapshot: *newWorkflowSnapshot,
			NewWorkflowEvents:   newWorkflowEventsSeq,
		},
		isWorkflow,
	)
	if persistence.OperationPossiblySucceeded(err) {
		NotifyOnExecutionSnapshot(engine, newWorkflowSnapshot)
	}
	if err != nil {
		return 0, err
	}

	if err := NotifyNewHistorySnapshotEvent(engine, newWorkflowSnapshot); err != nil {
		t.logger.Error("unable to notify workflow creation", tag.Error(err))
	}

	return int64(resp.NewMutableStateStats.HistoryStatistics.SizeDiff), nil
}

func (t *TransactionImpl) ConflictResolveWorkflowExecution(
	ctx context.Context,
	conflictResolveMode persistence.ConflictResolveWorkflowMode,
	archetypeID chasm.ArchetypeID,
	resetWorkflowFailoverVersion int64,
	resetWorkflowSnapshot *persistence.WorkflowSnapshot,
	resetWorkflowEventsSeq []*persistence.WorkflowEvents,
	newWorkflowFailoverVersion *int64,
	newWorkflowSnapshot *persistence.WorkflowSnapshot,
	newWorkflowEventsSeq []*persistence.WorkflowEvents,
	currentWorkflowFailoverVersion *int64,
	currentWorkflowMutation *persistence.WorkflowMutation,
	currentWorkflowEventsSeq []*persistence.WorkflowEvents,
	isWorkflow bool,
) (int64, int64, int64, error) {

	engine, err := t.shard.GetEngine(ctx)
	if err != nil {
		return 0, 0, 0, err
	}

	resp, err := conflictResolveWorkflowExecution(
		ctx,
		t.shard,
		resetWorkflowFailoverVersion,
		newWorkflowFailoverVersion,
		currentWorkflowFailoverVersion,
		&persistence.ConflictResolveWorkflowExecutionRequest{
			ShardID: t.shard.GetShardID(),
			// RangeID , this is set by shard context
			Mode:                    conflictResolveMode,
			ArchetypeID:             archetypeID,
			ResetWorkflowSnapshot:   *resetWorkflowSnapshot,
			ResetWorkflowEvents:     resetWorkflowEventsSeq,
			NewWorkflowSnapshot:     newWorkflowSnapshot,
			NewWorkflowEvents:       newWorkflowEventsSeq,
			CurrentWorkflowMutation: currentWorkflowMutation,
			CurrentWorkflowEvents:   currentWorkflowEventsSeq,
		},
		isWorkflow,
	)
	if persistence.OperationPossiblySucceeded(err) {
		NotifyOnExecutionSnapshot(engine, resetWorkflowSnapshot)
		NotifyOnExecutionSnapshot(engine, newWorkflowSnapshot)
		NotifyOnExecutionMutation(engine, currentWorkflowMutation)
	}
	if err != nil {
		return 0, 0, 0, err
	}

	if err := NotifyNewHistorySnapshotEvent(engine, resetWorkflowSnapshot); err != nil {
		t.logger.Error("unable to notify workflow reset", tag.Error(err))
	}
	if err := NotifyNewHistorySnapshotEvent(engine, newWorkflowSnapshot); err != nil {
		t.logger.Error("unable to notify workflow creation", tag.Error(err))
	}
	if err := NotifyNewHistoryMutationEvent(engine, currentWorkflowMutation); err != nil {
		t.logger.Error("unable to notify workflow mutation", tag.Error(err))
	}
	resetHistorySizeDiff := int64(resp.ResetMutableStateStats.HistoryStatistics.SizeDiff)
	newHistorySizeDiff := int64(0)
	if resp.NewMutableStateStats != nil {
		newHistorySizeDiff = int64(resp.NewMutableStateStats.HistoryStatistics.SizeDiff)
	}
	currentHistorySizeDiff := int64(0)
	if resp.CurrentMutableStateStats != nil {
		currentHistorySizeDiff = int64(resp.CurrentMutableStateStats.HistoryStatistics.SizeDiff)
	}
	return resetHistorySizeDiff, newHistorySizeDiff, currentHistorySizeDiff, nil
}

func (t *TransactionImpl) UpdateWorkflowExecution(
	ctx context.Context,
	updateMode persistence.UpdateWorkflowMode,
	archetypeID chasm.ArchetypeID,
	currentWorkflowFailoverVersion int64,
	currentWorkflowMutation *persistence.WorkflowMutation,
	currentWorkflowEventsSeq []*persistence.WorkflowEvents,
	newWorkflowFailoverVersion *int64,
	newWorkflowSnapshot *persistence.WorkflowSnapshot,
	newWorkflowEventsSeq []*persistence.WorkflowEvents,
	isWorkflow bool,
) (int64, int64, error) {

	engine, err := t.shard.GetEngine(ctx)
	if err != nil {
		return 0, 0, err
	}
	resp, err := updateWorkflowExecution(
		ctx,
		t.shard,
		currentWorkflowFailoverVersion,
		newWorkflowFailoverVersion,
		&persistence.UpdateWorkflowExecutionRequest{
			ShardID: t.shard.GetShardID(),
			// RangeID , this is set by shard context
			Mode:                   updateMode,
			ArchetypeID:            archetypeID,
			UpdateWorkflowMutation: *currentWorkflowMutation,
			UpdateWorkflowEvents:   currentWorkflowEventsSeq,
			NewWorkflowSnapshot:    newWorkflowSnapshot,
			NewWorkflowEvents:      newWorkflowEventsSeq,
		},
		isWorkflow,
	)
	if persistence.OperationPossiblySucceeded(err) {
		NotifyOnExecutionMutation(engine, currentWorkflowMutation)
		NotifyOnExecutionSnapshot(engine, newWorkflowSnapshot)
	}
	if err != nil {
		return 0, 0, err
	}

	if err := NotifyNewHistoryMutationEvent(engine, currentWorkflowMutation); err != nil {
		t.logger.Error("unable to notify workflow mutation", tag.Error(err))
	}
	if err := NotifyNewHistorySnapshotEvent(engine, newWorkflowSnapshot); err != nil {
		t.logger.Error("unable to notify workflow creation", tag.Error(err))
	}
	updateHistorySizeDiff := int64(resp.UpdateMutableStateStats.HistoryStatistics.SizeDiff)
	newHistorySizeDiff := int64(0)
	if resp.NewMutableStateStats != nil {
		newHistorySizeDiff = int64(resp.NewMutableStateStats.HistoryStatistics.SizeDiff)
	}
	return updateHistorySizeDiff, newHistorySizeDiff, nil
}

func (t *TransactionImpl) SetWorkflowExecution(
	ctx context.Context,
	archetypeID chasm.ArchetypeID,
	workflowSnapshot *persistence.WorkflowSnapshot,
) error {

	engine, err := t.shard.GetEngine(ctx)
	if err != nil {
		return err
	}
	_, err = setWorkflowExecution(ctx, t.shard, &persistence.SetWorkflowExecutionRequest{
		ShardID: t.shard.GetShardID(),
		// RangeID , this is set by shard context
		ArchetypeID:         archetypeID,
		SetWorkflowSnapshot: *workflowSnapshot,
	})
	if persistence.OperationPossiblySucceeded(err) {
		NotifyOnExecutionSnapshot(engine, workflowSnapshot)
	}
	if err != nil {
		return err
	}

	return nil
}

func PersistWorkflowEvents(
	ctx context.Context,
	shardContext historyi.ShardContext,
	workflowEventsSlice ...*persistence.WorkflowEvents,
) (int64, error) {

	var totalSize int64
	for _, workflowEvents := range workflowEventsSlice {
		if len(workflowEvents.Events) == 0 {
			continue // allow update workflow without events
		}

		firstEventID := workflowEvents.Events[0].EventId
		if firstEventID == common.FirstEventID {
			size, err := persistFirstWorkflowEvents(ctx, shardContext, workflowEvents)
			if err != nil {
				return 0, err
			}
			totalSize += size
		} else {
			size, err := persistNonFirstWorkflowEvents(ctx, shardContext, workflowEvents)
			if err != nil {
				return 0, err
			}
			totalSize += size
		}
	}
	return totalSize, nil
}

func persistFirstWorkflowEvents(
	ctx context.Context,
	shardContext historyi.ShardContext,
	workflowEvents *persistence.WorkflowEvents,
) (int64, error) {

	namespaceID := namespace.ID(workflowEvents.NamespaceID)
	workflowID := workflowEvents.WorkflowID
	runID := workflowEvents.RunID
	execution := &commonpb.WorkflowExecution{
		WorkflowId: workflowEvents.WorkflowID,
		RunId:      workflowEvents.RunID,
	}
	branchToken := workflowEvents.BranchToken
	events := workflowEvents.Events
	prevTxnID := workflowEvents.PrevTxnID
	txnID := workflowEvents.TxnID

	size, err := appendHistoryEvents(
		ctx,
		shardContext,
		namespaceID,
		execution,
		&persistence.AppendHistoryNodesRequest{
			IsNewBranch:       true,
			Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID.String(), workflowID, runID),
			BranchToken:       branchToken,
			Events:            events,
			PrevTransactionID: prevTxnID,
			TransactionID:     txnID,
		},
	)
	return size, err
}

func persistNonFirstWorkflowEvents(
	ctx context.Context,
	shardContext historyi.ShardContext,
	workflowEvents *persistence.WorkflowEvents,
) (int64, error) {

	if len(workflowEvents.Events) == 0 {
		return 0, nil // allow update workflow without events
	}

	namespaceID := namespace.ID(workflowEvents.NamespaceID)
	execution := commonpb.WorkflowExecution{
		WorkflowId: workflowEvents.WorkflowID,
		RunId:      workflowEvents.RunID,
	}
	branchToken := workflowEvents.BranchToken
	events := workflowEvents.Events
	prevTxnID := workflowEvents.PrevTxnID
	txnID := workflowEvents.TxnID

	size, err := appendHistoryEvents(
		ctx,
		shardContext,
		namespaceID,
		&execution,
		&persistence.AppendHistoryNodesRequest{
			IsNewBranch:       false,
			BranchToken:       branchToken,
			Events:            events,
			PrevTransactionID: prevTxnID,
			TransactionID:     txnID,
		},
	)
	return size, err
}

func appendHistoryEvents(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	request *persistence.AppendHistoryNodesRequest,
) (int64, error) {

	resp, err := shardContext.AppendHistoryEvents(ctx, request, namespaceID, execution)
	return int64(resp), err
}

func createWorkflowExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	mutableStateFailoverVersion int64,
	request *persistence.CreateWorkflowExecutionRequest,
	isWorkflow bool,
) (*persistence.CreateWorkflowExecutionResponse, error) {

	resp, err := shardContext.CreateWorkflowExecution(ctx, request)
	if err != nil {
		switch err.(type) {
		case *persistence.CurrentWorkflowConditionFailedError,
			*persistence.WorkflowConditionFailedError,
			*persistence.ConditionFailedError,
			*serviceerror.ResourceExhausted:
			// it is possible that workflow already exists and caller need to apply
			// workflow ID reuse policy, or the error is resource exhausted.
			return nil, err
		default:
			shardContext.GetLogger().Error(
				"Persistent store operation Failure",
				tag.WorkflowNamespaceID(request.NewWorkflowSnapshot.ExecutionInfo.NamespaceId),
				tag.WorkflowID(request.NewWorkflowSnapshot.ExecutionInfo.WorkflowId),
				tag.WorkflowRunID(request.NewWorkflowSnapshot.ExecutionState.RunId),
				tag.StoreOperationCreateWorkflowExecution,
				tag.Error(err),
			)
			return nil, err
		}
	}

	if namespaceEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(request.NewWorkflowSnapshot.ExecutionInfo.NamespaceId),
	); err == nil {
		emitMutationMetrics(
			shardContext,
			namespaceEntry,
			request.ArchetypeID,
			&resp.NewMutableStateStats,
		)
		emitCompletionMetrics(
			shardContext,
			namespaceEntry,
			snapshotToCompletionMetric(
				namespaceState(shardContext.GetClusterMetadata(), &mutableStateFailoverVersion),
				&request.NewWorkflowSnapshot,
				request.NewWorkflowEvents,
				isWorkflow,
			),
		)
	}
	return resp, nil
}

func conflictResolveWorkflowExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	resetWorkflowFailoverVersion int64,
	newWorkflowFailoverVersion *int64,
	currentWorkflowFailoverVersion *int64,
	request *persistence.ConflictResolveWorkflowExecutionRequest,
	isWorkflow bool,
) (*persistence.ConflictResolveWorkflowExecutionResponse, error) {

	resp, err := shardContext.ConflictResolveWorkflowExecution(ctx, request)
	if err != nil {
		shardContext.GetLogger().Error(
			"Persistent store operation Failure",
			tag.WorkflowNamespaceID(request.ResetWorkflowSnapshot.ExecutionInfo.NamespaceId),
			tag.WorkflowID(request.ResetWorkflowSnapshot.ExecutionInfo.WorkflowId),
			tag.WorkflowRunID(request.ResetWorkflowSnapshot.ExecutionState.RunId),
			tag.StoreOperationConflictResolveWorkflowExecution,
			tag.Error(err),
		)
		return nil, err
	}

	if namespaceEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(request.ResetWorkflowSnapshot.ExecutionInfo.NamespaceId),
	); err == nil {
		emitMutationMetrics(
			shardContext,
			namespaceEntry,
			request.ArchetypeID,
			&resp.ResetMutableStateStats,
			resp.NewMutableStateStats,
			resp.CurrentMutableStateStats,
		)
		emitCompletionMetrics(
			shardContext,
			namespaceEntry,
			snapshotToCompletionMetric(
				namespaceState(shardContext.GetClusterMetadata(), &resetWorkflowFailoverVersion),
				&request.ResetWorkflowSnapshot,
				request.ResetWorkflowEvents,
				isWorkflow,
			),
			snapshotToCompletionMetric(
				namespaceState(shardContext.GetClusterMetadata(), newWorkflowFailoverVersion),
				request.NewWorkflowSnapshot,
				request.NewWorkflowEvents,
				isWorkflow,
			),
			mutationToCompletionMetric(
				namespaceState(shardContext.GetClusterMetadata(), currentWorkflowFailoverVersion),
				request.CurrentWorkflowMutation,
				request.CurrentWorkflowEvents,
				isWorkflow,
			),
		)
	}
	return resp, nil
}

func getWorkflowExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	request *persistence.GetWorkflowExecutionRequest,
) (*persistence.GetWorkflowExecutionResponse, error) {

	resp, err := shardContext.GetWorkflowExecution(ctx, request)
	if err != nil {
		switch err.(type) {
		case *serviceerror.NotFound:
			// It is possible that workflow does not exist.
			return nil, err
		default:
			shardContext.GetLogger().Error(
				"Persistent fetch operation Failure",
				tag.WorkflowNamespaceID(request.NamespaceID),
				tag.WorkflowID(request.WorkflowID),
				tag.WorkflowRunID(request.RunID),
				tag.StoreOperationGetWorkflowExecution,
				tag.Error(err),
			)
			return nil, err
		}
	}

	if namespaceEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(resp.State.ExecutionInfo.NamespaceId),
	); err == nil {
		emitGetMetrics(
			shardContext,
			namespaceEntry,
			request.ArchetypeID,
			&resp.MutableStateStats,
		)
	}
	return resp, nil
}

func updateWorkflowExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	updateWorkflowFailoverVersion int64,
	newWorkflowFailoverVersion *int64,
	request *persistence.UpdateWorkflowExecutionRequest,
	isWorkflow bool,
) (*persistence.UpdateWorkflowExecutionResponse, error) {

	resp, err := shardContext.UpdateWorkflowExecution(ctx, request)
	if err != nil {
		shardContext.GetLogger().Error(
			"Update workflow execution operation failed.",
			tag.WorkflowNamespaceID(request.UpdateWorkflowMutation.ExecutionInfo.NamespaceId),
			tag.WorkflowID(request.UpdateWorkflowMutation.ExecutionInfo.WorkflowId),
			tag.WorkflowRunID(request.UpdateWorkflowMutation.ExecutionState.RunId),
			tag.StoreOperationUpdateWorkflowExecution,
			tag.Error(err),
		)
		return nil, err
	}

	if namespaceEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(request.UpdateWorkflowMutation.ExecutionInfo.NamespaceId),
	); err == nil {
		emitMutationMetrics(
			shardContext,
			namespaceEntry,
			request.ArchetypeID,
			&resp.UpdateMutableStateStats,
			resp.NewMutableStateStats,
		)

		emitCompletionMetrics(
			shardContext,
			namespaceEntry,
			mutationToCompletionMetric(
				namespaceState(shardContext.GetClusterMetadata(), &updateWorkflowFailoverVersion),
				&request.UpdateWorkflowMutation,
				request.UpdateWorkflowEvents,
				isWorkflow,
			),
			snapshotToCompletionMetric(
				namespaceState(shardContext.GetClusterMetadata(), newWorkflowFailoverVersion),
				request.NewWorkflowSnapshot,
				request.NewWorkflowEvents,
				isWorkflow,
			),
		)
	}

	return resp, nil
}

func setWorkflowExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	request *persistence.SetWorkflowExecutionRequest,
) (*persistence.SetWorkflowExecutionResponse, error) {

	resp, err := shardContext.SetWorkflowExecution(ctx, request)
	if err != nil {
		shardContext.GetLogger().Error(
			"Set workflow execution operation failed.",
			tag.WorkflowNamespaceID(request.SetWorkflowSnapshot.ExecutionInfo.NamespaceId),
			tag.WorkflowID(request.SetWorkflowSnapshot.ExecutionInfo.WorkflowId),
			tag.WorkflowRunID(request.SetWorkflowSnapshot.ExecutionState.RunId),
			tag.StoreOperationUpdateWorkflowExecution,
			tag.Error(err),
		)
		return nil, err
	}
	return resp, nil
}

func NotifyOnExecutionSnapshot(
	engine historyi.Engine,
	workflowSnapshot *persistence.WorkflowSnapshot,
) {
	if workflowSnapshot == nil {
		return
	}
	engine.NotifyNewTasks(workflowSnapshot.Tasks)
	if len(workflowSnapshot.ChasmNodes) > 0 {
		engine.NotifyChasmExecution(chasm.ExecutionKey{
			NamespaceID: workflowSnapshot.ExecutionInfo.NamespaceId,
			BusinessID:  workflowSnapshot.ExecutionInfo.WorkflowId,
			RunID:       workflowSnapshot.ExecutionState.RunId,
		}, nil)
	}
	notifyFastForwardUpdate(engine, workflowSnapshot.ExecutionInfo, workflowSnapshot.ExecutionState)
}

func NotifyOnExecutionMutation(
	engine historyi.Engine,
	workflowMutation *persistence.WorkflowMutation,
) {
	if workflowMutation == nil {
		return
	}
	engine.NotifyNewTasks(workflowMutation.Tasks)
	if len(workflowMutation.UpsertChasmNodes) > 0 ||
		len(workflowMutation.DeleteChasmNodes) > 0 {
		engine.NotifyChasmExecution(chasm.ExecutionKey{
			NamespaceID: workflowMutation.ExecutionInfo.NamespaceId,
			BusinessID:  workflowMutation.ExecutionInfo.WorkflowId,
			RunID:       workflowMutation.ExecutionState.RunId,
		}, nil)
	}
	notifyFastForwardUpdate(engine, workflowMutation.ExecutionInfo, workflowMutation.ExecutionState)
}

// notifyFastForwardUpdate wakes any PollWorkflowExecutionTimeSkipping waiters for
// this execution whenever a persisted transaction carries fast-forward state.
// It fires on every persist that has fast-forward info (not only on change); the
// waiter re-evaluates the delivered info and keeps waiting on a no-op wake.
//
// When the run has closed without a continuation (no retry / cron / CaN, i.e.
// terminal state and no NewExecutionRunId), the notification is flagged Closed so
// waiters return instead of blocking on a fast-forward that can no longer complete.
func notifyFastForwardUpdate(
	engine historyi.Engine,
	executionInfo *persistencespb.WorkflowExecutionInfo,
	executionState *persistencespb.WorkflowExecutionState,
) {
	ffInfo := NewTimeSkippingInfoUtil(executionInfo.GetTimeSkippingInfo()).ToFastForwardInfo()
	if ffInfo == nil {
		return
	}
	// STATE_COMPLETED means the run is closed (any terminal status: completed,
	// failed, canceled, terminated, timed-out), not "succeeded". NewExecutionRunId
	// is empty only when there is no successor run — retry / cron / CaN set it.
	closed := executionState.GetState() == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED &&
		executionInfo.GetNewExecutionRunId() == ""
	key := notification.NewKey(executionInfo.GetNamespaceId(), executionInfo.GetWorkflowId())
	engine.NotifyFastForwardUpdate(key, &notification.FastForwardNotification{
		FastForwardInfo: ffInfo,
		Closed:          closed,
	})
}

func NotifyNewHistorySnapshotEvent(
	engine historyi.Engine,
	workflowSnapshot *persistence.WorkflowSnapshot,
) error {

	if workflowSnapshot == nil {
		return nil
	}

	executionInfo := workflowSnapshot.ExecutionInfo
	executionState := workflowSnapshot.ExecutionState

	namespaceID := executionInfo.NamespaceId
	workflowID := executionInfo.WorkflowId
	runID := executionState.RunId
	workflowState := executionState.State
	workflowStatus := executionState.Status
	lastFirstEventID := executionInfo.LastFirstEventId
	lastFirstEventTxnID := executionInfo.LastFirstEventTxnId
	lastWorkflowTaskStartEventID := executionInfo.LastCompletedWorkflowTaskStartedEventId
	nextEventID := workflowSnapshot.NextEventID

	engine.NotifyNewHistoryEvent(events.NewNotification(
		namespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		lastFirstEventID,
		lastFirstEventTxnID,
		nextEventID,
		lastWorkflowTaskStartEventID,
		workflowState,
		workflowStatus,
		executionInfo.VersionHistories,
		executionInfo.TransitionHistory,
	))
	return nil
}

func NotifyNewHistoryMutationEvent(
	engine historyi.Engine,
	workflowMutation *persistence.WorkflowMutation,
) error {

	if workflowMutation == nil {
		return nil
	}

	executionInfo := workflowMutation.ExecutionInfo
	executionState := workflowMutation.ExecutionState

	namespaceID := executionInfo.NamespaceId
	workflowID := executionInfo.WorkflowId
	runID := executionState.RunId
	workflowState := executionState.State
	workflowStatus := executionState.Status
	lastFirstEventID := executionInfo.LastFirstEventId
	lastFirstEventTxnID := executionInfo.LastFirstEventTxnId
	lastWorkflowTaskStartEventID := executionInfo.LastCompletedWorkflowTaskStartedEventId
	nextEventID := workflowMutation.NextEventID

	engine.NotifyNewHistoryEvent(events.NewNotification(
		namespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		lastFirstEventID,
		lastFirstEventTxnID,
		nextEventID,
		lastWorkflowTaskStartEventID,
		workflowState,
		workflowStatus,
		executionInfo.VersionHistories,
		executionInfo.TransitionHistory,
	))
	return nil
}

func emitMutationMetrics(
	shardContext historyi.ShardContext,
	namespace *namespace.Namespace,
	archetypeID chasm.ArchetypeID,
	stats ...*persistence.MutableStateStatistics,
) {
	metricsHandler := shardContext.GetMetricsHandler()
	chasmRegistry := shardContext.ChasmRegistry()
	namespaceName := namespace.Name()
	for _, stat := range stats {
		emitMutableStateStatus(
			metricsHandler.WithTags(metrics.OperationTag(metrics.SessionStatsScope), metrics.NamespaceTag(namespaceName.String())),
			chasmRegistry,
			archetypeID,
			stat,
		)
	}
}

func emitGetMetrics(
	shardContext historyi.ShardContext,
	namespace *namespace.Namespace,
	archetypeID chasm.ArchetypeID,
	stats ...*persistence.MutableStateStatistics,
) {
	metricsHandler := shardContext.GetMetricsHandler()
	chasmRegistry := shardContext.ChasmRegistry()
	namespaceName := namespace.Name()
	for _, stat := range stats {
		emitMutableStateStatus(
			metricsHandler.WithTags(metrics.OperationTag(metrics.ExecutionStatsScope), metrics.NamespaceTag(namespaceName.String())),
			chasmRegistry,
			archetypeID,
			stat,
		)
	}
}

// wroteEvents reports whether the run wrote any history events in this transaction.
func wroteEvents(eventsSeq []*persistence.WorkflowEvents) bool {
	for _, batch := range eventsSeq {
		if len(batch.Events) > 0 {
			return true
		}
	}
	return false
}

func snapshotToCompletionMetric(
	namespaceState string,
	workflowSnapshot *persistence.WorkflowSnapshot,
	eventsSeq []*persistence.WorkflowEvents,
	isWorkflow bool,
) completionMetric {
	if workflowSnapshot == nil {
		return completionMetric{shouldRecord: false}
	}

	return completionMetric{
		// Record a completion only when the run wrote events (closed) in this transaction.
		shouldRecord:     wroteEvents(eventsSeq),
		isWorkflow:       isWorkflow,
		taskQueue:        workflowSnapshot.ExecutionInfo.TaskQueue,
		namespaceState:   namespaceState,
		workflowTypeName: workflowSnapshot.ExecutionInfo.WorkflowTypeName,
		status:           workflowSnapshot.ExecutionState.Status,
		startTime:        workflowSnapshot.ExecutionState.StartTime,
		closeTime:        workflowSnapshot.ExecutionInfo.CloseTime,
	}
}

func mutationToCompletionMetric(
	namespaceState string,
	workflowMutation *persistence.WorkflowMutation,
	eventsSeq []*persistence.WorkflowEvents,
	isWorkflow bool,
) completionMetric {
	if workflowMutation == nil {
		return completionMetric{shouldRecord: false}
	}

	return completionMetric{
		shouldRecord:     wroteEvents(eventsSeq),
		isWorkflow:       isWorkflow,
		taskQueue:        workflowMutation.ExecutionInfo.TaskQueue,
		namespaceState:   namespaceState,
		workflowTypeName: workflowMutation.ExecutionInfo.WorkflowTypeName,
		status:           workflowMutation.ExecutionState.Status,
		startTime:        workflowMutation.ExecutionState.StartTime,
		closeTime:        workflowMutation.ExecutionInfo.CloseTime,
	}
}

func emitCompletionMetrics(
	shardContext historyi.ShardContext,
	namespace *namespace.Namespace,
	completionMetrics ...completionMetric,
) {
	metricsHandler := shardContext.GetMetricsHandler()
	namespaceName := namespace.Name()

	for _, completionMetric := range completionMetrics {
		if !completionMetric.shouldRecord {
			continue
		}

		emitWorkflowCompletionStats(
			metricsHandler,
			namespaceName,
			completionMetric,
			shardContext.GetConfig(),
		)
	}
}
