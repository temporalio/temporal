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

package workflow

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
)

type (
	completionMetric struct {
		initialized    bool
		taskQueue      string
		namespaceState string
		status         enumspb.WorkflowExecutionStatus
	}
	TransactionImpl struct {
		shard  shard.Context
		logger log.Logger
	}
)

var _ Transaction = (*TransactionImpl)(nil)

func NewTransaction(
	shard shard.Context,
) *TransactionImpl {
	return &TransactionImpl{
		shard:  shard,
		logger: shard.GetLogger(),
	}
}

func (t *TransactionImpl) CreateWorkflowExecution(
	ctx context.Context,
	createMode persistence.CreateWorkflowMode,
	newWorkflowFailoverVersion int64,
	newWorkflowSnapshot *persistence.WorkflowSnapshot,
	newWorkflowEventsSeq []*persistence.WorkflowEvents,
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
			NewWorkflowSnapshot: *newWorkflowSnapshot,
			NewWorkflowEvents:   newWorkflowEventsSeq,
		},
	)
	if shard.OperationPossiblySucceeded(err) {
		NotifyWorkflowSnapshotTasks(engine, newWorkflowSnapshot)
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
	resetWorkflowFailoverVersion int64,
	resetWorkflowSnapshot *persistence.WorkflowSnapshot,
	resetWorkflowEventsSeq []*persistence.WorkflowEvents,
	newWorkflowFailoverVersion *int64,
	newWorkflowSnapshot *persistence.WorkflowSnapshot,
	newWorkflowEventsSeq []*persistence.WorkflowEvents,
	currentWorkflowFailoverVersion *int64,
	currentWorkflowMutation *persistence.WorkflowMutation,
	currentWorkflowEventsSeq []*persistence.WorkflowEvents,
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
			ResetWorkflowSnapshot:   *resetWorkflowSnapshot,
			ResetWorkflowEvents:     resetWorkflowEventsSeq,
			NewWorkflowSnapshot:     newWorkflowSnapshot,
			NewWorkflowEvents:       newWorkflowEventsSeq,
			CurrentWorkflowMutation: currentWorkflowMutation,
			CurrentWorkflowEvents:   currentWorkflowEventsSeq,
		},
	)
	if shard.OperationPossiblySucceeded(err) {
		NotifyWorkflowSnapshotTasks(engine, resetWorkflowSnapshot)
		NotifyWorkflowSnapshotTasks(engine, newWorkflowSnapshot)
		NotifyWorkflowMutationTasks(engine, currentWorkflowMutation)
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
	currentWorkflowFailoverVersion int64,
	currentWorkflowMutation *persistence.WorkflowMutation,
	currentWorkflowEventsSeq []*persistence.WorkflowEvents,
	newWorkflowFailoverVersion *int64,
	newWorkflowSnapshot *persistence.WorkflowSnapshot,
	newWorkflowEventsSeq []*persistence.WorkflowEvents,
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
			UpdateWorkflowMutation: *currentWorkflowMutation,
			UpdateWorkflowEvents:   currentWorkflowEventsSeq,
			NewWorkflowSnapshot:    newWorkflowSnapshot,
			NewWorkflowEvents:      newWorkflowEventsSeq,
		},
	)
	if shard.OperationPossiblySucceeded(err) {
		NotifyWorkflowMutationTasks(engine, currentWorkflowMutation)
		NotifyWorkflowSnapshotTasks(engine, newWorkflowSnapshot)
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
	workflowSnapshot *persistence.WorkflowSnapshot,
) error {

	engine, err := t.shard.GetEngine(ctx)
	if err != nil {
		return err
	}
	_, err = setWorkflowExecution(ctx, t.shard, &persistence.SetWorkflowExecutionRequest{
		ShardID: t.shard.GetShardID(),
		// RangeID , this is set by shard context
		SetWorkflowSnapshot: *workflowSnapshot,
	})
	if shard.OperationPossiblySucceeded(err) {
		NotifyWorkflowSnapshotTasks(engine, workflowSnapshot)
	}
	if err != nil {
		return err
	}

	return nil
}

func PersistWorkflowEvents(
	ctx context.Context,
	shard shard.Context,
	workflowEventsSlice ...*persistence.WorkflowEvents,
) (int64, error) {

	var totalSize int64
	for _, workflowEvents := range workflowEventsSlice {
		if len(workflowEvents.Events) == 0 {
			continue // allow update workflow without events
		}

		firstEventID := workflowEvents.Events[0].EventId
		if firstEventID == common.FirstEventID {
			size, err := persistFirstWorkflowEvents(ctx, shard, workflowEvents)
			if err != nil {
				return 0, err
			}
			totalSize += size
		} else {
			size, err := persistNonFirstWorkflowEvents(ctx, shard, workflowEvents)
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
	shard shard.Context,
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
		shard,
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
	shard shard.Context,
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
		shard,
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
	shard shard.Context,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	request *persistence.AppendHistoryNodesRequest,
) (int64, error) {

	resp, err := shard.AppendHistoryEvents(ctx, request, namespaceID, execution)
	return int64(resp), err
}

func createWorkflowExecution(
	ctx context.Context,
	shard shard.Context,
	mutableStateFailoverVersion int64,
	request *persistence.CreateWorkflowExecutionRequest,
) (*persistence.CreateWorkflowExecutionResponse, error) {

	resp, err := shard.CreateWorkflowExecution(ctx, request)
	if err != nil {
		switch err.(type) {
		case *persistence.CurrentWorkflowConditionFailedError,
			*persistence.WorkflowConditionFailedError,
			*persistence.ConditionFailedError:
			// it is possible that workflow already exists and caller need to apply
			// workflow ID reuse policy
			return nil, err
		default:
			shard.GetLogger().Error(
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

	if namespaceEntry, err := shard.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(request.NewWorkflowSnapshot.ExecutionInfo.NamespaceId),
	); err == nil {
		emitMutationMetrics(
			shard,
			namespaceEntry,
			&resp.NewMutableStateStats,
		)
		emitCompletionMetrics(
			shard,
			namespaceEntry,
			snapshotToCompletionMetric(
				namespaceState(shard.GetClusterMetadata(), &mutableStateFailoverVersion),
				&request.NewWorkflowSnapshot,
			),
		)
	}
	return resp, nil
}

func conflictResolveWorkflowExecution(
	ctx context.Context,
	shard shard.Context,
	resetWorkflowFailoverVersion int64,
	newWorkflowFailoverVersion *int64,
	currentWorkflowFailoverVersion *int64,
	request *persistence.ConflictResolveWorkflowExecutionRequest,
) (*persistence.ConflictResolveWorkflowExecutionResponse, error) {

	resp, err := shard.ConflictResolveWorkflowExecution(ctx, request)
	if err != nil {
		shard.GetLogger().Error(
			"Persistent store operation Failure",
			tag.WorkflowNamespaceID(request.ResetWorkflowSnapshot.ExecutionInfo.NamespaceId),
			tag.WorkflowID(request.ResetWorkflowSnapshot.ExecutionInfo.WorkflowId),
			tag.WorkflowRunID(request.ResetWorkflowSnapshot.ExecutionState.RunId),
			tag.StoreOperationConflictResolveWorkflowExecution,
			tag.Error(err),
		)
		return nil, err
	}

	if namespaceEntry, err := shard.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(request.ResetWorkflowSnapshot.ExecutionInfo.NamespaceId),
	); err == nil {
		emitMutationMetrics(
			shard,
			namespaceEntry,
			&resp.ResetMutableStateStats,
			resp.NewMutableStateStats,
			resp.CurrentMutableStateStats,
		)
		emitCompletionMetrics(
			shard,
			namespaceEntry,
			snapshotToCompletionMetric(
				namespaceState(shard.GetClusterMetadata(), &resetWorkflowFailoverVersion),
				&request.ResetWorkflowSnapshot,
			),
			snapshotToCompletionMetric(
				namespaceState(shard.GetClusterMetadata(), newWorkflowFailoverVersion),
				request.NewWorkflowSnapshot,
			),
			mutationToCompletionMetric(
				namespaceState(shard.GetClusterMetadata(), currentWorkflowFailoverVersion),
				request.CurrentWorkflowMutation,
			),
		)
	}
	return resp, nil
}

func getWorkflowExecution(
	ctx context.Context,
	shard shard.Context,
	request *persistence.GetWorkflowExecutionRequest,
) (*persistence.GetWorkflowExecutionResponse, error) {

	resp, err := shard.GetWorkflowExecution(ctx, request)
	if err != nil {
		switch err.(type) {
		case *serviceerror.NotFound:
			// It is possible that workflow does not exist.
			return nil, err
		default:
			shard.GetLogger().Error(
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

	if namespaceEntry, err := shard.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(resp.State.ExecutionInfo.NamespaceId),
	); err == nil {
		emitGetMetrics(
			shard,
			namespaceEntry,
			&resp.MutableStateStats,
		)
	}
	return resp, nil
}

func updateWorkflowExecution(
	ctx context.Context,
	shard shard.Context,
	updateWorkflowFailoverVersion int64,
	newWorkflowFailoverVersion *int64,
	request *persistence.UpdateWorkflowExecutionRequest,
) (*persistence.UpdateWorkflowExecutionResponse, error) {

	resp, err := shard.UpdateWorkflowExecution(ctx, request)
	if err != nil {
		shard.GetLogger().Error(
			"Update workflow execution operation failed.",
			tag.WorkflowNamespaceID(request.UpdateWorkflowMutation.ExecutionInfo.NamespaceId),
			tag.WorkflowID(request.UpdateWorkflowMutation.ExecutionInfo.WorkflowId),
			tag.WorkflowRunID(request.UpdateWorkflowMutation.ExecutionState.RunId),
			tag.StoreOperationUpdateWorkflowExecution,
			tag.Error(err),
		)
		return nil, err
	}

	if namespaceEntry, err := shard.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(request.UpdateWorkflowMutation.ExecutionInfo.NamespaceId),
	); err == nil {
		emitMutationMetrics(
			shard,
			namespaceEntry,
			&resp.UpdateMutableStateStats,
			resp.NewMutableStateStats,
		)
		emitCompletionMetrics(
			shard,
			namespaceEntry,
			mutationToCompletionMetric(
				namespaceState(shard.GetClusterMetadata(), &updateWorkflowFailoverVersion),
				&request.UpdateWorkflowMutation,
			),
			snapshotToCompletionMetric(
				namespaceState(shard.GetClusterMetadata(), newWorkflowFailoverVersion),
				request.NewWorkflowSnapshot,
			),
		)
	}

	return resp, nil
}

func setWorkflowExecution(
	ctx context.Context,
	shard shard.Context,
	request *persistence.SetWorkflowExecutionRequest,
) (*persistence.SetWorkflowExecutionResponse, error) {

	resp, err := shard.SetWorkflowExecution(ctx, request)
	if err != nil {
		shard.GetLogger().Error(
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

func NotifyWorkflowSnapshotTasks(
	engine shard.Engine,
	workflowSnapshot *persistence.WorkflowSnapshot,
) {
	if workflowSnapshot == nil {
		return
	}
	engine.NotifyNewTasks(workflowSnapshot.Tasks)
}

func NotifyWorkflowMutationTasks(
	engine shard.Engine,
	workflowMutation *persistence.WorkflowMutation,
) {
	if workflowMutation == nil {
		return
	}
	engine.NotifyNewTasks(workflowMutation.Tasks)
}

func NotifyNewHistorySnapshotEvent(
	engine shard.Engine,
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
	lastWorkflowTaskStartEventID := executionInfo.LastWorkflowTaskStartedEventId
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
	))
	return nil
}

func NotifyNewHistoryMutationEvent(
	engine shard.Engine,
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
	lastWorkflowTaskStartEventID := executionInfo.LastWorkflowTaskStartedEventId
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
	))
	return nil
}

func emitMutationMetrics(
	shard shard.Context,
	namespace *namespace.Namespace,
	stats ...*persistence.MutableStateStatistics,
) {
	metricsHandler := shard.GetMetricsHandler()
	namespaceName := namespace.Name()
	for _, stat := range stats {
		emitMutableStateStatus(
			metricsHandler.WithTags(metrics.OperationTag(metrics.SessionStatsScope), metrics.NamespaceTag(namespaceName.String())),
			stat,
		)
	}
}

func emitGetMetrics(
	shard shard.Context,
	namespace *namespace.Namespace,
	stats ...*persistence.MutableStateStatistics,
) {
	metricsHandler := shard.GetMetricsHandler()
	namespaceName := namespace.Name()
	for _, stat := range stats {
		emitMutableStateStatus(
			metricsHandler.WithTags(metrics.OperationTag(metrics.ExecutionStatsScope), metrics.NamespaceTag(namespaceName.String())),
			stat,
		)
	}
}

func snapshotToCompletionMetric(
	namespaceState string,
	workflowSnapshot *persistence.WorkflowSnapshot,
) completionMetric {
	if workflowSnapshot == nil {
		return completionMetric{initialized: false}
	}
	return completionMetric{
		initialized:    true,
		taskQueue:      workflowSnapshot.ExecutionInfo.TaskQueue,
		namespaceState: namespaceState,
		status:         workflowSnapshot.ExecutionState.Status,
	}
}

func mutationToCompletionMetric(
	namespaceState string,
	workflowMutation *persistence.WorkflowMutation,
) completionMetric {
	if workflowMutation == nil {
		return completionMetric{initialized: false}
	}
	return completionMetric{
		initialized:    true,
		taskQueue:      workflowMutation.ExecutionInfo.TaskQueue,
		namespaceState: namespaceState,
		status:         workflowMutation.ExecutionState.Status,
	}
}

func emitCompletionMetrics(
	shard shard.Context,
	namespace *namespace.Namespace,
	completionMetrics ...completionMetric,
) {
	metricsHandler := shard.GetMetricsHandler()
	namespaceName := namespace.Name()

	for _, completionMetric := range completionMetrics {
		if !completionMetric.initialized {
			continue
		}
		emitWorkflowCompletionStats(
			metricsHandler,
			namespaceName,
			completionMetric.namespaceState,
			completionMetric.taskQueue,
			completionMetric.status,
		)
	}
}
