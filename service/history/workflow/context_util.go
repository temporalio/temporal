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
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
)

func CreateWorkflowExecution(
	shard shard.Context,
	createMode persistence.CreateWorkflowMode,
	newWorkflowSnapshot *persistence.WorkflowSnapshot,
	newWorkflowEventsSeq []*persistence.WorkflowEvents,
) (int64, error) {

	newWorkflowHistorySizeDiff := int64(0)

	for _, workflowEvents := range newWorkflowEventsSeq {
		eventsSize, err := PersistWorkflowEvents(shard, workflowEvents)
		if err != nil {
			return 0, err
		}
		newWorkflowHistorySizeDiff += eventsSize
	}
	newWorkflowSnapshot.ExecutionInfo.ExecutionStats.HistorySize += newWorkflowHistorySizeDiff

	if err := createWorkflowExecutionWithRetry(shard, &persistence.CreateWorkflowExecutionRequest{
		// RangeID , this is set by shard context
		Mode:                createMode,
		NewWorkflowSnapshot: *newWorkflowSnapshot,
	}); err != nil {
		return 0, err
	}
	return newWorkflowHistorySizeDiff, nil
}

func ConflictResolveWorkflowExecution(
	shard shard.Context,
	conflictResolveMode persistence.ConflictResolveWorkflowMode,
	resetWorkflowSnapshot *persistence.WorkflowSnapshot,
	resetWorkflowEventsSeq []*persistence.WorkflowEvents,
	newWorkflowSnapshot *persistence.WorkflowSnapshot,
	newWorkflowEventsSeq []*persistence.WorkflowEvents,
	currentWorkflowMutation *persistence.WorkflowMutation,
	currentWorkflowEventsSeq []*persistence.WorkflowEvents,
) (int64, int64, int64, error) {

	resetHistorySizeDiff := int64(0)
	newWorkflowHistorySizeDiff := int64(0)
	currentWorkflowHistorySizeDiff := int64(0)

	for _, workflowEvents := range resetWorkflowEventsSeq {
		eventsSize, err := PersistWorkflowEvents(shard, workflowEvents)
		if err != nil {
			return 0, 0, 0, err
		}
		resetHistorySizeDiff += eventsSize
	}
	resetWorkflowSnapshot.ExecutionInfo.ExecutionStats.HistorySize += resetHistorySizeDiff

	if newWorkflowSnapshot != nil {
		for _, workflowEvents := range newWorkflowEventsSeq {
			eventsSize, err := PersistWorkflowEvents(shard, workflowEvents)
			if err != nil {
				return 0, 0, 0, err
			}
			newWorkflowHistorySizeDiff += eventsSize
		}
		newWorkflowSnapshot.ExecutionInfo.ExecutionStats.HistorySize += newWorkflowHistorySizeDiff
	}

	if currentWorkflowMutation != nil {
		for _, workflowEvents := range currentWorkflowEventsSeq {
			eventsSize, err := PersistWorkflowEvents(shard, workflowEvents)
			if err != nil {
				return 0, 0, 0, err
			}
			currentWorkflowHistorySizeDiff += eventsSize
		}
		currentWorkflowMutation.ExecutionInfo.ExecutionStats.HistorySize += currentWorkflowHistorySizeDiff
	}

	if err := shard.ConflictResolveWorkflowExecution(&persistence.ConflictResolveWorkflowExecutionRequest{
		// RangeID , this is set by shard context
		Mode:                    conflictResolveMode,
		ResetWorkflowSnapshot:   *resetWorkflowSnapshot,
		NewWorkflowSnapshot:     newWorkflowSnapshot,
		CurrentWorkflowMutation: currentWorkflowMutation,
	}); err != nil {
		return 0, 0, 0, err
	}
	return resetHistorySizeDiff, newWorkflowHistorySizeDiff, currentWorkflowHistorySizeDiff, nil
}

func UpdateWorkflowExecution(
	shard shard.Context,
	updateMode persistence.UpdateWorkflowMode,
	currentWorkflowMutation *persistence.WorkflowMutation,
	currentWorkflowEventsSeq []*persistence.WorkflowEvents,
	newWorkflowSnapshot *persistence.WorkflowSnapshot,
	newWorkflowEventsSeq []*persistence.WorkflowEvents,
) (int64, int64, error) {

	currentWorkflowHistorySizeDiff := int64(0)
	newWorkflowHistorySizeDiff := int64(0)

	for _, workflowEvents := range currentWorkflowEventsSeq {
		eventsSize, err := PersistWorkflowEvents(shard, workflowEvents)
		if err != nil {
			return 0, 0, err
		}
		currentWorkflowHistorySizeDiff += eventsSize
	}
	currentWorkflowMutation.ExecutionInfo.ExecutionStats.HistorySize += currentWorkflowHistorySizeDiff

	if newWorkflowSnapshot != nil {
		for _, workflowEvents := range newWorkflowEventsSeq {
			eventsSize, err := PersistWorkflowEvents(shard, workflowEvents)
			if err != nil {
				return 0, 0, err
			}
			newWorkflowHistorySizeDiff += eventsSize
		}
		newWorkflowSnapshot.ExecutionInfo.ExecutionStats.HistorySize += newWorkflowHistorySizeDiff
	}

	if err := updateWorkflowExecutionWithRetry(shard, &persistence.UpdateWorkflowExecutionRequest{
		// RangeID , this is set by shard context
		Mode:                   updateMode,
		UpdateWorkflowMutation: *currentWorkflowMutation,
		NewWorkflowSnapshot:    newWorkflowSnapshot,
	}); err != nil {
		return 0, 0, err
	}
	return currentWorkflowHistorySizeDiff, newWorkflowHistorySizeDiff, nil
}

func PersistWorkflowEvents(
	shard shard.Context,
	workflowEvents *persistence.WorkflowEvents,
) (int64, error) {

	if len(workflowEvents.Events) == 0 {
		return 0, nil // allow update workflow without events
	}

	firstEventID := workflowEvents.Events[0].EventId
	if firstEventID == common.FirstEventID {
		return persistFirstWorkflowEvents(shard, workflowEvents)
	}
	return persistNonFirstWorkflowEvents(shard, workflowEvents)
}

func persistFirstWorkflowEvents(
	shard shard.Context,
	workflowEvents *persistence.WorkflowEvents,
) (int64, error) {

	namespaceID := workflowEvents.NamespaceID
	workflowID := workflowEvents.WorkflowID
	runID := workflowEvents.RunID
	execution := commonpb.WorkflowExecution{
		WorkflowId: workflowEvents.WorkflowID,
		RunId:      workflowEvents.RunID,
	}
	branchToken := workflowEvents.BranchToken
	events := workflowEvents.Events
	prevTxnID := workflowEvents.PrevTxnID
	txnID := workflowEvents.TxnID

	size, err := appendHistoryV2EventsWithRetry(
		shard,
		namespaceID,
		execution,
		&persistence.AppendHistoryNodesRequest{
			IsNewBranch:       true,
			Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, workflowID, runID),
			BranchToken:       branchToken,
			Events:            events,
			PrevTransactionID: prevTxnID,
			TransactionID:     txnID,
		},
	)
	return size, err
}

func persistNonFirstWorkflowEvents(
	shard shard.Context,
	workflowEvents *persistence.WorkflowEvents,
) (int64, error) {

	if len(workflowEvents.Events) == 0 {
		return 0, nil // allow update workflow without events
	}

	namespaceID := workflowEvents.NamespaceID
	execution := commonpb.WorkflowExecution{
		WorkflowId: workflowEvents.WorkflowID,
		RunId:      workflowEvents.RunID,
	}
	branchToken := workflowEvents.BranchToken
	events := workflowEvents.Events
	prevTxnID := workflowEvents.PrevTxnID
	txnID := workflowEvents.TxnID

	size, err := appendHistoryV2EventsWithRetry(
		shard,
		namespaceID,
		execution,
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

func appendHistoryV2EventsWithRetry(
	shard shard.Context,
	namespaceID string,
	execution commonpb.WorkflowExecution,
	request *persistence.AppendHistoryNodesRequest,
) (int64, error) {

	resp := 0
	op := func() error {
		var err error
		resp, err = shard.AppendHistoryEvents(request, namespaceID, execution)
		return err
	}

	err := backoff.Retry(
		op,
		PersistenceOperationRetryPolicy,
		common.IsPersistenceTransientError,
	)
	return int64(resp), err
}

func createWorkflowExecutionWithRetry(
	shard shard.Context,
	request *persistence.CreateWorkflowExecutionRequest,
) error {

	op := func() error {
		_, err := shard.CreateWorkflowExecution(request)
		return err
	}

	err := backoff.Retry(
		op,
		PersistenceOperationRetryPolicy,
		common.IsPersistenceTransientError,
	)
	switch err.(type) {
	case nil:
		return nil
	case *persistence.WorkflowExecutionAlreadyStartedError:
		// it is possible that workflow already exists and caller need to apply
		// workflow ID reuse policy
		return err
	default:
		shard.GetLogger().Error(
			"Persistent store operation Failure",
			tag.WorkflowNamespaceID(request.NewWorkflowSnapshot.ExecutionInfo.NamespaceId),
			tag.WorkflowID(request.NewWorkflowSnapshot.ExecutionInfo.WorkflowId),
			tag.WorkflowRunID(request.NewWorkflowSnapshot.ExecutionState.RunId),
			tag.StoreOperationCreateWorkflowExecution,
			tag.Error(err),
		)
		return err
	}
}

func getWorkflowExecutionWithRetry(
	shard shard.Context,
	request *persistence.GetWorkflowExecutionRequest,
) (*persistence.GetWorkflowExecutionResponse, error) {

	var resp *persistence.GetWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = shard.GetExecutionManager().GetWorkflowExecution(request)

		return err
	}

	err := backoff.Retry(
		op,
		PersistenceOperationRetryPolicy,
		common.IsPersistenceTransientError,
	)
	switch err.(type) {
	case nil:
		return resp, nil
	case *serviceerror.NotFound:
		// it is possible that workflow does not exists
		return nil, err
	default:
		shard.GetLogger().Error(
			"Persistent fetch operation Failure",
			tag.WorkflowNamespaceID(request.NamespaceID),
			tag.WorkflowID(request.Execution.WorkflowId),
			tag.WorkflowRunID(request.Execution.RunId),
			tag.StoreOperationGetWorkflowExecution,
			tag.Error(err),
		)
		return nil, err
	}
}

func updateWorkflowExecutionWithRetry(
	shard shard.Context,
	request *persistence.UpdateWorkflowExecutionRequest,
) error {

	var resp *persistence.UpdateWorkflowExecutionResponse
	var err error
	op := func() error {
		resp, err = shard.UpdateWorkflowExecution(request)
		return err
	}

	err = backoff.Retry(
		op, PersistenceOperationRetryPolicy,
		common.IsPersistenceTransientError,
	)
	switch err.(type) {
	case nil:
		// TODO @wxing1292
		//  temporarily move the emission of per update mutable state metrics
		//  to here, long term story, this emission of metrics should have a
		//  dedicated layer
		if namespaceEntry, err := shard.GetNamespaceCache().GetNamespaceByID(
			request.UpdateWorkflowMutation.ExecutionInfo.NamespaceId,
		); err == nil {
			emitSessionUpdateStats(
				shard.GetMetricsClient(),
				namespaceEntry.GetInfo().Name,
				resp.MutableStateUpdateSessionStats,
			)
		}
		return nil
	case *persistence.ConditionFailedError:
		// TODO get rid of ErrConflict
		return consts.ErrConflict
	default:
		shard.GetLogger().Error(
			"Persistent store operation Failure",
			tag.WorkflowNamespaceID(request.UpdateWorkflowMutation.ExecutionInfo.NamespaceId),
			tag.WorkflowID(request.UpdateWorkflowMutation.ExecutionInfo.WorkflowId),
			tag.WorkflowRunID(request.UpdateWorkflowMutation.ExecutionState.RunId),
			tag.StoreOperationUpdateWorkflowExecution,
			tag.Error(err),
		)
		return err
	}
}

func notifyWorkflowSnapshotTasks(
	engine shard.Engine,
	workflowSnapshot *persistence.WorkflowSnapshot,
) {
	if workflowSnapshot == nil {
		return
	}
	notifyTasks(
		engine,
		workflowSnapshot.TransferTasks,
		workflowSnapshot.TimerTasks,
		workflowSnapshot.ReplicationTasks,
		workflowSnapshot.VisibilityTasks,
	)
}

func notifyWorkflowMutationTasks(
	engine shard.Engine,
	workflowMutation *persistence.WorkflowMutation,
) {
	if workflowMutation == nil {
		return
	}
	notifyTasks(
		engine,
		workflowMutation.TransferTasks,
		workflowMutation.TimerTasks,
		workflowMutation.ReplicationTasks,
		workflowMutation.VisibilityTasks,
	)
}

func notifyTasks(
	engine shard.Engine,
	transferTasks []persistence.Task,
	timerTasks []persistence.Task,
	replicationTasks []persistence.Task,
	visibilityTasks []persistence.Task,
) {
	engine.NotifyNewTransferTasks(transferTasks)
	engine.NotifyNewTimerTasks(timerTasks)
	engine.NotifyNewVisibilityTasks(visibilityTasks)
	engine.NotifyNewReplicationTasks(replicationTasks)
}
