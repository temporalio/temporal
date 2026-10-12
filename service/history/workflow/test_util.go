package workflow

import (
	"context"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
)

func TestLocalMutableState(
	shard historyi.ShardContext,
	eventsCache events.Cache,
	ns *namespace.Namespace,
	workflowID string,
	runID string,
	logger log.Logger,
) *MutableStateImpl {

	ms := NewMutableState(shard, eventsCache, logger, ns, workflowID, runID, time.Now().UTC())
	ms.executionInfo.NamespaceId = string(ns.ID())
	ms.executionInfo.WorkflowId = workflowID
	ms.executionState.RunId = runID
	ms.GetExecutionInfo().ExecutionTime = ms.GetExecutionState().StartTime
	_ = ms.SetHistoryTree(nil, nil, runID)

	return ms
}

// NewMapEventCache is a functional event cache mock that wraps a simple Go map
func TestGlobalMutableState(
	shard historyi.ShardContext,
	eventsCache events.Cache,
	logger log.Logger,
	version int64,
	workflowID string,
	runID string,
) *MutableStateImpl {

	ms := NewMutableState(shard, eventsCache, logger, tests.GlobalNamespaceEntry, workflowID, runID, time.Now().UTC())
	ms.GetExecutionInfo().ExecutionTime = ms.GetExecutionState().StartTime
	ms.GetExecutionInfo().TransitionHistory = UpdatedTransitionHistory(ms.GetExecutionInfo().TransitionHistory, version)
	_ = ms.UpdateCurrentVersion(version, false)
	_ = ms.SetHistoryTree(nil, nil, runID)

	return ms
}

func TestCloneToProto(
	ctx context.Context,
	mutableState historyi.MutableState,
) *persistencespb.WorkflowMutableState {
	if mutableState.HasBufferedEvents() {
		_, _, _ = mutableState.CloseTransactionAsMutation(ctx, historyi.TransactionPolicyActive)
	} else {
		_, _, _ = mutableState.CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyActive)
	}
	return mutableState.CloneToProto()
}
