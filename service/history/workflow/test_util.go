package workflow

import (
	"context"
	"testing"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
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
func NewMapEventCache(
	t *testing.T,
	m map[events.EventKey]*historypb.HistoryEvent,
) events.Cache {
	cache := events.NewMockCache(gomock.NewController(t))
	cache.EXPECT().DeleteEvent(gomock.Any()).AnyTimes().Do(
		func(k events.EventKey) { delete(m, k) },
	)
	cache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes().Do(
		func(k events.EventKey, event *historypb.HistoryEvent) {
			m[k] = event
		},
	)
	cache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(
			func(
				_ context.Context,
				_ int32,
				key events.EventKey,
				_ int64,
				_ []byte,
			) (*historypb.HistoryEvent, error) {
				if event, ok := m[key]; ok {
					return event, nil
				}
				return nil, serviceerror.NewNotFoundf("event %#v not found", key)
			},
		)
	return cache
}

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
