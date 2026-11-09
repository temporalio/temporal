package persistence_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	mockp "go.temporal.io/server/common/persistence/mock"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

type testUpdateReceipt struct {
	done chan struct{}
	err  error
}

func newTestUpdateReceipt() *testUpdateReceipt {
	return &testUpdateReceipt{done: make(chan struct{})}
}

func (r *testUpdateReceipt) complete(err error) {
	r.err = err
	close(r.done)
}

func (r *testUpdateReceipt) Wait(ctx context.Context) error {
	select {
	case <-r.done:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newSubmitTestManager(store p.ExecutionStore, deleteTasks bool) p.ExecutionManager {
	return p.NewExecutionManager(
		store,
		serialization.NewSerializer(),
		nil,
		log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFn(1024*1024),
		dynamicconfig.GetBoolPropertyFn(deleteTasks),
	)
}

func TestObserveWorkflowExecutionUpdate_ObservesFirstResultOnce(t *testing.T) {
	inner := newTestUpdateReceipt()
	var observed []error
	receipt := p.ObserveWorkflowExecutionUpdate(inner, func(_ context.Context, err error) {
		observed = append(observed, err)
	})

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, receipt.Wait(canceled), context.Canceled)

	writeErr := errors.New("write failed")
	inner.complete(writeErr)
	require.ErrorIs(t, receipt.Wait(t.Context()), writeErr)
	require.Len(t, observed, 1)
	require.ErrorIs(t, observed[0], context.Canceled)
}

func TestExecutionManager_SubmitWorkflowExecutionUpdate_CompletesAfterWait(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mockp.NewMockExecutionStore(ctrl)
	store.EXPECT().GetName().AnyTimes().Return("mock-store")
	inner := newTestUpdateReceipt()
	store.EXPECT().SubmitWorkflowExecutionUpdate(gomock.Any(), gomock.Any()).Return(inner, nil)
	var deleted atomic.Int32
	store.EXPECT().CompleteHistoryTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *p.CompleteHistoryTaskRequest) error {
			deleted.Add(1)
			return nil
		},
	)
	em := newSubmitTestManager(store, true)

	response, receipt, err := em.SubmitWorkflowExecutionUpdate(t.Context(), newTestUpdateRequest([]tasks.Key{tasks.NewKey(time.Now().UTC(), 1)}))
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Zero(t, deleted.Load(), "best-effort deletes must wait for durability")

	inner.complete(nil)
	require.NoError(t, receipt.Wait(t.Context()))
	require.NoError(t, receipt.Wait(t.Context()))
	require.Equal(t, int32(1), deleted.Load())
}

func TestExecutionManager_SubmitWorkflowExecutionUpdate_FailedWriteSkipsDeletes(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mockp.NewMockExecutionStore(ctrl)
	store.EXPECT().GetName().AnyTimes().Return("mock-store")
	inner := newTestUpdateReceipt()
	store.EXPECT().SubmitWorkflowExecutionUpdate(gomock.Any(), gomock.Any()).Return(inner, nil)
	store.EXPECT().CompleteHistoryTask(gomock.Any(), gomock.Any()).Times(0)
	em := newSubmitTestManager(store, true)

	_, receipt, err := em.SubmitWorkflowExecutionUpdate(t.Context(), newTestUpdateRequest([]tasks.Key{tasks.NewKey(time.Now().UTC(), 1)}))
	require.NoError(t, err)
	writeErr := serviceerror.NewUnavailable("write failed")
	inner.complete(writeErr)
	require.ErrorIs(t, receipt.Wait(t.Context()), writeErr)
}

func TestExecutionManager_SubmitWorkflowExecutionUpdate_ConditionFailureTrimsAfterWait(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mockp.NewMockExecutionStore(ctrl)
	store.EXPECT().GetName().AnyTimes().Return("mock-store")
	inner := newTestUpdateReceipt()
	store.EXPECT().SubmitWorkflowExecutionUpdate(gomock.Any(), gomock.Any()).Return(inner, nil)
	var trimReads atomic.Int32
	store.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *p.GetWorkflowExecutionRequest) (*p.InternalGetWorkflowExecutionResponse, error) {
			trimReads.Add(1)
			return nil, serviceerror.NewNotFound("gone")
		},
	)
	em := newSubmitTestManager(store, false)

	_, receipt, err := em.SubmitWorkflowExecutionUpdate(t.Context(), newTestUpdateRequest(nil))
	require.NoError(t, err)
	require.Zero(t, trimReads.Load())
	inner.complete(&p.ConditionFailedError{Msg: "conflict"})
	var conditionFailed *p.ConditionFailedError
	require.ErrorAs(t, receipt.Wait(t.Context()), &conditionFailed)
	require.Equal(t, int32(1), trimReads.Load())
}

func TestExecutionManager_SubmitWorkflowExecutionUpdate_RejectsNewRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mockp.NewMockExecutionStore(ctrl)
	store.EXPECT().GetName().AnyTimes().Return("mock-store")
	em := newSubmitTestManager(store, false)

	request := newTestUpdateRequest(nil)
	request.NewWorkflowSnapshot = &p.WorkflowSnapshot{}
	_, _, err := em.SubmitWorkflowExecutionUpdate(t.Context(), request)
	var invalid *p.InvalidPersistenceRequestError
	require.ErrorAs(t, err, &invalid)
}

func TestExecutionManager_SubmitWorkflowExecutionUpdate_FailedWaitRevertsHistorySize(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mockp.NewMockExecutionStore(ctrl)
	store.EXPECT().GetName().AnyTimes().Return("mock-store")
	serializer := serialization.NewSerializer()
	store.EXPECT().GetHistoryBranchUtil().AnyTimes().Return(p.NewHistoryBranchUtil(serializer))
	inner := newTestUpdateReceipt()
	store.EXPECT().SubmitWorkflowExecutionUpdate(gomock.Any(), gomock.Any()).Return(inner, nil)
	em := newSubmitTestManager(store, false)

	request := newTestUpdateRequest(nil)
	branchToken, err := p.NewHistoryBranchUtil(serializer).NewHistoryBranch("ns", "wid", "rid", "tree", nil, nil, 0, 0, 0)
	require.NoError(t, err)
	request.UpdateWorkflowMutation.ExecutionInfo.VersionHistories = versionhistory.NewVersionHistories(
		versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(2, 1)}),
	)
	request.UpdateWorkflowEvents = []*p.WorkflowEvents{{
		NamespaceID: "ns", WorkflowID: "wid", RunID: "rid", BranchToken: branchToken, PrevTxnID: 1, TxnID: 2,
		Events: []*historypb.HistoryEvent{{EventId: 2, Version: 1}},
	}}
	stats := request.UpdateWorkflowMutation.ExecutionInfo.ExecutionStats

	_, receipt, err := em.SubmitWorkflowExecutionUpdate(t.Context(), request)
	require.NoError(t, err)
	require.Positive(t, stats.HistorySize, "submission applies the history size diff")
	inner.complete(serviceerror.NewUnavailable("write failed"))
	require.Error(t, receipt.Wait(t.Context()))
	require.Zero(t, stats.HistorySize, "a failed write must not leave the history size diff applied")
}
