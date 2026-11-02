package workflow

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testhooks"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func pipelineTestRequest(t *testing.T, condition int64) *persistence.UpdateWorkflowExecutionRequest {
	t.Helper()
	return &persistence.UpdateWorkflowExecutionRequest{
		ShardID: 1, Mode: persistence.UpdateWorkflowModeUpdateCurrent, ArchetypeID: chasm.WorkflowArchetypeID,
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{NamespaceId: tests.WorkflowKey.NamespaceID, WorkflowId: tests.WorkflowKey.WorkflowID, ExecutionStats: &persistencespb.ExecutionStats{HistorySize: 100}, VersionHistories: &historyspb.VersionHistories{}},
			ExecutionState: &persistencespb.WorkflowExecutionState{RunId: tests.WorkflowKey.RunID, State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
			Condition:      condition, NextEventID: condition + 1, DBRecordVersion: 42,
		},
	}
}

func pipelineTestContext(t *testing.T, coalescing bool, write func(context.Context, *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error)) *ContextImpl {
	t.Helper()
	ctrl := gomock.NewController(t)
	shard := historyi.NewMockShardContext(ctrl)
	engine := historyi.NewMockEngine(ctrl)
	registry := namespace.NewMockRegistry(ctrl)
	shard.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	shard.EXPECT().GetLogger().Return(log.NewNoopLogger()).AnyTimes()
	shard.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	shard.EXPECT().GetLifecycleContext().Return(t.Context()).AnyTimes()
	shard.EXPECT().GetNamespaceRegistry().Return(registry).AnyTimes()
	shard.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(write).AnyTimes()
	registry.EXPECT().GetNamespaceByID(gomock.Any()).Return(nil, serviceerror.NewNotFound("namespace")).AnyTimes()
	engine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	engine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()
	config := tests.NewDynamicConfig()
	config.WorkflowPersistencePipeline = true
	config.WorkflowPersistenceCoalescing = coalescing
	c := NewContext(config, tests.WorkflowKey, chasm.WorkflowArchetypeID, log.NewNoopLogger(), log.NewNoopLogger(), metrics.NoopMetricsHandler, nil, testhooks.TestHooks{})
	c.pipelineShard = shard
	c.pipeline.committedDBRecordVersion = 41
	c.pipeline.committedHistorySize = 100
	return c
}

func TestPersistencePipelinePhysicalVersions(t *testing.T) {
	t.Parallel()
	type written struct{ version, condition, next, size int64 }
	writes := make(chan written, 3)
	c := pipelineTestContext(t, true, func(_ context.Context, r *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		m := r.UpdateWorkflowMutation
		writes <- written{m.DBRecordVersion, m.Condition, m.NextEventID, m.ExecutionInfo.ExecutionStats.HistorySize}
		return pipelineTestResponse(10), nil
	})
	b, err := c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 10))
	require.NoError(t, err)
	d, err := c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 11))
	require.NoError(t, err)
	bFuture, dFuture := b.completion, d.completion
	require.NoError(t, c.completeOperation(&operation{ctx: t.Context(), submissions: []*submission{b, d}}))
	require.Equal(t, written{42, 10, 12, 100}, await.Rcv(t, writes))
	require.Same(t, bFuture, b.completion)
	require.Same(t, dFuture, d.completion)
	e, err := c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 12))
	require.NoError(t, err)
	require.NoError(t, c.completeSubmission(e))
	require.Equal(t, written{43, 12, 13, 110}, await.Rcv(t, writes))
	require.Equal(t, int64(43), c.pipeline.committedDBRecordVersion)
	require.Equal(t, int64(120), c.pipeline.committedHistorySize)
	require.Equal(t, int64(42), b.request.UpdateWorkflowMutation.DBRecordVersion)
	require.Equal(t, int64(100), b.request.UpdateWorkflowMutation.ExecutionInfo.ExecutionStats.HistorySize)
}

func TestPersistencePipelineCanceledSuffix(t *testing.T) {
	t.Parallel()
	for _, cancelIndex := range []int{1, 2} {
		t.Run(map[int]string{1: "B", 2: "C"}[cancelIndex], func(t *testing.T) {
			t.Parallel()
			started, finish := make(chan struct{}, 1), make(chan struct{})
			var calls atomic.Int64
			c := pipelineTestContext(t, true, func(_ context.Context, _ *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
				if calls.Add(1) == 1 {
					started <- struct{}{}
					<-finish
				}
				return pipelineTestResponse(0), nil
			})
			var pins atomic.Int64
			c.SetPersistencePin(func() { pins.Add(1) }, func() { pins.Add(-1) })
			a, err := c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 10))
			require.NoError(t, err)
			aDone := make(chan error, 1)
			go func() { aDone <- c.completeSubmission(a) }()
			await.Rcv(t, started)
			queued := make([]*submission, 3)
			var cancel context.CancelFunc
			for i := range queued {
				ctx := t.Context()
				if i == cancelIndex {
					ctx, cancel = context.WithCancel(ctx)
				}
				queued[i], err = c.pipeline.submit(ctx, 1, pipelineTestRequest(t, int64(11+i)))
				require.NoError(t, err)
			}
			old := queued[cancelIndex].completion
			cancel()
			require.ErrorIs(t, c.completeSubmission(queued[cancelIndex]), context.Canceled)
			for _, s := range queued[cancelIndex+1:] {
				require.Error(t, c.completeSubmission(s))
				require.True(t, s.completion.Ready())
			}
			require.False(t, a.completion.Ready())
			require.Equal(t, int64(1), pins.Load())
			_, err = c.PersistenceFuture().Get(t.Context())
			require.Error(t, err)
			_, err = c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 15))
			require.Error(t, err)
			close(finish)
			require.NoError(t, await.Rcv(t, aDone))
			for _, s := range queued[:cancelIndex] {
				require.NoError(t, c.completeSubmission(s))
			}
			require.Equal(t, int64(0), pins.Load())
			_, err = old.Get(t.Context())
			require.ErrorIs(t, err, context.Canceled)
			require.NoError(t, c.Lock(t.Context(), locks.PriorityHigh))
			require.Error(t, c.pipeline.err)
			require.NoError(t, c.Unlock())
			_, err = old.Get(t.Context())
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

func TestPersistencePipelineClaimSurvivesCancellation(t *testing.T) {
	t.Parallel()
	started, finish := make(chan struct{}, 1), make(chan struct{})
	c := pipelineTestContext(t, true, func(ctx context.Context, _ *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		started <- struct{}{}
		<-finish
		return pipelineTestResponse(0), ctx.Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	b, err := c.pipeline.submit(ctx, 1, pipelineTestRequest(t, 10))
	require.NoError(t, err)
	d, err := c.pipeline.submit(ctx, 1, pipelineTestRequest(t, 11))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- c.completeSubmission(b) }()
	await.Rcv(t, started)
	cancel()
	require.ErrorIs(t, c.completeSubmission(d), context.Canceled)
	require.False(t, b.completion.Ready())
	require.False(t, d.completion.Ready())
	close(finish)
	require.NoError(t, await.Rcv(t, done))
	_, err = d.completion.Get(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(42), c.pipeline.committedDBRecordVersion)
}

func TestPersistencePipelineFailureWakesDependent(t *testing.T) {
	t.Parallel()
	writeErr := serviceerror.NewUnavailable("write failed")
	c := pipelineTestContext(t, true, func(_ context.Context, _ *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		return nil, writeErr
	})
	a, err := c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 10))
	require.NoError(t, err)
	b, err := c.pipeline.submit(t.Context(), 2, pipelineTestRequest(t, 11))
	require.NoError(t, err)
	require.ErrorIs(t, c.completeSubmission(a), writeErr)
	require.Error(t, c.completeSubmission(b))
	require.Equal(t, int64(41), c.pipeline.committedDBRecordVersion)
	require.Empty(t, c.pipeline.pending)
	require.Empty(t, c.pipeline.inFlight)
}

func TestPersistencePipelineAllOwnedSubmissions(t *testing.T) {
	t.Parallel()
	versions := make(chan int64, 3)
	c := pipelineTestContext(t, false, func(_ context.Context, r *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		versions <- r.UpdateWorkflowMutation.DBRecordVersion
		return pipelineTestResponse(0), nil
	})
	var submissions []*submission
	for i := range 3 {
		s, err := c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, int64(10+i)))
		require.NoError(t, err)
		submissions = append(submissions, s)
	}
	require.NoError(t, c.completeOperation(&operation{ctx: t.Context(), submissions: submissions}))
	for _, want := range []int64{42, 43, 44} {
		require.Equal(t, want, await.Rcv(t, versions))
	}
	for _, s := range submissions {
		require.True(t, s.completion.Ready())
	}
}

func TestPersistencePipelinePanicSettlesClaim(t *testing.T) {
	t.Parallel()
	c := pipelineTestContext(t, false, func(_ context.Context, _ *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		panic("writer panic")
	})
	a, err := c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 10))
	require.NoError(t, err)
	b, err := c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 11))
	require.NoError(t, err)
	require.PanicsWithValue(t, "writer panic", func() { _ = c.completeSubmission(a) })
	require.True(t, a.completion.Ready())
	require.True(t, b.completion.Ready())
	require.Empty(t, c.pipeline.inFlight)
	require.Empty(t, c.pipeline.pending)
	require.Error(t, c.completeSubmission(b))
}

func TestPersistencePipelineFallbackDrainsRetainedPrefix(t *testing.T) {
	t.Parallel()
	started, finish := make(chan struct{}, 1), make(chan struct{})
	var calls atomic.Int64
	c := pipelineTestContext(t, false, func(_ context.Context, _ *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		if calls.Add(1) == 1 {
			started <- struct{}{}
			<-finish
		}
		return pipelineTestResponse(0), nil
	})
	a, err := c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 10))
	require.NoError(t, err)
	aDone := make(chan error, 1)
	go func() { aDone <- c.completeSubmission(a) }()
	await.Rcv(t, started)
	b, err := c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 11))
	require.NoError(t, err)
	bDone := make(chan error, 1)
	go func() { bDone <- c.completeSubmission(b) }()
	ctx, cancel := context.WithCancel(t.Context())
	d, err := c.pipeline.submit(ctx, 1, pipelineTestRequest(t, 12))
	require.NoError(t, err)
	require.NoError(t, c.LockSpeculative(t.Context(), locks.PriorityHigh))
	fallback := make(chan error, 1)
	go func() {
		err := c.makeOperationCommitted(t.Context(), true)
		unlockErr := c.Unlock()
		if err == nil {
			err = unlockErr
		}
		fallback <- err
	}()
	waitPipeline(t, c.pipeline, func() bool { return len(c.pipeline.waiters) == 1 })
	cancel()
	require.ErrorIs(t, c.completeSubmission(d), context.Canceled)
	select {
	case <-fallback:
		t.Fatal("fallback reused state before retained writes settled")
	default:
	}
	close(finish)
	require.NoError(t, await.Rcv(t, aDone))
	require.NoError(t, await.Rcv(t, bDone))
	err = await.Rcv(t, fallback)
	require.Error(t, err)
	require.NotErrorIs(t, err, context.Canceled)
	require.Empty(t, c.pipeline.inFlight)
	require.Empty(t, c.pipeline.pending)
	require.Error(t, c.pipeline.err)
}

func TestPersistencePipelineCancelReleasesIdlePin(t *testing.T) {
	t.Parallel()
	c := pipelineTestContext(t, false, func(_ context.Context, _ *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		t.Error("canceled pending write was dispatched")
		return pipelineTestResponse(0), nil
	})
	var pins atomic.Int64
	c.SetPersistencePin(func() { pins.Add(1) }, func() { pins.Add(-1) })
	ctx, cancel := context.WithCancel(t.Context())
	s, err := c.pipeline.submit(ctx, 1, pipelineTestRequest(t, 10))
	require.NoError(t, err)
	require.Equal(t, int64(1), pins.Load())
	cancel()
	require.ErrorIs(t, c.completeSubmission(s), context.Canceled)
	require.Equal(t, int64(0), pins.Load())
	require.Error(t, c.pipeline.err)
}

func TestPersistencePipelineReadUsesRequestContext(t *testing.T) {
	t.Parallel()
	started, finish := make(chan struct{}, 1), make(chan struct{})
	c := pipelineTestContext(t, false, func(_ context.Context, _ *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		started <- struct{}{}
		<-finish
		return pipelineTestResponse(0), nil
	})
	a, err := c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 10))
	require.NoError(t, err)
	aDone := make(chan error, 1)
	go func() { aDone <- c.completeSubmission(a) }()
	await.Rcv(t, started)
	lockCtx, cancelLock := context.WithCancel(WithPipelinedPersistence(t.Context()))
	require.NoError(t, c.LockSpeculative(lockCtx, locks.PriorityHigh))
	cancelLock()
	read := make(chan error, 1)
	go func() { read <- c.Unlock() }()
	select {
	case err := <-read:
		t.Fatalf("read used the canceled lock-acquisition context: %v", err)
	default:
	}
	close(finish)
	require.NoError(t, await.Rcv(t, aDone))
	require.NoError(t, await.Rcv(t, read))
}

func TestPersistencePipelineFailureLatchRequiresReload(t *testing.T) {
	t.Parallel()
	c := pipelineTestContext(t, false, func(_ context.Context, _ *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		return pipelineTestResponse(0), nil
	})
	state := historyi.NewMockMutableState(gomock.NewController(t))
	state.EXPECT().GetUpdateCondition().Return(int64(20), int64(50)).AnyTimes()
	state.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{ExecutionStats: &persistencespb.ExecutionStats{HistorySize: 200}}).AnyTimes()
	c.MutableState = state
	c.pipeline.err = pipelineDependencyError()
	c.refreshPersistenceBaseline(false)
	require.Error(t, c.pipeline.err)
	require.Equal(t, int64(41), c.pipeline.committedDBRecordVersion)
	c.refreshPersistenceBaseline(true)
	require.NoError(t, c.pipeline.err)
	require.Equal(t, int64(50), c.pipeline.committedDBRecordVersion)
	require.Equal(t, int64(200), c.pipeline.committedHistorySize)
}

func TestComposePipelineHistory(t *testing.T) {
	t.Parallel()
	first, next := pipelineTestRequest(t, 10), pipelineTestRequest(t, 11)
	batch := func(r *persistence.UpdateWorkflowExecutionRequest, id, txn, prev int64) *persistence.WorkflowEvents {
		return &persistence.WorkflowEvents{NamespaceID: r.UpdateWorkflowMutation.ExecutionInfo.NamespaceId, WorkflowID: r.UpdateWorkflowMutation.ExecutionInfo.WorkflowId, RunID: r.UpdateWorkflowMutation.ExecutionState.RunId, BranchToken: []byte("branch"), TxnID: txn, PrevTxnID: prev, Events: []*historypb.HistoryEvent{{EventId: id, Version: 1}}}
	}
	first.UpdateWorkflowEvents = []*persistence.WorkflowEvents{batch(first, 10, 42, 41)}
	next.UpdateWorkflowEvents = []*persistence.WorkflowEvents{batch(next, 11, 43, 42)}
	merged, err := composeRequests(first, next)
	require.NoError(t, err)
	require.Len(t, merged.UpdateWorkflowEvents, 2)
	require.Equal(t, int64(42), merged.UpdateWorkflowEvents[0].TxnID)
	require.Equal(t, int64(42), merged.UpdateWorkflowEvents[1].PrevTxnID)
	merged.UpdateWorkflowEvents[0].Events[0].EventId = 99
	merged.UpdateWorkflowEvents[1].BranchToken[0] = 'x'
	require.Equal(t, int64(10), first.UpdateWorkflowEvents[0].Events[0].EventId)
	require.Equal(t, []byte("branch"), next.UpdateWorkflowEvents[0].BranchToken)
	next.UpdateWorkflowEvents[0].BranchToken = []byte("different")
	_, err = composeRequests(first, next)
	require.ErrorIs(t, err, persistence.ErrIncompatibleWorkflowMutations)
	next.UpdateWorkflowEvents[0].Events = nil
	_, err = composeRequests(first, next)
	require.Error(t, err)
	require.NotErrorIs(t, err, persistence.ErrIncompatibleWorkflowMutations)
}

func TestPersistencePipelineFallbackKeepsCurrentAdmission(t *testing.T) {
	t.Parallel()
	c := pipelineTestContext(t, false, func(_ context.Context, _ *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		return pipelineTestResponse(0), nil
	})
	require.NoError(t, c.LockSpeculative(t.Context(), locks.PriorityHigh))
	committed, committedDone := make(chan error, 1), make(chan error, 1)
	releaseCommitted := make(chan struct{})
	go func() {
		err := c.Lock(t.Context(), locks.PriorityHigh)
		committed <- err
		if err == nil {
			<-releaseCommitted
			committedDone <- c.Unlock()
		}
	}()
	waitPipeline(t, c.pipeline, func() bool { return len(c.pipeline.waiters) == 1 })
	fallback, fallbackDone := make(chan error, 1), make(chan error, 1)
	releaseFallback := make(chan struct{})
	go func() {
		fallback <- c.makeOperationCommitted(t.Context(), false)
		<-releaseFallback
		fallbackDone <- c.Unlock()
	}()
	select {
	case err := <-fallback:
		require.NoError(t, err)
	case err := <-committed:
		close(releaseCommitted)
		require.NoError(t, err)
		require.NoError(t, await.Rcv(t, committedDone))
		require.NoError(t, await.Rcv(t, fallback))
		close(releaseFallback)
		require.NoError(t, await.Rcv(t, fallbackDone))
		t.Fatal("committed waiter observed the current holder's tentative state")
	}
	select {
	case <-committed:
		t.Fatal("committed waiter overtook the fallback holder")
	default:
	}
	close(releaseFallback)
	require.NoError(t, await.Rcv(t, fallbackDone))
	require.NoError(t, await.Rcv(t, committed))
	close(releaseCommitted)
	require.NoError(t, await.Rcv(t, committedDone))
}

func TestPersistencePipelinePreparedFallbackVersion(t *testing.T) {
	t.Parallel()
	c := pipelineTestContext(t, false, func(_ context.Context, _ *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		return pipelineTestResponse(7), nil
	})
	state := historyi.NewMockMutableState(gomock.NewController(t))
	next, version := int64(13), int64(42)
	state.EXPECT().GetUpdateCondition().DoAndReturn(func() (int64, int64) { return next, version }).AnyTimes()
	state.EXPECT().SetUpdateCondition(gomock.Any(), gomock.Any()).Do(func(n, v int64) { next, version = n, v }).AnyTimes()
	state.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{ExecutionStats: &persistencespb.ExecutionStats{HistorySize: 100}}).AnyTimes()
	c.MutableState = state
	require.NoError(t, c.LockSpeculative(t.Context(), locks.PriorityHigh))
	a, err := c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 10))
	require.NoError(t, err)
	b, err := c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 11))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- c.completeOperation(&operation{ctx: t.Context(), submissions: []*submission{a, b}}) }()
	prepared := pipelineTestRequest(t, 12)
	require.NoError(t, c.commitPreparedMutation(t.Context(), &prepared.UpdateWorkflowMutation))
	require.NoError(t, await.Rcv(t, done))
	require.Equal(t, int64(44), prepared.UpdateWorkflowMutation.DBRecordVersion)
	require.Equal(t, int64(44), version)
	require.Equal(t, int64(13), next)
	require.Equal(t, int64(43), c.pipeline.committedDBRecordVersion)
	require.Equal(t, int64(114), prepared.UpdateWorkflowMutation.ExecutionInfo.ExecutionStats.HistorySize)
	require.NoError(t, c.Unlock())
}

func TestPersistencePipelineOversizedWriteStaysSynchronous(t *testing.T) {
	t.Parallel()
	c := pipelineTestContext(t, true, func(_ context.Context, _ *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		t.Error("oversized write was dispatched")
		return pipelineTestResponse(0), nil
	})
	request := pipelineTestRequest(t, 10)
	request.UpdateWorkflowMutation.ExecutionInfo.WorkflowTypeName = strings.Repeat("x", maxPipelineBatchBytes)
	_, err := c.pipeline.submit(t.Context(), 1, request)
	require.ErrorIs(t, err, persistence.ErrIncompatibleWorkflowMutations)
	require.Empty(t, c.pipeline.pending)
	require.Empty(t, c.pipeline.inFlight)
}

func TestPersistencePipelineCommittedFairness(t *testing.T) {
	t.Parallel()
	c := pipelineTestContext(t, false, func(_ context.Context, _ *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		return pipelineTestResponse(0), nil
	})
	require.NoError(t, c.LockSpeculative(t.Context(), locks.PriorityHigh))
	committed := make(chan error, 1)
	releaseCommitted := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		err := c.Lock(t.Context(), locks.PriorityHigh)
		committed <- err
		if err == nil {
			<-releaseCommitted
			done <- c.Unlock()
		}
	}()
	waitPipeline(t, c.pipeline, func() bool { return len(c.pipeline.waiters) == 1 })
	cancelCtx, cancel := context.WithCancel(t.Context())
	canceled := make(chan error, 1)
	go func() { canceled <- c.Lock(cancelCtx, locks.PriorityHigh) }()
	waitPipeline(t, c.pipeline, func() bool { return len(c.pipeline.waiters) == 2 })
	cancel()
	require.ErrorIs(t, await.Rcv(t, canceled), context.Canceled)
	waitPipeline(t, c.pipeline, func() bool { return len(c.pipeline.waiters) == 1 })
	speculative := make(chan error, 1)
	go func() {
		err := c.LockSpeculative(t.Context(), locks.PriorityHigh)
		if err == nil {
			err = c.Unlock()
		}
		speculative <- err
	}()
	require.NoError(t, c.Unlock())
	require.NoError(t, await.Rcv(t, committed))
	select {
	case <-speculative:
		t.Fatal("speculative admission overtook committed access")
	default:
	}
	close(releaseCommitted)
	require.NoError(t, await.Rcv(t, done))
	require.NoError(t, await.Rcv(t, speculative))
}

func waitPipeline(t *testing.T, p *persistencePipeline, predicate func() bool) {
	t.Helper()
	for {
		p.mu.Lock()
		ok := predicate()
		changed := p.changed
		p.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-changed:
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		}
	}
}

func pipelineTestResponse(delta int) *persistence.UpdateWorkflowExecutionResponse {
	return &persistence.UpdateWorkflowExecutionResponse{UpdateMutableStateStats: persistence.MutableStateStatistics{HistoryStatistics: &persistence.HistoryStatistics{SizeDiff: delta}}}
}
