package concurrency

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testLibrary struct {
	chasm.UnimplementedLibrary
}

func (*testLibrary) Name() string {
	return "flowcontrol_test"
}

func (*testLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Component]("concurrency_limiter"),
	}
}

func newTestMutableContext(now time.Time) *chasm.MockMutableContext {
	return &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return now },
		},
	}
}

type testHandlerContext struct {
	ctx        context.Context
	handler    *Handler
	key        batchKey
	timeSource *clock.EventTimeSource
}

func newTestHandlerContext(t *testing.T) *testHandlerContext {
	t.Helper()
	registry := chasm.NewRegistry(log.NewNoopLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(&testLibrary{}))
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	engine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(timeSource))
	return &testHandlerContext{
		ctx:        chasm.NewEngineContext(context.Background(), engine),
		handler:    NewHandler(log.NewTestLogger(), dynamicconfig.NewNoopCollection()),
		key:        batchKey{namespaceID: "namespace", key: "limiter"},
		timeSource: timeSource,
	}
}

func (tc *testHandlerContext) start(t *testing.T, limit int32) {
	t.Helper()
	req := &fcpb.ConcurrencyBatchRequest{
		NamespaceId:         tc.key.namespaceID,
		Key:                 tc.key.key,
		ConfigUpdate:        &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: limit},
		ConfigUpdateVersion: 1,
	}
	_, err := chasm.StartExecution(
		tc.ctx,
		chasm.ExecutionKey{NamespaceID: tc.key.namespaceID, BusinessID: tc.key.key},
		initFn,
		chasmReq{items: []batchReq{{ctx: tc.ctx, req: req}}},
	)
	require.NoError(t, err)
}

func (tc *testHandlerContext) apply(reqs ...*fcpb.ConcurrencyBatchRequest) []batchRes {
	items := make([]batchReq, len(reqs))
	for i, req := range reqs {
		items[i] = batchReq{ctx: tc.ctx, req: req}
	}
	return tc.handler.applyBatch(tc.key, items)
}

func TestHandlerWaiterEntries(t *testing.T) {
	h := NewHandler(log.NewTestLogger(), dynamicconfig.NewNoopCollection())
	key := batchKey{namespaceID: "namespace", key: "limiter"}
	otherKey := batchKey{namespaceID: "namespace", key: "other"}

	h.registerWaiter(key, 10, 2)
	h.registerWaiter(key, 20, 2)
	h.registerWaiter(key, 20, 1)
	h.registerWaiter(key, 30, 1)
	h.registerWaiter(otherKey, 5, 1)

	tests := []struct {
		wantTokens   int32
		wantWakeUpTo int64
		wantWakeAll  bool
	}{
		{wantTokens: 1, wantWakeUpTo: 10},
		{wantTokens: 2, wantWakeUpTo: 10},
		{wantTokens: 3, wantWakeUpTo: 20},
		{wantTokens: 5, wantWakeUpTo: 20},
		{wantTokens: 6, wantWakeAll: true},
		{wantTokens: 7, wantWakeAll: true},
	}
	for _, tt := range tests {
		wakeUpTo, wakeAll := h.getWakeLevel(key, tt.wantTokens)
		require.Equal(t, tt.wantWakeUpTo, wakeUpTo)
		require.Equal(t, tt.wantWakeAll, wakeAll)
	}

	h.unregisterWaiter(key, 20, 1)
	wakeUpTo, wakeAll := h.getWakeLevel(key, 5)
	require.Zero(t, wakeUpTo)
	require.True(t, wakeAll)

	h.unregisterWaiter(key, 10, 2)
	h.unregisterWaiter(key, 20, 2)
	h.unregisterWaiter(key, 30, 1)
	require.NotContains(t, h.waiterShardFor(key).waiters, key)
	require.Contains(t, h.waiterShardFor(otherKey).waiters, otherKey)

	wakeUpTo, wakeAll = h.getWakeLevel(key, 1)
	require.Zero(t, wakeUpTo)
	require.True(t, wakeAll)
}

func TestHandlerWaitRetriesWithCurrentGeneration(t *testing.T) {
	tc := newTestHandlerContext(t)
	tc.start(t, 1)
	res := tc.apply(&fcpb.ConcurrencyBatchRequest{ReserveSlots: []string{"slot"}})
	require.NoError(t, res[0].err)
	require.Equal(t, int64(1), res[0].res.Generation)
	res = tc.apply(&fcpb.ConcurrencyBatchRequest{CommitSlots: []string{"slot"}})
	require.NoError(t, res[0].err)
	require.Equal(t, []bool{true}, res[0].res.CommitSuccess)
	res = tc.apply(&fcpb.ConcurrencyBatchRequest{ReleaseSlots: []string{"slot"}})
	require.NoError(t, res[0].err)
	req := &fcpb.ConcurrencyWaitRequest{
		NamespaceId: tc.key.namespaceID,
		Key:         tc.key.key,
	}

	waitRes, err := tc.handler.Wait(tc.ctx, req)
	require.NoError(t, err)
	require.Equal(t, int64(1), waitRes.Generation)
	require.Equal(t, int32(1), waitRes.WakeTokens)
	require.Zero(t, req.Generation)
	require.Zero(t, req.WakePriority)
	require.Zero(t, req.RequestedWakeTokens)
	require.Empty(t, tc.handler.waiterShardFor(tc.key).waiters)
}

func TestHandlerWaitLongPollTimeoutReturnsRequestGeneration(t *testing.T) {
	tc := newTestHandlerContext(t)
	tc.start(t, 0)
	ctx, cancel := context.WithCancel(tc.ctx)
	cancel()

	res, err := tc.handler.Wait(ctx, &fcpb.ConcurrencyWaitRequest{
		NamespaceId:  tc.key.namespaceID,
		Key:          tc.key.key,
		WakePriority: 100,
	})
	require.NoError(t, err)
	require.Zero(t, res.Generation)
	require.Zero(t, res.WakeTokens)
	require.Empty(t, tc.handler.waiterShardFor(tc.key).waiters)
}

func TestHandlerWaitDeadlineExceededReturnsRequestGeneration(t *testing.T) {
	engine := chasm.NewMockEngine(gomock.NewController(t))
	engine.EXPECT().PollComponent(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, context.DeadlineExceeded)
	h := NewHandler(log.NewTestLogger(), dynamicconfig.NewNoopCollection())
	ctx := chasm.NewEngineContext(context.Background(), engine)

	res, err := h.Wait(ctx, &fcpb.ConcurrencyWaitRequest{
		NamespaceId:  "namespace",
		Key:          "limiter",
		Generation:   2,
		WakePriority: 100,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), res.Generation)
	require.Zero(t, res.WakeTokens)
	require.Empty(t, h.waiterShardFor(batchKey{namespaceID: "namespace", key: "limiter"}).waiters)
}

func TestHandlerWaitPropagatesPollError(t *testing.T) {
	testErr := errors.New("poll failed")
	engine := chasm.NewMockEngine(gomock.NewController(t))
	engine.EXPECT().PollComponent(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, testErr)
	h := NewHandler(log.NewTestLogger(), dynamicconfig.NewNoopCollection())
	ctx := chasm.NewEngineContext(context.Background(), engine)
	key := batchKey{namespaceID: "namespace", key: "limiter"}

	res, err := h.Wait(ctx, &fcpb.ConcurrencyWaitRequest{
		NamespaceId:  key.namespaceID,
		Key:          key.key,
		Generation:   2,
		WakePriority: 100,
	})
	require.ErrorIs(t, err, testErr)
	require.Nil(t, res)
	require.Empty(t, h.waiterShardFor(key).waiters)
}

func TestHandlerApplyBatchSlotLifecycle(t *testing.T) {
	tc := newTestHandlerContext(t)

	ress := tc.apply(&fcpb.ConcurrencyBatchRequest{
		ReserveSlots:        []string{"first"},
		ConfigUpdate:        &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 1},
		ConfigUpdateVersion: 1,
	})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{true}, ress[0].res.ReserveSuccess)
	require.Equal(t, int64(1), ress[0].res.Generation)

	ress = tc.apply(&fcpb.ConcurrencyBatchRequest{CommitSlots: []string{"first", "first"}})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{true, true}, ress[0].res.CommitSuccess)

	ress = tc.apply(&fcpb.ConcurrencyBatchRequest{CancelReservationSlots: []string{"first"}})
	require.NoError(t, ress[0].err)
	ress = tc.apply(&fcpb.ConcurrencyBatchRequest{ReserveSlots: []string{"second"}})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{false}, ress[0].res.ReserveSuccess)

	ress = tc.apply(&fcpb.ConcurrencyBatchRequest{ReleaseSlots: []string{"first", "first"}})
	require.NoError(t, ress[0].err)
	ress = tc.apply(&fcpb.ConcurrencyBatchRequest{ReserveSlots: []string{"second", "second"}})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{true, true}, ress[0].res.ReserveSuccess)
	require.Equal(t, int64(2), ress[0].res.Generation)

	ress = tc.apply(&fcpb.ConcurrencyBatchRequest{CancelReservationSlots: []string{"second", "second"}})
	require.NoError(t, ress[0].err)
	ress = tc.apply(&fcpb.ConcurrencyBatchRequest{ReserveSlots: []string{"third"}})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{true}, ress[0].res.ReserveSuccess)
	require.Equal(t, int64(3), ress[0].res.Generation)
}

func TestHandlerApplyBatchCommitAfterReservationExpiry(t *testing.T) {
	tc := newTestHandlerContext(t)

	ress := tc.apply(&fcpb.ConcurrencyBatchRequest{
		ReserveSlots:        []string{"expired"},
		ConfigUpdate:        &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 1},
		ConfigUpdateVersion: 1,
	})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{true}, ress[0].res.ReserveSuccess)
	tc.timeSource.Update(tc.timeSource.Now().Add(reserveTimeout + time.Second))

	ress = tc.apply(&fcpb.ConcurrencyBatchRequest{CommitSlots: []string{"expired"}})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{false}, ress[0].res.CommitSuccess)
	ress = tc.apply(&fcpb.ConcurrencyBatchRequest{ReserveSlots: []string{"replacement"}})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{true}, ress[0].res.ReserveSuccess)
	require.Equal(t, int64(2), ress[0].res.Generation)
}

func TestHandlerApplyBatchWithoutReserveRequiresExistingComponent(t *testing.T) {
	tc := newTestHandlerContext(t)

	ress := tc.apply(
		&fcpb.ConcurrencyBatchRequest{CommitSlots: []string{"missing"}},
		&fcpb.ConcurrencyBatchRequest{ReleaseSlots: []string{"missing"}},
	)
	require.Len(t, ress, 2)
	require.Error(t, ress[0].err)
	require.ErrorIs(t, ress[1].err, ress[0].err)
	require.Nil(t, ress[0].res)
	require.Nil(t, ress[1].res)
}

func TestInitFn(t *testing.T) {
	tests := []struct {
		name        string
		limit       int32
		wantWakeAll bool
	}{
		{name: "positive limit starts unblocked", limit: 2, wantWakeAll: true},
		{name: "zero limit starts blocked", limit: 0, wantWakeAll: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := initFn(nil, chasmReq{items: []batchReq{{
				req: &fcpb.ConcurrencyBatchRequest{
					ConfigUpdate:        &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: tt.limit},
					ConfigUpdateVersion: 4,
				},
			}}})
			require.NoError(t, err)
			require.Equal(t, tt.limit, c.Config.ConcurrentTasks)
			require.Equal(t, int64(4), c.ConfigVersion)
			require.Equal(t, tt.wantWakeAll, c.WakeAll)
		})
	}
}

func TestInitFnUsesNewestConfigInBatch(t *testing.T) {
	c, err := initFn(nil, chasmReq{items: []batchReq{
		{req: &fcpb.ConcurrencyBatchRequest{
			ConfigUpdate:        &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 3},
			ConfigUpdateVersion: 3,
		}},
		{req: &fcpb.ConcurrencyBatchRequest{
			ConfigUpdate:        &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 1},
			ConfigUpdateVersion: 2,
		}},
	}})
	require.NoError(t, err)
	require.Equal(t, int32(3), c.Config.ConcurrentTasks)
	require.Equal(t, int64(3), c.ConfigVersion)
	require.True(t, c.WakeAll)
}

func TestUpdateFnReserveLastSlotIncrementsGeneration(t *testing.T) {
	now := time.Now().UTC()
	c := newTestComponent(1)
	c.Generation = 3
	c.WakeUpTo = 100
	c.WakeAll = true
	c.WakeStage = 4

	ress, err := updateFn(c, newTestMutableContext(now), chasmReq{items: []batchReq{{
		req: &fcpb.ConcurrencyBatchRequest{ReserveSlots: []string{"new"}},
	}}})
	require.NoError(t, err)
	require.Len(t, ress, 1)
	require.Equal(t, []bool{true}, ress[0].ReserveSuccess)
	require.Equal(t, int64(4), ress[0].Generation)
	require.Equal(t, int64(4), c.Generation)
	require.Zero(t, c.WakeUpTo)
	require.False(t, c.WakeAll)
	require.Zero(t, c.WakeStage)
}

func TestUpdateFnFailedReserveDoesNotIncrementGeneration(t *testing.T) {
	now := time.Now().UTC()
	c := newTestComponent(1)
	c.Generation = 3
	c.Slots = []*fcpb.ConcurrencyState_Slot{{SlotId: "existing", Committed: true}}

	ress, err := updateFn(c, newTestMutableContext(now), chasmReq{items: []batchReq{{
		req: &fcpb.ConcurrencyBatchRequest{ReserveSlots: []string{"new"}},
	}}})
	require.NoError(t, err)
	require.Equal(t, []bool{false}, ress[0].ReserveSuccess)
	require.Equal(t, int64(3), ress[0].Generation)
	require.Equal(t, int64(3), c.Generation)
}

func TestUpdateFnIgnoresStaleConfig(t *testing.T) {
	now := time.Now().UTC()
	c := newTestComponent(2)
	c.ConfigVersion = 2

	ress, err := updateFn(c, newTestMutableContext(now), chasmReq{items: []batchReq{{
		req: &fcpb.ConcurrencyBatchRequest{
			ConfigUpdate:        &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 0},
			ConfigUpdateVersion: 1,
			ReserveSlots:        []string{"first", "second"},
		},
	}}})
	require.NoError(t, err)
	require.Equal(t, []bool{true, true}, ress[0].ReserveSuccess)
	require.Equal(t, int64(1), ress[0].Generation)
	require.Equal(t, int32(2), c.Config.ConcurrentTasks)
	require.Equal(t, int64(2), c.ConfigVersion)
}

func TestUpdateFnPreservesPerRequestResults(t *testing.T) {
	now := time.Now().UTC()
	c := newTestComponent(4)
	c.Slots = []*fcpb.ConcurrencyState_Slot{
		{SlotId: "first", Expires: timestamppb.New(now.Add(time.Minute))},
		{SlotId: "second", Expires: timestamppb.New(now.Add(time.Minute))},
	}

	ress, err := updateFn(c, newTestMutableContext(now), chasmReq{items: []batchReq{
		{req: &fcpb.ConcurrencyBatchRequest{
			CommitSlots:  []string{"first", "missing"},
			ReserveSlots: []string{"third"},
		}},
		{req: &fcpb.ConcurrencyBatchRequest{
			CommitSlots:  []string{"second"},
			ReserveSlots: []string{"fourth", "fifth"},
		}},
	}})
	require.NoError(t, err)
	require.Len(t, ress, 2)
	require.Equal(t, []bool{true, false}, ress[0].CommitSuccess)
	require.Equal(t, []bool{true}, ress[0].ReserveSuccess)
	require.Equal(t, []bool{true}, ress[1].CommitSuccess)
	require.Equal(t, []bool{true, false}, ress[1].ReserveSuccess)
	require.Equal(t, int64(1), ress[0].Generation)
	require.Equal(t, int64(1), ress[1].Generation)
}

func TestUpdateFnCancelAndReleaseFreeCapacityBeforeReserve(t *testing.T) {
	now := time.Now().UTC()
	c := newTestComponent(2)
	c.Slots = []*fcpb.ConcurrencyState_Slot{
		{SlotId: "committed", Committed: true},
		{SlotId: "reserved", Expires: timestamppb.New(now.Add(time.Minute))},
	}

	ress, err := updateFn(c, newTestMutableContext(now), chasmReq{items: []batchReq{
		{req: &fcpb.ConcurrencyBatchRequest{
			ReleaseSlots: []string{"committed"},
			ReserveSlots: []string{"first"},
		}},
		{req: &fcpb.ConcurrencyBatchRequest{
			CancelReservationSlots: []string{"reserved"},
			ReserveSlots:           []string{"second"},
		}},
	}})
	require.NoError(t, err)
	require.Equal(t, []bool{true}, ress[0].ReserveSuccess)
	require.Equal(t, []bool{true}, ress[1].ReserveSuccess)
	require.Equal(t, int64(0), c.Generation)
	require.Len(t, c.Slots, 2)
	require.ElementsMatch(t, []string{"first", "second"}, []string{c.Slots[0].SlotId, c.Slots[1].SlotId})
}

func TestUpdateFnExpireAndReserveIncrementsGeneration(t *testing.T) {
	now := time.Now().UTC()
	c := newTestComponent(1)
	c.Generation = 3
	c.Slots = []*fcpb.ConcurrencyState_Slot{{
		SlotId:  "expired",
		Expires: timestamppb.New(now.Add(-time.Second)),
	}}

	ress, err := updateFn(c, newTestMutableContext(now), chasmReq{items: []batchReq{{
		req: &fcpb.ConcurrencyBatchRequest{ReserveSlots: []string{"replacement"}},
	}}})
	require.NoError(t, err)
	require.Equal(t, []bool{true}, ress[0].ReserveSuccess)
	require.Equal(t, int64(4), ress[0].Generation)
	require.Equal(t, int64(4), c.Generation)
	require.Equal(t, "replacement", c.Slots[0].SlotId)
}

func TestUpdateFnAtomicReleaseAndReserveDoesNotIncrementGeneration(t *testing.T) {
	now := time.Now().UTC()
	c := newTestComponent(1)
	c.Generation = 3
	c.Slots = []*fcpb.ConcurrencyState_Slot{{SlotId: "old", Committed: true}}

	ress, err := updateFn(c, newTestMutableContext(now), chasmReq{items: []batchReq{
		{req: &fcpb.ConcurrencyBatchRequest{ReleaseSlots: []string{"old"}}},
		{req: &fcpb.ConcurrencyBatchRequest{ReserveSlots: []string{"new"}}},
	}})
	require.NoError(t, err)
	require.Len(t, ress, 2)
	require.Equal(t, []bool{true}, ress[1].ReserveSuccess)
	require.Equal(t, int64(3), ress[0].Generation)
	require.Equal(t, int64(3), ress[1].Generation)
	require.Equal(t, int64(3), c.Generation)
	require.Equal(t, "new", c.Slots[0].SlotId)
}

func TestUpdateFnConfigDecreaseIncrementsGeneration(t *testing.T) {
	now := time.Now().UTC()
	c := newTestComponent(2)
	c.ConfigVersion = 1
	c.Generation = 3
	c.WakeAll = true
	c.Slots = []*fcpb.ConcurrencyState_Slot{{SlotId: "existing", Committed: true}}

	ress, err := updateFn(c, newTestMutableContext(now), chasmReq{items: []batchReq{{
		req: &fcpb.ConcurrencyBatchRequest{
			ConfigUpdate:        &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 1},
			ConfigUpdateVersion: 2,
		},
	}}})
	require.NoError(t, err)
	require.Equal(t, int64(4), ress[0].Generation)
	require.Equal(t, int64(4), c.Generation)
	require.False(t, c.WakeAll)
}

func TestUpdateFnConfigDecreaseDoesNotEvictSlots(t *testing.T) {
	now := time.Now().UTC()
	c := newTestComponent(3)
	c.ConfigVersion = 1
	c.Generation = 1
	c.Slots = []*fcpb.ConcurrencyState_Slot{
		{SlotId: "first", Committed: true},
		{SlotId: "second", Committed: true},
		{SlotId: "third", Committed: true},
	}
	getWakeLevel := func(int32) (int64, bool) { return 0, true }

	ress, err := updateFn(c, newTestMutableContext(now), chasmReq{
		items: []batchReq{{req: &fcpb.ConcurrencyBatchRequest{
			ConfigUpdate:        &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 1},
			ConfigUpdateVersion: 2,
		}}},
		getWakeLevel: getWakeLevel,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), ress[0].Generation)
	require.Len(t, c.Slots, 3)
	require.Zero(t, c.availableSlots())

	for _, slotID := range []string{"first", "second"} {
		_, err = updateFn(c, newTestMutableContext(now), chasmReq{
			items:        []batchReq{{req: &fcpb.ConcurrencyBatchRequest{ReleaseSlots: []string{slotID}}}},
			getWakeLevel: getWakeLevel,
		})
		require.NoError(t, err)
		require.Zero(t, c.availableSlots())
	}

	_, err = updateFn(c, newTestMutableContext(now), chasmReq{
		items:        []batchReq{{req: &fcpb.ConcurrencyBatchRequest{ReleaseSlots: []string{"third"}}}},
		getWakeLevel: getWakeLevel,
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), c.availableSlots())
	require.Empty(t, c.Slots)
	require.True(t, c.WakeAll)
}

func TestUpdateFnConfigIncreaseWakesWaiters(t *testing.T) {
	now := time.Now().UTC()
	c := newTestComponent(1)
	c.ConfigVersion = 1
	c.Generation = 1
	c.Slots = []*fcpb.ConcurrencyState_Slot{{SlotId: "existing", Committed: true}}
	var wantTokens int32

	ress, err := updateFn(c, newTestMutableContext(now), chasmReq{
		items: []batchReq{{req: &fcpb.ConcurrencyBatchRequest{
			ConfigUpdate:        &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 3},
			ConfigUpdateVersion: 2,
		}}},
		getWakeLevel: func(tokens int32) (int64, bool) {
			wantTokens = tokens
			return 0, true
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), ress[0].Generation)
	require.Equal(t, int32(3), c.Config.ConcurrentTasks)
	require.Equal(t, int64(2), c.ConfigVersion)
	require.Equal(t, int32(2), wantTokens)
	require.True(t, c.WakeAll)
}

func TestUpdateFnAvailabilityIncreaseStartsWake(t *testing.T) {
	now := time.Now().UTC()
	cctx := newTestMutableContext(now)
	c := newTestComponent(1)
	c.Generation = 3
	c.Slots = []*fcpb.ConcurrencyState_Slot{{SlotId: "existing", Committed: true}}
	wantTokens := int32(0)

	ress, err := updateFn(c, cctx, chasmReq{
		items: []batchReq{{req: &fcpb.ConcurrencyBatchRequest{ReleaseSlots: []string{"existing"}}}},
		getWakeLevel: func(tokens int32) (int64, bool) {
			wantTokens = tokens
			return 100, false
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), ress[0].Generation)
	require.Equal(t, int32(1), wantTokens)
	require.Equal(t, int64(100), c.WakeUpTo)
	require.False(t, c.WakeAll)
	require.Len(t, cctx.Tasks, 1)
	require.Equal(t, now.Add(stagedWakeInterval), cctx.Tasks[0].Attributes.ScheduledTime)
	require.IsType(t, &stagedWake{}, cctx.Tasks[0].Payload)
}

func TestUpdateFnNoAvailabilityIncreaseDoesNotRestartWake(t *testing.T) {
	now := time.Now().UTC()
	cctx := newTestMutableContext(now)
	c := newTestComponent(2)
	c.Generation = 3
	c.WakeUpTo = 100
	c.Slots = []*fcpb.ConcurrencyState_Slot{{SlotId: "existing", Committed: true}}

	ress, err := updateFn(c, cctx, chasmReq{
		items: []batchReq{{req: &fcpb.ConcurrencyBatchRequest{ReleaseSlots: []string{"missing"}}}},
		getWakeLevel: func(int32) (int64, bool) {
			t.Fatal("getWakeLevel called without an availability increase")
			return 0, false
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), ress[0].Generation)
	require.Equal(t, int64(100), c.WakeUpTo)
	require.Empty(t, cctx.Tasks)
}

func TestUpdateFnExpirationStartsWake(t *testing.T) {
	now := time.Now().UTC()
	cctx := newTestMutableContext(now)
	c := newTestComponent(2)
	c.Generation = 3
	c.Slots = []*fcpb.ConcurrencyState_Slot{
		{
			SlotId:  "expired",
			Expires: timestamppb.New(now.Add(-time.Second)),
		},
		{
			SlotId:  "active",
			Expires: timestamppb.New(now.Add(time.Minute)),
		},
	}
	wantTokens := int32(0)

	ress, err := updateFn(c, cctx, chasmReq{
		items: []batchReq{{req: &fcpb.ConcurrencyBatchRequest{CommitSlots: []string{"active"}}}},
		getWakeLevel: func(tokens int32) (int64, bool) {
			wantTokens = tokens
			return 0, true
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), ress[0].Generation)
	require.Equal(t, []bool{true}, ress[0].CommitSuccess)
	require.Equal(t, int32(1), wantTokens)
	require.Len(t, c.Slots, 1)
	require.True(t, c.Slots[0].Committed)
	require.True(t, c.WakeAll)
	require.Empty(t, cctx.Tasks)
}
