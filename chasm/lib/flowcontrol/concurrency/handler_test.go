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
	req := &fcpb.ConcurrencyServiceBatchRequest{
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

// testUpdateFn calls updateFn with default dynamic config values.
func testUpdateFn(c *Component, cctx chasm.MutableContext, creq chasmReq) ([]*fcpb.ConcurrencyServiceBatchResponse, error) {
	creq.reserveTimeout = defaultReserveTimeout
	return updateFn(c, cctx, creq)
}

func (tc *testHandlerContext) apply(reqs ...*fcpb.ConcurrencyServiceBatchRequest) []batchRes {
	items := make([]batchReq, len(reqs))
	for i, req := range reqs {
		items[i] = batchReq{ctx: tc.ctx, req: req}
	}
	return tc.handler.applyBatch(tc.key, items)
}

func TestHandlerWaitRetriesWithCurrentGeneration(t *testing.T) {
	tc := newTestHandlerContext(t)
	tc.start(t, 1)
	res := tc.apply(&fcpb.ConcurrencyServiceBatchRequest{ReserveSlots: []string{"slot"}})
	require.NoError(t, res[0].err)
	require.Equal(t, int64(1), res[0].res.Generation)
	res = tc.apply(&fcpb.ConcurrencyServiceBatchRequest{CommitSlots: []string{"slot"}})
	require.NoError(t, res[0].err)
	require.Equal(t, []bool{true}, res[0].res.CommitSuccess)
	res = tc.apply(&fcpb.ConcurrencyServiceBatchRequest{ReleaseSlots: []string{"slot"}})
	require.NoError(t, res[0].err)
	req := &fcpb.ConcurrencyServiceWaitRequest{
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
}

func TestHandlerWaitLongPollTimeoutReturnsRequestGeneration(t *testing.T) {
	tc := newTestHandlerContext(t)
	tc.start(t, 0)
	ctx, cancel := context.WithCancel(tc.ctx)
	cancel()

	res, err := tc.handler.Wait(ctx, &fcpb.ConcurrencyServiceWaitRequest{
		NamespaceId:  tc.key.namespaceID,
		Key:          tc.key.key,
		WakePriority: 100,
	})
	require.NoError(t, err)
	require.Zero(t, res.Generation)
	require.Zero(t, res.WakeTokens)
}

func TestHandlerWaitDeadlineExceededReturnsRequestGeneration(t *testing.T) {
	engine := chasm.NewMockEngine(gomock.NewController(t))
	engine.EXPECT().PollComponent(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, context.DeadlineExceeded)
	h := NewHandler(log.NewTestLogger(), dynamicconfig.NewNoopCollection())
	ctx := chasm.NewEngineContext(context.Background(), engine)

	res, err := h.Wait(ctx, &fcpb.ConcurrencyServiceWaitRequest{
		NamespaceId:  "namespace",
		Key:          "limiter",
		Generation:   2,
		WakePriority: 100,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), res.Generation)
	require.Zero(t, res.WakeTokens)
}

func TestHandlerWaitUsesLongPollConfig(t *testing.T) {
	longPoll := WaitLongPollOptions{Timeout: 5 * time.Second, Buffer: 2 * time.Second}
	dc := dynamicconfig.NewCollection(dynamicconfig.StaticClient{
		WaitLongPoll.Key(): longPoll,
	}, log.NewNoopLogger())
	h := NewHandler(log.NewTestLogger(), dc)

	tests := []struct {
		name          string
		parentTimeout time.Duration
		wantTimeout   time.Duration
	}{
		{name: "no parent deadline", wantTimeout: longPoll.Timeout},
		{name: "long parent deadline", parentTimeout: time.Minute, wantTimeout: longPoll.Timeout},
		{name: "short parent deadline", parentTimeout: 4 * time.Second, wantTimeout: 4*time.Second - longPoll.Buffer},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotTimeout time.Duration
			engine := chasm.NewMockEngine(gomock.NewController(t))
			engine.EXPECT().PollComponent(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, _ chasm.ComponentRef, _ func(chasm.Context, chasm.Component) (bool, error), _ ...chasm.TransitionOption) ([]byte, error) {
					deadline, ok := ctx.Deadline()
					require.True(t, ok)
					gotTimeout = time.Until(deadline)
					return nil, context.DeadlineExceeded
				})
			ctx := chasm.NewEngineContext(context.Background(), engine)
			if tt.parentTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.parentTimeout)
				defer cancel()
			}

			_, err := h.Wait(ctx, &fcpb.ConcurrencyServiceWaitRequest{NamespaceId: "namespace", Key: "limiter"})
			require.NoError(t, err)
			require.InDelta(t, tt.wantTimeout, gotTimeout, float64(time.Second))
		})
	}
}

func TestHandlerWaitPropagatesPollError(t *testing.T) {
	testErr := errors.New("poll failed")
	engine := chasm.NewMockEngine(gomock.NewController(t))
	engine.EXPECT().PollComponent(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, testErr)
	h := NewHandler(log.NewTestLogger(), dynamicconfig.NewNoopCollection())
	ctx := chasm.NewEngineContext(context.Background(), engine)
	key := batchKey{namespaceID: "namespace", key: "limiter"}

	res, err := h.Wait(ctx, &fcpb.ConcurrencyServiceWaitRequest{
		NamespaceId:  key.namespaceID,
		Key:          key.key,
		Generation:   2,
		WakePriority: 100,
	})
	require.ErrorIs(t, err, testErr)
	require.Nil(t, res)
}

func TestHandlerApplyBatchSlotLifecycle(t *testing.T) {
	tc := newTestHandlerContext(t)

	ress := tc.apply(&fcpb.ConcurrencyServiceBatchRequest{
		ReserveSlots:        []string{"first"},
		ConfigUpdate:        &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 1},
		ConfigUpdateVersion: 1,
	})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{true}, ress[0].res.ReserveSuccess)
	require.Equal(t, int64(1), ress[0].res.Generation)

	ress = tc.apply(&fcpb.ConcurrencyServiceBatchRequest{CommitSlots: []string{"first", "first"}})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{true, true}, ress[0].res.CommitSuccess)

	ress = tc.apply(&fcpb.ConcurrencyServiceBatchRequest{CancelReservationSlots: []string{"first"}})
	require.NoError(t, ress[0].err)
	ress = tc.apply(&fcpb.ConcurrencyServiceBatchRequest{ReserveSlots: []string{"second"}})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{false}, ress[0].res.ReserveSuccess)

	ress = tc.apply(&fcpb.ConcurrencyServiceBatchRequest{ReleaseSlots: []string{"first", "first"}})
	require.NoError(t, ress[0].err)
	ress = tc.apply(&fcpb.ConcurrencyServiceBatchRequest{ReserveSlots: []string{"second", "second"}})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{true, true}, ress[0].res.ReserveSuccess)
	require.Equal(t, int64(2), ress[0].res.Generation)

	ress = tc.apply(&fcpb.ConcurrencyServiceBatchRequest{CancelReservationSlots: []string{"second", "second"}})
	require.NoError(t, ress[0].err)
	ress = tc.apply(&fcpb.ConcurrencyServiceBatchRequest{ReserveSlots: []string{"third"}})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{true}, ress[0].res.ReserveSuccess)
	require.Equal(t, int64(3), ress[0].res.Generation)
}

func TestHandlerApplyBatchCommitAfterReservationExpiry(t *testing.T) {
	tc := newTestHandlerContext(t)

	ress := tc.apply(&fcpb.ConcurrencyServiceBatchRequest{
		ReserveSlots:        []string{"expired"},
		ConfigUpdate:        &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 1},
		ConfigUpdateVersion: 1,
	})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{true}, ress[0].res.ReserveSuccess)
	tc.timeSource.Update(tc.timeSource.Now().Add(defaultReserveTimeout + time.Second))

	ress = tc.apply(&fcpb.ConcurrencyServiceBatchRequest{CommitSlots: []string{"expired"}})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{false}, ress[0].res.CommitSuccess)
	ress = tc.apply(&fcpb.ConcurrencyServiceBatchRequest{ReserveSlots: []string{"replacement"}})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{true}, ress[0].res.ReserveSuccess)
	require.Equal(t, int64(2), ress[0].res.Generation)
}

func TestHandlerApplyBatchUsesReserveTimeoutConfig(t *testing.T) {
	tc := newTestHandlerContext(t)
	reserveTimeout := 5 * time.Second
	tc.handler = NewHandler(log.NewTestLogger(), dynamicconfig.NewCollection(dynamicconfig.StaticClient{
		ReserveTimeout.Key(): reserveTimeout,
	}, log.NewNoopLogger()))

	ress := tc.apply(&fcpb.ConcurrencyServiceBatchRequest{
		ReserveSlots:        []string{"short", "long"},
		ConfigUpdate:        &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 2},
		ConfigUpdateVersion: 1,
	})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{true, true}, ress[0].res.ReserveSuccess)
	tc.timeSource.Update(tc.timeSource.Now().Add(reserveTimeout - 2*time.Second))
	ress = tc.apply(&fcpb.ConcurrencyServiceBatchRequest{CommitSlots: []string{"long"}})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{true}, ress[0].res.CommitSuccess)

	tc.timeSource.Update(tc.timeSource.Now().Add(4 * time.Second))
	ress = tc.apply(&fcpb.ConcurrencyServiceBatchRequest{CommitSlots: []string{"short"}})
	require.NoError(t, ress[0].err)
	require.Equal(t, []bool{false}, ress[0].res.CommitSuccess)
}

func TestHandlerApplyBatchWithoutReserveRequiresExistingComponent(t *testing.T) {
	tc := newTestHandlerContext(t)

	ress := tc.apply(
		&fcpb.ConcurrencyServiceBatchRequest{CommitSlots: []string{"missing"}},
		&fcpb.ConcurrencyServiceBatchRequest{ReleaseSlots: []string{"missing"}},
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
				req: &fcpb.ConcurrencyServiceBatchRequest{
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
		{req: &fcpb.ConcurrencyServiceBatchRequest{
			ConfigUpdate:        &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 3},
			ConfigUpdateVersion: 3,
		}},
		{req: &fcpb.ConcurrencyServiceBatchRequest{
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
	c.WakeAll = true

	ress, err := testUpdateFn(c, newTestMutableContext(now), chasmReq{items: []batchReq{{
		req: &fcpb.ConcurrencyServiceBatchRequest{ReserveSlots: []string{"new"}},
	}}})
	require.NoError(t, err)
	require.Len(t, ress, 1)
	require.Equal(t, []bool{true}, ress[0].ReserveSuccess)
	require.Equal(t, int64(4), ress[0].Generation)
	require.Equal(t, int64(4), c.Generation)
	require.False(t, c.WakeAll)
}

func TestUpdateFnFailedReserveDoesNotIncrementGeneration(t *testing.T) {
	now := time.Now().UTC()
	c := newTestComponent(1)
	c.Generation = 3
	c.Slots = []*fcpb.ConcurrencyState_Slot{{SlotId: "existing", Committed: true}}

	ress, err := testUpdateFn(c, newTestMutableContext(now), chasmReq{items: []batchReq{{
		req: &fcpb.ConcurrencyServiceBatchRequest{ReserveSlots: []string{"new"}},
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

	ress, err := testUpdateFn(c, newTestMutableContext(now), chasmReq{items: []batchReq{{
		req: &fcpb.ConcurrencyServiceBatchRequest{
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

	ress, err := testUpdateFn(c, newTestMutableContext(now), chasmReq{items: []batchReq{
		{req: &fcpb.ConcurrencyServiceBatchRequest{
			CommitSlots:  []string{"first", "missing"},
			ReserveSlots: []string{"third"},
		}},
		{req: &fcpb.ConcurrencyServiceBatchRequest{
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

	ress, err := testUpdateFn(c, newTestMutableContext(now), chasmReq{items: []batchReq{
		{req: &fcpb.ConcurrencyServiceBatchRequest{
			ReleaseSlots: []string{"committed"},
			ReserveSlots: []string{"first"},
		}},
		{req: &fcpb.ConcurrencyServiceBatchRequest{
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

	ress, err := testUpdateFn(c, newTestMutableContext(now), chasmReq{items: []batchReq{{
		req: &fcpb.ConcurrencyServiceBatchRequest{ReserveSlots: []string{"replacement"}},
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

	ress, err := testUpdateFn(c, newTestMutableContext(now), chasmReq{items: []batchReq{
		{req: &fcpb.ConcurrencyServiceBatchRequest{ReleaseSlots: []string{"old"}}},
		{req: &fcpb.ConcurrencyServiceBatchRequest{ReserveSlots: []string{"new"}}},
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

	ress, err := testUpdateFn(c, newTestMutableContext(now), chasmReq{items: []batchReq{{
		req: &fcpb.ConcurrencyServiceBatchRequest{
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

	ress, err := testUpdateFn(c, newTestMutableContext(now), chasmReq{
		items: []batchReq{{req: &fcpb.ConcurrencyServiceBatchRequest{
			ConfigUpdate:        &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 1},
			ConfigUpdateVersion: 2,
		}}},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), ress[0].Generation)
	require.Len(t, c.Slots, 3)
	require.Zero(t, c.availableSlots())

	for _, slotID := range []string{"first", "second"} {
		_, err = testUpdateFn(c, newTestMutableContext(now), chasmReq{
			items: []batchReq{{req: &fcpb.ConcurrencyServiceBatchRequest{ReleaseSlots: []string{slotID}}}},
		})
		require.NoError(t, err)
		require.Zero(t, c.availableSlots())
	}

	_, err = testUpdateFn(c, newTestMutableContext(now), chasmReq{
		items: []batchReq{{req: &fcpb.ConcurrencyServiceBatchRequest{ReleaseSlots: []string{"third"}}}},
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

	ress, err := testUpdateFn(c, newTestMutableContext(now), chasmReq{
		items: []batchReq{{req: &fcpb.ConcurrencyServiceBatchRequest{
			ConfigUpdate:        &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 3},
			ConfigUpdateVersion: 2,
		}}},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), ress[0].Generation)
	require.Equal(t, int32(3), c.Config.ConcurrentTasks)
	require.Equal(t, int64(2), c.ConfigVersion)
	require.True(t, c.WakeAll)
}

func TestUpdateFnAvailabilityIncreaseStartsWake(t *testing.T) {
	now := time.Now().UTC()
	cctx := newTestMutableContext(now)
	c := newTestComponent(1)
	c.Generation = 3
	c.Slots = []*fcpb.ConcurrencyState_Slot{{SlotId: "existing", Committed: true}}

	ress, err := testUpdateFn(c, cctx, chasmReq{
		items: []batchReq{{req: &fcpb.ConcurrencyServiceBatchRequest{ReleaseSlots: []string{"existing"}}}},
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), ress[0].Generation)
	require.True(t, c.WakeAll)
	require.Empty(t, cctx.Tasks)
}

func TestUpdateFnNoAvailabilityIncreaseDoesNotRestartWake(t *testing.T) {
	now := time.Now().UTC()
	cctx := newTestMutableContext(now)
	c := newTestComponent(2)
	c.Generation = 3
	c.Slots = []*fcpb.ConcurrencyState_Slot{{SlotId: "existing", Committed: true}}

	ress, err := testUpdateFn(c, cctx, chasmReq{
		items: []batchReq{{req: &fcpb.ConcurrencyServiceBatchRequest{ReleaseSlots: []string{"missing"}}}},
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), ress[0].Generation)
	require.False(t, c.WakeAll)
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

	ress, err := testUpdateFn(c, cctx, chasmReq{
		items: []batchReq{{req: &fcpb.ConcurrencyServiceBatchRequest{CommitSlots: []string{"active"}}}},
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), ress[0].Generation)
	require.Equal(t, []bool{true}, ress[0].CommitSuccess)
	require.Len(t, c.Slots, 1)
	require.True(t, c.Slots[0].Committed)
	require.True(t, c.WakeAll)
	require.Empty(t, cctx.Tasks)
}
