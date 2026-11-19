package concurrency

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDoWakeNoAvailableSlots(t *testing.T) {
	now := time.Now().UTC()
	cctx := newTestMutableContext(now)
	c := newTestComponent(1)
	c.Slots = []*fcpb.ConcurrencyState_Slot{{SlotId: "committed", Committed: true}}

	doWake(cctx, c, defaultStagedWakeOptions, func(int32) (int64, bool) {
		t.Fatal("getWakeTime called without capacity")
		return 0, false
	})

	require.False(t, c.WakeAll)
	require.Empty(t, cctx.Tasks)
}

func TestDoWakeAlreadyWakingAll(t *testing.T) {
	now := time.Now().UTC()
	cctx := newTestMutableContext(now)
	c := newTestComponent(1)
	c.WakeAll = true

	doWake(cctx, c, defaultStagedWakeOptions, func(int32) (int64, bool) {
		t.Fatal("getWakeTime called after wake all")
		return 0, false
	})

	require.True(t, c.WakeAll)
	require.Empty(t, cctx.Tasks)
}

func TestDoWakePartialWakeSchedulesNextStage(t *testing.T) {
	now := time.Now().UTC()
	cctx := newTestMutableContext(now)
	c := newTestComponent(1)
	c.WakeStage = 1
	var gotTokens int32

	doWake(cctx, c, defaultStagedWakeOptions, func(tokens int32) (int64, bool) {
		gotTokens = tokens
		return 100, false
	})

	require.Equal(t, int32(2), gotTokens)
	require.Equal(t, int64(100), c.WakeUpTo)
	require.False(t, c.WakeAll)
	require.Len(t, cctx.Tasks, 1)
	require.Equal(t, now.Add(defaultStagedWakeOptions.Interval), cctx.Tasks[0].Attributes.ScheduledTime)
	require.IsType(t, &stagedWake{}, cctx.Tasks[0].Payload)
}

func TestDoWakeDoesNotMoveCutoffBackward(t *testing.T) {
	now := time.Now().UTC()
	cctx := newTestMutableContext(now)
	c := newTestComponent(1)
	c.WakeUpTo = 100

	doWake(cctx, c, defaultStagedWakeOptions, func(int32) (int64, bool) { return 50, false })

	require.Equal(t, int64(100), c.WakeUpTo)
	require.False(t, c.WakeAll)
	require.Len(t, cctx.Tasks, 1)
}

func TestDoWakeAllDoesNotScheduleNextStage(t *testing.T) {
	now := time.Now().UTC()
	cctx := newTestMutableContext(now)
	c := newTestComponent(1)

	doWake(cctx, c, defaultStagedWakeOptions, func(int32) (int64, bool) { return 0, true })

	require.Zero(t, c.WakeUpTo)
	require.True(t, c.WakeAll)
	require.Empty(t, cctx.Tasks)
}

func TestDoWakeStageCapWakesAll(t *testing.T) {
	now := time.Now().UTC()
	cctx := newTestMutableContext(now)
	c := newTestComponent(1)
	c.WakeStage = defaultStagedWakeOptions.MaxStage
	c.WakeUpTo = 100

	doWake(cctx, c, defaultStagedWakeOptions, func(int32) (int64, bool) {
		t.Fatal("getWakeTime called at stage cap")
		return 0, false
	})

	require.Zero(t, c.WakeUpTo)
	require.True(t, c.WakeAll)
	require.Empty(t, cctx.Tasks)
}

func TestDoWakeUsesOptions(t *testing.T) {
	now := time.Now().UTC()
	opts := StagedWakeOptions{Interval: 5 * time.Second, MaxStage: 2}

	cctx := newTestMutableContext(now)
	c := newTestComponent(1)
	c.WakeStage = 1
	doWake(cctx, c, opts, func(int32) (int64, bool) { return 100, false })
	require.False(t, c.WakeAll)
	require.Len(t, cctx.Tasks, 1)
	require.Equal(t, now.Add(opts.Interval), cctx.Tasks[0].Attributes.ScheduledTime)

	cctx = newTestMutableContext(now)
	c.WakeStage = 2
	doWake(cctx, c, opts, func(int32) (int64, bool) {
		t.Fatal("getWakeTime called at stage cap")
		return 0, false
	})
	require.True(t, c.WakeAll)
	require.Empty(t, cctx.Tasks)
}

func TestDoWakeLargeStageDoesNotOverflow(t *testing.T) {
	now := time.Now().UTC()
	opts := StagedWakeOptions{Interval: time.Second, MaxStage: 1000}

	cctx := newTestMutableContext(now)
	c := newTestComponent(1 << 25)
	c.WakeStage = 9 // one below max
	var gotTokens int32
	doWake(cctx, c, opts, func(tokens int32) (int64, bool) {
		gotTokens = tokens
		return 100, false
	})
	// exact value doesn't matter, just that it's larger than we'll ever use, and positive
	require.Greater(t, gotTokens, int32(100_000_000))
	require.False(t, c.WakeAll)
	require.Len(t, cctx.Tasks, 1)

	cctx = newTestMutableContext(now)
	c.WakeStage = 10
	doWake(cctx, c, opts, func(int32) (int64, bool) {
		t.Fatal("getWakeTime called at stage cap")
		return 0, false
	})
	require.True(t, c.WakeAll)
	require.Empty(t, cctx.Tasks)
}

func TestStagedWakeHandlerValidate(t *testing.T) {
	h := &StagedWakeHandler{}

	tests := []struct {
		name string
		c    *Component
		want bool
	}{
		{name: "available and staged", c: newTestComponent(1), want: true},
		{name: "no capacity", c: func() *Component {
			c := newTestComponent(1)
			c.Slots = []*fcpb.ConcurrencyState_Slot{{SlotId: "committed", Committed: true}}
			return c
		}()},
		{name: "already wake all", c: func() *Component {
			c := newTestComponent(1)
			c.WakeAll = true
			return c
		}()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, err := h.Validate(&chasm.MockContext{}, tt.c, chasm.TaskInvocation{}, &stagedWake{})
			require.NoError(t, err)
			require.Equal(t, tt.want, ok)
		})
	}
}

func TestStagedWakeHandlerExecuteExpiresAndExpandsWake(t *testing.T) {
	now := time.Now().UTC()
	key := batchKey{namespaceID: "namespace", key: "limiter"}
	h := NewHandler(log.NewTestLogger(), dynamicconfig.NewNoopCollection())
	for startTime := int64(10); startTime <= 50; startTime += 10 {
		h.registerWaiter(key, startTime, 1)
	}
	taskHandler := NewStagedWakeHandler(h)
	cctx := newTestMutableContext(now)
	cctx.HandleExecutionKey = func() chasm.ExecutionKey {
		return chasm.ExecutionKey{NamespaceID: key.namespaceID, BusinessID: key.key}
	}
	c := newTestComponent(2)
	c.Slots = []*fcpb.ConcurrencyState_Slot{{
		SlotId:  "expired",
		Expires: timestamppb.New(now.Add(-time.Second)),
	}}

	err := taskHandler.Execute(cctx, c, chasm.TaskAttributes{}, &stagedWake{})
	require.NoError(t, err)
	require.Empty(t, c.Slots)
	require.Equal(t, int32(1), c.WakeStage)
	require.Equal(t, int64(40), c.WakeUpTo)
	require.False(t, c.WakeAll)
	require.Len(t, cctx.Tasks, 1)
	require.Equal(t, now.Add(defaultStagedWakeOptions.Interval), cctx.Tasks[0].Attributes.ScheduledTime)
}
