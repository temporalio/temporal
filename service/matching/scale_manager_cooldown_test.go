package matching

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
)

func TestScaleManagerCooldownPreservesSteadyArrivalRate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		maxRate float32
	}{
		{name: "short cooldown control", maxRate: 100},
		{name: "default cooldown", maxRate: 0.33},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testScaleManagerSteadyArrivalRate(t, tc.maxRate)
		})
	}
}

func testScaleManagerSteadyArrivalRate(t *testing.T, maxRate float32) {
	t.Helper()
	ts := clock.NewEventTimeSource()
	logger := log.NewNoopLogger()
	scaler := newSimplePartitionScaler(
		dynamicconfig.GetTypedPropertyFn(dynamicconfig.SimplePartitionScalerSettings{
			Enabled: true,
			Ups: []dynamicconfig.SimplePartitionScalerThreshold{
				{TargetRate: 500, Window: 10 * time.Second},
				{TargetRate: 600, Window: 5 * time.Second},
				{TargetRate: 800, Window: 2 * time.Second},
			},
			Downs: []dynamicconfig.SimplePartitionScalerThreshold{
				{TargetRate: 100, Window: 4 * time.Minute},
			},
			BacklogCap: 2_000_000,
			Min:        1,
			Max:        200,
		}), nil, ts, logger, metrics.NoopMetricsHandler,
	)
	settings := dynamicconfig.PartitionScaleManagerSettings{
		Enabled: true, MaxRate: maxRate, BatchSize: 100,
		ShrinkRatio: 0.1, ShrinkDelta: 8,
	}
	ctrl := gomock.NewController(t)
	userData := NewMockuserDataManager(ctrl)
	userData.EXPECT().SetPartitionScale(gomock.Any()).AnyTimes()
	db := NewMockscaleDB(ctrl)
	var persisted *persistencespb.PartitionScaleState
	db.EXPECT().UpdateScaleState(gomock.Any(), true).DoAndReturn(
		func(state *persistencespb.PartitionScaleState, _ bool) error {
			persisted = common.CloneProto(state)
			return nil
		},
	).AnyTimes()
	sm := newScaleManager(
		context.Background(),
		tqid.MustNormalPartitionFromRpcName("tq", "ns-id", enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		logger, metrics.NoopMetricsHandler, userData, nil, scaler, ts,
		dynamicconfig.GetTypedPropertyFn(settings), dynamicconfig.GetIntPropertyFn(1),
		dynamicconfig.GetBoolPropertyFn(false),
	)
	// Drive the worker's wakeup branch synchronously so fake-clock advances cannot
	// race with scaler calls or persistence. No background worker is started.
	sm.scaleDB = db
	sm.setState(&persistencespb.PartitionScaleState{Target: 1}, settings)
	sm.callScaler()
	addSteadyTasks := func(ticks int) {
		for range ticks {
			ts.Advance(100 * time.Millisecond)
			for range 100 {
				sm.AddedTasks(1)
			}
			select {
			case <-sm.wakeup:
				sm.callScaler()
			default:
			}
		}
	}

	// Warm every window at 1,000 tasks/s before the first persisted decision.
	addSteadyTasks(2400)
	require.NotNil(t, persisted)
	require.Equal(t, int32(2), persisted.Target)
	require.True(t, ts.Now().Before(sm.nextDecision), "warmup must enter cooldown")

	// Once the pending batch crosses its threshold, each further arrival wakes
	// the worker. The first 100ms tick after the default ~3.03s cooldown is 3.1s.
	addSteadyTasks(31)
	require.LessOrEqual(t, sm.batch.Load(), int64(100), "cooldown backlog must have reached the real scaler")
	rate, full := scaler.trackers[2*time.Second].rateAndFull()
	require.True(t, full)
	// One 200ms tracker bucket at this arrival rate permits 100 tasks/s of
	// boundary error. Cooldown must not turn steady arrivals into a rate spike.
	require.InDelta(t, 1000, rate, 100, "steady 1,000 tasks/s must retain its arrival timing across cooldown")
	require.Equal(t, int32(2), persisted.Target)
}
