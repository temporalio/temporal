package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
)

// scalerWindow is the measurement window used by the single-window tests below.
// With this window a tracker becomes "full" exactly one window after the priming
// call, and the measured rate over that window equals the number of tasks fed on
// the decisive call (see primedScaler).
const scalerWindow = time.Second

func newTestScaler(ts clock.TimeSource, settings dynamicconfig.SimplePartitionScalerSettings) *simplePartitionScaler {
	return newSimplePartitionScaler(dynamicconfig.GetTypedPropertyFn(settings), ts)
}

// primedScaler builds a scaler, issues the priming call that lazily creates the
// trackers (no full window has elapsed, so it must report NoChange), then
// advances the clock by one full window. The next OnTasks call with NumTasks=R
// therefore measures a rate of exactly R tasks/second.
func primedScaler(
	t *testing.T,
	ts *clock.EventTimeSource,
	settings dynamicconfig.SimplePartitionScalerSettings,
) *simplePartitionScaler {
	t.Helper()
	scaler := newTestScaler(ts, settings)
	dec := scaler.OnTasks(PartitionScalerInput{NumTasks: 0, CurrentTarget: 0})
	require.True(t, dec.NoChange, "priming call before a full window should not change target")
	ts.Advance(scalerWindow)
	return scaler
}

// TestSimplePartitionScalerFactory covers the factory wiring: New returns a
// usable scaler for a task queue and Stop is a safe no-op.
func TestSimplePartitionScalerFactory(t *testing.T) {
	t.Parallel()

	factory := newSimplePartitionScalerFactory(
		dynamicconfig.GetTypedPropertyFnFilteredByTaskQueue(dynamicconfig.SimplePartitionScalerSettings{
			Enabled: true,
			Ups: []dynamicconfig.SimplePartitionScalerThreshold{
				{Window: scalerWindow, TargetRate: 10},
			},
		}),
	)

	scaler := factory.New(namespace.Name("ns"), "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	require.NotNil(t, scaler)

	// Drive one call so the factory's config closure is evaluated. This scaler
	// uses a real clock, so no full window has elapsed and it reports NoChange.
	dec := scaler.OnTasks(PartitionScalerInput{NumTasks: 1, CurrentTarget: 1})
	require.True(t, dec.NoChange, "first call on a fresh scaler is never full")

	require.NotPanics(t, scaler.Stop)
}

// TestSimplePartitionScalerDisabled verifies the disabled guard: OnTasks returns
// NewTarget 0 (fall back to dynamic config) and never touches the tracker map.
func TestSimplePartitionScalerDisabled(t *testing.T) {
	t.Parallel()

	scaler := newTestScaler(clock.NewEventTimeSource(),
		dynamicconfig.SimplePartitionScalerSettings{
			Enabled: false,
			Ups: []dynamicconfig.SimplePartitionScalerThreshold{
				{Window: scalerWindow, TargetRate: 100},
			},
		})

	dec := scaler.OnTasks(PartitionScalerInput{NumTasks: 1, CurrentTarget: 3})
	require.False(t, dec.NoChange)
	require.Equal(t, 0, dec.NewTarget)
	require.Empty(t, scaler.trackers, "disabled scaler must not create trackers")
}

// TestSimplePartitionScalerFixed verifies that a non-zero Fixed value overrides
// rate-based scaling before any tracker work happens.
func TestSimplePartitionScalerFixed(t *testing.T) {
	t.Parallel()

	scaler := newTestScaler(clock.NewEventTimeSource(),
		dynamicconfig.SimplePartitionScalerSettings{
			Enabled: true,
			Fixed:   7,
			Ups: []dynamicconfig.SimplePartitionScalerThreshold{
				{Window: scalerWindow, TargetRate: 100},
			},
		})

	dec := scaler.OnTasks(PartitionScalerInput{NumTasks: 1, CurrentTarget: 1})
	require.Equal(t, 7, dec.NewTarget)
	require.Empty(t, scaler.trackers, "Fixed short-circuits before trackers are created")
}

// TestSimplePartitionScalerEnabledNoWindows verifies the documented behavior for
// Enabled with no Up/Down windows: the current target is preserved as-is.
func TestSimplePartitionScalerEnabledNoWindows(t *testing.T) {
	t.Parallel()

	scaler := newTestScaler(clock.NewEventTimeSource(),
		dynamicconfig.SimplePartitionScalerSettings{Enabled: true})

	dec := scaler.OnTasks(PartitionScalerInput{NumTasks: 5, CurrentTarget: 3})
	require.False(t, dec.NoChange)
	require.Equal(t, 3, dec.NewTarget, "with no windows the current target is used as-is")
	require.Empty(t, scaler.trackers)
}

// TestSimplePartitionScalerScalesUp drives a sustained rate above the Up target
// rate and asserts the partition target rises to rate/TargetRate.
func TestSimplePartitionScalerScalesUp(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	scaler := primedScaler(t, ts, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Ups: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 10},
		},
	})

	// 100 tasks/s against TargetRate 10 => target 10.
	dec := scaler.OnTasks(PartitionScalerInput{NumTasks: 100, CurrentTarget: 1})
	require.False(t, dec.NoChange)
	require.Equal(t, 10, dec.NewTarget)
	require.NotNil(t, dec.PrivateState, "a real decision carries private state")
}

// TestSimplePartitionScalerScalesDown drives a rate below the current target's
// capacity and asserts the target shrinks toward rate/TargetRate.
func TestSimplePartitionScalerScalesDown(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	scaler := primedScaler(t, ts, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Downs: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 10},
		},
	})

	// Current target 20, only 30 tasks/s against TargetRate 10 => target 3.
	dec := scaler.OnTasks(PartitionScalerInput{NumTasks: 30, CurrentTarget: 20})
	require.False(t, dec.NoChange)
	require.Equal(t, 3, dec.NewTarget)
}

// TestSimplePartitionScalerScalesDownFlooredAtOne verifies the max(1, ...) floor:
// even with zero load the target never drops below one partition.
func TestSimplePartitionScalerScalesDownFlooredAtOne(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	scaler := primedScaler(t, ts, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Downs: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 10},
		},
	})

	dec := scaler.OnTasks(PartitionScalerInput{NumTasks: 0, CurrentTarget: 5})
	require.False(t, dec.NoChange)
	require.Equal(t, 1, dec.NewTarget, "scale-down is floored at one partition")
}

// TestSimplePartitionScalerMaxBound verifies the Max ceiling caps a rate-driven
// target. This is the safety bound that limits partition growth under load.
func TestSimplePartitionScalerMaxBound(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	scaler := primedScaler(t, ts, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Max:     5,
		Ups: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 10},
		},
	})

	// Rate would compute a target of 10; Max clamps it to 5.
	dec := scaler.OnTasks(PartitionScalerInput{NumTasks: 100, CurrentTarget: 1})
	require.False(t, dec.NoChange)
	require.Equal(t, 5, dec.NewTarget)
}

// TestSimplePartitionScalerMinBound verifies the Min floor raises a target that
// rate would otherwise leave below Min.
func TestSimplePartitionScalerMinBound(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	scaler := primedScaler(t, ts, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Min:     8,
		Ups: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 10},
		},
	})

	// Rate would compute a target of 2; Min raises it to 8.
	dec := scaler.OnTasks(PartitionScalerInput{NumTasks: 20, CurrentTarget: 1})
	require.False(t, dec.NoChange)
	require.Equal(t, 8, dec.NewTarget)
}

// TestSimplePartitionScalerHysteresisDeadband verifies that a rate landing between
// the Down and Up target rates leaves the current target unchanged (no flapping).
// Down TargetRate 50, Up TargetRate 150: at 150 tasks/s a current target of 2 is
// neither pulled down (150/50=3 >= 2) nor pushed up (150/150=1 <= 2).
func TestSimplePartitionScalerHysteresisDeadband(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	scaler := primedScaler(t, ts, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Downs: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 50},
		},
		Ups: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 150},
		},
	})

	dec := scaler.OnTasks(PartitionScalerInput{NumTasks: 150, CurrentTarget: 2})
	require.False(t, dec.NoChange)
	require.Equal(t, 2, dec.NewTarget, "a rate inside the deadband holds the current target")
}

// TestSimplePartitionScalerPrivateStateRoundTrip verifies that the AddTarget
// carried in a decision's PrivateState is fed back on the next call: the prior
// (higher) target is preserved even though CurrentTarget is passed low, because
// Ups only raises the target relative to the carried state.
func TestSimplePartitionScalerPrivateStateRoundTrip(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	scaler := primedScaler(t, ts, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Ups: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 10},
		},
	})

	// First decision scales to 10 and emits private state carrying AddTarget=10.
	dec1 := scaler.OnTasks(PartitionScalerInput{NumTasks: 100, CurrentTarget: 1})
	require.Equal(t, 10, dec1.NewTarget)
	require.NotNil(t, dec1.PrivateState)

	// Next window: no new load and a deliberately low CurrentTarget. Because the
	// carried AddTarget (10) is fed back, the target is held at 10 rather than
	// collapsing to CurrentTarget=1.
	ts.Advance(scalerWindow)
	dec2 := scaler.OnTasks(PartitionScalerInput{
		NumTasks:      0,
		CurrentTarget: 1,
		PrivateState:  dec1.PrivateState,
	})
	require.Equal(t, 10, dec2.NewTarget, "carried AddTarget must be preserved via PrivateState")
}

// TestSimplePartitionScalerMultipleWindows verifies that distinct windows create
// distinct trackers and that a decision is produced only once every window is
// full (the longest window gates the first decision).
func TestSimplePartitionScalerMultipleWindows(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	scaler := newTestScaler(ts, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Ups: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: 1 * time.Second, TargetRate: 1000},
			{Window: 2 * time.Second, TargetRate: 1000},
		},
	})

	dec := scaler.OnTasks(PartitionScalerInput{NumTasks: 10, CurrentTarget: 1})
	require.True(t, dec.NoChange, "no window is full on the first call")
	require.Len(t, scaler.trackers, 2, "distinct windows must create distinct trackers")

	// After 1s the 1s window is full but the 2s window is not: still no decision.
	ts.Advance(1 * time.Second)
	dec = scaler.OnTasks(PartitionScalerInput{NumTasks: 10, CurrentTarget: 1})
	require.True(t, dec.NoChange, "a decision requires every window to be full")

	// After 2s total both windows are full and a decision is produced.
	ts.Advance(1 * time.Second)
	dec = scaler.OnTasks(PartitionScalerInput{NumTasks: 10, CurrentTarget: 1})
	require.False(t, dec.NoChange, "all windows full -> decision produced")
}
