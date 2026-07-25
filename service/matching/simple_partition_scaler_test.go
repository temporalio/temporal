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
// A tracker becomes "full" exactly one window after the call to primedScaler.
// The measured rate over this window equals the number of tasks fed on
// the decisive call (see primedScaler).
const scalerWindow = time.Second

func newTestScaler(ts clock.TimeSource, settings dynamicconfig.SimplePartitionScalerSettings) *simplePartitionScaler {
	return newSimplePartitionScaler(dynamicconfig.GetTypedPropertyFn(settings), ts)
}

// primedScaler builds a scaler, issues the priming call that lazily creates the
// trackers (no full window has elapsed, so it must report NoChange), then
// advances the clock by scalerWindow. The next OnTasks call with NumTasks=R
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

// onTasksLoop calls OnTasks with the same numTasks for numRepetitions.
func onTasksLoop(initialTarget int, scaler *simplePartitionScaler, ts *clock.EventTimeSource, numTasks, numRepetitions int, delay time.Duration) (dec PartitionScalerDecision) {
	for i := 0; i < numRepetitions; i++ {
		dec = scaler.OnTasks(PartitionScalerInput{NumTasks: numTasks, CurrentTarget: initialTarget})
		if !dec.NoChange {
			initialTarget = dec.NewTarget
		}
		if ts != nil {
			ts.Advance(delay)
		}
	}
	return dec
}

// TestSimplePartitionScalerFactory tests that New returns a
// usable scaler for a task queue and that Stop is a safe no-op.
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
// NewTarget 0 and never touches the tracker map.
func TestSimplePartitionScalerDisabled(t *testing.T) {
	t.Parallel()

	scaler := newTestScaler(clock.NewEventTimeSource(),
		dynamicconfig.SimplePartitionScalerSettings{
			Enabled: false,
			Ups: []dynamicconfig.SimplePartitionScalerThreshold{
				{Window: scalerWindow, TargetRate: 100},
			},
		})

	dec := onTasksLoop(3, scaler, nil, 1, 10, 0)
	require.False(t, dec.NoChange)
	require.Equal(t, 0, dec.NewTarget)
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
	dec := onTasksLoop(1, scaler, nil, 1, 10, 0)
	require.Equal(t, 7, dec.NewTarget)
}

// TestSimplePartitionScalerEnabledNoWindows verifies the documented behavior for
// Enabled with no Up/Down windows: the current target is preserved as-is.
func TestSimplePartitionScalerEnabledNoWindows(t *testing.T) {
	t.Parallel()

	scaler := newTestScaler(clock.NewEventTimeSource(),
		dynamicconfig.SimplePartitionScalerSettings{Enabled: true})

	dec := onTasksLoop(3, scaler, nil, 1, 10, 0)
	require.False(t, dec.NoChange)
	require.Equal(t, 3, dec.NewTarget, "with no windows the current target is used as-is")
}

// TestSimplePartitionScalerScalesUp drives a sustained rate above the Up target
// rate and asserts the partition target rises to rate/TargetRate.
func TestSimplePartitionScalerScalesUp(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	scaler := primedScaler(t, ts, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Ups: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 100},
		},
	})

	// 1000 tasks/s against TargetRate 100 => target 10.
	dec := onTasksLoop(1, scaler, ts, 100, 10, 100*time.Millisecond)
	require.False(t, dec.NoChange)
	require.Equal(t, 10, dec.NewTarget)
}

// TestSimplePartitionScalerScalesDown drives a rate below the current target's
// capacity and asserts the target shrinks toward rate/TargetRate.
func TestSimplePartitionScalerScalesDown(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	scaler := newTestScaler(ts, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Downs: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 100},
		},
	})

	// Current target 20, only 300 tasks/s against TargetRate 100 => target 3.
	_ = onTasksLoop(20, scaler, ts, 30, 10, 100*time.Millisecond)

	// A full window has now elapsed (t=1s) and the buckets hold 300 tasks, so
	// this is the first full read. Add no tasks so the rate is exactly 300.
	dec := scaler.OnTasks(PartitionScalerInput{NumTasks: 0, CurrentTarget: 20})
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

	dec := onTasksLoop(0, scaler, ts, 5, 10, 100*time.Millisecond)
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
	dec := onTasksLoop(10, scaler, ts, 1, 10, 100*time.Millisecond)
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
	dec := onTasksLoop(2, scaler, ts, 1, 10, 100*time.Millisecond)
	require.False(t, dec.NoChange)
	require.Equal(t, 8, dec.NewTarget)
}

// TestSimplePartitionScalerHysteresisDeadband verifies that a rate landing between
// the Down and Up target rates leaves the current target unchanged (no flapping).
// Let rate be slow, then fast again, and observe hysteresis in the deadband.
func TestSimplePartitionScalerHysteresisDeadband(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	scaler := newTestScaler(ts, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Downs: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 50},
		},
		Ups: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 150},
		},
	})

	// Feed a sustained rate inside the deadband with a fresh (unprimed) scaler:
	// 15 tasks per 100ms bucket => ~150 tasks/s over the 1s window.
	// The first ~10 calls warm up the window and report NoChange (target untouched);
	// every call after the window fills is a full read (sliding buffer) that is in between
	// the Ups and Downs threshold, so the target holds at 2 across many repetitions --
	// i.e. no flapping when rate drops below 150, but above 50.
	dec := onTasksLoop(2, scaler, ts, 15, 10, 100*time.Millisecond)
	require.True(t, dec.NoChange)
	require.Equal(t, 0, dec.NewTarget, "we have not reached 1s yet, so no decision")

	// 6 100ms windows of 0 tasks each brings the sliding average rate just above 50 -> still 2
	dec = onTasksLoop(2, scaler, ts, 0, 6, 100*time.Millisecond)
	require.False(t, dec.NoChange)
	require.Equal(t, 2, dec.NewTarget, "a rate inside the deadband holds the current target")

	// Another 100ms window of 0 tasks brings the sliding average rate to <50 -> scale to 1
	dec = onTasksLoop(2, scaler, ts, 0, 1, 100*time.Millisecond)
	require.False(t, dec.NoChange)
	require.Equal(t, 1, dec.NewTarget, "a rate below the deadband drops the current target")

	// Scaling up is gated by the Up threshold (150):
	// from target 1 we must exceed round(rate/150) >= 2, i.e. rate >= ~225/s.

	// A rate back inside the deadband (~150/s) holds at 1.
	dec = onTasksLoop(1, scaler, ts, 15, 12, 100*time.Millisecond)
	require.False(t, dec.NoChange)
	require.Equal(t, 1, dec.NewTarget, "meeting the Up threshold does not scale back up")

	// 25 tasks per 100ms bucket (~275/s) clears the Up threshold scales back to 2
	dec = onTasksLoop(1, scaler, ts, 25, 12, 100*time.Millisecond)
	require.False(t, dec.NoChange)
	require.Equal(t, 2, dec.NewTarget, "exceeding the Up threshold scales back up")
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

	// After 1s the 1s window is full but the 2s window is not: still no decision.
	ts.Advance(1 * time.Second)
	dec = scaler.OnTasks(PartitionScalerInput{NumTasks: 10, CurrentTarget: 1})
	require.True(t, dec.NoChange, "a decision requires every window to be full")

	// After 2s total both windows are full and a decision is produced.
	ts.Advance(1 * time.Second)
	dec = scaler.OnTasks(PartitionScalerInput{NumTasks: 10, CurrentTarget: 1})
	require.False(t, dec.NoChange, "all windows full -> decision produced")
}
