package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/number"
	"google.golang.org/protobuf/types/known/anypb"
)

// scalerWindow is the measurement window used by the single-window tests below.
const scalerWindow = time.Second

// scalerBucketSize is computed here the same as it is in simplePartitionScaler.getTracker
const scalerBucketSize = scalerWindow / simplePartitionScalerTrackerBuckets

func newTestScaler(ts clock.TimeSource, settings dynamicconfig.SimplePartitionScalerSettings) *simplePartitionScaler {
	return newSimplePartitionScaler(dynamicconfig.GetTypedPropertyFn(settings), ts)
}

// feedRateForOneWindow adds a steady tasksPerSecond to the tracker for one
// scalerWindow, one bucket at a time, then does a zero-task call to advance
// the clock and return the final decision.
//
// The zero-task call is a test device, not a model of production.
// The zero-task read call means that the measured rate equals tasksPerSecond exactly,
// so tests asserting on rate/TargetRate arithmetic are not also asserting on the
// tracker's add-then-read behavior.
//
// (A zero-task call is realistic on its own, since the timer can fire with an empty
// batch, but production never uses one as an intentional separate reading step.)
// TestSimplePartitionScalerRateReadsHighOnAddThenRead covers what production does see.
func feedRateForOneWindow(
	t *testing.T,
	scaler *simplePartitionScaler,
	ts *clock.EventTimeSource,
	initialTarget, tasksPerSecond int,
) PartitionScalerDecision {
	t.Helper()

	target, dec := feedRate(t, scaler, ts, initialTarget, tasksPerSecond, scalerWindow, scalerBucketSize)

	// The load phase ends one bucket short of the boundary, so nothing was decided yet.
	require.True(t, dec.NoChange, "the load phase should end just short of a full window")

	return readRate(scaler, target)
}

// feedRate adds tasksPerSecond of load for dur, in interval-sized increments, and returns
// the resulting target along with the decision from the final call.
func feedRate(
	t *testing.T,
	scaler *simplePartitionScaler,
	ts *clock.EventTimeSource,
	target, tasksPerSecond int,
	dur, interval time.Duration,
) (int, PartitionScalerDecision) {
	t.Helper()

	perCall := tasksPerSecond * int(interval) / int(time.Second)
	require.Equal(t, tasksPerSecond, perCall*int(time.Second)/int(interval),
		"tasksPerSecond must divide evenly into interval-sized increments")
	reps := int(dur / interval)
	require.Equal(t, dur, time.Duration(reps)*interval, "dur must be a whole number of intervals")

	dec := onTasksLoop(target, scaler, ts, perCall, reps, interval)
	if !dec.NoChange {
		target = dec.NewTarget
	}
	return target, dec
}

// readRate takes a reading without adding anything. Because it adds nothing, the measured
// rate is exact for every configured window at once, however their bucket sizes differ --
// see TestSimplePartitionScalerRateReadsHighOnAddThenRead for why adding and reading in one
// call is not.
func readRate(scaler *simplePartitionScaler, target int) PartitionScalerDecision {
	return scaler.OnTasks(PartitionScalerInput{NumTasks: 0, CurrentTarget: target})
}

// crossoverUps is a potential production Ups config: a long window with a lower TargetRate
// to govern sustained load, and a short window with a higher TargetRate to catch bursts.
// Which one decides depends on the traffic, since Ups take the max across windows.
func crossoverUps() dynamicconfig.SimplePartitionScalerSettings {
	return dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Ups: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: 2 * scalerWindow, TargetRate: 500},
			{Window: scalerWindow, TargetRate: 600},
		},
	}
}

// multiWindowInterval divides evenly into the bucket size of both windows used by the
// multi-window tests (scalerWindow/10 and 2*scalerWindow/10).
const multiWindowInterval = scalerBucketSize / 5

// onTasksLoop calls OnTasks with the same numTasks for numRepetitions.
func onTasksLoop(initialTarget int, scaler *simplePartitionScaler, ts *clock.EventTimeSource, numTasks, numRepetitions int, delay time.Duration) (dec PartitionScalerDecision) {
	var privateState *anypb.Any
	for i := 0; i < numRepetitions; i++ {
		dec = scaler.OnTasks(PartitionScalerInput{
			NumTasks:      numTasks,
			CurrentTarget: initialTarget,
			PrivateState:  privateState,
		})
		if !dec.NoChange {
			initialTarget = dec.NewTarget
			privateState = dec.PrivateState
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
// rate and asserts the partition target rises to rate/TargetRate after the first
// full window.
func TestSimplePartitionScalerScalesUp(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	scaler := newTestScaler(ts, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Ups: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 100},
		},
	})

	// 1000 tasks/s against TargetRate 100 => target 10.
	var privateState *anypb.Any
	var dec PartitionScalerDecision
	initialTarget := 1
	for _ = range 11 {
		ts.Advance(100 * time.Millisecond)
		dec = scaler.OnTasks(PartitionScalerInput{
			NumTasks:      100,
			CurrentTarget: initialTarget,
			PrivateState:  privateState,
		})
		if !dec.NoChange {
			initialTarget = dec.NewTarget
			privateState = dec.PrivateState
		}
		// same result if ts.Advance happens here
	}

	require.False(t, dec.NoChange)
	require.Equal(t, 11, dec.NewTarget) // I actually expected 10
}

// TestSimplePartitionScalerScalesDown drives a rate below the current target's
// capacity and asserts the target shrinks toward rate/TargetRate after the first
// full window.
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
	dec := feedRateForOneWindow(t, scaler, ts, 20, 300)
	require.False(t, dec.NoChange)
	require.Equal(t, 3, dec.NewTarget)
}

// TestSimplePartitionScalerRateReadsHighOnAddThenRead hits the issue that
// feedRateForOneWindow is built to avoid, so it is covered explicitly.
//
// Any call that adds tasks and then reads the rate measures a span bounded by increment
// instants at both ends, so the count holds one increment more than the span's length
// accounts for -- the oldest retained increment's tasks arrived before the span began:
//
//	measured rate / true rate = (floor(x) + 1) / x    where x = (window + offset)/interval
//
// Here interval is the gap between OnTasks calls, not the bucket size. This test calls
// once per bucket, so interval is window/10 and every call lands on a bucket boundary with
// offset 0: x = 10, the ratio is 11/10, and a true 1000 tasks/s reads as 1100/s. That +10%
// is enough to move the decision.
//
// This is not a bucket-boundary effect: the span starts at now - (window + offset)
// regardless of the bucket size. Offset only varies when calls are more frequent than
// buckets, and then it is second order, since a later read spans more time but retains
// proportionally more increments, so the two mostly cancel. At interval = window/50, for
// instance, the ratio moves only from 1.0200 at offset 0 to 1.0185 a full bucket later.
// Offset 0 is always the worst case, where the ratio simplifies to 1 + interval/window.
//
// None of this is only a test concern: production always adds and reads in one call,
// at arbitrary bucket offsets
func TestSimplePartitionScalerRateReadsHighOnAddThenRead(t *testing.T) {
	t.Parallel()

	settings := dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Ups: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 100},
		},
	}

	// 100 tasks per bucket for 11 calls spans t=0..1s: a true 1000 tasks/s, but 1100
	// tasks counted against 1s of elapsed time, so it reads 1100/s and scales to 11.
	ts := clock.NewEventTimeSource()
	dec := onTasksLoop(1, newTestScaler(ts, settings), ts,
		100, int(scalerWindow/scalerBucketSize)+1, scalerBucketSize)
	require.False(t, dec.NoChange)
	require.Equal(t, 11, dec.NewTarget,
		"adding and reading in one call counts the tasks it just added")

	// The same 1000 tasks/s, read on its own call, measures exactly and gives 10.
	tsSplit := clock.NewEventTimeSource()
	decSplit := feedRateForOneWindow(t, newTestScaler(tsSplit, settings), tsSplit, 1, 1000)
	require.Equal(t, 10, decSplit.NewTarget)
}

// TestSimplePartitionScalerScalesDownMinBound verifies that even with zero load,
// the max(1, ...) floor never drops below one partition, and if there is a Min
// setting, it raises the target to the min.
//
//	It is realistic to call OnTasks with zero tasks. That is possible if OnTasks is called due to
//	timer instead of due to a full batch.
func TestSimplePartitionScalerScalesDownMinBound(t *testing.T) {
	t.Parallel()

	// Zero tasks, scaler with no Min setting
	ts := clock.NewEventTimeSource()
	scaler := newTestScaler(ts, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Downs: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 10},
		},
	})
	dec := onTasksLoop(4, scaler, ts, 0, 11, 100*time.Millisecond)
	require.False(t, dec.NoChange)
	require.Equal(t, 1, dec.NewTarget, "scale-down is floored at one partition")

	// Now, do the same thing with Min enabled in settings
	scalerWithMin := newTestScaler(ts, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Downs: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 10},
		},
	})
	dec = onTasksLoop(4, scalerWithMin, ts, 0, 11, 100*time.Millisecond)
	require.False(t, dec.NoChange)
	require.Equal(t, 1, dec.NewTarget, "scale-down is floored at one partition")
}

// TestSimplePartitionScalerMaxBound verifies the Max ceiling caps a rate-driven
// target. This is the safety bound that limits partition growth under load.
func TestSimplePartitionScalerMaxBound(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	scaler := newTestScaler(ts, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Max:     5,
		Ups: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 10}, // low target rate -> scale up
		},
	})

	// Rate would compute a target of 10; Max clamps it to 5.
	dec := onTasksLoop(10, scaler, ts, 1, 11, 100*time.Millisecond)
	require.False(t, dec.NoChange)
	require.Equal(t, 5, dec.NewTarget)
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

	// Feed 15 tasks per 100ms bucket with a fresh (unprimed) scaler. Calling once per
	// bucket reads 1 + interval/window = 10% high (see the boundary test above), so a
	// sustained feed at this cadence reads as 165 tasks/s, not 150.
	//
	// These 10 calls only warm up the window -- the last lands at t=900ms, so none of
	// them sees a full window and they all report NoChange, leaving the target alone.
	dec := onTasksLoop(2, scaler, ts, 15, 10, 100*time.Millisecond)
	require.True(t, dec.NoChange)
	require.Equal(t, 0, dec.NewTarget, "we have not reached 1s yet, so no decision")

	// Stop feeding. The first full read is 150/s at t=1s, and the window then drains 15
	// tasks per empty bucket down to 75/s on the call asserted here. This is the deadband:
	// Ups want round(75/150) = 1 and Downs want round(75/50) = 2, and since Ups only raise
	// (max) and Downs only lower (min), the current target of 2 is left alone.
	dec = onTasksLoop(2, scaler, ts, 0, 6, 100*time.Millisecond)
	require.False(t, dec.NoChange)
	require.Equal(t, 2, dec.NewTarget, "a rate inside the deadband holds the current target")

	// One more empty bucket drops the rate to 60/s, which leaves the deadband: Downs now
	// want round(60/50) = 1. Note the rate never actually falls below the Downs target
	// rate of 50 -- it is the rounding in round(rate/TargetRate) that crosses the
	// threshold, so scale-down happens a little earlier than the bare 50/s suggests.
	dec = onTasksLoop(2, scaler, ts, 0, 1, 100*time.Millisecond)
	require.False(t, dec.NoChange)
	require.Equal(t, 1, dec.NewTarget, "a rate below the deadband drops the current target")

	// Scaling up is gated by the Up threshold (150):
	// from target 1 we must exceed round(rate/150) >= 2, i.e. rate >= ~225/s.

	// Resuming the 15/bucket feed reads 165/s once the window refills. That is above the
	// Up target rate of 150, but round(165/150) = 1 still does not clear 225/s, so the
	// target holds at 1 -- exceeding the Up rate is not by itself enough to scale up.
	dec = onTasksLoop(1, scaler, ts, 15, 12, 100*time.Millisecond)
	require.False(t, dec.NoChange)
	require.Equal(t, 1, dec.NewTarget, "meeting the Up threshold does not scale back up")

	// 25 tasks per 100ms bucket reads 275/s, which clears 225/s and scales back to 2.
	dec = onTasksLoop(1, scaler, ts, 25, 12, 100*time.Millisecond)
	require.False(t, dec.NoChange)
	require.Equal(t, 2, dec.NewTarget, "exceeding the Up threshold scales back up")
}

// TestSimplePartitionScalerLongWindowGovernsSustainedLoad checks which of two windows with
// different TargetRates decides, when both see the same sustained rate.
func TestSimplePartitionScalerLongWindowGovernsSustainedLoad(t *testing.T) {
	t.Parallel()

	// Under steady load every window measures the same rate, so the one with the lower
	// TargetRate asks for more partitions and wins the max: the 2s window wants
	// 3000/500 = 6 while the 1s window only wants 3000/600 = 5.
	ts := clock.NewEventTimeSource()
	scaler := newTestScaler(ts, crossoverUps())
	target, _ := feedRate(t, scaler, ts, 1, 3000, 2*scalerWindow, multiWindowInterval)
	dec := readRate(scaler, target)
	require.False(t, dec.NoChange)
	require.Equal(t, 6, dec.NewTarget, "the window asking for more partitions wins")

	// Ups are combined with max, so listing them in the other order changes nothing.
	tsRev := clock.NewEventTimeSource()
	scalerRev := newTestScaler(tsRev, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Ups: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 600},
			{Window: 2 * scalerWindow, TargetRate: 500},
		},
	})
	targetRev, _ := feedRate(t, scalerRev, tsRev, 1, 3000, 2*scalerWindow, multiWindowInterval)
	require.Equal(t, 6, readRate(scalerRev, targetRev).NewTarget, "Ups order does not matter")
}

// TestSimplePartitionScalerShortWindowCatchesBurst is the other half of the crossover: the
// same config as above, but a burst too short for the long window to notice.
func TestSimplePartitionScalerShortWindowCatchesBurst(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	scaler := newTestScaler(ts, crossoverUps())

	// Idle long enough to fill both windows.
	target, _ := feedRate(t, scaler, ts, 1, 0, 2*scalerWindow, multiWindowInterval)
	require.Equal(t, 1, target, "no load holds at the floor of one partition")

	// Now burst for exactly the length of the short window. The 1s window holds nothing
	// but burst traffic (6000/s => 10), while the 2s window averages the burst with the
	// idle period before it (3000/s => 6), so this time the short window governs.
	target, _ = feedRate(t, scaler, ts, target, 6000, scalerWindow, multiWindowInterval)
	dec := readRate(scaler, target)
	require.False(t, dec.NoChange)
	require.Equal(t, 10, dec.NewTarget, "a burst shorter than the long window is caught by the short one")
}

// TestSimplePartitionScalerUpsTakePriorityOverDowns pins the order of the two loops in
// updateAddTarget, which the SimplePartitionScalerSettings doc calls out ("Ups take
// priority") but nothing previously checked.
func TestSimplePartitionScalerUpsTakePriorityOverDowns(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	scaler := newTestScaler(ts, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Downs:   []dynamicconfig.SimplePartitionScalerThreshold{{Window: scalerWindow, TargetRate: 100}},
		Ups:     []dynamicconfig.SimplePartitionScalerThreshold{{Window: 2 * scalerWindow, TargetRate: 50}},
	})

	// At 500/s the Down window wants 500/100 = 5 and the Up window wants 500/50 = 10.
	// Downs run first with min and Ups second with max, so starting from 20 the Down pulls
	// the target to 5 and the Up pushes it back up to 10. All three orderings give
	// different answers -- 10 as written, 5 if Ups ran first, 20 if neither applied -- so
	// this cannot pass by coincidence.
	target, _ := feedRate(t, scaler, ts, 20, 500, 2*scalerWindow, multiWindowInterval)
	dec := readRate(scaler, target)
	require.False(t, dec.NoChange)
	require.Equal(t, 10, dec.NewTarget, "Ups run after Downs and so win")
}

// TestSimplePartitionScalerMostConservativeDownWindowWins is the Downs mirror of the Ups
// max: Downs are combined with min, so the window asking for the fewest partitions decides.
// Worth knowing before adding Downs -- a short Down window will pull the target down on a
// brief lull no matter what the longer windows think.
func TestSimplePartitionScalerMostConservativeDownWindowWins(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	scaler := newTestScaler(ts, dynamicconfig.SimplePartitionScalerSettings{
		Enabled: true,
		Downs: []dynamicconfig.SimplePartitionScalerThreshold{
			{Window: scalerWindow, TargetRate: 100},
			{Window: 2 * scalerWindow, TargetRate: 300},
		},
	})

	// At 600/s the 1s window wants 600/100 = 6 and the 2s window wants 600/300 = 2.
	target, _ := feedRate(t, scaler, ts, 20, 600, 2*scalerWindow, multiWindowInterval)
	dec := readRate(scaler, target)
	require.False(t, dec.NoChange)
	require.Equal(t, 2, dec.NewTarget, "the window asking for fewer partitions wins")
}

// TestSimplePartitionScalerLongestWindowGatesFirstDecision verifies that no decision is
// produced until every window is full, so the longest one sets the warm-up time.
func TestSimplePartitionScalerLongestWindowGatesFirstDecision(t *testing.T) {
	t.Parallel()

	// The two windows have different TargetRates, so they are not interchangeable: the
	// answer once a decision appears is the long window's 6, not the short window's 5.
	for _, fed := range []time.Duration{scalerWindow / 2, scalerWindow, 3 * scalerWindow / 2} {
		ts := clock.NewEventTimeSource()
		scaler := newTestScaler(ts, crossoverUps())
		target, _ := feedRate(t, scaler, ts, 1, 3000, fed, multiWindowInterval)
		require.True(t, readRate(scaler, target).NoChange,
			"only %v has elapsed, so the 2s window is not full yet", fed)
	}

	ts := clock.NewEventTimeSource()
	scaler := newTestScaler(ts, crossoverUps())
	target, _ := feedRate(t, scaler, ts, 1, 3000, 2*scalerWindow, multiWindowInterval)
	dec := readRate(scaler, target)
	require.False(t, dec.NoChange, "every window full -> decision produced")
	require.Equal(t, 6, dec.NewTarget)
}

// encodeCounts builds a Compact8-encoded backlog-count slice from raw values,
// matching the on-the-wire form updateBacklogTarget consumes.
func encodeCounts(values ...int64) []byte {
	b := make([]byte, len(values))
	for i, v := range values {
		b[i] = number.EncodeCompact8(v)
	}
	return b
}

// TestUpdateBacklogTargetSetsBitsAboveBase verifies that partitions whose
// backlog exceeds BacklogBase count toward the target, and the corresponding
// bits are recorded in the private bitset.
func TestUpdateBacklogTargetSetsBitsAboveBase(t *testing.T) {
	t.Parallel()
	cfg := dynamicconfig.SimplePartitionScalerSettings{BacklogReset: 100, BacklogBase: 300}

	var bs bitSet
	// p0 well above base, p1 below reset, p2 above base.
	counts := encodeCounts(500, 32, 500)
	target := updateBacklogTarget(cfg, counts, &bs)

	require.Equal(t, 2, target, "two partitions above base should count toward target")
	require.True(t, bs.get(0))
	require.False(t, bs.get(1))
	require.True(t, bs.get(2))
}

// TestUpdateBacklogTargetHysteresis verifies the dead zone between BacklogReset
// and BacklogBase: a set bit stays set and a clear bit stays clear when the
// count lands between the two thresholds.
func TestUpdateBacklogTargetHysteresis(t *testing.T) {
	t.Parallel()
	cfg := dynamicconfig.SimplePartitionScalerSettings{BacklogReset: 100, BacklogBase: 300}

	// p0 starts set, p1 starts clear. Both get a count of ~200 (between
	// reset=100 and base=300), so neither should flip.
	bs := bitSet(nil).set(0)
	counts := encodeCounts(200, 200)
	require.Greater(t, number.DecodeCompact8(counts[0]), int64(100), "quantization moved too much")
	require.Less(t, number.DecodeCompact8(counts[0]), int64(300), "quantization moved too much")
	target := updateBacklogTarget(cfg, counts, &bs)

	require.Equal(t, 1, target, "only the already-set partition counts")
	require.True(t, bs.get(0), "set bit stays set in dead zone")
	require.False(t, bs.get(1), "clear bit stays clear in dead zone")
}

// TestUpdateBacklogTargetClearsBelowReset verifies that a previously-set bit is
// cleared once its backlog drops below BacklogReset.
func TestUpdateBacklogTargetClearsBelowReset(t *testing.T) {
	t.Parallel()
	cfg := dynamicconfig.SimplePartitionScalerSettings{BacklogReset: 100, BacklogBase: 300}

	bs := bitSet(nil).set(0).set(1)

	// p0 drops below reset (cleared), p1 stays in the dead zone (kept).
	counts := encodeCounts(32, 200)
	target := updateBacklogTarget(cfg, counts, &bs)

	require.Equal(t, 1, target)
	require.False(t, bs.get(0), "bit cleared once below reset")
	require.True(t, bs.get(1), "bit in dead zone retained")
}

// TestOnTasksFixedIncludesBacklogCap verifies the fixed-target fast path now
// propagates BacklogCap into the decision.
func TestOnTasksFixedIncludesBacklogCap(t *testing.T) {
	t.Parallel()
	cfg := dynamicconfig.SimplePartitionScalerSettings{
		Enabled:    true,
		Fixed:      2,
		BacklogCap: 1000,
	}
	scaler := newSimplePartitionScaler(
		dynamicconfig.GetTypedPropertyFn(cfg),
		nil, // time source unused on the fixed path
	)
	decision := scaler.OnTasks(PartitionScalerInput{CurrentTarget: 1})
	require.Equal(t, 2, decision.NewTarget)
	require.Equal(t, 1000, decision.BacklogCap)
}

// TestOnTasksFloorsAddTargetAtOne verifies that with no rate windows configured
// the add-based target is floored at 1 (never 0, which would disable scaling).
// This baseline is what lets backlog-based scaling grow.
func TestOnTasksFloorsAddTargetAtOne(t *testing.T) {
	t.Parallel()
	cfg := dynamicconfig.SimplePartitionScalerSettings{Enabled: true}
	scaler := newSimplePartitionScaler(dynamicconfig.GetTypedPropertyFn(cfg), nil)

	decision := scaler.OnTasks(PartitionScalerInput{CurrentTarget: 0})
	require.Equal(t, 1, decision.NewTarget, "add baseline must floor at 1, not disable scaling")
}

// TestOnTasksBacklogScalesUpAndDown verifies that with no rate windows, backlog
// pressure grows the target one partition at a time (baseline 1 + occupied count)
// and shrinks back to the baseline once partitions drain below BacklogReset.
func TestOnTasksBacklogScalesUpAndDown(t *testing.T) {
	t.Parallel()
	cfg := dynamicconfig.SimplePartitionScalerSettings{
		Enabled:      true,
		BacklogReset: 100,
		BacklogBase:  300,
		BacklogCap:   1000,
		Max:          4,
	}
	scaler := newSimplePartitionScaler(dynamicconfig.GetTypedPropertyFn(cfg), nil)

	// One partition, occupied: baseline 1 + 1 occupied = 2.
	d := scaler.OnTasks(PartitionScalerInput{CurrentTarget: 1, BacklogCounts: encodeCounts(500)})
	require.Equal(t, 2, d.NewTarget)

	// Two partitions, both occupied: baseline 1 + 2 occupied = 3.
	d = scaler.OnTasks(PartitionScalerInput{
		CurrentTarget: 2,
		BacklogCounts: encodeCounts(500, 500),
		PrivateState:  d.PrivateState,
	})
	require.Equal(t, 3, d.NewTarget)

	// A newly-opened partition that is not yet occupied does not add more capacity:
	// baseline 1 + 2 occupied = 3 (unchanged).
	d = scaler.OnTasks(PartitionScalerInput{
		CurrentTarget: 3,
		BacklogCounts: encodeCounts(500, 500, 32),
		PrivateState:  d.PrivateState,
	})
	require.Equal(t, 3, d.NewTarget)

	// All drain below reset: bits clear, target falls back to the baseline of 1.
	d = scaler.OnTasks(PartitionScalerInput{
		CurrentTarget: 3,
		BacklogCounts: encodeCounts(32, 32, 32),
		PrivateState:  d.PrivateState,
	})
	require.Equal(t, 1, d.NewTarget)
}
