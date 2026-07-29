package matching

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
)

func TestAddTasksRate(t *testing.T) {
	// define a fake clock and it's time for testing
	timeSource := clock.NewEventTimeSource()
	currentTime := time.Now()
	timeSource.Update(currentTime)

	tr := newTaskTracker(timeSource, 5*time.Second, 30*time.Second)

	// mini windows will have the following format : (start time, end time)
	// (0 - 4), (5 - 9), (10 - 14), (15 - 19), (20 - 24), (25 - 29), (30 - 34), ...

	// rate should be zero when no time is passed
	rate, full := tr.rateAndFull() // time: 0
	require.InDelta(t, float32(0), rate, 1e-9)
	require.False(t, full)
	tr.inc(100)
	rate, full = tr.rateAndFull() // still zero because no time is passed
	require.InDelta(t, float32(0), rate, 1e-9)
	require.False(t, full)

	// tasks should be placed in the first mini-window
	timeSource.Advance(1 * time.Second) // time: 1 second
	rate, full = tr.rateAndFull()
	require.InEpsilon(t, float32(100), rate, 0.001) // 100 tasks added in 1 second = 100 / 1 = 100
	require.False(t, full)                          // 1s < 30s total interval

	// tasks should be placed in the second mini-window with 6 total seconds elapsed
	timeSource.Advance(5 * time.Second) // time: 6 second
	tr.inc(100)
	tr.inc(100)
	rate, full = tr.rateAndFull()
	require.InEpsilon(t, float32(50), rate, 0.001) // (100 + 200) tasks added in 6 seconds = 300/6 = 50
	require.False(t, full)                         // 6s < 30s total interval

	timeSource.Advance(24 * time.Second) // time: 30 second
	tr.inc(100)
	tr.inc(100)
	tr.inc(100)
	rate, full = tr.rateAndFull()
	require.InEpsilon(t, float32(20), rate, 0.001) // (100 + 200 + 300) tasks added in (30 + 0 (current window)) seconds = 600/30 = 20
	require.True(t, full)                          // full 30s interval has now elapsed

	// this should clear out the first mini-window of 100 tasks
	timeSource.Advance(5 * time.Second) // time: 35 second
	tr.inc(10)
	require.InEpsilon(t, float32(17), tr.rate(), 0.001) // (10 + 200 + 300) tasks added in (30 + 0 (current window)) seconds = 510/30 = 17

	// this should clear out the second and third mini-windows
	timeSource.Advance(15 * time.Second) // time: 50 second
	tr.inc(10)
	require.InEpsilon(t, float32(10.666667), tr.rate(), 0.001) // (10 + 10 + 300) tasks added in (30 + 0 (current window)) seconds = 320/30 = 10.66

	// a minute passes and no tasks are added
	timeSource.Advance(60 * time.Second)
	require.Equal(t, float32(0), tr.rate()) // 0 tasks have been added in the last 30 seconds
}

// partitionScalerBucketSize is the bucket size the partition scaler uses in production today.
//
// simplePartitionScaler.getTracker always derives the bucket size as
// window/simplePartitionScalerTrackerBuckets, where simplePartitionScalerTrackerBuckets = 10,
// so 100ms buckets go with a 1s window.
// The tests below construct the tracker directly and hold the bucket size at 100ms while
// varying the window, which isolates the effect of the window: the bias derived below
// depends on the window and the caller's cadence, and only second-order on the bucket
// size.
const partitionScalerBucketSize = 100 * time.Millisecond

// steadyRate feeds tr a steady tasksPerCall tasks every interval and returns the measured
// rate along with the true rate, taking the reading on the same call that adds the last
// increment (which is what production does -- see simplePartitionScaler.OnTasks, which
// calls inc then rateAndFull).
//
// Every call lands exactly offset into a bucket. Holding that fixed requires interval to
// be a whole number of buckets, and the offset that the tracker actually ends up at is
// read back and checked, so the caller cannot silently ask for something else.
func steadyRate(
	t *testing.T,
	window, interval, offset time.Duration,
	tasksPerCall int,
) (measured, trueRate float64) {
	t.Helper()
	require.Zero(t, interval%partitionScalerBucketSize,
		"interval must be a whole number of buckets to hold the bucket offset fixed")
	require.Less(t, offset, partitionScalerBucketSize,
		"an offset of a whole bucket is offset 0 of the next bucket")

	ts := clock.NewEventTimeSource()
	ts.Update(time.Unix(0, 0))
	tr := newTaskTracker(ts, partitionScalerBucketSize, window)

	// Shift off the bucket boundary before the first increment. Because interval is a whole
	// number of buckets, every later call sits at this same offset.
	ts.Advance(offset)

	// Feed for long enough that the window is covered by the feed itself, so this is the
	// steady-state reading and not a warm-up one. The reading is periodic in interval after
	// that, so extra calls do not change it.
	calls := int(window/interval) + 3
	for i := range calls {
		if i > 0 {
			ts.Advance(interval)
		}
		tr.inc(tasksPerCall)
	}

	rate, full := tr.rateAndFull()
	require.True(t, full, "window %v should be full after %d calls at %v", window, calls, interval)
	require.Equal(t, offset, ts.Now().Sub(tr.bucketStartTime), "offset into the current bucket")

	return float64(rate), float64(tasksPerCall) / interval.Seconds()
}

// predictedRatio is the closed form under test:
//
//	measured rate / true rate = (floor(x) + 1) / x    where x = (window + offset) / interval
func predictedRatio(window, interval, offset time.Duration) float64 {
	x := float64(window+offset) / float64(interval)
	return (math.Floor(x) + 1) / x
}

// TestTaskTrackerMeasuredRateFormula pins the closed form for the tracker's steady-state
// measurement bias under a caller that adds and reads in the same call.
//
// Where it comes from: a reading retains everything in the current bucket plus the previous
// `window` worth of buckets, so it divides by window+offset, where offset is how far into
// the current bucket the read lands. But both ends of that span are increment instants, so
// the span holds floor((window+offset)/interval) + 1 increments -- one more than its length
// accounts for, because the oldest retained increment's tasks all arrived at the instant the
// span opened. Hence
//
//	measured / true = interval * (floor(x) + 1) / (window + offset) = (floor(x) + 1) / x
//
// Note interval is the gap between calls, not the bucket size.
func TestTaskTrackerMeasuredRateFormula(t *testing.T) {
	t.Parallel()

	cases := []struct {
		window, interval, offset time.Duration
		wantRatio                float64 // stated here as well, so the closed form is cross-checked
	}{
		// Window a whole number of intervals and offset 0, so x is an integer and the
		// formula collapses to 1 + interval/window.
		{window: time.Second, interval: 100 * time.Millisecond, wantRatio: 1.1}, // x = 10
		{window: time.Second, interval: 200 * time.Millisecond, wantRatio: 1.2}, // x = 5
		{window: time.Second, interval: 500 * time.Millisecond, wantRatio: 1.5}, // x = 2
		{window: 2 * time.Second, interval: 100 * time.Millisecond, wantRatio: 1.05},
		{window: 5 * time.Second, interval: 100 * time.Millisecond, wantRatio: 1.02},
		{window: 30 * time.Second, interval: 100 * time.Millisecond, wantRatio: 1 + 1.0/300},
		{window: 30 * time.Second, interval: time.Second, wantRatio: 1 + 1.0/30},
		{window: 30 * time.Second, interval: 3 * time.Second, wantRatio: 1.1},  // x = 10
		{window: 60 * time.Second, interval: 3 * time.Second, wantRatio: 1.05}, // x = 20

		// A non-zero offset only ever helps, because it lengthens the span the tracker
		// divides by without necessarily retaining another increment.
		{window: time.Second, interval: 100 * time.Millisecond, offset: 50 * time.Millisecond,
			wantRatio: 11.0 / 10.5},
		{window: time.Second, interval: 100 * time.Millisecond, offset: 90 * time.Millisecond,
			wantRatio: 11.0 / 10.9},
		{window: time.Second, interval: 200 * time.Millisecond, offset: 60 * time.Millisecond,
			wantRatio: 6 / 5.3},
		{window: 60 * time.Second, interval: 3 * time.Second, offset: 90 * time.Millisecond,
			wantRatio: 21 / 20.03},

		// x need not be an integer. Here floor(x) truncates hard: 10s/700ms = 14.28, so 15
		// increments are retained over 10s.
		{window: 10 * time.Second, interval: 700 * time.Millisecond, wantRatio: 15 / (10 / 0.7)},
		{window: 10 * time.Second, interval: 1500 * time.Millisecond, wantRatio: 7 / (10 / 1.5)},

		// x <= 1: the window is no longer than the gap between calls, so exactly one
		// increment is ever retained and the reading is that whole lump divided by the
		// window. This is the regime the MaxRate cooldown creates -- see
		// TestTaskTrackerWindowSizingForProdSettings.
		{window: time.Second, interval: time.Second, wantRatio: 2},     // x = 1
		{window: time.Second, interval: 3 * time.Second, wantRatio: 3}, // x = 1/3
		{window: time.Second, interval: 3 * time.Second, offset: 50 * time.Millisecond,
			wantRatio: 1 / 0.35},
	}

	for _, c := range cases {
		name := fmt.Sprintf("window=%v/interval=%v/offset=%v", c.window, c.interval, c.offset)
		t.Run(name, func(t *testing.T) {
			// The closed form and the value written out in the table must agree.
			require.InEpsilon(t, c.wantRatio, predictedRatio(c.window, c.interval, c.offset), 1e-9,
				"closed form disagrees with the ratio stated in the table")

			// The tracker must agree with both. tasksPerCall cancels out of the ratio, so
			// check two values to show that.
			for _, tasksPerCall := range []int{1, 100} {
				measured, trueRate := steadyRate(t, c.window, c.interval, c.offset, tasksPerCall)
				require.InEpsilon(t, c.wantRatio, measured/trueRate, 1e-5,
					"tasksPerCall=%d: measured %v/s vs true %v/s", tasksPerCall, measured, trueRate)
			}
		})
	}
}

// TestTaskTrackerWindowSizingReference turns the formula into the reference the sizing
// decision actually needs: for a rough OnTasks interval, which windows read close to the
// true rate.
//
// The usable bound is that (floor(x)+1)/x <= (x+1)/x = 1 + interval/(window+offset), so
//
//	measured / true <= 1 + interval/window
//
// for every offset, with equality when the window is a whole number of intervals and the
// read lands on a bucket boundary. So the worst-case relative bias is just
// interval/window: within 10% needs window >= 10x the interval, within 5% needs 20x.
func TestTaskTrackerWindowSizingReference(t *testing.T) {
	t.Parallel()

	// Intervals worth having a reference for. 3s is the cooldown implied by the default
	// MaxRate of 0.33, and 23s is the default BackgroundInterval, the slowest cadence
	// OnTasks is ever called at.
	intervals := []time.Duration{
		100 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
		3 * time.Second,
		10 * time.Second,
		23 * time.Second,
	}
	windows := []time.Duration{
		time.Second,
		2 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
		60 * time.Second,
		120 * time.Second,
	}

	// worstOverOffsets is the largest ratio over every bucket offset, i.e. what the sizing
	// decision has to tolerate, since production reads at an arbitrary offset.
	worstOverOffsets := func(window, interval time.Duration) float64 {
		worst := 0.0
		for offset := time.Duration(0); offset < partitionScalerBucketSize; offset += 10 * time.Millisecond {
			measured, trueRate := steadyRate(t, window, interval, offset, 100)
			require.InEpsilon(t, predictedRatio(window, interval, offset), measured/trueRate, 1e-5)
			worst = max(worst, measured/trueRate)
		}
		return worst
	}

	header := "  window |"
	for _, interval := range intervals {
		header += fmt.Sprintf(" %9v", interval)
	}
	t.Log("worst-case measured rate / true rate, by window (rows) and OnTasks interval (columns):")
	t.Log(header)

	for _, window := range windows {
		row := fmt.Sprintf(" %7v |", window)
		for _, interval := range intervals {
			worst := worstOverOffsets(window, interval)
			row += fmt.Sprintf(" %9.2f", worst)

			// 1 + interval/window bounds every offset, and is exactly attained when the
			// window is a whole number of intervals (the offset-0 reading). The slack is
			// for float32 rounding in taskTracker.rateAndFull, not for the bound itself.
			const slack = 1 + 1e-6
			bound := 1 + float64(interval)/float64(window)
			require.LessOrEqual(t, worst, bound*slack,
				"window=%v interval=%v: 1+interval/window must bound the ratio", window, interval)
			if window%interval == 0 {
				require.InEpsilon(t, bound, worst, 1e-5,
					"window=%v interval=%v: bound must be attained at offset 0", window, interval)
			}

			// The reference ranges themselves.
			if window >= 10*interval {
				require.LessOrEqual(t, worst, 1.10*slack,
					"window=%v is >= 10x interval=%v, so it should read within 10%%", window, interval)
			}
			if window >= 20*interval {
				require.LessOrEqual(t, worst, 1.05*slack,
					"window=%v is >= 20x interval=%v, so it should read within 5%%", window, interval)
			}
		}
		t.Log(row)
	}

	t.Log("rule of thumb: worst-case relative bias = interval/window.")
	t.Log("  within 10% -> window >= 10x the OnTasks interval; within 5% -> window >= 20x.")
	for _, interval := range intervals {
		t.Logf("  interval %-6v -> window >= %-6v for 10%%, >= %-6v for 5%%",
			interval, 10*interval, 20*interval)
	}
}

// TestTaskTrackerBiasIsOneCallWorthOfTasks restates the bias in absolute terms, which is
// the more useful form when OnTasks is triggered by a batch of tasks rather than by a timer.
//
// At offset 0 with an integer x, measured = tasksPerCall*(window/interval + 1)/window,
// which is exactly trueRate + tasksPerCall/window. So the tracker reads high by one
// OnTasks call's worth of tasks spread over the window, independent of the rate:
//
//	measured - true = tasksPerCall / window     (tasks/second)
//
// That matters because scale_manager triggers OnTasks once a batch has accumulated
// (AddedTasks signals at BatchSize x partitions), so in that regime tasksPerCall is
// roughly constant and the interval shrinks as the rate grows -- the absolute bias stays
// put while the relative bias falls away.
func TestTaskTrackerBiasIsOneCallWorthOfTasks(t *testing.T) {
	t.Parallel()

	const window = 10 * time.Second
	const tasksPerCall = 1000

	// Same batch size at four different rates, so the interval between calls is the thing
	// that changes: rate 1000/s fills a 1000-task batch every 1s, rate 10000/s every 100ms.
	for _, interval := range []time.Duration{
		time.Second,
		500 * time.Millisecond,
		200 * time.Millisecond,
		100 * time.Millisecond,
	} {
		measured, trueRate := steadyRate(t, window, interval, 0, tasksPerCall)
		require.InEpsilon(t, float64(tasksPerCall)/window.Seconds(), measured-trueRate, 1e-4,
			"interval=%v: bias should be one call's worth of tasks over the window", interval)
		t.Logf("interval=%-6v true=%7.0f/s measured=%7.0f/s bias=%+.0f/s (%.1f%%)",
			interval, trueRate, measured, measured-trueRate, 100*(measured/trueRate-1))
	}
}

// TestTaskTrackerWindowSizingForProdSettings applies both forms of the bias to the two
// production settings that set the OnTasks cadence, to get concrete window bounds.
//
// scale_manager.callScaler returns before it swaps the batch while
// now < nextDecision, and nextDecision is set to now + 1/MaxRate whenever a target change
// is applied. So after every applied change: OnTasks is not called for a whole cooldown,
// tasks keep accumulating in the batch counter, and the next call hands the tracker the
// entire cooldown's worth of tasks at once. The cooldown is therefore a floor on the
// OnTasks interval, and it is the regime that decides how small the window can be.
func TestTaskTrackerWindowSizingForProdSettings(t *testing.T) {
	t.Parallel()

	// MatchingPartitionScaleManager defaults to MaxRate 0.33, i.e. a cooldown of ~3.03s.
	// Rounded to 3s here so calls land on bucket boundaries; the 30ms does not move
	// anything below.
	const cooldown = 3 * time.Second

	ratioAt := func(window time.Duration) float64 {
		measured, trueRate := steadyRate(t, window, cooldown, 0, 3000)
		return measured / trueRate
	}

	// A 1s window -- the window that goes with today's 100ms buckets -- is shorter than the
	// cooldown, so it retains exactly the one post-cooldown lump and divides 3s of tasks by
	// 1s. It reads 3x the true rate, which is 3x the partition target.
	require.InEpsilon(t, 3.0, ratioAt(time.Second), 1e-5,
		"a window shorter than the cooldown reads cooldown/window times high")

	// Getting that post-change reading under 10% takes a window >= 10x the cooldown.
	require.Greater(t, ratioAt(10*time.Second), 1.10)
	require.InEpsilon(t, 1.1, ratioAt(30*time.Second), 1e-5)
	require.InEpsilon(t, 1.05, ratioAt(60*time.Second), 1e-5)

	t.Logf("cooldown %v (MaxRate 0.33): window 1s reads %.2fx, 10s %.2fx, 30s %.2fx, 60s %.2fx",
		cooldown, ratioAt(time.Second), ratioAt(10*time.Second),
		ratioAt(30*time.Second), ratioAt(60*time.Second))
	t.Log("=> post-change overshoot is multiplicative: window >= 10/MaxRate for 10%, >= 20/MaxRate for 5%")

	// The other regime: between changes there is no cooldown, so OnTasks fires once per
	// batch and the bias is the absolute one from
	// TestTaskTrackerBiasIsOneCallWorthOfTasks -- BatchSize x partitions tasks spread over
	// the window. In partition terms that is (tasksPerCall/window)/TargetRate, so a window
	// of 10 x BatchSize/TargetRate holds it under 0.1 partitions.
	const batchSize = 100 // MatchingPartitionScaleManager default
	for _, targetRate := range []int{100, 500, 1000} {
		for _, partitions := range []int{1, 10} {
			tasksPerCall := batchSize * partitions
			for _, window := range []time.Duration{time.Second, 10 * time.Second, 60 * time.Second} {
				biasPartitions := (float64(tasksPerCall) / window.Seconds()) / float64(targetRate)
				t.Logf("TargetRate=%-5d partitions=%-3d window=%-4v steady-state bias = %+.2f partitions",
					targetRate, partitions, window, biasPartitions)
			}
		}
	}
	t.Log("=> steady-state bias in partitions = BatchSize*partitions/(window*TargetRate);")
	t.Log("   window >= 10*BatchSize*partitions/TargetRate keeps it under 0.1 partitions")
}
