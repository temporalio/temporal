package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIncompleteBufferedStartCount exercises the shared discount at its
// boundaries: below, at, and past completedCount, plus the clamp that
// keeps the result from going negative when completedCount overstates
// what's actually buffered.
func TestIncompleteBufferedStartCount(t *testing.T) {
	cases := []struct {
		name          string
		bufferedCount int
		completed     int
		want          int
	}{
		{"buffered below completed is fully discounted", 5, 10, 0},
		{"buffered equal to completed is fully discounted", 10, 10, 0},
		{"buffered one past completed costs one slot", 11, 10, 1},
		{"buffered well past completed costs 1:1", 60, 10, 50},
		{"zero completed means no discount", 20, 0, 20},
		{"completed equal to buffered discounts all", 20, 20, 0},
		{"completed above buffered clamps at zero", 20, 100, 0},
		{"everything zero", 0, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, incompleteBufferedStartCount(tc.bufferedCount, tc.completed))
		})
	}
}

// TestGeneratorBufferCapacity exercises the Generator's capacity formula
// around its boundaries. Unlike the Backfiller's, this one is not clamped at
// zero: a negative result signals the buffer is already over capacity, which
// SpecProcessor.ProcessTimeRange relies on to start dropping immediately.
//
// Formula:
//
//	capacity = maxBufferSize - incompleteBufferedStartCount(bufferedCount, completedCount)
func TestGeneratorBufferCapacity(t *testing.T) {
	cases := []struct {
		name          string
		bufferedCount int
		completed     int
		maxBufferSize int
		want          int
	}{
		// completedCount discount boundary.
		{"buffered below completed leaves full capacity", 5, 10, 1000, 1000},
		{"buffered equal to completed leaves full capacity", 10, 10, 1000, 1000},
		{"buffered one past completed costs one slot", 11, 10, 1000, 999},
		{"buffered well past completed costs 1:1", 60, 10, 1000, 950},

		// the boundary where admission capacity is exhausted.
		{"one below max leaves one slot", 99, 0, 100, 1},
		{"at max exhausts capacity exactly", 100, 0, 100, 0},
		{"past max goes negative, signaling overrun", 110, 0, 100, -10},
		// the completed-count discount pushes that fill point out by
		// completedCount, which is the bug this function fixes: completed
		// history no longer eats into this boundary.
		{"discount lets the buffer fill to max plus completed", 109, 10, 100, 1},
		{"buffer at max plus completed exhausts capacity", 110, 10, 100, 0},

		// maxBufferSize edges.
		{"zero max with an empty buffer yields zero", 0, 0, 0, 0},
		{"zero max with any buffered content goes negative", 1, 0, 0, -1},

		// completedCount above bufferedCount clamps pending at zero,
		// so capacity is never inflated past maxBufferSize.
		{"completed above buffered clamps pending, not capacity", 5, 100, 1000, 1000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := generatorBufferCapacity(tc.bufferedCount, tc.completed, tc.maxBufferSize)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestBackfillerBufferCapacity exercises each parameter of the capacity formula
// around its boundaries.
//
// Parameters:
//   - bufferedCount:    total entries currently in the Invoker's buffer,
//     including both actionable starts and completed history retained for
//     reporting.
//   - completedCount:   the actual number of those entries that are completed
//     (GetCompleted() != nil), not the recentActionCount retention cap --
//     subtracting the cap unconditionally would discount unrelated
//     pending/running starts whenever the buffer holds fewer completions
//     than the cap allows.
//   - maxBufferSize:    the buffer's total size limit; backfillers collectively
//     get at most half of it (maxBufferSize/2).
//   - generatorReserve: slots held back from the shared half-buffer for the
//     Generator (which produces the schedule's automatic actions), taken once so
//     backfillers cannot starve regular scheduling.
//   - backfillerCount:  number of concurrently active backfillers; the
//     remaining capacity is split evenly across them (clamped to >= 1 when calculating
//     the allowed backfill capacity).
//
// Formula:
//
//	pending   = max(0, bufferedCount - completedCount)
//	available = max(0, maxBufferSize/2 - pending - generatorReserve)
//	result    = available / max(1, backfillerCount)
func TestBackfillerBufferCapacity(t *testing.T) {
	cases := []struct {
		name             string
		bufferedCount    int
		completed        int
		maxBufferSize    int
		generatorReserve int
		backfillerCount  int
		want             int
	}{
		// bufferedCount vs completedCount: the completed-count discount boundary.
		{"buffered below completed is fully discounted", 5, 10, 1000, 0, 1, 500},
		{"buffered equal to completed is fully discounted", 10, 10, 1000, 0, 1, 500},
		{"buffered one past completed costs one slot", 11, 10, 1000, 0, 1, 499},
		{"buffered well past completed costs 1:1", 60, 10, 1000, 0, 1, 450},

		// bufferedCount filling the available half-buffer: the boundary where
		// admission capacity is exhausted (completed=0 here, so pending==bufferedCount).
		{"buffered one below the half-buffer leaves one slot", 49, 0, 100, 0, 1, 1},
		{"buffered at the half-buffer exhausts capacity", 50, 0, 100, 0, 1, 0},
		{"buffered past the half-buffer stays exhausted", 60, 0, 100, 0, 1, 0},
		// the completed-count discount pushes that fill point out by completedCount.
		{"discount lets the buffer fill to half plus completed", 59, 10, 100, 0, 1, 1},
		{"buffer at half plus completed exhausts capacity", 60, 10, 100, 0, 1, 0},
		// the generator reserve pulls that fill point in by generatorReserve.
		{"reserve lowers the fill point that exhausts capacity", 40, 0, 100, 10, 1, 0},

		// completedCount: no discount vs. over-discount clamp.
		{"zero completed means no discount", 20, 0, 1000, 0, 1, 480},
		{"completed equal to buffered discounts all", 20, 20, 1000, 0, 1, 500},
		{"completed above buffered clamps pending at zero", 20, 100, 1000, 0, 1, 500},

		// maxBufferSize: disabled, and integer-division of the half.
		{"zero max buffer yields zero", 0, 0, 0, 0, 1, 0},
		{"max buffer of one halves to zero", 0, 0, 1, 0, 1, 0},
		{"max buffer of two halves to one", 0, 0, 2, 0, 1, 1},
		{"odd max buffer truncates the half", 0, 0, 3, 0, 1, 1},

		// generatorReserve: below, at, and above the available half-buffer.
		{"reserve below half leaves remainder", 0, 0, 100, 49, 1, 1},
		{"reserve equal to half yields zero", 0, 0, 100, 50, 1, 0},
		{"reserve above half clamps at zero", 0, 0, 100, 60, 1, 0},

		// backfillerCount: the shared reserve split, integer-division truncation,
		// and the divide-by-zero guard.
		{"single backfiller gets the whole remainder", 0, 0, 1000, 50, 1, 450},
		{"ten backfillers each get an even share (regression)", 0, 0, 1000, 50, 10, 45},
		{"backfillers exactly dividing remainder get one each", 0, 0, 1000, 50, 450, 1},
		{"more backfillers than remainder truncate to zero", 0, 0, 1000, 50, 451, 0},
		{"zero backfillers are clamped to one", 0, 0, 1000, 50, 0, 450},
		{"negative backfillers are clamped to one", 0, 0, 1000, 50, -3, 450},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := backfillerBufferCapacity(
				tc.bufferedCount, tc.completed, tc.maxBufferSize, tc.generatorReserve, tc.backfillerCount,
			)
			require.Equal(t, tc.want, got)
		})
	}
}

// Bounds for the exhaustive invariant sweeps below. Used as both
// bufferedCount and completedCount, so it straddles the real
// recentActionCount from both directions.
var countRange = []int{
	0,                     // empty buffer
	1,                     // smallest nonzero
	2,                     // small, still well under retention
	3,                     // small, still well under retention
	recentActionCount / 2, // below recentActionCount, but not tiny
	recentActionCount,     // exactly recentActionCount: the real production retention limit
	recentActionCount + 1, // one past recentActionCount: first value where discounting has any effect
	3 * recentActionCount, // well past recentActionCount (3x): exercises the 1:1 linear region
}

// maxBufferSizeRange hits the parity/off-by-one values that matter for the
// maxBufferSize/2 floor division, plus a couple of larger, realistic-scale
// values relative to recentActionCount.
var maxBufferSizeRange = []int{
	0,                       // limit disabled / degenerate
	1,                       // smallest nonzero; halves to 0
	2,                       // smallest value that halves to a nonzero 1
	3,                       // odd: exercises floor truncation of the half
	2 * recentActionCount,   // 2x recentActionCount: matches the hand-picked table-test scale
	2*recentActionCount + 1, // odd counterpart to the above, same scale
	10 * recentActionCount,  // larger scale, still small enough to keep the sweep fast
}

// reserveRange covers "no reserve" and a couple of magnitudes, including the
// actual DefaultTweakables.GeneratorBufferReserveSize.
var reserveRange = []int{
	0,                 // reserve disabled
	1,                 // smallest nonzero
	recentActionCount, // matches recentActionCount's magnitude
	DefaultTweakables.GeneratorBufferReserveSize, // matches the real production default
}

// backfillerRange covers the divide-by-zero/negative-clamp inputs (which are
// never valid but must not panic), then counts on both sides of typical
// per-backfiller capacities.
var backfillerRange = []int{
	-3,                     // negative: must clamp to 1, not panic or go negative
	0,                      // zero: same clamp, guards the division
	1,                      // single backfiller gets the whole pool
	2,                      // smallest case where division actually splits the pool
	3,                      // small count, non-trivial remainder
	recentActionCount,      // matches recentActionCount's magnitude
	recentActionCount + 1,  // one more than typical retention-sized counts
	10 * recentActionCount, // large count: likely to floor-divide to 0 against a modest pool
}

// TestIncompleteBufferedStartCount_Invariants exhaustively checks the shared
// discount never produces a negative count and never discounts more than what
// was actually buffered.
func TestIncompleteBufferedStartCount_Invariants(t *testing.T) {
	for _, b := range countRange {
		for _, r := range countRange {
			got := incompleteBufferedStartCount(b, r)
			require.GreaterOrEqualf(t, got, 0, "bufferedCount=%d completedCount=%d", b, r)
			require.LessOrEqualf(t, got, b, "bufferedCount=%d completedCount=%d", b, r)
		}
	}
}

// TestGeneratorBufferCapacity_Invariants exhaustively checks two properties
// across every combination in range:
//
//  1. Capacity never exceeds maxBufferSize (completed history can only give
//     capacity back, never take more than the naive bufferedCount would).
//  2. When the buffer holds nothing but completed history (bufferedCount <=
//     completedCount), capacity is exactly maxBufferSize — this is the
//     SCH-093 property: retained completion history must never, by itself,
//     make a due action look like it doesn't fit.
func TestGeneratorBufferCapacity_Invariants(t *testing.T) {
	for _, b := range countRange {
		for _, r := range countRange {
			for _, maxSize := range maxBufferSizeRange {
				got := generatorBufferCapacity(b, r, maxSize)

				require.LessOrEqualf(t, got, maxSize,
					"bufferedCount=%d completedCount=%d maxBufferSize=%d: capacity must never exceed the configured limit",
					b, r, maxSize)

				// Never regress below what the pre-fix (buggy) formula would have
				// given: capacity must be at least maxBufferSize - bufferedCount.
				require.GreaterOrEqualf(t, got, maxSize-b,
					"bufferedCount=%d completedCount=%d maxBufferSize=%d: discounting completed history must never reduce capacity below the naive formula",
					b, r, maxSize)

				if b <= r {
					require.Equalf(t, maxSize, got,
						"bufferedCount=%d completedCount=%d maxBufferSize=%d: a buffer holding only completed history must report full capacity",
						b, r, maxSize)
				}
			}
		}
	}
}

// TestBackfillerBufferCapacity_Invariants exhaustively checks that a single
// call never allocates a negative amount, never allocates more than the
// shared half-buffer pool actually has (no overcommit), and — the "no
// livelock" property — that every backfiller gets at least one slot whenever
// the pool can genuinely cover one each.
func TestBackfillerBufferCapacity_Invariants(t *testing.T) {
	for _, b := range countRange {
		for _, r := range countRange {
			for _, maxSize := range maxBufferSizeRange {
				for _, reserve := range reserveRange {
					for _, n := range backfillerRange {
						got := backfillerBufferCapacity(b, r, maxSize, reserve, n)

						require.GreaterOrEqualf(t, got, 0,
							"bufferedCount=%d completedCount=%d maxBufferSize=%d generatorReserve=%d backfillerCount=%d: allocation must never be negative",
							b, r, maxSize, reserve, n)

						available := max(0, maxSize/2-incompleteBufferedStartCount(b, r)-reserve)
						effectiveN := max(1, n)

						// No overcommit: N backfillers each taking their share must never
						// exceed the shared pool, even after accounting for floor division.
						require.LessOrEqualf(t, got*effectiveN, available,
							"bufferedCount=%d completedCount=%d maxBufferSize=%d generatorReserve=%d backfillerCount=%d: N backfillers must never collectively be allowed to exceed the shared pool",
							b, r, maxSize, reserve, n)

						// No livelock: if the pool can cover at least one slot per
						// backfiller, nobody should be starved down to zero.
						if available >= effectiveN {
							require.GreaterOrEqualf(t, got, 1,
								"bufferedCount=%d completedCount=%d maxBufferSize=%d generatorReserve=%d backfillerCount=%d: pool can cover one slot each, so no backfiller should be starved to zero",
								b, r, maxSize, reserve, n)
						}
					}
				}
			}
		}
	}
}

// TestBackfillerBufferCapacity_SequentialAdmissionNeverOverruns simulates
// several backfillers each admitting their full allotment in turn (mutating
// the live buffered count in between, the way the real Invoker does) and
// checks the running total never pushes pending work past the reserved
// half-buffer. A single static call can't catch cross-call overcommit; this
// walks the same recurrence the production code drives.
func TestBackfillerBufferCapacity_SequentialAdmissionNeverOverruns(t *testing.T) {
	const (
		maxBufferSize    = 100
		generatorReserve = 10
		backfillerCount  = 4
		completed        = 10
		rounds           = 25 // more than enough ticks to reach steady state
	)
	budget := maxBufferSize/2 - generatorReserve

	buffered := completed // buffer starts out holding only retention history
	for round := range rounds {
		for backfiller := range backfillerCount {
			allotted := backfillerBufferCapacity(buffered, completed, maxBufferSize, generatorReserve, backfillerCount)
			require.GreaterOrEqualf(t, allotted, 0, "round=%d backfiller=%d", round, backfiller)
			buffered += allotted

			pending := incompleteBufferedStartCount(buffered, completed)
			require.LessOrEqualf(t, pending, budget,
				"round=%d backfiller=%d: cumulative admissions must never push pending work past the shared half-buffer budget",
				round, backfiller)
		}
	}
}
