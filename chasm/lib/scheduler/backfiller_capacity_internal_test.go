package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBackfillerBufferCapacity exercises each parameter of the capacity formula
// around its boundaries.
//
// Parameters:
//   - bufferedCount:       total entries currently in the Invoker's buffer,
//     including both actionable starts and retained completed history.
//   - retainedActionCount: how many completed actions are kept in the buffer for
//     reporting (the recentActionCount cap). These are discounted so retained
//     history does not consume admission capacity.
//   - maxBufferSize:       the buffer's total size limit; backfillers collectively
//     get at most half of it (maxBufferSize/2).
//   - generatorReserve:    slots held back from the shared half-buffer for the
//     Generator (which produces the schedule's automatic actions), taken once so
//     backfillers cannot starve regular scheduling.
//   - backfillerCount:     number of concurrently active backfillers; the
//     remaining capacity is split evenly across them (clamped to >= 1 so an empty
//     set of backfillers does not divide by zero).
//
// Formula:
//
//	pending   = max(0, bufferedCount - retainedActionCount)
//	available = max(0, maxBufferSize/2 - pending - generatorReserve)
//	result    = available / max(1, backfillerCount)
func TestBackfillerBufferCapacity(t *testing.T) {
	cases := []struct {
		name             string
		bufferedCount    int
		retainedActions  int
		maxBufferSize    int
		generatorReserve int
		backfillerCount  int
		want             int
	}{
		// bufferedCount vs retainedActionCount: the retained-history discount boundary.
		{"buffered below retained is fully discounted", 5, 10, 1000, 0, 1, 500},
		{"buffered equal to retained is fully discounted", 10, 10, 1000, 0, 1, 500},
		{"buffered one past retained costs one slot", 11, 10, 1000, 0, 1, 499},
		{"buffered well past retained costs 1:1", 60, 10, 1000, 0, 1, 450},

		// retainedActionCount: no discount vs. over-discount clamp.
		{"zero retained means no discount", 20, 0, 1000, 0, 1, 480},
		{"retained equal to buffered discounts all", 20, 20, 1000, 0, 1, 500},
		{"retained above buffered clamps pending at zero", 20, 100, 1000, 0, 1, 500},

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
				tc.bufferedCount, tc.retainedActions, tc.maxBufferSize, tc.generatorReserve, tc.backfillerCount,
			)
			require.Equal(t, tc.want, got)
		})
	}
}
