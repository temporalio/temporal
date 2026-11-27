package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// pending   = max(0, bufferedCount - retainedActionCount)
// available = max(0, maxBufferSize/2 - pending - generatorReserve)
// result    = available / max(1, backfillerCount)
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
		{"buffered below retained is fully discounted", 5, 10, 1000, 0, 1, 500},
		{"buffered equal to retained is fully discounted", 10, 10, 1000, 0, 1, 500},
		{"buffered one past retained costs one slot", 11, 10, 1000, 0, 1, 499},
		{"buffered well past retained costs 1:1", 60, 10, 1000, 0, 1, 450},

		{"buffered one below the half-buffer leaves one slot", 49, 0, 100, 0, 1, 1},
		{"buffered at the half-buffer exhausts capacity", 50, 0, 100, 0, 1, 0},
		{"buffered past the half-buffer stays exhausted", 60, 0, 100, 0, 1, 0},
		{"discount lets the buffer fill to half plus retained", 59, 10, 100, 0, 1, 1},
		{"buffer at half plus retained exhausts capacity", 60, 10, 100, 0, 1, 0},
		{"reserve lowers the fill point that exhausts capacity", 40, 0, 100, 10, 1, 0},

		{"zero retained means no discount", 20, 0, 1000, 0, 1, 480},
		{"retained equal to buffered discounts all", 20, 20, 1000, 0, 1, 500},
		{"retained above buffered clamps pending at zero", 20, 100, 1000, 0, 1, 500},

		{"zero max buffer yields zero", 0, 0, 0, 0, 1, 0},
		{"max buffer of one halves to zero", 0, 0, 1, 0, 1, 0},
		{"max buffer of two halves to one", 0, 0, 2, 0, 1, 1},
		{"odd max buffer truncates the half", 0, 0, 3, 0, 1, 1},

		{"reserve below half leaves remainder", 0, 0, 100, 49, 1, 1},
		{"reserve equal to half yields zero", 0, 0, 100, 50, 1, 0},
		{"reserve above half clamps at zero", 0, 0, 100, 60, 1, 0},

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
