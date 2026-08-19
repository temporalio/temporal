package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIncompleteBufferedStartCount(t *testing.T) {
	testCases := []struct {
		name           string
		bufferedCount  int
		completedCount int
		want           int
	}{
		{
			name:           "below completed count",
			bufferedCount:  recentActionCount - 1,
			completedCount: recentActionCount,
			want:           0,
		},
		{
			name:           "at completed count",
			bufferedCount:  recentActionCount,
			completedCount: recentActionCount,
			want:           0,
		},
		{
			name:           "above completed count",
			bufferedCount:  recentActionCount + 1,
			completedCount: recentActionCount,
			want:           1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, incompleteBufferedStartCount(tc.bufferedCount, tc.completedCount))
		})
	}
}

func TestGeneratorBufferCapacity(t *testing.T) {
	maxBufferSize := DefaultTweakables.MaxBufferSize
	testCases := []struct {
		name           string
		bufferedCount  int
		completedCount int
		want           int
	}{
		{
			name:          "one pending slot below limit",
			bufferedCount: maxBufferSize - 1,
			want:          1,
		},
		{
			name:          "pending slots at limit",
			bufferedCount: maxBufferSize,
			want:          0,
		},
		{
			name:          "one pending slot above limit",
			bufferedCount: maxBufferSize + 1,
			want:          -1,
		},
		{
			name:           "completed retention with one pending slot below limit",
			bufferedCount:  recentActionCount + maxBufferSize - 1,
			completedCount: recentActionCount,
			want:           1,
		},
		{
			name:           "completed retention with pending slots at limit",
			bufferedCount:  recentActionCount + maxBufferSize,
			completedCount: recentActionCount,
			want:           0,
		},
		{
			name:           "completed retention with one pending slot above limit",
			bufferedCount:  recentActionCount + maxBufferSize + 1,
			completedCount: recentActionCount,
			want:           -1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(
				t,
				tc.want,
				generatorBufferCapacity(tc.bufferedCount, tc.completedCount, maxBufferSize),
			)
		})
	}

	t.Run("completed work always reopens capacity", func(t *testing.T) {
		for completedCount := 0; completedCount <= recentActionCount; completedCount++ {
			require.Positivef(
				t,
				generatorBufferCapacity(completedCount, completedCount, maxBufferSize),
				"generator must make progress with completedCount=%d",
				completedCount,
			)
		}
	})
}

func TestBackfillerBufferCapacity(t *testing.T) {
	maxBufferSize := DefaultTweakables.MaxBufferSize
	generatorReserve := DefaultTweakables.GeneratorBufferReserveSize
	backfillerPoolSize := backfillerBufferPoolSize(maxBufferSize, generatorReserve)

	testCases := []struct {
		name            string
		pendingCount    int
		completedCount  int
		backfillerCount int
		want            int
	}{
		{
			name:            "one slot per backfiller remains",
			pendingCount:    backfillerPoolSize - maxBackfillers,
			completedCount:  recentActionCount,
			backfillerCount: maxBackfillers,
			want:            1,
		},
		{
			name:            "one fewer than one slot per backfiller remains",
			pendingCount:    backfillerPoolSize - maxBackfillers + 1,
			completedCount:  recentActionCount,
			backfillerCount: maxBackfillers,
			want:            0,
		},
		{
			name:            "one pending slot below pool limit",
			pendingCount:    backfillerPoolSize - 1,
			completedCount:  recentActionCount,
			backfillerCount: 1,
			want:            1,
		},
		{
			name:            "pending slots at pool limit",
			pendingCount:    backfillerPoolSize,
			completedCount:  recentActionCount,
			backfillerCount: 1,
			want:            0,
		},
		{
			name:            "one pending slot above pool limit",
			pendingCount:    backfillerPoolSize + 1,
			completedCount:  recentActionCount,
			backfillerCount: 1,
			want:            0,
		},
		{
			name:            "no completed starts grants no retention discount",
			pendingCount:    backfillerPoolSize,
			completedCount:  0,
			backfillerCount: 1,
			want:            0,
		},
		{
			name:            "partial completed retention grants only the live discount",
			pendingCount:    backfillerPoolSize,
			completedCount:  recentActionCount - 1,
			backfillerCount: 1,
			want:            0,
		},
		{
			name:            "retention-sized backfiller group splits the pool",
			completedCount:  recentActionCount,
			backfillerCount: recentActionCount,
			want:            backfillerPoolSize / recentActionCount,
		},
		{
			name:           "zero backfillers is treated as one",
			completedCount: recentActionCount,
			want:           backfillerPoolSize,
		},
		{
			name:            "negative backfillers is treated as one",
			completedCount:  recentActionCount,
			backfillerCount: -1,
			want:            backfillerPoolSize,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bufferedCount := tc.completedCount + tc.pendingCount
			require.Equal(
				t,
				tc.want,
				backfillerBufferCapacity(
					bufferedCount,
					tc.completedCount,
					maxBufferSize,
					generatorReserve,
					tc.backfillerCount,
				),
			)
		})
	}

	t.Run("pool size bounds", func(t *testing.T) {
		const smallestBufferWithNonzeroPool = 2
		halfBufferSize := maxBufferSize / 2
		testCases := []struct {
			name             string
			maxBufferSize    int
			generatorReserve int
			want             int
		}{
			{
				name: "disabled buffer",
				want: 0,
			},
			{
				name:          "buffer below smallest nonzero pool",
				maxBufferSize: smallestBufferWithNonzeroPool - 1,
				want:          0,
			},
			{
				name:          "smallest nonzero pool",
				maxBufferSize: smallestBufferWithNonzeroPool,
				want:          1,
			},
			{
				name:          "odd buffer rounds down",
				maxBufferSize: smallestBufferWithNonzeroPool + 1,
				want:          1,
			},
			{
				name:             "reserve one below half buffer",
				maxBufferSize:    maxBufferSize,
				generatorReserve: halfBufferSize - 1,
				want:             1,
			},
			{
				name:             "reserve at half buffer",
				maxBufferSize:    maxBufferSize,
				generatorReserve: halfBufferSize,
				want:             0,
			},
			{
				name:             "reserve one above half buffer",
				maxBufferSize:    maxBufferSize,
				generatorReserve: halfBufferSize + 1,
				want:             0,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				require.Equal(t, tc.want, backfillerBufferPoolSize(tc.maxBufferSize, tc.generatorReserve))
			})
		}
	})

	// Retained completed actions are the only entries that can remain after all
	// admitted work has finished. Exhaust every possible retained count and
	// supported backfiller count to prove that history alone cannot keep a
	// backfiller at zero capacity under the production defaults.
	t.Run("completed work always reopens capacity", func(t *testing.T) {
		for completedCount := 0; completedCount <= recentActionCount; completedCount++ {
			for backfillerCount := 1; backfillerCount <= maxBackfillers; backfillerCount++ {
				require.Positivef(
					t,
					backfillerBufferCapacity(
						completedCount,
						completedCount,
						maxBufferSize,
						generatorReserve,
						backfillerCount,
					),
					"backfiller must make progress with completedCount=%d backfillerCount=%d",
					completedCount,
					backfillerCount,
				)
			}
		}
	})
}
