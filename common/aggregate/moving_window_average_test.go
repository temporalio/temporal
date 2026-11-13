package aggregate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMovingWindowAvgCapacity(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		capacity int
		values   []int64
		want     float64
	}{
		{name: "single item", capacity: 1, values: []int64{7}, want: 7},
		{name: "single item overwritten", capacity: 1, values: []int64{7, 11}, want: 11},
		{name: "two items fill buffer", capacity: 2, values: []int64{7, 11}, want: 9},
		{name: "two items evict oldest", capacity: 2, values: []int64{7, 11, 17}, want: 14},
		{name: "three items fill buffer", capacity: 3, values: []int64{7, 11, 17}, want: 35.0 / 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			avg := NewMovingWindowAvgImpl(time.Hour, tc.capacity)
			for _, value := range tc.values {
				avg.Record(value)
			}
			require.Equal(t, tc.want, avg.Average())
		})
	}
}

func TestMovingWindowAvgExpireFullBuffer(t *testing.T) {
	t.Parallel()

	avg := NewMovingWindowAvgImpl(time.Hour, 2)
	avg.Record(3)
	avg.Record(7)

	avg.buffer[avg.headIdx].timestamp = time.Now().Add(-2 * time.Hour)
	require.Equal(t, float64(7), avg.Average())

	avg.buffer[avg.headIdx].timestamp = time.Now().Add(-2 * time.Hour)
	require.Zero(t, avg.Average())

	avg.Record(11)
	require.Equal(t, float64(11), avg.Average())
}
