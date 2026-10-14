package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestJitteredSyncIntervalSpreadsPhases simulates many backlog managers that
// start at the same moment -- e.g. every task queue loaded by a matching host
// when it starts -- and compute their first periodic sync delay. Without
// jitter, all of them would fire exactly one interval later, in phase, and
// hit persistence in a single burst. With jitter, the first ticks must land
// spread across the jitter range.
func TestJitteredSyncIntervalSpreadsPhases(t *testing.T) {
	interval := time.Minute
	// backoff.Jitter with coefficient c returns values in [(1-c)*I, (1+c)*I).
	minDelay := interval / 2
	maxDelay := interval + interval/2

	const partitions = 200
	delays := make([]time.Duration, partitions)
	for i := range delays {
		delays[i] = jitteredSyncInterval(interval)
		require.GreaterOrEqual(t, delays[i], minDelay)
		require.LessOrEqual(t, delays[i], maxDelay)
	}

	lo, hi := delays[0], delays[0]
	distinct := make(map[time.Duration]bool, partitions)
	for _, d := range delays {
		lo = min(lo, d)
		hi = max(hi, d)
		distinct[d] = true
	}

	// The first ticks must cover a large fraction of the jitter range rather
	// than clustering at the un-jittered interval, and partitions must not
	// share a handful of identical phases.
	require.Greater(t, hi-lo, interval/2)
	require.Greater(t, len(distinct), partitions*9/10)
}
