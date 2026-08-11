package namespacereplication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPeerRetryBackoff pins the capped-exponential backoff schedule for peer
// retries: base doubling per attempt, clamped at peerRetryMaxInterval, and
// overflow-safe for large attempt counts.
func TestPeerRetryBackoff(t *testing.T) {
	testCases := []struct {
		name    string
		attempt int32
		want    time.Duration
	}{
		{name: "attempt below 1 uses base", attempt: 0, want: peerRetryBaseInterval},
		{name: "negative attempt uses base", attempt: -3, want: peerRetryBaseInterval},
		{name: "attempt 1 = base", attempt: 1, want: peerRetryBaseInterval},
		{name: "attempt 2 = 2x base", attempt: 2, want: 2 * peerRetryBaseInterval},
		{name: "attempt 3 = 4x base", attempt: 3, want: 4 * peerRetryBaseInterval},
		{name: "attempt 9 still under cap", attempt: 9, want: 256 * peerRetryBaseInterval},
		{name: "attempt 10 exceeds cap -> clamped", attempt: 10, want: peerRetryMaxInterval},
		{name: "attempt 20 clamped", attempt: 20, want: peerRetryMaxInterval},
		{name: "very large attempt clamped (overflow-safe)", attempt: 1000, want: peerRetryMaxInterval},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := peerRetryBackoff(tc.attempt)
			require.Equal(t, tc.want, got)
			require.LessOrEqual(t, got, peerRetryMaxInterval)
			require.GreaterOrEqual(t, got, peerRetryBaseInterval)
		})
	}
}
