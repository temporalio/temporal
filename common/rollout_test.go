package common

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRolloutAccepts_Boundaries(t *testing.T) {
	t.Parallel()

	require.True(t, RolloutAccepts("ns", "id", 100))
	require.True(t, RolloutAccepts("ns", "id", 101)) // clamped
	require.False(t, RolloutAccepts("ns", "id", 0))
	require.False(t, RolloutAccepts("ns", "id", -5)) // clamped
}

func TestRolloutAccepts_Monotonic(t *testing.T) {
	t.Parallel()

	// An ID accepted at percent P must also be accepted at every percent >= P.
	for i := range 500 {
		id := fmt.Sprintf("id-%d", i)
		firstAcceptedAt := -1
		for pct := 0; pct <= 100; pct++ {
			if RolloutAccepts("ns", id, pct) {
				if firstAcceptedAt < 0 {
					firstAcceptedAt = pct
				}
			} else if firstAcceptedAt >= 0 {
				t.Fatalf("id %s accepted at pct=%d but rejected at pct=%d", id, firstAcceptedAt, pct)
			}
		}
	}
}
