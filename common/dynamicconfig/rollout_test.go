package dynamicconfig

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func rolloutKey(namespace, id string) []byte {
	return fmt.Appendf(nil, "%s\x00%s", namespace, id)
}

func TestRolloutAccepts_Boundaries(t *testing.T) {
	t.Parallel()

	key := rolloutKey("ns", "id")
	require.True(t, RolloutAccepts(key, 100))
	require.True(t, RolloutAccepts(key, 101)) // clamped
	require.False(t, RolloutAccepts(key, 0))
	require.False(t, RolloutAccepts(key, -5)) // clamped
}

func TestRolloutAccepts_Monotonic(t *testing.T) {
	t.Parallel()

	// An ID accepted at percent P must also be accepted at every percent >= P.
	for i := range 500 {
		key := rolloutKey("ns", fmt.Sprintf("id-%d", i))
		firstAcceptedAt := -1
		for pct := 0; pct <= 100; pct++ {
			if RolloutAccepts(key, pct) {
				if firstAcceptedAt < 0 {
					firstAcceptedAt = pct
				}
			} else if firstAcceptedAt >= 0 {
				t.Fatalf("id %d accepted at pct=%d but rejected at pct=%d", i, firstAcceptedAt, pct)
			}
		}
	}
}
