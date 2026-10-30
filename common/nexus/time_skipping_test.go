package nexus

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetryBackoffTimeSkippingTargets(t *testing.T) {
	now := time.Date(2027, time.January, 1, 12, 0, 0, 0, time.UTC)

	t.Run("returns retry and operation deadlines", func(t *testing.T) {
		targets, ok := RetryBackoffTimeSkippingTargets(
			now,
			now.Add(-time.Minute),
			now.Add(time.Minute),
			3*time.Minute,
			2*time.Minute,
		)
		require.True(t, ok)
		require.Equal(t, []time.Time{
			now.Add(time.Minute),
			now.Add(2 * time.Minute),
			now.Add(time.Minute),
		}, targets)
	})

	t.Run("due operation deadline blocks skipping", func(t *testing.T) {
		targets, ok := RetryBackoffTimeSkippingTargets(
			now,
			now.Add(-2*time.Minute),
			now.Add(time.Minute),
			2*time.Minute,
			0,
		)
		require.False(t, ok)
		require.Nil(t, targets)
	})
}
