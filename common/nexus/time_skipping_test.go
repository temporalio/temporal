package nexus

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetryBackoffTimeSkippingTargets(t *testing.T) {
	t.Parallel()
	now := time.Date(2027, time.January, 1, 12, 0, 0, 0, time.UTC)

	t.Run("RetryAndTimeoutDeadlines", func(t *testing.T) {
		t.Parallel()
		targets, skippable := RetryBackoffTimeSkippingTargets(
			now,
			now.Add(-time.Minute),
			now.Add(time.Minute),
			3*time.Minute,
			2*time.Minute,
		)
		require.True(t, skippable)
		require.Equal(t, []time.Time{
			now.Add(time.Minute),
			now.Add(2 * time.Minute),
			now.Add(time.Minute),
		}, targets)
	})

	t.Run("DueDeadlineBlocksSkipping", func(t *testing.T) {
		t.Parallel()
		targets, skippable := RetryBackoffTimeSkippingTargets(
			now,
			now.Add(-2*time.Minute),
			now.Add(time.Minute),
			2*time.Minute,
			0,
		)
		require.False(t, skippable)
		require.Nil(t, targets)
	})
}
