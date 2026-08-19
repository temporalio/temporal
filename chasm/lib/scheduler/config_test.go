package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServiceCallContext(t *testing.T) {
	t.Run("uses configured deadline", func(t *testing.T) {
		const timeout = time.Second
		config := &Config{ServiceCallTimeout: func() time.Duration { return timeout }}

		ctx, cancel := config.serviceCallContext(context.Background())
		defer cancel()

		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.Greater(t, time.Until(deadline), time.Duration(0))
		require.LessOrEqual(t, time.Until(deadline), timeout)
	})

	t.Run("preserves tighter parent deadline", func(t *testing.T) {
		const parentTimeout = time.Second
		config := &Config{ServiceCallTimeout: func() time.Duration { return time.Hour }}
		parentCtx, parentCancel := context.WithTimeout(context.Background(), parentTimeout)
		defer parentCancel()

		ctx, cancel := config.serviceCallContext(parentCtx)
		defer cancel()

		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.Greater(t, time.Until(deadline), time.Duration(0))
		require.LessOrEqual(t, time.Until(deadline), parentTimeout)
	})

	t.Run("propagates parent cancellation", func(t *testing.T) {
		config := &Config{ServiceCallTimeout: func() time.Duration { return time.Hour }}
		parentCtx, parentCancel := context.WithCancel(context.Background())
		defer parentCancel()
		ctx, cancel := config.serviceCallContext(parentCtx)
		defer cancel()

		parentCancel()
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})
}
