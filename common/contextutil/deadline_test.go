package contextutil

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testTolerance = 5 * time.Second

func TestWithDeadlineBuffer(t *testing.T) {
	const timeout = 10 * time.Minute
	const buffer = 1 * time.Minute
	start := time.Now()

	t.Run("parent is cancelled", func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		cancel()

		child, _ := WithDeadlineBuffer(parent, timeout, buffer)
		require.Equal(t, parent, child)
	})

	t.Run("parent has no deadline", func(t *testing.T) {
		parent := context.Background()

		t.Run("timeout specified", func(t *testing.T) {
			child, _ := WithDeadlineBuffer(parent, timeout, 0)
			dl, _ := child.Deadline()
			require.WithinDuration(t, start.Add(timeout), dl, testTolerance)
		})
	})

	t.Run("parent has deadline", func(t *testing.T) {
		parent, parentCancel := context.WithTimeout(context.Background(), timeout)
		defer parentCancel()
		parentDeadline, _ := parent.Deadline()

		t.Run("enough buffer left", func(t *testing.T) {
			child, _ := WithDeadlineBuffer(parent, math.MaxInt, buffer)
			dl, _ := child.Deadline()
			require.WithinDuration(t, parentDeadline.Add(-buffer), dl, testTolerance)
		})

		t.Run("no buffer left", func(t *testing.T) {
			child, _ := WithDeadlineBuffer(parent, math.MaxInt, math.MaxInt)
			require.Equal(t, child.Err(), context.DeadlineExceeded)
		})

		t.Run("enough buffer left but less than max timeout", func(t *testing.T) {
			child, _ := WithDeadlineBuffer(parent, timeout/2, buffer)
			dl, _ := child.Deadline()
			require.WithinDuration(t, parentDeadline.Add(-timeout/2), dl, testTolerance)
		})
	})
}
