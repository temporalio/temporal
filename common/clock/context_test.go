package clock_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/clock"
)

func TestContextWithTimeout_Canceled(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Unix(0, 0))
	ctx := context.Background()
	ctx, cancel := clock.ContextWithTimeout(ctx, time.Second, timeSource)
	deadline, ok := ctx.Deadline()
	assert.True(t, ok)
	assert.Equal(t, time.Unix(1, 0), deadline)
	cancel()
	select {
	case <-ctx.Done():
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
	default:
		t.Fatal("expected context to be canceled")
	}
}

func TestContextWithTimeout_Fire(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Unix(0, 0))
	ctx := context.Background()
	ctx, cancel := clock.ContextWithTimeout(ctx, time.Second, timeSource)
	deadline, ok := ctx.Deadline()
	assert.True(t, ok)
	assert.Equal(t, time.Unix(1, 0), deadline)
	timeSource.Advance(time.Second - time.Millisecond)
	select {
	case <-ctx.Done():
		t.Fatal("expected context to not be canceled")
	default:
		assert.NoError(t, ctx.Err())
	}
	timeSource.Advance(time.Millisecond)
	select {
	case <-ctx.Done():
		assert.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	default:
		t.Fatal("expected context to be canceled")
	}
	cancel() // should be a no-op
}
