package testcontext

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWithDeadline(t *testing.T) {
	t.Parallel()

	t.Run("attributes a testcontext deadline", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := WithDeadline(For(t), time.Now().Add(time.Second))
			defer cancel()

			time.Sleep(time.Second) //nolint:forbidigo // advance to the derived deadline
			<-ctx.Done()

			require.Equal(t, context.DeadlineExceeded, ctx.Err())
			require.ErrorIs(t, context.Cause(ctx), DeadlineExceeded)
			require.ErrorIs(t, context.Cause(ctx), context.DeadlineExceeded)
		})
	})

	t.Run("preserves a foreign deadline", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := WithDeadline(t.Context(), time.Now().Add(time.Second))
			defer cancel()

			time.Sleep(time.Second) //nolint:forbidigo // advance to the derived deadline
			<-ctx.Done()

			require.Equal(t, context.DeadlineExceeded, ctx.Err())
			require.NotErrorIs(t, context.Cause(ctx), DeadlineExceeded)
			require.ErrorIs(t, context.Cause(ctx), context.DeadlineExceeded)
		})
	})

	t.Run("preserves a tighter caller deadline", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			parentDeadline := time.Now().Add(time.Second)
			parent := reportedDeadlineContext{
				Context:  For(t),
				deadline: parentDeadline,
			}
			ctx, cancel := WithDeadline(parent, parentDeadline)
			defer cancel()

			time.Sleep(time.Second) //nolint:forbidigo // advance to the caller-owned deadline
			<-ctx.Done()

			require.Equal(t, context.DeadlineExceeded, ctx.Err())
			require.NotErrorIs(t, context.Cause(ctx), DeadlineExceeded)
			require.ErrorIs(t, context.Cause(ctx), context.DeadlineExceeded)
		})
	})

	t.Run("preserves a parent-capped testcontext deadline", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ownerTB := newRecordingTB()
			deadline := time.Now().Add(time.Second)
			ownerTB.ctx = reportedDeadlineContext{
				Context:  ownerTB.ctx,
				deadline: deadline,
			}
			parent := For(ownerTB, WithTimeout(10*time.Second))
			owner := parent.Value(ownerKey{}).(*contextState)
			owner.timeoutContext.timer.Stop() // simulate the testcontext timer lagging at the shared deadline

			ctx, cancel := WithDeadline(parent, deadline)
			defer cancel()
			time.Sleep(time.Second) //nolint:forbidigo // advance to the parent-owned deadline
			<-ctx.Done()

			require.Equal(t, context.DeadlineExceeded, ctx.Err())
			require.NotErrorIs(t, context.Cause(ctx), DeadlineExceeded)
			require.ErrorIs(t, context.Cause(ctx), context.DeadlineExceeded)
			ownerTB.runCleanups()
		})
	})
}

type reportedDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c reportedDeadlineContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

func TestFailIfDeadlineExceeded(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		tb := newRecordingTB()
		tb.run(func() {
			ctx, cancel := WithDeadline(For(tb, WithTimeout(10*time.Second)), time.Now().Add(time.Second))
			defer cancel()

			time.Sleep(time.Second) //nolint:forbidigo // advance to the owned deadline
			<-ctx.Done()
			FailIfDeadlineExceeded(ctx, tb, "operation = Require\nattempts = 3")
		})

		require.Equal(t, "testcontext deadline exceeded after 1s\ndetails:\n  operation = Require\n  attempts = 3", tb.fatal())
		require.Empty(t, tb.error())
	})
}

func TestFailIfDeadlineExceededIgnoresForeignDeadline(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		time.Sleep(time.Second) //nolint:forbidigo // advance to the foreign deadline
		<-ctx.Done()

		tb := &returningReportingTB{}
		require.False(t, FailIfDeadlineExceeded(ctx, tb, "ignored"))
		require.Zero(t, tb.fatals.Load())
		require.Zero(t, tb.failNows.Load())
	})
}

func TestFailIfDeadlineExceededReportsOnce(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ownerTB := newRecordingTB()
		ctx := For(ownerTB, WithTimeout(time.Second))
		time.Sleep(time.Second) //nolint:forbidigo // advance to the owned deadline
		<-ctx.Done()

		tb := &returningReportingTB{}
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				require.True(t, FailIfDeadlineExceeded(ctx, tb, "attempts = 3"))
			})
		}
		wg.Wait()
		ownerTB.runCleanups()

		require.Equal(t, int32(1), tb.fatals.Load())
		require.Equal(t, int32(7), tb.failNows.Load())
		require.Empty(t, ownerTB.error())
	})
}

func TestFailIfDeadlineExceededSharesClaimForSameDeadline(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ownerTB := newRecordingTB()
		parent := For(ownerTB, WithTimeout(10*time.Second))
		deadline := time.Now().Add(time.Second)
		first, cancelFirst := WithDeadline(parent, deadline)
		defer cancelFirst()
		second, cancelSecond := WithDeadline(parent, deadline)
		defer cancelSecond()

		time.Sleep(time.Second) //nolint:forbidigo // advance to the shared deadline
		<-first.Done()
		<-second.Done()

		tb := &returningReportingTB{}
		require.True(t, FailIfDeadlineExceeded(first, tb, "first"))
		require.True(t, FailIfDeadlineExceeded(second, tb, "second"))
		ownerTB.runCleanups()

		require.Equal(t, int32(1), tb.fatals.Load())
		require.Equal(t, int32(1), tb.failNows.Load())
		require.Empty(t, ownerTB.error())
	})
}

type returningReportingTB struct {
	testing.TB
	fatals   atomic.Int32
	failNows atomic.Int32

	mu      sync.Mutex
	message string
}

func (r *returningReportingTB) Helper() {}

func (r *returningReportingTB) Fatalf(format string, args ...any) {
	r.fatals.Add(1)
	r.mu.Lock()
	r.message = fmt.Sprintf(format, args...)
	r.mu.Unlock()
}

func (r *returningReportingTB) FailNow() {
	r.failNows.Add(1)
}
