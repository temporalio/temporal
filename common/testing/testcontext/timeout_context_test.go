package testcontext

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimeoutContextReportsCeilingAndExpiresAtActiveExpiration(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		ctx := newTimeoutContext(t.Context(), start.Add(time.Minute), start.Add(10*time.Second), nil)

		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.Equal(t, start.Add(time.Minute), deadline)

		time.Sleep(10 * time.Second) //nolint:forbidigo // advance the synctest clock to the active deadline
		<-ctx.Done()
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	})
}

func TestTimeoutContextInheritsParentDeadline(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		parent, cancel := context.WithDeadline(t.Context(), start.Add(time.Second))
		defer cancel()
		ctx := newTimeoutContext(parent, start.Add(time.Minute), start.Add(10*time.Second), nil)

		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.Equal(t, start.Add(time.Second), deadline)

		time.Sleep(time.Second) //nolint:forbidigo // advance to the parent deadline
		<-ctx.Done()
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	})
}

func TestTimeoutContextStartsCanceledWithCanceledParent(t *testing.T) {
	t.Parallel()

	parent, cancelParent := context.WithCancel(t.Context())
	cancelParent()
	start := time.Now()
	ctx := newTimeoutContext(parent, start.Add(time.Minute), start.Add(10*time.Second), nil)

	<-ctx.Done()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestTimeoutContextExtensionKeepsIdentityAlivePastOldDeadline(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		ctx := newTimeoutContext(t.Context(), start.Add(time.Minute), start.Add(10*time.Second), nil)

		ctx.extend(start.Add(20 * time.Second))
		time.Sleep(11 * time.Second) //nolint:forbidigo // advance past the original active deadline
		require.NoError(t, ctx.Err())

		time.Sleep(9 * time.Second) //nolint:forbidigo // advance to the extended active deadline
		<-ctx.Done()
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	})
}

func TestTimeoutContextExtensionsAreMonotonic(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		ctx := newTimeoutContext(t.Context(), start.Add(time.Minute), start.Add(10*time.Second), nil)

		var wg sync.WaitGroup
		for _, extension := range []time.Duration{15 * time.Second, 30 * time.Second, 20 * time.Second, 25 * time.Second} {
			wg.Go(func() {
				ctx.extend(start.Add(extension))
			})
		}
		wg.Wait()

		require.Equal(t, start.Add(30*time.Second), ctx.effectiveExpiration())
		time.Sleep(29 * time.Second) //nolint:forbidigo // advance close to the furthest extension
		require.NoError(t, ctx.Err())
		time.Sleep(time.Second) //nolint:forbidigo // advance to the furthest extension
		<-ctx.Done()
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	})
}

func TestTimeoutContextCancellationIsTerminal(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		cancel func(context.CancelFunc, *timeoutContext)
	}{
		{
			name: "parent",
			cancel: func(cancelParent context.CancelFunc, _ *timeoutContext) {
				cancelParent()
			},
		},
		{
			name: "cleanup",
			cancel: func(_ context.CancelFunc, ctx *timeoutContext) {
				ctx.cancel()
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				parent, cancelParent := context.WithCancel(t.Context())
				ctx := newTimeoutContext(parent, start.Add(time.Minute), start.Add(10*time.Second), nil)

				tc.cancel(cancelParent, ctx)
				<-ctx.Done()
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				ctx.extend(start.Add(20 * time.Second))
				require.ErrorIs(t, ctx.Err(), context.Canceled)
			})
		})
	}
}

func TestTimeoutContextDerivedDeadlineRemainsTighter(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		ctx := newTimeoutContext(t.Context(), start.Add(time.Minute), start.Add(10*time.Second), nil)
		derived, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()

		ctx.extend(start.Add(30 * time.Second))
		deadline, ok := derived.Deadline()
		require.True(t, ok)
		require.Equal(t, start.Add(20*time.Second), deadline)

		time.Sleep(20 * time.Second) //nolint:forbidigo // advance past the original active expiration to the derived deadline
		<-derived.Done()
		require.ErrorIs(t, derived.Err(), context.DeadlineExceeded)
		require.NoError(t, ctx.Err())
	})
}

func TestTimeoutContextCauseRemainsDeadlineExceededAfterParentCancellation(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		parent, cancelParent := context.WithCancel(t.Context())
		ctx := newTimeoutContext(parent, start.Add(time.Minute), start.Add(time.Second), nil)

		time.Sleep(time.Second) //nolint:forbidigo // advance to the active expiration
		<-ctx.Done()
		require.Equal(t, context.DeadlineExceeded, ctx.Err())
		require.ErrorIs(t, context.Cause(ctx), DeadlineExceeded)
		require.ErrorIs(t, context.Cause(ctx), context.DeadlineExceeded)

		cancelParent()
		require.ErrorIs(t, context.Cause(ctx), context.DeadlineExceeded)
	})
}

func TestTimeoutContextPreservesParentCause(t *testing.T) {
	t.Parallel()

	parent, cancelParent := context.WithCancelCause(t.Context())
	start := time.Now()
	ctx := newTimeoutContext(parent, start.Add(time.Minute), start.Add(10*time.Second), nil)
	parentCause := errors.New("parent stopped")
	cancelParent(parentCause)

	<-ctx.Done()
	require.Equal(t, context.Canceled, ctx.Err())
	require.ErrorIs(t, context.Cause(ctx), parentCause)
}

func TestTimeoutContextDerivedContextPreservesCause(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		ctx := newTimeoutContext(t.Context(), start.Add(time.Minute), start.Add(time.Second), nil)
		derived, cancel := context.WithCancel(ctx)
		defer cancel()

		time.Sleep(time.Second) //nolint:forbidigo // advance to the active expiration
		<-derived.Done()

		require.Equal(t, context.DeadlineExceeded, derived.Err())
		require.ErrorIs(t, context.Cause(derived), DeadlineExceeded)
	})
}

func TestTimeoutContextExpirationIsTerminal(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		ctx := newTimeoutContext(t.Context(), start.Add(time.Minute), start.Add(time.Second), nil)

		time.Sleep(time.Second) //nolint:forbidigo // advance to the active expiration
		<-ctx.Done()
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)

		ctx.extend(start.Add(2 * time.Second))
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	})
}

func TestTimeoutContextIgnoresStaleExpirationAfterExtension(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		ctx := newTimeoutContext(t.Context(), start.Add(time.Minute), start.Add(time.Second), nil)

		ctx.extend(start.Add(2 * time.Second))
		ctx.expire() // simulate the original timer callback racing after extension

		time.Sleep(time.Second) //nolint:forbidigo // advance past the original active expiration
		require.NoError(t, ctx.Err())
		time.Sleep(time.Second) //nolint:forbidigo // advance to the extended active expiration
		<-ctx.Done()
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	})
}
