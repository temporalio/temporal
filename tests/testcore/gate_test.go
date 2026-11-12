package testcore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
)

func TestGate_BlocksUntilReleased(t *testing.T) {
	t.Parallel()

	g := newGate()

	done := make(chan error, 1)
	go func() { done <- g.arrive(context.Background()) }()

	require.NoError(t, g.waitForArrived(context.Background(), 1))
	require.Equal(t, 1, g.numArrived())

	require.Never(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 50*time.Millisecond, time.Millisecond)

	g.release()
	require.NoError(t, await.Rcv(t, done))
}

func TestGate_ReleaseWithError(t *testing.T) {
	t.Parallel()

	g := newGate()
	errX := errors.New("x")

	done := make(chan error, 1)
	go func() { done <- g.arrive(context.Background()) }()

	require.NoError(t, g.waitForArrived(context.Background(), 1))
	g.releaseWithError(errX)
	require.ErrorIs(t, await.Rcv(t, done), errX)
	require.NoError(t, g.arrive(context.Background()))
	require.Equal(t, 1, g.numArrived())
}

func TestGate_OneShot(t *testing.T) {
	t.Parallel()

	g := newGate()
	g.release()

	// A call arriving after release should not block.
	done := make(chan error, 1)
	go func() { done <- g.arrive(context.Background()) }()

	require.NoError(t, await.Rcv(t, done))
	require.Zero(t, g.numArrived())
}

func TestGate_ConcurrentArrivals(t *testing.T) {
	t.Parallel()

	g := newGate()

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			_ = g.arrive(context.Background())
		}()
	}

	require.NoError(t, g.waitForArrived(context.Background(), n))
	require.Equal(t, n, g.numArrived())

	g.release()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	await.Rcv(t, done)
}

func TestGate_WaitForArrived_TimesOut(t *testing.T) {
	t.Parallel()

	g := newGate()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, g.waitForArrived(ctx, 1), context.DeadlineExceeded)
}

func TestGate_WaitForArrived_UnblocksOnRelease(t *testing.T) {
	t.Parallel()

	g := newGate()
	done := make(chan error, 1)
	go func() {
		done <- g.waitForArrived(context.Background(), 5) // no one will ever arrive
	}()

	g.release()
	require.ErrorIs(t, await.Rcv(t, done), errGateReleased)
}

func TestGate_DoubleReleaseIsSafe(t *testing.T) {
	t.Parallel()

	g := newGate()
	g.release()
	g.releaseWithError(errors.New("should be ignored"))

	require.NoError(t, g.arrive(context.Background()), "the first release should win")
}

func TestGate_ArriveStopsWhenContextIsCanceled(t *testing.T) {
	t.Parallel()

	g := newGate()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, g.arrive(ctx), context.Canceled)
	require.Equal(t, 1, g.numArrived())
}
