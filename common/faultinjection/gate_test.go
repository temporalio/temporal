package faultinjection

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

	g := NewGate()

	done := make(chan error, 1)
	go func() { done <- g.Wait(context.Background()) }()

	require.NoError(t, g.WaitForArrived(context.Background(), 1))
	require.Equal(t, 1, g.Arrived())

	require.Never(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 50*time.Millisecond, time.Millisecond)

	g.Release()
	require.NoError(t, await.Rcv(t, done))
}

func TestGate_ReleaseWithError(t *testing.T) {
	t.Parallel()

	g := NewGate()
	errX := errors.New("x")

	done := make(chan error, 1)
	go func() { done <- g.Wait(context.Background()) }()

	require.NoError(t, g.WaitForArrived(context.Background(), 1))
	g.ReleaseWithError(errX)
	require.ErrorIs(t, await.Rcv(t, done), errX)
	require.NoError(t, g.Wait(context.Background()))
	require.Equal(t, 1, g.Arrived())
}

func TestGate_OneShot(t *testing.T) {
	t.Parallel()

	g := NewGate()
	g.Release()

	// A call arriving after Release should not block.
	done := make(chan error, 1)
	go func() { done <- g.Wait(context.Background()) }()

	require.NoError(t, await.Rcv(t, done))
	require.Zero(t, g.Arrived())
}

func TestGate_ConcurrentArrivals(t *testing.T) {
	t.Parallel()

	g := NewGate()

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			_ = g.Wait(context.Background())
		}()
	}

	require.NoError(t, g.WaitForArrived(context.Background(), n))
	require.Equal(t, n, g.Arrived())

	g.Release()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	await.Rcv(t, done)
}

func TestGate_WaitForArrived_TimesOut(t *testing.T) {
	t.Parallel()

	g := NewGate()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, g.WaitForArrived(ctx, 1), context.DeadlineExceeded)
}

func TestGate_WaitForArrived_UnblocksOnRelease(t *testing.T) {
	t.Parallel()

	g := NewGate()
	done := make(chan error, 1)
	go func() {
		done <- g.WaitForArrived(context.Background(), 5) // no one will ever arrive
	}()

	g.Release()
	require.ErrorIs(t, await.Rcv(t, done), ErrGateReleased)
}

func TestGate_DoubleReleaseIsSafe(t *testing.T) {
	t.Parallel()

	g := NewGate()
	g.Release()
	g.ReleaseWithError(errors.New("should be ignored"))

	require.NoError(t, g.Wait(context.Background()), "the first Release should win")
}

func TestGate_WaitStopsWhenContextIsCanceled(t *testing.T) {
	t.Parallel()

	g := NewGate()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, g.Wait(ctx), context.Canceled)
	require.Equal(t, 1, g.Arrived())
}
