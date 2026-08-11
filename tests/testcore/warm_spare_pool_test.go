package testcore

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
)

func TestWarmSparePoolFillsAndReplenishes(t *testing.T) {
	var created atomic.Int64
	pool := newWarmSparePool(2, func() (int64, error) {
		return created.Add(1), nil
	}, func(int64) error { return nil })
	t.Cleanup(pool.close)

	pool.start()
	await.RequireTrue(t, func() bool { return pool.ready() == 2 }, time.Second, time.Millisecond)

	first, ok, err := pool.take()
	require.NoError(t, err)
	require.True(t, ok)
	require.NotZero(t, first)
	await.RequireTrue(t, func() bool { return created.Load() == 3 && pool.ready() == 2 }, time.Second, time.Millisecond)
}

func TestWarmSparePoolPropagatesBootFailure(t *testing.T) {
	wantErr := errors.New("boot failed")
	pool := newWarmSparePool(1, func() (int, error) {
		return 0, wantErr
	}, func(int) error { return nil })
	t.Cleanup(pool.close)

	require.ErrorIs(t, pool.startAndWait(), wantErr)
}

func TestWarmSparePoolCloseDestroysReadySpares(t *testing.T) {
	var destroyed atomic.Int64
	pool := newWarmSparePool(3, func() (int, error) {
		return 1, nil
	}, func(int) error {
		destroyed.Add(1)
		return nil
	})

	pool.start()
	await.RequireTrue(t, func() bool { return pool.ready() == 3 }, time.Second, time.Millisecond)
	pool.close()
	require.Equal(t, int64(3), destroyed.Load())
}

func TestWarmSparePoolCloseBeforeStartDoesNotCreate(t *testing.T) {
	var created atomic.Int64
	pool := newWarmSparePool(1, func() (int, error) {
		created.Add(1)
		return 1, nil
	}, func(int) error { return nil })

	pool.close()
	require.Zero(t, created.Load())
}
