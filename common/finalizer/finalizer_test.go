package finalizer_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	. "go.temporal.io/server/common/finalizer"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
)

func TestFinalizer(t *testing.T) {

	t.Run("register", func(t *testing.T) {
		t.Run("succeeds", func(t *testing.T) {
			f := newFinalizer(metrics.NoopMetricsHandler)
			require.NoError(t, f.Register("1", nil))
			require.NoError(t, f.Deregister("1"))
		})

		t.Run("fails when ID already registered", func(t *testing.T) {
			f := newFinalizer(metrics.NoopMetricsHandler)
			require.NoError(t, f.Register("1", nil))
			require.ErrorIs(t, f.Register("1", nil), FinalizerDuplicateIdErr)
		})

		t.Run("fails after already run before", func(t *testing.T) {
			f := newFinalizer(metrics.NoopMetricsHandler)
			f.Run(1 * time.Second)
			require.ErrorIs(t, f.Register("1", nil), FinalizerAlreadyDoneErr)
		})
	})

	t.Run("deregister", func(t *testing.T) {
		t.Run("succeeds", func(t *testing.T) {
			f := newFinalizer(metrics.NoopMetricsHandler)
			require.NoError(t, f.Register("1", nil))
			require.NoError(t, f.Deregister("1"))
			require.Zero(t, f.Run(1*time.Second))
		})

		t.Run("fails if callback does not exist", func(t *testing.T) {
			f := newFinalizer(metrics.NoopMetricsHandler)
			require.ErrorIs(t, f.Deregister("does-not-exist"), FinalizerUnknownIdErr)
		})

		t.Run("fails after already run before", func(t *testing.T) {
			f := newFinalizer(metrics.NoopMetricsHandler)
			f.Run(1 * time.Second)
			require.ErrorIs(t, f.Deregister("1"), FinalizerAlreadyDoneErr)
		})
	})

	t.Run("run", func(t *testing.T) {
		t.Run("invokes all callbacks", func(t *testing.T) {
			mh := metricstest.NewCaptureHandler()
			f := newFinalizer(mh)
			capture := mh.StartCapture()

			var completed atomic.Int32
			for i := 0; i < 5; i += 1 {
				require.NoError(t, f.Register(
					fmt.Sprintf("%v", i),
					func(ctx context.Context) error {
						completed.Add(1)
						//nolint:forbidigo
						time.Sleep(50 * time.Millisecond)
						return nil
					}))
			}

			require.EqualValues(t, 5, f.Run(1*time.Second))
			require.EqualValues(t, 5, completed.Load())

			snap := capture.Snapshot()
			require.Equal(t, int64(5), snap[metrics.FinalizerItemsCompleted.Name()][0].Value)
			require.Equal(t, int64(0), snap[metrics.FinalizerItemsUnfinished.Name()][0].Value)
			require.NotZero(t, snap[metrics.FinalizerLatency.Name()][0].Value.(time.Duration).Nanoseconds())
		})

		t.Run("returns once timeout has been reached", func(t *testing.T) {
			timeout := 50 * time.Millisecond
			mh := metricstest.NewCaptureHandler()
			f := newFinalizer(mh)

			require.NoError(t, f.Register(
				"before-timeout",
				func(ctx context.Context) error {
					return nil
				}))
			require.NoError(t, f.Register(
				"after-timeout",
				func(ctx context.Context) error {
					//nolint:forbidigo
					time.Sleep(2 * timeout)
					return nil
				}))

			capture := mh.StartCapture()
			completed := f.Run(timeout)
			require.EqualValues(t, 1, completed, "expected only one callback to complete")

			snap := capture.Snapshot()
			require.Equal(t, int64(1), snap[metrics.FinalizerItemsCompleted.Name()][0].Value)
			require.Equal(t, int64(1), snap[metrics.FinalizerItemsUnfinished.Name()][0].Value)
		})

		t.Run("does not execute when no callbacks are registered", func(t *testing.T) {
			mh := metricstest.NewCaptureHandler()
			f := newFinalizer(mh)

			require.Zero(t, f.Run(0), "expected no callbacks to complete")
			require.Empty(t, mh.StartCapture().Snapshot())
		})

		t.Run("does not execute if timeout is zero", func(t *testing.T) {
			mh := metricstest.NewCaptureHandler()
			f := newFinalizer(mh)
			require.NoError(t, f.Register(
				"0",
				func(ctx context.Context) error {
					return nil
				}))
			require.Zero(t, f.Run(0), "expected no callbacks to complete")
			require.Empty(t, mh.StartCapture().Snapshot())
		})

		t.Run("does not execute more than once", func(t *testing.T) {
			mh := metricstest.NewCaptureHandler()
			f := newFinalizer(mh)

			// 1st call
			require.NoError(t, f.Register(
				"0",
				func(ctx context.Context) error {
					return nil
				}))
			require.EqualValues(t, 1, f.Run(1*time.Second))

			// 2nd call
			require.Zero(t, f.Run(1*time.Second), "expected no callbacks to complete")
			require.Empty(t, mh.StartCapture().Snapshot())
		})
	})
}

func newFinalizer(mh metrics.Handler) *Finalizer {
	return New(log.NewNoopLogger(), mh)
}

func newPool() *goro.AdaptivePool {
	return goro.NewAdaptivePool(clock.NewRealTimeSource(), 1, 2, 10*time.Millisecond, 10)
}
