package matching

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/testing/await"
)

type retainingTimeSource struct {
	clock.TimeSource
	callback func()
}

func (s *retainingTimeSource) AfterFunc(_ time.Duration, callback func()) clock.Timer {
	s.callback = callback
	return retainingTimer{}
}

type retainingTimer struct{}

func (retainingTimer) Reset(time.Duration) bool { return true }
func (retainingTimer) Stop() bool               { return true }

type livenessCallbackOwner struct {
	_ [64]byte
}

func TestLiveness(t *testing.T) {
	t.Parallel()
	var idleCalled atomic.Int32
	ttl := func() time.Duration { return 2500 * time.Millisecond }
	timeSource := clock.NewEventTimeSource()
	liveness := newLiveness(timeSource, ttl, func() { idleCalled.Store(1) })
	liveness.Start()
	timeSource.Advance(1 * time.Second)
	require.Equal(t, int32(0), idleCalled.Load())
	liveness.markAlive()
	timeSource.Advance(1 * time.Second)
	require.Equal(t, int32(0), idleCalled.Load())
	liveness.markAlive()
	timeSource.Advance(1 * time.Second)
	require.Equal(t, int32(0), idleCalled.Load())
	timeSource.Advance(1 * time.Second)
	require.Equal(t, int32(0), idleCalled.Load())
	timeSource.Advance(1 * time.Second)
	require.Equal(t, int32(1), idleCalled.Load())
	liveness.Stop()
}

func TestLivenessStop(t *testing.T) {
	t.Parallel()
	var idleCalled atomic.Int32
	ttl := func() time.Duration { return 1000 * time.Millisecond }
	timeSource := clock.NewEventTimeSource()
	liveness := newLiveness(timeSource, ttl, func() { idleCalled.Store(1) })
	liveness.Start()
	timeSource.Advance(500 * time.Millisecond)
	liveness.Stop()
	timeSource.Advance(1 * time.Second)
	require.Equal(t, int32(0), idleCalled.Load())
	liveness.markAlive() // should not panic
}

func TestLivenessStopReleasesCallbackOwner(t *testing.T) {
	t.Parallel()
	timeSource := &retainingTimeSource{}
	collected := make(chan struct{})
	func() {
		owner := &livenessCallbackOwner{}
		_ = runtime.AddCleanup(owner, func(collected chan struct{}) {
			close(collected)
		}, collected)
		liveness := newLiveness(timeSource, func() time.Duration { return time.Hour }, func() {
			runtime.KeepAlive(owner)
		})
		liveness.Start()
		liveness.Stop()
	}()

	await.RequireTrue(t, func() bool {
		runtime.GC()
		select {
		case <-collected:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)
	runtime.KeepAlive(timeSource)
}
