package matching

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/clock"
)

func TestLiveness(t *testing.T) {
	t.Parallel()
	var idleCalled atomic.Int32
	ttl := func() time.Duration { return 2500 * time.Millisecond }
	timeSource := clock.NewEventTimeSource()
	liveness := newLiveness(timeSource, ttl, func() { idleCalled.Store(1) })
	liveness.Start()
	timeSource.Advance(1 * time.Second)
	assert.Equal(t, int32(0), idleCalled.Load())
	liveness.markAlive()
	timeSource.Advance(1 * time.Second)
	assert.Equal(t, int32(0), idleCalled.Load())
	liveness.markAlive()
	timeSource.Advance(1 * time.Second)
	assert.Equal(t, int32(0), idleCalled.Load())
	timeSource.Advance(1 * time.Second)
	assert.Equal(t, int32(0), idleCalled.Load())
	timeSource.Advance(1 * time.Second)
	assert.Equal(t, int32(1), idleCalled.Load())
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
	assert.Equal(t, int32(0), idleCalled.Load())
	liveness.markAlive() // should not panic
}
