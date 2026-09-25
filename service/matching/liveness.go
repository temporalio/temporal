package matching

import (
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/clock"
)

type (
	liveness struct {
		timeSource  clock.TimeSource
		ttl         func() time.Duration
		onIdleToken *atomic.Pointer[func()]
		timer       atomic.Value
	}

	timerWrapper struct {
		clock.Timer
	}
)

func newLiveness(
	timeSource clock.TimeSource,
	ttl func() time.Duration,
	onIdle func(),
) *liveness {
	onIdleToken := &atomic.Pointer[func()]{}
	onIdleToken.Store(&onIdle)
	return &liveness{
		timeSource:  timeSource,
		ttl:         ttl,
		onIdleToken: onIdleToken,
	}
}

func (l *liveness) Start() {
	// Capture only the token because a stopped runtime timer may retain its callback.
	onIdleToken := l.onIdleToken
	l.timer.Store(timerWrapper{l.timeSource.AfterFunc(l.ttl(), func() {
		if onIdle := onIdleToken.Load(); onIdle != nil {
			(*onIdle)()
		}
	})})
}

func (l *liveness) Stop() {
	l.onIdleToken.Store(nil)
	if t, ok := l.timer.Swap(timerWrapper{}).(timerWrapper); ok && t.Timer != nil {
		t.Stop()
	}
}

func (l *liveness) markAlive() {
	if t, ok := l.timer.Load().(timerWrapper); ok && t.Timer != nil {
		t.Reset(l.ttl())
	}
}
