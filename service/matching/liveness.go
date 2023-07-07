// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package matching

import (
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"
)

type (
	// liveness is a helper for tracking whether a component is alive or not. It is alive if it is marked alive within
	// a certain time period, and dead otherwise. It is useful for tracking whether a poller is alive or not.
	liveness[T simpleTimer] struct {
		clock  timerFactory[T]
		ttl    func() time.Duration
		onIdle func()
		timer  atomic.Value
	}
	// timerFactory is a subset of clockwork.Clock that only has the AfterFunc method, which returns a simpleTimer.
	timerFactory[T simpleTimer] interface {
		AfterFunc(d time.Duration, f func()) T
	}
	// simpleTimer is similar to clockwork.Timer, but the Reset and Stop methods return nothing because we don't care about
	// those results.
	simpleTimer interface {
		Reset(d time.Duration)
		Stop()
	}
	// clockworkClock adapts a clockwork.Clock to the timerFactory interface.
	clockworkClock struct {
		clockwork.Clock
	}
	// clockworkTimer adapts a clockwork.Timer to the simpleTimer interface.
	clockworkTimer struct {
		clockwork.Timer
	}
	// monomorphicTimer is a Timer that always has the same concrete type, so that it can be stored in an atomic.Value.
	monomorphicTimer struct {
		simpleTimer
	}
)

func (c clockworkClock) AfterFunc(d time.Duration, f func()) clockworkTimer {
	return clockworkTimer{c.Clock.AfterFunc(d, f)}
}

func (t clockworkTimer) Reset(d time.Duration) {
	t.Timer.Reset(d)
}

func (t clockworkTimer) Stop() {
	t.Timer.Stop()
}

func newLiveness[T simpleTimer](
	clock timerFactory[T],
	ttl func() time.Duration,
	onIdle func(),
) *liveness[T] {
	return &liveness[T]{
		clock:  clock,
		ttl:    ttl,
		onIdle: onIdle,
	}
}

func (l *liveness[T]) Start() {
	l.timer.Store(monomorphicTimer{l.clock.AfterFunc(l.ttl(), l.onIdle)})
}

func (l *liveness[T]) Stop() {
	if t, ok := l.timer.Swap(monomorphicTimer{}).(monomorphicTimer); ok && t.simpleTimer != nil {
		t.Stop()
	}
}

func (l *liveness[T]) markAlive() {
	if t, ok := l.timer.Load().(monomorphicTimer); ok && t.simpleTimer != nil {
		t.Reset(l.ttl())
	}
}
