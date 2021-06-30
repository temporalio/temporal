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
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
)

type (
	liveness struct {
		status     int32
		timeSource clock.TimeSource
		ttl        time.Duration
		// internal shutdown channel
		shutdownChan chan struct{}

		// broadcast shutdown functions
		broadcastShutdownFn func()

		sync.Mutex
		lastEventTime time.Time
	}
)

var _ common.Daemon = (*liveness)(nil)

func newLiveness(
	timeSource clock.TimeSource,
	ttl time.Duration,
	broadcastShutdownFn func(),
) *liveness {
	return &liveness{
		status:       common.DaemonStatusInitialized,
		timeSource:   timeSource,
		ttl:          ttl,
		shutdownChan: make(chan struct{}),

		broadcastShutdownFn: broadcastShutdownFn,

		lastEventTime: timeSource.Now(),
	}
}

func (l *liveness) Start() {
	if !atomic.CompareAndSwapInt32(
		&l.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	go l.eventLoop()
}

func (l *liveness) Stop() {
	if !atomic.CompareAndSwapInt32(
		&l.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	close(l.shutdownChan)
	l.broadcastShutdownFn()
}

func (l *liveness) eventLoop() {
	ttlTimer := time.NewTicker(l.ttl)
	defer ttlTimer.Stop()

	for {
		select {
		case <-ttlTimer.C:
			if !l.isAlive() {
				l.Stop()
			}

		case <-l.shutdownChan:
			return
		}
	}
}

func (l *liveness) isAlive() bool {
	l.Lock()
	defer l.Unlock()
	return l.lastEventTime.Add(l.ttl).After(l.timeSource.Now())
}

func (l *liveness) markAlive(
	now time.Time,
) {
	l.Lock()
	defer l.Unlock()
	if l.lastEventTime.Before(now) {
		l.lastEventTime = now.UTC()
	}
}
