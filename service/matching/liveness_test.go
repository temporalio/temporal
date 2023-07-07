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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/service/matching/matchingtest"
)

func TestLiveness_Start(t *testing.T) {
	t.Parallel()

	var idleCalled atomic.Bool

	ttl := func() time.Duration { return 2999 * time.Millisecond }
	clock := matchingtest.NewSynchronousTimerFactory()
	l := newLiveness(timerFactory[*matchingtest.SynchronousTimer](clock), ttl, func() { idleCalled.Store(true) })
	l.Start()
	clock.Advance(1 * time.Second)
	assert.False(t, idleCalled.Load(), "2999ms ttl should not be considered idle after 1s")
	l.markAlive()
	clock.Advance(2 * time.Second)
	assert.False(t, idleCalled.Load(), "markAlive should reset the ttl")
	clock.Advance(1 * time.Second)
	assert.True(t, idleCalled.Load(), "2999ms ttl should be considered idle after 3s")
}

func TestLiveness_Stop(t *testing.T) {
	t.Parallel()

	var idleCalled atomic.Bool

	ttl := func() time.Duration { return 1000 * time.Millisecond }
	clock := matchingtest.NewSynchronousTimerFactory()
	l := newLiveness(timerFactory[*matchingtest.SynchronousTimer](clock), ttl, func() { idleCalled.Store(true) })
	l.Start()
	clock.Advance(999 * time.Millisecond)
	assert.False(t, idleCalled.Load(), "1000ms ttl should not be considered idle after 999ms")
	l.Stop()
	clock.Advance(1 * time.Second)
	assert.False(t, idleCalled.Load(), "should not be marked idle after stop even if ttl has passed")
	l.markAlive() // should not panic
}
