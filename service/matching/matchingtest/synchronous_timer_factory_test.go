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

package matchingtest_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/service/matching/matchingtest"
)

func TestSynchronousClock_Advance(t *testing.T) {
	t.Parallel()

	clock := matchingtest.NewSynchronousTimerFactory()

	var fired atomic.Bool

	clock.AfterFunc(1*time.Second, func() { fired.Store(true) })
	clock.Advance(999 * time.Millisecond)
	assert.False(t, fired.Load(), "1s timer should not fire after 999ms")
	clock.Advance(1 * time.Millisecond)
	assert.True(t, fired.Load(), "1s timer should fire after 1000ms")
}

func TestSynchronousTimer_Reset(t *testing.T) {
	t.Parallel()

	clock := matchingtest.NewSynchronousTimerFactory()

	var fired atomic.Bool

	timer := clock.AfterFunc(1*time.Second, func() { fired.Store(true) })
	clock.Advance(999 * time.Millisecond)
	assert.False(t, fired.Load(), "1s timer should not fire after 999ms")
	timer.Reset(1 * time.Second)
	clock.Advance(999 * time.Millisecond)
	assert.False(t, fired.Load(), "timer reset to 1s should not fire after 999ms")
	clock.Advance(1 * time.Millisecond)
	assert.True(t, fired.Load(), "timer reset to 1s should fire after 1000ms")
}

func TestSynchronousTimer_Stop(t *testing.T) {
	t.Parallel()

	clock := matchingtest.NewSynchronousTimerFactory()

	var fired atomic.Bool

	timer := clock.AfterFunc(1*time.Second, func() { fired.Store(true) })
	clock.Advance(999 * time.Millisecond)
	assert.False(t, fired.Load(), "1s timer should not fire after 999ms")
	timer.Stop()
	clock.Advance(1 * time.Second)
	assert.False(t, fired.Load(), "stopped timer should not fire even after its duration")
	timer.Stop() // stop again to make sure it's idempotent
}
