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

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
)

func TestLiveness(t *testing.T) {
	var idleCalled atomic.Int32
	ttl := func() time.Duration { return 2500 * time.Millisecond }
	clock := clockwork.NewFakeClock()
	liveness := newLiveness(clock, ttl, func() { idleCalled.Store(1) })
	liveness.Start()
	clock.Advance(1 * time.Second)
	time.Sleep(50 * time.Millisecond) // need actual time to pass since onIdle still runs async
	assert.Equal(t, int32(0), idleCalled.Load())
	liveness.markAlive()
	clock.Advance(1 * time.Second)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(0), idleCalled.Load())
	liveness.markAlive()
	clock.Advance(1 * time.Second)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(0), idleCalled.Load())
	clock.Advance(1 * time.Second)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(0), idleCalled.Load())
	clock.Advance(1 * time.Second)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(1), idleCalled.Load())
	liveness.Stop()
}

func TestLivenessStop(t *testing.T) {
	var idleCalled atomic.Int32
	ttl := func() time.Duration { return 1000 * time.Millisecond }
	clock := clockwork.NewFakeClock()
	liveness := newLiveness(clock, ttl, func() { idleCalled.Store(1) })
	liveness.Start()
	clock.Advance(500 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	liveness.Stop()
	clock.Advance(1 * time.Second)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(0), idleCalled.Load())
	liveness.markAlive() // should not panic
}
