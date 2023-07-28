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
