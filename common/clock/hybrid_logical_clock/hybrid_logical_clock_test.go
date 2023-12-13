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

package hybrid_logical_clock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/clock"
)

func Test_Next_ReturnsGreaterClock(t *testing.T) {
	t.Parallel()
	t0 := Zero(1)
	timesource := clock.NewEventTimeSource()

	// Same wallclock
	timesource.Update(time.Unix(0, 0).UTC())
	t1 := Next(t0, timesource)
	assert.Equal(t, 1, Compare(t0, t1))
	// Greater wallclock
	timesource.Update(time.Unix(0, 1).UTC())
	t2 := Next(t1, timesource)
	assert.Equal(t, 1, Compare(t1, t2))
}

func Test_Compare(t *testing.T) {
	t.Parallel()
	t0 := Clock{WallClock: 1, Version: 1, ClusterId: 1}
	t1 := Clock{WallClock: 1, Version: 1, ClusterId: 1}
	assert.Equal(t, 0, Compare(&t0, &t1))
	assert.True(t, Equal(&t0, &t1))

	t0 = Clock{WallClock: 1, Version: 1, ClusterId: 1}
	t1 = Clock{WallClock: 1, Version: 1, ClusterId: 2}
	assert.Equal(t, 1, Compare(&t0, &t1))
	// Let's get a -1 in there for sanity
	assert.Equal(t, -1, Compare(&t1, &t0))

	t0 = Clock{WallClock: 1, Version: 1, ClusterId: 1}
	t1 = Clock{WallClock: 1, Version: 2, ClusterId: 1}
	assert.Equal(t, 1, Compare(&t0, &t1))

	t0 = Clock{WallClock: 1, Version: 1, ClusterId: 1}
	t1 = Clock{WallClock: 2, Version: 1, ClusterId: 1}
	assert.Equal(t, 1, Compare(&t0, &t1))

	assert.True(t, Greater(&t1, &t0))
	assert.True(t, Less(&t0, &t1))
}

func Test_Max_ReturnsMaximum(t *testing.T) {
	t.Parallel()
	t0 := Zero(1)
	t1 := Zero(2)

	max := Max(t0, t1)
	assert.Equal(t, max, t1)
	// Just in case it doesn't work in reverse order...
	max = Max(t1, t0)
	assert.Equal(t, max, t1)
}

func Test_Min_ReturnsMinimum(t *testing.T) {
	t.Parallel()
	t0 := Zero(1)
	t1 := Zero(2)

	min := Min(t0, t1)
	assert.Equal(t, min, t0)
	// Just in case it doesn't work in reverse order...
	min = Min(t1, t0)
	assert.Equal(t, min, t0)
}

func Test_UTC_ReturnsTimeInUTC(t *testing.T) {
	t.Parallel()
	assert.Equal(t, time.Unix(0, 0).UTC(), UTC(Zero(0)))

	timesource := clock.NewEventTimeSource()
	now := time.Date(1999, 12, 31, 23, 59, 59, 999000000, time.UTC)
	timesource.Update(now)
	assert.Equal(t, now, UTC(Next(Zero(0), timesource)))
}
