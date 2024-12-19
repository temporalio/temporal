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

package clock_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/clock"
)

func TestNewRealClock_Now(t *testing.T) {
	t.Parallel()

	source := clock.NewRealTimeSource()
	location := source.Now().Location()
	assert.Equal(t, "UTC", location.String())
}

func TestNewRealClock_Since(t *testing.T) {
	t.Parallel()

	source := clock.NewRealTimeSource()
	start := source.Now()
	assert.Eventually(
		t,
		func() bool {
			return source.Since(start) >= 5*time.Millisecond
		},
		time.Second,
		time.Millisecond,
	)
}

func TestNewRealClock_AfterFunc(t *testing.T) {
	t.Parallel()

	source := clock.NewRealTimeSource()
	ch := make(chan struct{})
	timer := source.AfterFunc(0, func() {
		close(ch)
	})

	<-ch
	assert.False(t, timer.Stop())
}

func TestNewRealClock_NewTimer(t *testing.T) {
	t.Parallel()

	source := clock.NewRealTimeSource()
	ch, timer := source.NewTimer(0)
	<-ch
	assert.False(t, timer.Stop())
}

func TestNewRealClock_NewTimer_Stop(t *testing.T) {
	t.Parallel()

	source := clock.NewRealTimeSource()
	_, timer := source.NewTimer(time.Second)
	assert.True(t, timer.Stop())
}
