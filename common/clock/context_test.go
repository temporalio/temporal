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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/clock"
)

func TestContextWithTimeout_Canceled(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Unix(0, 0))
	ctx := context.Background()
	ctx, cancel := clock.ContextWithTimeout(ctx, time.Second, timeSource)
	deadline, ok := ctx.Deadline()
	assert.True(t, ok)
	assert.Equal(t, time.Unix(1, 0), deadline)
	cancel()
	select {
	case <-ctx.Done():
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
	default:
		t.Fatal("expected context to be canceled")
	}
}

func TestContextWithTimeout_Fire(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Unix(0, 0))
	ctx := context.Background()
	ctx, cancel := clock.ContextWithTimeout(ctx, time.Second, timeSource)
	deadline, ok := ctx.Deadline()
	assert.True(t, ok)
	assert.Equal(t, time.Unix(1, 0), deadline)
	timeSource.Advance(time.Second - time.Millisecond)
	select {
	case <-ctx.Done():
		t.Fatal("expected context to not be canceled")
	default:
		assert.NoError(t, ctx.Err())
	}
	timeSource.Advance(time.Millisecond)
	select {
	case <-ctx.Done():
		assert.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	default:
		t.Fatal("expected context to be canceled")
	}
	cancel() // should be a no-op
}
