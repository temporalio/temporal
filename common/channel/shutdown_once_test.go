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

package channel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShutdownOnceImpl_PropagateCancel(t *testing.T) {
	t.Run("context is canceled when shutdown is called", func(t *testing.T) {
		t.Parallel()
		so := NewShutdownOnce()
		ctx, cancel := so.PropagateShutdown(context.Background())
		defer cancel()
		select {
		case <-ctx.Done():
			t.Fatal("ctx should not be done")
		default:
		}
		so.Shutdown()
		<-ctx.Done()
		err := ctx.Err()
		assert.ErrorIs(t, err, context.Canceled)
		require.True(t, so.IsShutdown())
		select {
		case _, ok := <-so.Channel():
			assert.False(t, ok, "channel should be closed")
		default:
			t.Fatal("channel should be closed")
		}
	})
	t.Run("input context is canceled", func(t *testing.T) {
		t.Parallel()
		so := NewShutdownOnce()
		inCtx, cancelInputCtx := context.WithCancel(context.Background())
		outCtx, _ := so.PropagateShutdown(inCtx)
		cancelInputCtx()
		so.wg.Wait()
		require.False(t, so.IsShutdown(), "should not shutdown when input context is canceled")
		select {
		case <-outCtx.Done():
		default:
			t.Fatal("output context should be canceled when input context is canceled")
		}
	})
	t.Run("output context is canceled", func(t *testing.T) {
		t.Parallel()
		so := NewShutdownOnce()
		_, cancel := so.PropagateShutdown(context.Background())
		cancel()
		so.wg.Wait()
		require.False(t, so.IsShutdown(), "should not shutdown when spawned context is canceled")
		select {
		case <-so.Channel():
			t.Fatal("channel should not be closed")
		default:
		}
	})
}
