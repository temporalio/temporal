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

package goro_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/internal/goro"
)

func TestMultiCancelAndWait(t *testing.T) {
	var g goro.Group
	g.Go(blockOnCtxReturnNil)
	g.Go(blockOnCtxReturnNil)
	g.Go(blockOnCtxReturnNil)
	g.Cancel()
	g.Wait()
}

func TestCancelBeforeGo(t *testing.T) {
	var g goro.Group
	g.Cancel()
	g.Go(func(ctx context.Context) error {
		require.ErrorIs(t, context.Canceled, ctx.Err())
		return nil
	})
	g.Wait()
}

func TestWaitOnNothing(t *testing.T) {
	var g goro.Group
	g.Wait() // nothing running, should return immediately
}

func TestWaitOnDifferentThread(t *testing.T) {
	var g goro.Group
	g.Go(blockOnCtxReturnNil)
	h := goro.Go(context.TODO(), func(context.Context) error {
		g.Wait()
		return nil
	})
	g.Cancel()
	<-h.Done()
}
