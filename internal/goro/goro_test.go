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
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.temporal.io/server/internal/goro"
)

func blockOnCtxReturnErr(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func blockOnCtxReturnNil(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func TestGoroParentTimeout(t *testing.T) {
	pctx, pcancel := context.WithTimeout(context.TODO(), 5*time.Microsecond)
	defer pcancel()
	g := goro.NewHandle(pctx).Go(blockOnCtxReturnErr)
	<-g.Done()
	require.Equal(t, pctx.Err(), g.Err())
}

func TestGoroCancelParentCtx(t *testing.T) {
	pctx, pcancel := context.WithCancel(context.TODO())
	g := goro.NewHandle(pctx).Go(blockOnCtxReturnErr)
	pcancel()
	<-g.Done()
	require.Equal(t, pctx.Err(), g.Err())
}

func TestGoroCancel(t *testing.T) {
	pctx := context.Background()
	g := goro.NewHandle(pctx).Go(blockOnCtxReturnErr)
	g.Cancel()
	<-g.Done()
	require.ErrorIs(t, context.Canceled, g.Err())
	require.Nil(t, pctx.Err())
}

func TestGoroMultiCancel(t *testing.T) {
	g := goro.NewHandle(context.TODO()).Go(blockOnCtxReturnErr)
	g.Cancel()
	require.NotPanics(t, g.Cancel)
	require.NotPanics(t, g.Cancel)
	require.NotPanics(t, g.Cancel)
	<-g.Done()
	require.NotPanics(t, g.Cancel)
	require.NotPanics(t, g.Cancel)
	require.NotPanics(t, g.Cancel)
}

func TestGoroDoneNoErr(t *testing.T) {
	g := goro.NewHandle(context.TODO()).Go(blockOnCtxReturnNil)
	g.Cancel()
	<-g.Done()
	require.NoError(t, g.Err())
}

func TestGoroSimpleReturn(t *testing.T) {
	g := goro.NewHandle(context.TODO()).Go(func(context.Context) error {
		return nil
	})
	<-g.Done()
}

func TestGoroGoexit(t *testing.T) {
	g := goro.NewHandle(context.TODO()).Go(func(context.Context) error {
		runtime.Goexit()
		return nil
	})
	<-g.Done()
}

func ExampleHandle() {
	h := goro.NewHandle(context.Background()).Go(func(ctx context.Context) error {
		<-ctx.Done()
		fmt.Println("shutting down")
		return ctx.Err()
	})
	fmt.Println(h.Err())
	h.Cancel()
	<-h.Done()
	fmt.Println(h.Err())

	// Output:
	// <nil>
	// shutting down
	// context canceled
}
