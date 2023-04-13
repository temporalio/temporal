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

package future_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/future"
)

func readFuture(ctx context.Context, f future.Future[int], ch chan<- interface{}) {
	result, err := f.Get(ctx)
	if err != nil {
		ch <- err
		return
	}
	ch <- result
}

func TestBasicValueOutcome(t *testing.T) {
	t.Parallel()
	target := future.NewFuture[int]()
	proxy := future.NewProxy[int](target)
	outcomeChan := make(chan interface{}) // int | error
	go readFuture(context.TODO(), proxy, outcomeChan)
	require.False(t, proxy.Ready())
	want := 3
	target.Set(want, nil)
	require.True(t, proxy.Ready())
	outcome := <-outcomeChan
	require.EqualValues(t, want, outcome)
}

func TestBasicErrorOutcome(t *testing.T) {
	t.Parallel()
	target := future.NewFuture[int]()
	proxy := future.NewProxy[int](target)
	outcomeChan := make(chan interface{}) // int | error
	go readFuture(context.TODO(), proxy, outcomeChan)
	require.False(t, proxy.Ready())
	want := errors.New(t.Name())
	target.Set(0, want)
	require.True(t, proxy.Ready())
	outcome := <-outcomeChan
	require.EqualValues(t, want, outcome)
}

func TestCancelPollingContext(t *testing.T) {
	t.Parallel()
	proxy := future.NewProxy[int](future.NewFuture[int]())
	ctx, cancel := context.WithCancel(context.Background())
	outcomeChan := make(chan interface{}) // int | error
	go readFuture(ctx, proxy, outcomeChan)
	cancel()
	require.False(t, proxy.Ready())
	outcome := <-outcomeChan
	require.ErrorIs(t, outcome.(error), context.Canceled)
}

func TestConcurrentRebind(t *testing.T) {
	t.Parallel()
	proxy := future.NewProxy[int](future.NewFuture[int]())
	count := 100                          // concurrent Future.Get calls
	outcomeChan := make(chan interface{}) // int | error
	for i := 0; i < count; i++ {
		go readFuture(context.TODO(), proxy, outcomeChan)
	}

	expectNWaiters := func(n int) func() bool {
		return func() bool { return proxy.WaiterCount() == n }
	}

	require.Eventually(t, expectNWaiters(count), 3*time.Second, 10*time.Millisecond,
		"all the readFuture goroutines should become waiters in the Proxy")

	var replacementTarget *future.FutureImpl[int]
	for i := 0; i < 10; i++ { // rebind ten times
		replacementTarget = future.NewFuture[int]()
		proxy.Rebind(replacementTarget)
	}

	require.Eventually(t, expectNWaiters(count), 3*time.Second, 10*time.Millisecond,
		"all waiters should still be waiting in the Proxy")

	want := 12345
	replacementTarget.Set(want, nil)
	require.True(t, proxy.Ready())
	require.Eventually(t, expectNWaiters(0), 3*time.Second, 10*time.Millisecond,
		"number of waiters should to go zero after the target future completes")

	for i := 0; i < count; i++ {
		require.EqualValues(t, want, <-outcomeChan,
			"all waiters should get the outcome value")
	}
}

func TestProxyComposes(t *testing.T) {
	t.Parallel()
	target := future.NewFuture[int]()
	proxy := future.NewProxy[int](
		future.NewProxy[int](
			future.NewProxy[int](target),
		),
	)
	outcomeChan := make(chan interface{}) // int | error
	go readFuture(context.TODO(), proxy, outcomeChan)
	require.False(t, proxy.Ready())
	want := 123
	target.Set(want, nil)
	require.True(t, proxy.Ready())
	require.EqualValues(t, want, <-outcomeChan)
}
