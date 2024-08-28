// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package tasks

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/metrics"
)

type testLimiter struct {
}

// BufferSize implements DynamicWorkerPoolLimiter.
func (testLimiter) BufferSize() int {
	return 2
}

// Concurrency implements DynamicWorkerPoolLimiter.
func (testLimiter) Concurrency() int {
	return 2
}

var _ DynamicWorkerPoolLimiter = testLimiter{}

func TestDynamicWorkerPoolSchedulerLogic(t *testing.T) {
	stopCh := make(chan struct{}, 1)
	wg := sync.WaitGroup{}
	wg.Add(3)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	sched := NewDynamicWorkerPoolScheduler(testLimiter{}, metrics.NoopMetricsHandler)

	// First task, signals the wait group when done and allows more tasks to be processed in the spawned goroutine.
	task1 := NewMockRunnable(ctrl)
	task1.EXPECT().Run(gomock.Any()).DoAndReturn(func(context.Context) { wg.Done() }).Times(1)
	// Second task, signals the wait group when done, blocks the worker goroutine until stopCh is closed.
	task2 := NewMockRunnable(ctrl)
	task2.EXPECT().Run(gomock.Any()).DoAndReturn(func(context.Context) { wg.Done(); <-stopCh }).Times(1)
	// Third task, signals the wait group when done, blocks the worker goroutine until stopCh is closed.
	task3 := NewMockRunnable(ctrl)
	task3.EXPECT().Run(gomock.Any()).DoAndReturn(func(context.Context) { wg.Done(); <-stopCh }).Times(1)
	// Fourth task, should not be run, aborted at shutdown.
	task4 := NewMockRunnable(ctrl)
	task4.EXPECT().Abort().Times(1)
	// Fifth task, should not be run, aborted at shutdown.
	task5 := NewMockRunnable(ctrl)
	task5.EXPECT().Abort().Times(1)
	// Sixth task, exceeds the BufferSize limit.
	task6 := NewMockRunnable(ctrl)

	// Submit all tasks.
	require.True(t, sched.TrySubmit(task1))
	require.True(t, sched.TrySubmit(task2))
	require.True(t, sched.TrySubmit(task3))
	require.True(t, sched.TrySubmit(task4))
	// The buffer should eventually have capacity to take task5.
	require.Eventually(t, func() bool { return sched.TrySubmit(task5) }, time.Millisecond*100, time.Millisecond)
	// The two goroutines are blocked, and two tasks are buffered, reject.
	require.False(t, sched.TrySubmit(task6))

	// Wait for all three expected runnables to have been run before shutting down.
	wg.Wait()
	sched.InitiateShutdown()
	// Unblock the two concurrent worker goroutines.
	close(stopCh)
	sched.WaitShutdown()
}
