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
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

type allowOne struct {
	admitted atomic.Bool
}

func (l *allowOne) Wait(ctx context.Context) error {
	if l.admitted.CompareAndSwap(false, true) {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestLimiterLogic(t *testing.T) {
	lim := newLimiter[Task](LimiterOptions{
		Concurrency: func() int { return 2 },
		RateLimiter: &allowOne{},
	})
	task := &MockTask{}
	require.True(t, lim.reserve())
	require.True(t, lim.reserve())
	require.False(t, lim.reserve())
	lim.enqueue(task)
	nextTask, ok := lim.dequeue()
	require.True(t, ok)
	require.Equal(t, task, nextTask)
	_, ok = lim.dequeue()
	require.False(t, ok)
	lim.release()
	lim.release()
	require.Equal(t, 0, lim.reservations)
}

type taskWithID struct {
	ID string
	*MockTask
}

func TestSchedulerLogic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	logger := log.NewMockLogger(ctrl)
	logger.EXPECT().Debug(gomock.Any()).AnyTimes()
	sched := NewMultiDestinationScheduler[string, taskWithID](MultiDestinationSchedulerOptions[string, taskWithID]{
		Logger: logger,
		KeyFn:  func(t taskWithID) string { return t.ID },
		LimiterOptionsFn: func(string) LimiterOptions {
			return LimiterOptions{
				Concurrency: func() int { return 2 },
				RateLimiter: &allowOne{},
			}
		},
	})
	// first task, key a, executes once
	task1a := taskWithID{"a", NewMockTask(ctrl)}
	task1a.EXPECT().Execute().Times(1)
	task1a.EXPECT().Ack().Times(1)
	task1a.EXPECT().HandleErr(nil).Times(1)
	// second task, key a, accepted, rate limited
	task2a := taskWithID{"a", NewMockTask(ctrl)}
	task2a.EXPECT().Abort().Times(1)
	// third task, key a, accepted, bufferred
	task3a := taskWithID{"a", NewMockTask(ctrl)}
	task3a.EXPECT().Abort().Times(1)
	// fourth task, key b, executes once
	task4b := taskWithID{"b", NewMockTask(ctrl)}
	task4b.EXPECT().Execute().Times(1)
	task4b.EXPECT().Ack().Times(1)
	task4b.EXPECT().HandleErr(nil).Times(1)

	require.True(t, sched.TrySubmit(task1a))
	require.True(t, sched.TrySubmit(task2a))
	require.True(t, sched.TrySubmit(task3a))
	require.True(t, sched.TrySubmit(task4b))
	sched.Stop()
	// should not execute after stopped
	require.False(t, sched.TrySubmit(task4b))
}
