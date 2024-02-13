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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

type taskWithID struct {
	ID string
	*MockTask
}

type bufferingNoopScheduler struct {
	buffer  []Runnable
	stopped bool
	waited  bool
}

func (s *bufferingNoopScheduler) TrySubmit(r Runnable) bool {
	if len(s.buffer) > 0 {
		return false
	}
	s.buffer = append(s.buffer, r)
	return true
}

func (s *bufferingNoopScheduler) InitiateShutdown() {
	s.stopped = true
}

func (s *bufferingNoopScheduler) WaitShutdown() {
	s.waited = true
}

var _ RunnableScheduler = &bufferingNoopScheduler{}

func TestSchedulerLogic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	scheds := make(map[string]*bufferingNoopScheduler, 0)
	logger := log.NewMockLogger(ctrl)
	logger.EXPECT().Debug(gomock.Any()).AnyTimes()
	sched := NewGroupByScheduler[string, taskWithID](GroupBySchedulerOptions[string, taskWithID]{
		Logger: logger,
		KeyFn:  func(t taskWithID) string { return t.ID },
		SchedulerFactory: func(key string) RunnableScheduler {
			_, ok := scheds[key]
			// Assert that the factory is only caller once per key.
			require.False(t, ok)
			sched := &bufferingNoopScheduler{}
			scheds[key] = sched
			return sched
		},
		RunnableFactory: func(t taskWithID) Runnable { return RunnableTask{t} },
	})
	task1a := taskWithID{"a", NewMockTask(ctrl)}
	task2a := taskWithID{"a", NewMockTask(ctrl)}
	task3b := taskWithID{"b", NewMockTask(ctrl)}
	task4b := taskWithID{"b", NewMockTask(ctrl)}

	require.True(t, sched.TrySubmit(task1a))
	// Buffer accepts only one task.
	require.False(t, sched.TrySubmit(task2a))
	require.True(t, sched.TrySubmit(task3b))
	sched.Stop()

	// Should abort after shutdown.
	task4b.EXPECT().Abort().Times(1)
	require.True(t, sched.TrySubmit(task4b))

	require.Equal(t, 2, len(scheds))
	require.Equal(t, 1, len(scheds["a"].buffer))
	require.Equal(t, "a", scheds["a"].buffer[0].(RunnableTask).Task.(taskWithID).ID)
	require.Equal(t, 1, len(scheds["b"].buffer))
	require.Equal(t, "b", scheds["b"].buffer[0].(RunnableTask).Task.(taskWithID).ID)
	// Stop shuts down all groups.
	require.True(t, scheds["a"].stopped && scheds["b"].stopped)
	require.True(t, scheds["a"].waited && scheds["b"].waited)
}
