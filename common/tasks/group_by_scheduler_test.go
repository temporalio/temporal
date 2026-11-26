package tasks

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/await"
	"go.uber.org/mock/gomock"
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

type immediateNoopScheduler struct {
	stopped atomic.Bool
	waited  atomic.Bool
}

func (s *immediateNoopScheduler) TrySubmit(r Runnable) bool {
	r.Run(context.Background())
	return true
}

func (s *immediateNoopScheduler) InitiateShutdown() {
	s.stopped.Store(true)
}

func (s *immediateNoopScheduler) WaitShutdown() {
	s.waited.Store(true)
}

var _ RunnableScheduler = &immediateNoopScheduler{}

type noopRunnable struct{}

func (noopRunnable) Run(context.Context) {}

func (noopRunnable) Abort() {}

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

	require.Len(t, scheds, 2)
	require.Len(t, scheds["a"].buffer, 1)
	require.Equal(t, "a", scheds["a"].buffer[0].(*trackedRunnable).runnable.(RunnableTask).Task.(taskWithID).ID)
	require.Len(t, scheds["b"].buffer, 1)
	require.Equal(t, "b", scheds["b"].buffer[0].(*trackedRunnable).runnable.(RunnableTask).Task.(taskWithID).ID)
	// Stop shuts down all groups.
	require.True(t, scheds["a"].stopped && scheds["b"].stopped)
	require.True(t, scheds["a"].waited && scheds["b"].waited)
}

func TestGroupBySchedulerEvictsIdleGroups(t *testing.T) {
	schedulers := make(map[string]*immediateNoopScheduler)
	sched := NewGroupByScheduler[string, taskWithID](GroupBySchedulerOptions[string, taskWithID]{
		Logger:      log.NewNoopLogger(),
		IdleTimeout: time.Millisecond,
		KeyFn:       func(t taskWithID) string { return t.ID },
		RunnableFactory: func(taskWithID) Runnable {
			return noopRunnable{}
		},
		SchedulerFactory: func(key string) RunnableScheduler {
			group := &immediateNoopScheduler{}
			schedulers[key] = group
			return group
		},
	})

	require.True(t, sched.TrySubmit(taskWithID{ID: "a"}))
	group := schedulers["a"]
	await.RequireTrue(t, func() bool {
		sched.mu.RLock()
		_, exists := sched.schedulers["a"]
		sched.mu.RUnlock()
		return !exists && group.stopped.Load() && group.waited.Load()
	}, time.Second, time.Millisecond)

	sched.Stop()
}

func TestGroupBySchedulerIgnoresStaleIdleCallbacks(t *testing.T) {
	schedulers := make(map[string]*immediateNoopScheduler)
	sched := NewGroupByScheduler[string, taskWithID](GroupBySchedulerOptions[string, taskWithID]{
		Logger:      log.NewNoopLogger(),
		IdleTimeout: time.Hour,
		KeyFn:       func(t taskWithID) string { return t.ID },
		RunnableFactory: func(taskWithID) Runnable {
			return noopRunnable{}
		},
		SchedulerFactory: func(key string) RunnableScheduler {
			group := &immediateNoopScheduler{}
			schedulers[key] = group
			return group
		},
	})
	defer sched.Stop()

	require.True(t, sched.TrySubmit(taskWithID{ID: "a"}))
	sched.mu.RLock()
	group := sched.schedulers["a"]
	sched.mu.RUnlock()
	require.NotNil(t, group)
	group.mu.Lock()
	staleTimerID := group.idleTimerID
	group.mu.Unlock()
	staleCallback := group.onIdle

	// Reuse the group before the old callback runs. This models a callback that
	// was already queued when the idle timer was stopped.
	require.True(t, sched.TrySubmit(taskWithID{ID: "a"}))
	staleCallback(staleTimerID)

	sched.mu.RLock()
	_, exists := sched.schedulers["a"]
	sched.mu.RUnlock()
	require.True(t, exists)
}
