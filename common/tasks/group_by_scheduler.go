package tasks

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
)

const defaultGroupBySchedulerIdleTimeout = time.Minute

// GroupBySchedulerOptions are options for creating a [GroupByScheduler].
type GroupBySchedulerOptions[K comparable, T Task] struct {
	Logger log.Logger
	// A function to determine the group of a task.
	KeyFn func(T) K
	// Factory for creating a runnable from a task.
	RunnableFactory func(T) Runnable
	// When a new group is encountered, use this function to create a scheduler for that group.
	SchedulerFactory func(K) RunnableScheduler
	// How long an idle group remains before its scheduler is stopped and removed.
	// A non-positive value uses the default.
	IdleTimeout time.Duration
}

var _ Scheduler[Task] = &GroupByScheduler[string, Task]{}

// GroupByScheduler groups tasks based on a provided key function and submits that task for processing on a dedicated
// scheduler for that group.
type GroupByScheduler[K comparable, T Task] struct {
	stopped atomic.Bool
	options GroupBySchedulerOptions[K, T]
	// Synchronizes access to the schedulers map.
	mu         sync.RWMutex
	schedulers map[K]*groupScheduler
	shutdownWg sync.WaitGroup
}

type groupScheduler struct {
	scheduler   RunnableScheduler
	idleTimeout time.Duration
	onIdle      func()

	mu          sync.Mutex
	activeTasks int
	idleTimer   *time.Timer
	closed      bool
}

func (g *groupScheduler) acquire() {
	g.mu.Lock()
	if g.idleTimer != nil {
		g.idleTimer.Stop()
		g.idleTimer = nil
	}
	g.activeTasks++
	g.mu.Unlock()
}

func (g *groupScheduler) release() {
	g.mu.Lock()
	g.activeTasks--
	if g.activeTasks == 0 && !g.closed {
		g.idleTimer = time.AfterFunc(g.idleTimeout, g.onIdle)
	}
	g.mu.Unlock()
}

func (g *groupScheduler) close() {
	g.mu.Lock()
	g.closed = true
	if g.idleTimer != nil {
		g.idleTimer.Stop()
		g.idleTimer = nil
	}
	g.mu.Unlock()
}

func (g *groupScheduler) closeIfIdle() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.activeTasks != 0 {
		return false
	}
	g.closed = true
	if g.idleTimer != nil {
		g.idleTimer.Stop()
		g.idleTimer = nil
	}
	return true
}

type trackedRunnable struct {
	runnable   Runnable
	onFinish   func()
	finishOnce sync.Once
}

func (r *trackedRunnable) Run(ctx context.Context) {
	defer r.finish()
	r.runnable.Run(ctx)
}

func (r *trackedRunnable) Abort() {
	defer r.finish()
	r.runnable.Abort()
}

func (r *trackedRunnable) finish() {
	r.finishOnce.Do(r.onFinish)
}

// NewGroupByScheduler creates a new [GroupByScheduler] from given options.
func NewGroupByScheduler[K comparable, T Task](options GroupBySchedulerOptions[K, T]) *GroupByScheduler[K, T] {
	if options.IdleTimeout <= 0 {
		options.IdleTimeout = defaultGroupBySchedulerIdleTimeout
	}
	return &GroupByScheduler[K, T]{
		options:    options,
		schedulers: make(map[K]*groupScheduler),
	}
}

func (*GroupByScheduler[K, T]) Start() {
	// noop
}

// Stop signals running tasks to stop, aborts any pending tasks and waits up to a minute for all running tasks to
// complete.
func (s *GroupByScheduler[K, T]) Stop() {
	if !s.stopped.CompareAndSwap(false, true) {
		return
	}

	s.mu.Lock()
	groups := make([]*groupScheduler, 0, len(s.schedulers))
	for key, group := range s.schedulers {
		group.close()
		groups = append(groups, group)
		delete(s.schedulers, key)
	}
	s.mu.Unlock()

	for _, group := range groups {
		group.scheduler.InitiateShutdown()
	}

	if success := common.BlockWithTimeout(func() {
		for _, group := range groups {
			group.scheduler.WaitShutdown()
		}
		s.shutdownWg.Wait()
	}, time.Minute); !success {
		s.options.Logger.Warn("GroupByScheduler timed out waiting for groups to complete shutdown")
	} else {
		s.options.Logger.Debug("GroupByScheduler shutdown complete")
	}
}

func (s *GroupByScheduler[K, T]) Submit(task T) {
	if !s.TrySubmit(task) {
		task.Reschedule()
	}
}

// TrySubmit submits a task for processing. If called after the scheduler is shut down, the task will be accepted and
// aborted.
func (s *GroupByScheduler[K, T]) TrySubmit(task T) bool {
	if s.stopped.Load() {
		// No need to reschedule this task, just abort after we've shut down.
		task.Abort()
		return true
	}
	key := s.options.KeyFn(task)
	group := s.getOrCreateScheduler(key)
	if group == nil {
		task.Abort()
		return true
	}
	runnable := &trackedRunnable{
		runnable: s.options.RunnableFactory(task),
		onFinish: group.release,
	}
	accepted := group.scheduler.TrySubmit(runnable)
	if !accepted {
		runnable.finish()
	}
	return accepted
}

// getOrCreateSchedulerForTask gets an existing scheduler for the given key or creates one if needed.
func (s *GroupByScheduler[K, T]) getOrCreateScheduler(key K) *groupScheduler {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped.Load() {
		return nil
	}
	group, ok := s.schedulers[key]
	if !ok {
		group = &groupScheduler{
			scheduler:   s.options.SchedulerFactory(key),
			idleTimeout: s.options.IdleTimeout,
		}
		group.onIdle = func() { s.removeIfIdle(key, group) }
		s.schedulers[key] = group
	}
	group.acquire()
	return group
}

func (s *GroupByScheduler[K, T]) removeIfIdle(key K, group *groupScheduler) {
	s.mu.Lock()
	if current, ok := s.schedulers[key]; !ok || current != group || !group.closeIfIdle() {
		s.mu.Unlock()
		return
	}
	delete(s.schedulers, key)
	s.shutdownWg.Add(1)
	s.mu.Unlock()

	defer s.shutdownWg.Done()
	group.scheduler.InitiateShutdown()
	group.scheduler.WaitShutdown()
}
