package testrunner2

import (
	"context"
	"sync"
)

// queueItem represents a unit of work.
type queueItem struct {
	run func(ctx context.Context, emit func(...*queueItem))
}

type scheduler struct {
	parallelism int

	mu     sync.Mutex
	cond   *sync.Cond
	queue  []*queueItem
	active int  // number of workers currently running an item
	done   bool // set when all work is complete
}

func newScheduler(parallelism int) *scheduler {
	s := &scheduler{parallelism: parallelism}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// run processes items in parallel up to the parallelism limit.
func (s *scheduler) run(ctx context.Context, items []*queueItem) {
	if len(items) == 0 {
		return
	}

	s.queue = items

	var wg sync.WaitGroup
	for range s.parallelism {
		wg.Go(func() { s.worker(ctx) })
	}
	wg.Wait()
}

func (s *scheduler) worker(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		item := s.dequeue(ctx)
		if item == nil {
			return
		}

		item.run(ctx, s.enqueue)
		s.workerDone()
	}
}

// dequeue blocks until an item is available or all work is complete.
func (s *scheduler) dequeue(ctx context.Context) *queueItem {
	s.mu.Lock()
	defer s.mu.Unlock()

	for len(s.queue) == 0 && !s.done {
		// Check context while waiting
		if ctx.Err() != nil {
			return nil
		}
		s.cond.Wait()
	}

	if len(s.queue) == 0 {
		return nil
	}

	item := s.queue[0]
	s.queue = s.queue[1:]
	s.active++
	return item
}

// enqueue adds items to the queue. Safe to call from any goroutine.
func (s *scheduler) enqueue(items ...*queueItem) {
	if len(items) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queue = append(s.queue, items...)
	s.cond.Broadcast()
}

// workerDone signals that a worker has finished processing its item.
func (s *scheduler) workerDone() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
	if s.active == 0 && len(s.queue) == 0 {
		s.done = true
		s.cond.Broadcast()
	}
}
