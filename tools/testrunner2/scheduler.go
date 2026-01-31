package testrunner2

import (
	"context"
	"sync"
)

// queueItem represents a unit of work with lifecycle callbacks.
type queueItem struct {
	run  func(ctx context.Context) // execute the work
	next func() []*queueItem       // items to enqueue after completion
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
// Items are dequeued, run() is called, then next() items are enqueued.
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

		item.run(ctx)

		// Don't enqueue follow-up work if context is cancelled
		if ctx.Err() != nil {
			s.workerDone()
			return
		}

		var nextItems []*queueItem
		if item.next != nil {
			nextItems = item.next()
		}
		s.workerDoneWithItems(nextItems)
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

// workerDone signals that a worker has finished processing an item.
func (s *scheduler) workerDone() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.active--
	if s.active == 0 && len(s.queue) == 0 {
		s.done = true
		s.cond.Broadcast()
	}
}

// workerDoneWithItems signals completion and enqueues follow-up items.
func (s *scheduler) workerDoneWithItems(items []*queueItem) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.active--
	if len(items) > 0 {
		s.queue = append(s.queue, items...)
		s.cond.Broadcast()
	} else if s.active == 0 && len(s.queue) == 0 {
		s.done = true
		s.cond.Broadcast()
	}
}
