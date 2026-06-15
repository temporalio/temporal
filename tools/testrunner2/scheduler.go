package testrunner2

import (
	"context"
	"sync"
)

// queueItem represents a unit of work.
type queueItem struct {
	run       func(ctx context.Context, emit func(...*queueItem))
	onEnqueue func()
	exclusive bool
}

type scheduler struct {
	parallelism int

	mu             sync.Mutex
	cond           *sync.Cond
	queue          []*queueItem
	exclusiveQueue []*queueItem
	active         int // number of workers currently running an item

	exclusiveActive bool
	done            bool // set when all work is complete
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

	s.enqueue(items...)

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
		s.workerDone(item)
	}
}

// dequeue blocks until an item is available or all work is complete.
func (s *scheduler) dequeue(ctx context.Context) *queueItem {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		if ctx.Err() != nil {
			return nil
		}

		if s.done {
			return nil
		}

		if s.exclusiveActive {
			s.cond.Wait()
			continue
		}

		if len(s.exclusiveQueue) > 0 {
			if s.active == 0 {
				item := s.exclusiveQueue[0]
				s.exclusiveQueue = s.exclusiveQueue[1:]
				s.active++
				s.exclusiveActive = true
				return item
			}
			s.cond.Wait()
			continue
		}

		if len(s.queue) > 0 {
			item := s.queue[0]
			s.queue = s.queue[1:]
			s.active++
			return item
		}

		s.cond.Wait()
	}
}

// enqueue adds items to the queue. Safe to call from any goroutine.
func (s *scheduler) enqueue(items ...*queueItem) {
	if len(items) == 0 {
		return
	}
	for _, item := range items {
		if item.onEnqueue != nil {
			item.onEnqueue()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, item := range items {
		if item.exclusive {
			s.exclusiveQueue = append(s.exclusiveQueue, item)
		} else {
			s.queue = append(s.queue, item)
		}
	}
	s.cond.Broadcast()
}

// workerDone signals that a worker has finished processing its item.
func (s *scheduler) workerDone(item *queueItem) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
	if item.exclusive {
		s.exclusiveActive = false
	}
	if s.active == 0 && len(s.queue) == 0 && len(s.exclusiveQueue) == 0 {
		s.done = true
	}
	s.cond.Broadcast()
}
