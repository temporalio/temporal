package goro_test

import (
	"sync"
	"testing"
	"time"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/testing/parallelsuite"
)

type AdaptivePoolSuite struct {
	parallelsuite.Suite[*AdaptivePoolSuite]
}

func TestAdaptivePoolSuite(t *testing.T) {
	parallelsuite.Run(t, &AdaptivePoolSuite{})
}

func block()   { <-make(chan struct{}) }
func nothing() {}

func (s *AdaptivePoolSuite) TestCallsF() {
	ts := clock.NewEventTimeSource()

	const (
		minWorkers = 1
		maxWorkers = 10
	)

	p := goro.NewAdaptivePool(ts, minWorkers, maxWorkers, 10*time.Millisecond, 10)
	defer p.Stop()

	var wg sync.WaitGroup
	wg.Add(1)
	p.Do(wg.Done)
	wg.Wait()
}

func (s *AdaptivePoolSuite) TestGrows() {
	ts := clock.NewEventTimeSource()

	const (
		minWorkers         = 5
		maxWorkers         = 10
		workersBeforeDelay = minWorkers
		workersAfterGrowth = minWorkers + 1
	)

	p := goro.NewAdaptivePool(ts, minWorkers, maxWorkers, 10*time.Millisecond, 10)
	defer p.Stop()

	// Occupy five workers.
	p.Do(block)
	p.Do(block)
	p.Do(block)
	p.Do(block)
	p.Do(block)

	// The sixth call will still start a new worker after the delay.
	doneCh := make(chan struct{})
	go func() {
		p.Do(block)
		doneCh <- struct{}{}
	}()

	// Wait for the goroutine to block in Do.
	// There should be one timer.
	s.AwaitTrue(func() bool { return ts.NumTimers() == 1 }, time.Second, time.Millisecond)

	select {
	case <-doneCh:
		s.Fail("should be blocked")
		return
	default:
	}

	s.Equal(workersBeforeDelay, p.NumWorkers()) // still 5 here

	ts.Advance(15 * time.Millisecond)
	<-doneCh

	s.Equal(workersAfterGrowth, p.NumWorkers()) // now 6 here
}

func (s *AdaptivePoolSuite) TestDoesntGrowPastMax() {
	ts := clock.NewEventTimeSource()

	const (
		minWorkers = 5
		maxWorkers = minWorkers
	)

	p := goro.NewAdaptivePool(ts, minWorkers, maxWorkers, 10*time.Millisecond, 10)
	defer p.Stop()

	// Occupy five workers, one of which is interruptible.
	p.Do(block)
	p.Do(block)
	interruptCh := make(chan struct{})
	p.Do(func() { interruptCh <- struct{}{} })
	p.Do(block)
	p.Do(block)

	// The sixth call will block.
	doneCh := make(chan struct{})
	go func() {
		p.Do(block)
		doneCh <- struct{}{}
	}()

	// Wait for the goroutine to block in Do.
	// We can't use NumTimers since it doesn't create a timer.
	time.Sleep(10 * time.Millisecond)

	select {
	case <-doneCh:
		s.Fail("should be blocked")
		return
	default:
	}

	// Unblock the fifth worker, which will allow the sixth call to run immediately.
	<-interruptCh
	// Wait for the sixth call.
	<-doneCh

	s.Equal(maxWorkers, p.NumWorkers()) // still 5
}

func (s *AdaptivePoolSuite) TestShrinksAgain() {
	ts := clock.NewEventTimeSource()

	const (
		minWorkers         = 1
		maxWorkers         = 5
		workersAfterGrowth = 3
		workersAfterShrink = 2
	)

	p := goro.NewAdaptivePool(ts, minWorkers, maxWorkers, 10*time.Millisecond, 1)
	defer p.Stop()

	// Make 3 calls to force it to grow to 3 workers.
	p.Do(block)

	syncCh := make(chan struct{}, 10)
	go p.Do(func() { syncCh <- struct{}{}; block() })
	// Wait for the goroutine to block in Do.
	s.AwaitTrue(func() bool { return ts.NumTimers() == 1 }, time.Second, time.Millisecond)
	ts.Advance(10 * time.Millisecond) // allow it to start another
	<-syncCh                          // wait for it to call the function

	go p.Do(func() { syncCh <- struct{}{} })
	s.AwaitTrue(func() bool { return ts.NumTimers() == 1 }, time.Second, time.Millisecond)
	ts.Advance(10 * time.Millisecond) // allow it to start another
	<-syncCh                          // wait for it to call the function

	s.Equal(workersAfterGrowth, p.NumWorkers())

	// Now there are 3 workers with one free, another call or three should start immediately.
	p.Do(nothing)
	p.Do(nothing)
	p.Do(nothing)

	// After no more than 10ms, the free worker should exit.
	// Advance the next timer once the worker has registered it.
	s.Await(func(s *AdaptivePoolSuite) {
		if ts.NumTimers() > 0 {
			ts.AdvanceNext() // let timer fire
		}
		s.Equal(workersAfterShrink, p.NumWorkers())
	}, time.Second, time.Millisecond)
}
