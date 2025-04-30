package goro

import (
	"math/rand"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/clock"
)

type (
	// AdaptivePool manages a pool of goroutines to handle small items of work.
	// The size of the pool starts with minWorkers but can grow if needed, up to maxWorkers,
	// and can shrink back down to minWorkers.
	AdaptivePool struct {
		ts           clock.TimeSource
		minWorkers   int
		maxWorkers   int
		targetDelay  time.Duration
		shrinkFactor float64

		ch      chan func()
		stopCh  chan struct{}
		workers atomic.Int64
	}
)

func NewAdaptivePool(
	ts clock.TimeSource,
	minWorkers int,
	maxWorkers int,
	targetDelay time.Duration,
	shrinkFactor float64,
) *AdaptivePool {
	p := &AdaptivePool{
		ts:           ts,
		minWorkers:   minWorkers,
		maxWorkers:   maxWorkers,
		targetDelay:  targetDelay,
		shrinkFactor: shrinkFactor,
		ch:           make(chan func()),
		stopCh:       make(chan struct{}),
	}
	for i := 0; i < minWorkers; i++ {
		go p.work()
	}
	p.workers.Store(int64(minWorkers))
	return p
}

// Stops workers. Note that this does not wait for workers to exit.
// When Stop is called, concurrent calls to Do may or may not call their function, and future
// calls definitely won't.
func (p *AdaptivePool) Stop() {
	close(p.stopCh)
}

// Do calls f() on a worker goroutine. If the call can't be started within targetDelay, it adds
// another worker. If Stop is called concurrently, Do may or may not call f. If Stop has been
// called already, Do does nothing.
func (p *AdaptivePool) Do(f func()) {
	// try send first
	select {
	case p.ch <- f:
		return
	default:
	}

	// we might want to add a worker, send with timeout
	have := p.workers.Load()
	if have < int64(p.maxWorkers) {
		timech, timer := p.ts.NewTimer(p.targetDelay)
		select {
		case <-p.stopCh:
			timer.Stop()
			return
		case p.ch <- f:
			timer.Stop()
			return
		case <-timech:
		}

		if p.workers.CompareAndSwap(have, have+1) {
			go p.work()
		}
	}

	// blocking send
	select {
	case p.ch <- f:
	case <-p.stopCh:
	}
}

func (p *AdaptivePool) work() {
	for {
		// try receive first
		select {
		case f := <-p.ch:
			f()
			continue
		default:
		}

		have := p.workers.Load()
		if have > int64(p.minWorkers) {
			// we might want to exit, receive with timeout
			// jitter this so we shrink slower than we grow
			timech, timer := p.ts.NewTimer(time.Duration(float64(p.targetDelay) * p.shrinkFactor * rand.Float64()))
			select {
			case <-p.stopCh:
				timer.Stop()
				return
			case f := <-p.ch:
				timer.Stop()
				f()
				continue
			case <-timech:
			}
			if p.workers.CompareAndSwap(have, have-1) {
				return
			}
		}

		// blocking receive
		select {
		case <-p.stopCh:
			return
		case f := <-p.ch:
			f()
		}
	}
}

// NumWorkers returns the current number of workers. Probably only useful for testing.
func (p *AdaptivePool) NumWorkers() int {
	return int(p.workers.Load())
}
