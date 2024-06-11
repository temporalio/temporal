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

package goro

import (
	"math/rand"
	"sync/atomic"
	"time"
)

type (
	// AdaptivePool manages a pool of goroutines to handle small items of work.
	// The size of the pool starts with minWorkers but can grow if needed, up to maxWorkers,
	// and can shrink back down to minWorkers.
	AdaptivePool struct {
		minWorkers   int
		maxWorkers   int
		targetDelay  time.Duration
		shrinkFactor float64

		ch      chan func()
		workers atomic.Int64
	}
)

func NewAdaptivePool(
	minWorkers int,
	maxWorkers int,
	targetDelay time.Duration,
	shrinkFactor float64,
) *AdaptivePool {
	p := &AdaptivePool{
		minWorkers:   minWorkers,
		maxWorkers:   maxWorkers,
		targetDelay:  targetDelay,
		shrinkFactor: shrinkFactor,
		ch:           make(chan func()),
	}
	for i := 0; i < minWorkers; i++ {
		go p.work()
	}
	p.workers.Store(int64(minWorkers))
	return p
}

// Stops workers. Note that this does not wait for workers to exit.
func (p *AdaptivePool) Stop() {
	close(p.ch)
}

// Do calls f() on a worker. If the call can't be started within targetDelay, it adds another
// worker.
func (p *AdaptivePool) Do(f func()) {
	have := p.workers.Load()
	if have < int64(p.maxWorkers) {
		// we might want to add a worker, send with timeout
		timeout := time.NewTimer(p.targetDelay)
		select {
		case p.ch <- f:
			timeout.Stop()
			return
		case <-timeout.C:
		}

		if p.workers.CompareAndSwap(have, have+1) {
			go p.work()
		}
	}

	// blocking send
	p.ch <- f
}

func (p *AdaptivePool) work() {
	for {
		have := p.workers.Load()
		if have > int64(p.minWorkers) {
			// we might want to exit, receive with timeout
			// jitter this so we shrink slower than we grow
			timeout := time.NewTimer(time.Duration(float64(p.targetDelay) * (1 + p.shrinkFactor*rand.Float64())))
			select {
			case f, ok := <-p.ch:
				if !ok {
					return
				}
				f()
				continue
			case <-timeout.C:
			}
			if p.workers.CompareAndSwap(have, have-1) {
				return
			}
		}

		// blocking receive
		f, ok := <-p.ch
		if !ok {
			return
		}
		f()
	}
}
