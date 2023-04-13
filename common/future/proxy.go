// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package future

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

type (
	// Proxy is a future.Future implementation that wraps an underlying target
	// Future and which can be rebound to different targets without disrupting
	// the callers blocked in the calls to Get(context.Context).
	Proxy[T any] struct {
		mut     sync.Mutex
		waiters list.List
		target  Future[T]
	}
)

// NewProxy instantiates a future.Proxy with the provided Future instance as the
// intial target.
func NewProxy[T any](f Future[T]) *Proxy[T] {
	return &Proxy[T]{target: f}
}

// Get implements Future.Get and consequently is threadsafe for an unbounded
// population. There is non-trivial bookkeeping and locking done to track
// callers so clients should test their usage for scalability.
//
//nolint:revive // Yes, there is another Get in the pkg - it's the same interface
func (p *Proxy[T]) Get(parentCtx context.Context) (t T, err error) {
	var waiter *list.Element
	for {
		ctx, cancel := context.WithCancel(parentCtx)
		p.mut.Lock()
		if waiter == nil {
			waiter = p.waiters.PushBack(cancel)
		} else {
			waiter.Value = cancel
		}
		localTarget := p.target
		p.mut.Unlock()
		t, err = localTarget.Get(ctx)
		if errors.Is(err, context.Canceled) && parentCtx.Err() == nil {
			// local context was canceled but not the parent context -
			// this only happens via a call to Rebind so here we loop back to
			// call Get() again on the new target.
			continue
		}
		break
	}
	p.mut.Lock()
	defer p.mut.Unlock()
	p.waiters.Remove(waiter)
	return
}

// Ready implements Future.Ready by delegating to this Proxy instance's current
// target Future.
//
//nolint:revive // Yes, there is another Get in the pkg - it's the same interface
func (p *Proxy[T]) Ready() bool {
	p.mut.Lock()
	defer p.mut.Unlock()
	return p.target.Ready()
}

// Rebind changes the target Future instance used by this Proxy.
func (p *Proxy[T]) Rebind(newTarget Future[T]) {
	p.mut.Lock()
	defer p.mut.Unlock()
	p.target = newTarget
	for ele := p.waiters.Front(); ele != nil; ele = ele.Next() {
		ele.Value.(context.CancelFunc)()
	}
}

// WaiterCount access the current number of coroutines blocked in a call to this
// Proxy's Get function. Complexity is O(1).
func (p *Proxy[T]) WaiterCount() int {
	p.mut.Lock()
	defer p.mut.Unlock()
	return p.waiters.Len()
}
