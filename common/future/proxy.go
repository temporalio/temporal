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
func (p *Proxy[T]) Get(parentCtx context.Context) (T, error) {
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
		t, err := localTarget.Get(ctx)
		if errors.Is(err, context.Canceled) && parentCtx.Err() == nil {
			// local context was canceled but not the parent context -
			// this only happens via a call to Rebind so here we loop back to
			// call Get() again on the new target.
			continue
		}

		p.mut.Lock()
		defer p.mut.Unlock()
		p.waiters.Remove(waiter)
		return t, err
	}
}

// Ready implements Future.Ready by delegating to this Proxy instance's current
// target Future.
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
