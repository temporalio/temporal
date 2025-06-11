package goro

import (
	"context"
	"sync"
)

// Group manages a set of long-running goroutines. Goroutines are spawned
// individually with Group.Go and after that interrupted and waited-on as a
// single unit. The expected use-case for this type is as a member field of a
// background process type that spawns one or more goroutines in its Start()
// function and then stops those same goroutines in its Stop() function.
// The zero-value of this type is valid. A Group must not be copied after
// first use.
type Group struct {
	initOnce sync.Once
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// Go spawns a goroutine whose lifecycle will be managed via this Group object.
// All functions passed to Go on the same instance of Group will be given the
// same `context.Context` object, which will be used to indicate cancellation.
// If the supplied func does not abide by `ctx.Done()` of the provided
// `context.Context` then `Wait()` on this `Group` will hang until all functions
// exit on their own (possibly never).
// NOTE: Errors returned by the supplied function are ignored.
func (g *Group) Go(f func(ctx context.Context) error) {
	g.initOnce.Do(g.init)
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		_ = f(g.ctx)
	}()
}

// Cancel cancels the `context.Context` that was passed to all goroutines
// spawned via `Go` on this `Group`.
func (g *Group) Cancel() {
	g.initOnce.Do(g.init)
	g.cancel()
}

// Wait blocks waiting for all goroutines spawned via `Go` on this `Group`
// instance to complete. If `Go` has not been called then this function returns
// immediately.
func (g *Group) Wait() {
	g.wg.Wait()
}

func (g *Group) init() {
	g.ctx, g.cancel = context.WithCancel(context.Background())
}
