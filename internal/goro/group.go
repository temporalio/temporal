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

package goro

import (
	"context"
	"sync"
)

// Group manages a set of long-running goroutines. Goroutines are spawned
// individually with Group.Go and after that interrupted and waited-on as a
// single unit. The expected use-case for this type is as a member field of a
// `common.Daemon` (or similar) type that spawns one or more goroutines in
// its Start() function and then stops those same goroutines in its Stop()
// function. The zero-value of this type is valid. A Group must not be copied
// after first use.
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
func (g *Group) Go(f func(ctx context.Context) error) {
	g.initOnce.Do(g.init)
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		f(g.ctx)
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
