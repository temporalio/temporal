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
	"sync/atomic"
)

type (
	// Handle is a threadsafe and multi-stop safe handle to a single running
	// goroutine.
	Handle struct {
		context context.Context
		cancel  context.CancelFunc
		done    chan struct{}
		err     atomic.Value
	}
)

// Go launches the supplied function in its own goroutine and returns back a
// *Handle that serves as a handle to the goroutine itself.
func Go(ctx context.Context, f func(context.Context) error) *Handle {
	ctx, cancel := context.WithCancel(ctx)
	h := &Handle{
		context: ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	go func() {
		// use defer here so that the channel is closed even if the func calls
		// runtime.Goexit()
		defer close(h.done)
		if err := f(h.context); err != nil {
			h.err.Store(err)
		}
	}()
	return h
}

// Done exposes a channel that allows outside goroutines to block on this
// goroutine's completion. Whatever time passes between a call to Cancel() and
// the Done() channel closing is the time taken by the goroutine to shut itself
// down.
func (h *Handle) Done() <-chan struct{} {
	return h.done
}

// Cancel requests that this goroutine stop by cancelling the associated context
// object. This function is threadsafe and idempotent. Note that this function
// _requests_ termination, it does not forcefully kill the goroutine.
func (h *Handle) Cancel() {
	h.cancel()
}

// Error observes the error returned by the func passed to Go (if any). There is
// never any error (i.e. this function returns nil) while the goroutine is
// running.
func (h *Handle) Err() error {
	v := h.err.Load()
	if v == nil {
		return nil
	}
	return v.(error)
}
