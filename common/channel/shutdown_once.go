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

package channel

import (
	"context"
	"sync"
	"sync/atomic"
)

const (
	shutdownOnceStatusOpen   int32 = 0
	shutdownOnceStatusClosed int32 = 1
)

type (
	ShutdownOnce interface {
		// Shutdown broadcast shutdown signal
		Shutdown()
		// IsShutdown return true if already shutdown
		IsShutdown() bool
		// Channel for shutdown notification
		Channel() <-chan struct{}
		// PropagateShutdown propagates the shutdown signal to the input context, canceling it when shutdown is called.
		// This is useful when creating contexts for background tasks that should be terminated when the service that
		// started them is shutdown.
		// The cancel function returned by this method should be called in the same manner as the cancel function
		// returned by context.WithCancel. In essence, it should be called in a defer statement.
		PropagateShutdown(ctx context.Context) (context.Context, context.CancelFunc)
	}

	ShutdownOnceImpl struct {
		status  int32
		channel chan struct{}
		wg      sync.WaitGroup
	}
)

func NewShutdownOnce() *ShutdownOnceImpl {
	return &ShutdownOnceImpl{
		status:  shutdownOnceStatusOpen,
		channel: make(chan struct{}),
	}
}

func (c *ShutdownOnceImpl) Shutdown() {
	if atomic.CompareAndSwapInt32(
		&c.status,
		shutdownOnceStatusOpen,
		shutdownOnceStatusClosed,
	) {
		close(c.channel)
	}
}

func (c *ShutdownOnceImpl) IsShutdown() bool {
	return atomic.LoadInt32(&c.status) == shutdownOnceStatusClosed
}

func (c *ShutdownOnceImpl) Channel() <-chan struct{} {
	return c.channel
}

// PropagateShutdown wraps the given context with a cancel function. We then spawn a goroutine which waits for the
// context to be canceled or for the shutdown channel to be closed. When shutdown is called, the cancel function is
// also called.
func (c *ShutdownOnceImpl) PropagateShutdown(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		select {
		case <-c.channel:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
