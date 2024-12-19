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

package util

import (
	"context"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/clock"
)

type StreamBatcher[T, R any] struct {
	fn      func([]T) R
	opts    StreamBatcherOptions
	clock   clock.TimeSource
	ch      chan batchPair[T, R]
	running atomic.Bool
}

type batchPair[T, R any] struct {
	resp chan R
	item T
}

type StreamBatcherOptions struct {
	// MaxItems is the maximum number of items in a batch.
	MaxItems int
	// MinDelay is how long to wait for no more items to come in after any item before
	// finishing the batch.
	MinDelay time.Duration
	// MaxDelay is the maximum time to wait after the first item in a batch before finishing
	// the batch.
	MaxDelay time.Duration
	// IdleTime is the time after which the internal goroutine will exit, to avoid wasting
	// resources on idle streams.
	IdleTime time.Duration
}

// NewStreamBatcher creates a StreamBatcher. It collects item passed to Add into slices, and
// then calls `fn` on each batch.
// fn will be called on batches of items in a single-threaded manner, and Add will block while
// fn is running.
func NewStreamBatcher[T, R any](fn func([]T) R, opts StreamBatcherOptions, clock clock.TimeSource) *StreamBatcher[T, R] {
	return &StreamBatcher[T, R]{
		fn:    fn,
		opts:  opts,
		clock: clock,
		ch:    make(chan batchPair[T, R]),
	}
}

// Add adds an item to the stream and returns when it has been processed, or if the context is
// canceled or times out. It returns two values: the value that the batch processor returned,
// and a context error. Even if Add returns a context error, the item may still be processed in
// the future!
func (s *StreamBatcher[T, R]) Add(ctx context.Context, t T) (R, error) {
	if s.running.CompareAndSwap(false, true) {
		go s.loop()
	}

	resp := make(chan R)
	pair := batchPair[T, R]{resp: resp, item: t}

	select {
	case s.ch <- pair:
	case <-ctx.Done():
		var zeroR R
		return zeroR, ctx.Err()
	}

	select {
	case r := <-resp:
		return r, nil
	case <-ctx.Done():
		var zeroR R
		return zeroR, ctx.Err()
	}
}

func (s *StreamBatcher[T, R]) loop() {
	defer s.running.Store(false)

	var items []T
	var resps []chan R
	for {
		clear(items)
		clear(resps)
		items, resps = items[:0], resps[:0]

		// wait for first item. if no item after a while, exit the goroutine
		idleC, _ := s.clock.NewTimer(s.opts.IdleTime)
		select {
		case pair := <-s.ch:
			items = append(items, pair.item)
			resps = append(resps, pair.resp)
		case <-idleC:
			// FIXME: what if Add does running CAS here and sees running, so sends on the
			// channel, but then this returns.
			// maybe we need to close a channel when we return so Add can select on that and
			// try to start again?
			return
		}

		// try to add more items. stop after a gap of MaxGap, total time of MaxTotalWait, or
		// MaxItems items.
		maxWaitC, _ := s.clock.NewTimer(s.opts.MaxDelay)
	loop:
		for len(items) < s.opts.MaxItems {
			gapC, _ := s.clock.NewTimer(s.opts.MinDelay)
			select {
			case pair := <-s.ch:
				items = append(items, pair.item)
				resps = append(resps, pair.resp)
			case <-gapC:
				break loop
			case <-maxWaitC:
				break loop
			}
		}

		err := s.fn(items)

		// send responses
		for _, resp := range resps {
			resp <- err
		}
	}
}
