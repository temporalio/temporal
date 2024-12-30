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

package stream_batcher

import (
	"context"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/clock"
)

type StreamBatcher[T, R any] struct {
	fn    func([]T) R          // batch executor function
	opts  StreamBatcherOptions // timing/size options
	clock clock.TimeSource     // clock for testing
	ch    chan batchPair[T, R] // channel for submitting items
	// keeps track of goroutine state:
	// if goroutine is not running, running == nil.
	// if it is running, running points to a channel that will be closed when the goroutine exits.
	running atomic.Pointer[chan struct{}]
}

type batchPair[T, R any] struct {
	resp chan R // response channel
	item T      // item to add
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

// NewStreamBatcher creates a StreamBatcher. It collects items passed to Add into slices, and
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
	resp := make(chan R)
	pair := batchPair[T, R]{resp: resp, item: t}

	for {
		runningC := s.running.Load()
		for runningC == nil {
			// goroutine is not running, try to start it
			newRunningC := make(chan struct{})
			if s.running.CompareAndSwap(nil, &newRunningC) {
				// we were the first one to notice the nil, start it now
				go s.loop(&newRunningC)
			}
			// if CompareAndSwap failed, someone else was calling Add at the same time and
			// started the goroutine already. reload to get the new running channel.
			runningC = s.running.Load()
		}

		select {
		case <-(*runningC):
			// we loaded a non-nil running channel, but it closed while we're waiting to
			// submit. the goroutine must have just exited. try again.
			continue
		case s.ch <- pair:
			select {
			case r := <-resp:
				return r, nil
			case <-ctx.Done():
				var zeroR R
				return zeroR, ctx.Err()
			}
		case <-ctx.Done():
			var zeroR R
			return zeroR, ctx.Err()
		}
	}
}

func (s *StreamBatcher[T, R]) loop(runningC *chan struct{}) {
	defer func() {
		// store nil so that Add knows it should start a goroutine
		s.running.Store(nil)
		// if Add loaded s.running after we decided to stop but before we Stored nil, so it
		// thought we were running when we're not, then we need to wait it up so that can start
		// us again.
		close(*runningC)
	}()

	var items []T
	var resps []chan R
	for {
		clear(items)
		clear(resps)
		items, resps = items[:0], resps[:0]

		// wait for first item. if no item after a while, exit the goroutine
		idleC, idleT := s.clock.NewTimer(s.opts.IdleTime)
		select {
		case pair := <-s.ch:
			items = append(items, pair.item)
			resps = append(resps, pair.resp)
		case <-idleC:
			return
		}
		idleT.Stop()

		// try to add more items. stop after a gap of MaxGap, total time of MaxTotalWait, or
		// MaxItems items.
		maxWaitC, maxWaitT := s.clock.NewTimer(s.opts.MaxDelay)
	loop:
		for len(items) < s.opts.MaxItems {
			gapC, gapT := s.clock.NewTimer(s.opts.MinDelay)
			select {
			case pair := <-s.ch:
				items = append(items, pair.item)
				resps = append(resps, pair.resp)
			case <-gapC:
				break loop
			case <-maxWaitC:
				gapT.Stop()
				break loop
			}
			gapT.Stop()
		}
		maxWaitT.Stop()

		// process batch
		r := s.fn(items)

		// send responses
		for _, resp := range resps {
			resp <- r
		}
	}
}
