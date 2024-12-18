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
)

type BatchStream[T, R any] struct {
	fn      func([]T) R
	opts    BatchStreamOptions
	ch      chan batchPair[T, R]
	running atomic.Bool
}

type batchPair[T, R any] struct {
	resp chan R
	t    T
}

type BatchStreamOptions struct {
	MaxItems     int
	IdleTime     time.Duration
	MaxTotalWait time.Duration
	MaxGap       time.Duration
}

// FIXME: more doc
// fn must not hold on to the slice, it may be reused.
func NewBatchStream[T, R any](fn func([]T) R, opts BatchStreamOptions) *BatchStream[T, R] {
	return &BatchStream[T, R]{
		fn:   fn,
		opts: opts,
		ch:   make(chan batchPair[T, R]),
	}
}

func (s *BatchStream[T, R]) Add(ctx context.Context, t T) (R, error) {
	if s.running.CompareAndSwap(false, true) {
		go s.loop()
	}

	resp := make(chan R)
	pair := batchPair[T, R]{resp: resp, t: t}

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

func (s *BatchStream[T, R]) loop() {
	defer s.running.Store(false)

	var items []T
	var resps []chan R
	for {
		clear(items)
		clear(resps)
		items, resps = items[:0], resps[:0]

		// wait for first item. if no item after a while, exit the goroutine
		select {
		case pair := <-s.ch:
			items = append(items, pair.t)
			resps = append(resps, pair.resp)
		case <-time.After(s.opts.IdleTime):
			return
		}

		// try to add more items. stop after a gap of MaxGap, total time of MaxTotalWait, or
		// MaxItems items.
		maxWaitC := time.After(s.opts.MaxTotalWait)
	loop:
		for len(items) < s.opts.MaxItems {
			select {
			case pair := <-s.ch:
				items = append(items, pair.t)
				resps = append(resps, pair.resp)
			case <-time.After(s.opts.MaxGap):
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
