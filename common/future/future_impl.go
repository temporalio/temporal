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
	"context"
	"sync/atomic"
)

const (
	// pending status indicates future is not ready
	// setting status indicates future is in transition to be ready, used to prevent data race
	// ready   status indicates future is ready

	pending int32 = iota
	setting
	ready
)

type (
	FutureImpl[T any] struct {
		status  int32
		readyCh chan struct{}

		value T
		err   error
	}
)

func NewFuture[T any]() *FutureImpl[T] {
	var value T
	return &FutureImpl[T]{
		status:  pending,
		readyCh: make(chan struct{}),

		value: value,
		err:   nil,
	}
}

func (f *FutureImpl[T]) Get(
	ctx context.Context,
) (T, error) {
	if f.Ready() {
		return f.value, f.err
	}

	select {
	case <-f.readyCh:
		return f.value, f.err
	case <-ctx.Done():
		var value T
		return value, ctx.Err()
	}
}

func (f *FutureImpl[T]) Set(
	value T,
	err error,
) {
	// cannot directly set status to `ready`, to prevent data race in case multiple `Get` occurs
	// instead set status to `setting` to prevent concurrent completion of this future
	if !atomic.CompareAndSwapInt32(
		&f.status,
		pending,
		setting,
	) {
		panic("future has already been completed")
	}

	f.value = value
	f.err = err
	atomic.CompareAndSwapInt32(&f.status, setting, ready)
	close(f.readyCh)
}

func (f *FutureImpl[T]) Ready() bool {
	return atomic.LoadInt32(&f.status) == ready
}
