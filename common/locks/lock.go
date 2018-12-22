// Copyright (c) 2017 Uber Technologies, Inc.
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

package locks

import (
	"context"
	"sync"
	"sync/atomic"
)

type (
	// Mutex accepts a context in its Lock method.
	// It blocks the goroutine until either the lock is acquired or the context
	// is closed.
	Mutex interface {
		Lock(context.Context) error
		Unlock()
	}

	mutexImpl struct {
		sync.Mutex
	}
)

const (
	acquiring = iota
	acquired
	bailed
)

// NewMutex creates a new RWMutex
func NewMutex() Mutex {
	return &mutexImpl{}
}

func (m *mutexImpl) Lock(ctx context.Context) error {
	return m.lockInternal(ctx)
}

func (m *mutexImpl) lockInternal(ctx context.Context) error {
	var state int32 = acquiring

	acquiredCh := make(chan struct{})
	go func() {
		m.Mutex.Lock()
		if !atomic.CompareAndSwapInt32(&state, acquiring, acquired) {
			// already bailed due to context closing
			m.Unlock()
		}

		close(acquiredCh)
	}()

	select {
	case <-acquiredCh:
		return nil
	case <-ctx.Done():
		{
			if !atomic.CompareAndSwapInt32(&state, acquiring, bailed) {
				return nil
			}
			return ctx.Err()
		}
	}
}
