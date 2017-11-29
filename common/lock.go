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

package common

import (
	"context"
	"sync"
)

type (
	// RWMutex accepts a context in its Lock method.
	// It blocks the goroutine until either the lock is acquired or the context
	// is closed.
	RWMutex interface {
		Lock(context.Context) error
		Unlock()
		RLock(context.Context) error
		RUnlock()
	}

	rwMutexImpl struct {
		sync.RWMutex
	}
)

const (
	acquiring = iota
	acquired
	bailed
)

// NewRWMutex creates a new RWMutex
func NewRWMutex() RWMutex {
	return &rwMutexImpl{}
}

func (m *rwMutexImpl) Lock(ctx context.Context) error {
	lock := func() {
		m.RWMutex.Lock()
	}
	unlock := func() {
		m.RWMutex.Unlock()
	}
	return m.lockInternal(ctx, lock, unlock)
}

func (m *rwMutexImpl) RLock(ctx context.Context) error {
	lock := func() {
		m.RWMutex.RLock()
	}
	unlock := func() {
		m.RWMutex.RUnlock()
	}
	return m.lockInternal(ctx, lock, unlock)
}

func (m *rwMutexImpl) lockInternal(ctx context.Context, lock func(), unlock func()) error {
	var stateLock sync.Mutex
	state := acquiring

	acquiredCh := make(chan struct{})
	acquire := func() {
		lock()

		stateLock.Lock()
		defer stateLock.Unlock()
		if state == bailed {
			// already bailed due to context closing
			unlock()
		} else {
			state = acquired
		}

		close(acquiredCh)
	}
	go acquire()

	select {
	case <-acquiredCh:
		return nil
	case <-ctx.Done():
		{
			stateLock.Lock()
			defer stateLock.Unlock()
			if state == acquired {
				// Lock was already acquired before context expired
				return nil
			}
			state = bailed
			return ctx.Err()
		}
	}
}
