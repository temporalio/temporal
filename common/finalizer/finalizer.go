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

package finalizer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/internal/goro"
)

var (
	FinalizerAlreadyDoneErr = errors.New("finalizer already finalized")
	FinalizerUnknownIdErr   = errors.New("finalizer callback not found")
	FinalizerDuplicateIdErr = errors.New("finalizer callback already registered")
)

type Finalizer struct {
	logger    log.Logger
	mu        sync.Mutex
	wg        sync.WaitGroup
	finalized atomic.Bool
	callbacks map[string]func(context.Context) error
}

func NewFinalizer(
	logger log.Logger,
) *Finalizer {
	return &Finalizer{
		logger:    logger,
		callbacks: make(map[string]func(context.Context) error),
	}
}

func (f *Finalizer) Register(
	id string,
	callback func(context.Context) error,
) error {
	if f.finalized.Load() {
		return FinalizerAlreadyDoneErr
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.callbacks[id]; ok {
		return FinalizerDuplicateIdErr
	}
	f.callbacks[id] = callback
	return nil
}

func (f *Finalizer) Deregister(
	id string,
) (err error) {
	if f.finalized.Load() {
		return FinalizerAlreadyDoneErr
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.callbacks[id]; !ok {
		return FinalizerUnknownIdErr
	}
	delete(f.callbacks, id)
	return nil
}

func (f *Finalizer) Run(
	pool *goro.AdaptivePool,
	timeout time.Duration,
) int32 {
	if !f.finalized.CompareAndSwap(false, true) {
		f.logger.Warn("finalizer skipped: called more than once")
		return 0
	}

	if timeout == 0 {
		f.logger.Warn("finalizer skipped: zero timeout")
		return 0
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	totalCount := len(f.callbacks)
	f.logger.Info("finalizer starting",
		tag.NewInt("items", totalCount),
		tag.NewDurationTag("timeout", timeout))

	var completionCounter atomic.Int32
	var wg sync.WaitGroup
	wg.Add(totalCount)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	go func() {
		for _, callback := range f.callbacks {
			// NOTE: Once the pool is stopped due to a timeout,
			// any remaining callbacks will not be invoked anymore.
			pool.Do(func() {
				defer wg.Done()
				defer completionCounter.Add(1)
				_ = callback(ctx)
			})
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	var completed int32
	select {
	case <-done:
		completed = completionCounter.Load()
		f.logger.Info("finalizer completed",
			tag.NewInt32("completed-items", completed))
	case <-ctx.Done():
		pool.Stop()
		completed = completionCounter.Load()
		f.logger.Error("finalizer timed out",
			tag.NewInt32("completed-items", completed),
			tag.NewInt32("unfinished-items", int32(totalCount)-completed))
	}
	return completed
}
