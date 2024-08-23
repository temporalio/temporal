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
	"time"

	"github.com/pkg/errors"
	cclock "go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/internal/goro"
)

var (
	FinalizerAlreadyDoneErr = errors.New("finalizer already finalized")
	FinalizerUnknownIdErr   = errors.New("finalizer callback not found")
	FinalizerDuplicateIdErr = errors.New("finalizer callback already registered")
)

type Finalizer struct {
	logger         log.Logger
	metricsHandler metrics.Handler
	mu             sync.Mutex
	finalized      bool
	callbacks      map[string]func(context.Context) error
}

func New(
	logger log.Logger,
	metricsHandler metrics.Handler,
) *Finalizer {
	return &Finalizer{
		logger:         logger,
		metricsHandler: metricsHandler,
		callbacks:      make(map[string]func(context.Context) error),
	}
}

// Register adds a callback to the finalizer.
// Returns an error if the ID is already registered, or when the finalizer is/was already running.
func (f *Finalizer) Register(
	id string,
	callback func(context.Context) error,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.finalized {
		// aborting immediately once the finalizer is/was running
		return FinalizerAlreadyDoneErr
	}

	if _, ok := f.callbacks[id]; ok {
		return FinalizerDuplicateIdErr
	}
	f.callbacks[id] = callback
	return nil
}

// Deregister removes a callback from the finalizer.
// Returns an error if the ID is not found, or when the finalizer is/was already running.
func (f *Finalizer) Deregister(
	id string,
) (err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.finalized {
		// aborting immediately once the finalizer is/was running
		return FinalizerAlreadyDoneErr
	}

	if _, ok := f.callbacks[id]; !ok {
		return FinalizerUnknownIdErr
	}
	delete(f.callbacks, id)
	return nil
}

// Run executes all registered callback functions within the given timeout (zero timeout skips execution).
// It can only be invoked once; calling it again has no effect.
// Returns the number of completed callbacks.
func (f *Finalizer) Run(
	timeout time.Duration,
) int {
	if timeout == 0 {
		f.logger.Info("finalizer skipped: zero timeout")
		return 0
	}

	f.mu.Lock()
	if f.finalized {
		f.logger.Warn("finalizer skipped: called more than once")
		f.mu.Unlock()
		return 0
	}
	f.finalized = true
	f.mu.Unlock() // unlocking immediately to unblock any calls to Register/Deregister

	totalCount := len(f.callbacks)
	if totalCount == 0 {
		f.logger.Debug("finalizer skipped: no callbacks")
		return 0
	}

	f.logger.Info("finalizer starting",
		tag.NewInt("items", totalCount),
		tag.NewDurationTag("timeout", timeout))

	startTime := time.Now()
	defer func() { metrics.FinalizerLatency.With(f.metricsHandler).Record(time.Since(startTime)) }()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	pool := goro.NewAdaptivePool(cclock.NewRealTimeSource(), 5, 15, 10*time.Millisecond, 10)
	defer pool.Stop()

	completionChannel := make(chan struct{})
	go func() {
		defer func() { f.logger.Info("finalizer loop ended") }()
		for _, callback := range f.callbacks {
			// NOTE: Once `pool.Stop` is called, any remaining calls to `pool.Do` will do nothing.
			pool.Do(func() {
				defer func() { completionChannel <- struct{}{} }()
				_ = callback(ctx)
			})
		}
	}()

	var completedCallbacks int
	defer func() {
		metrics.FinalizerItemsCompleted.With(f.metricsHandler).Record(int64(completedCallbacks))
		metrics.FinalizerItemsUnfinished.With(f.metricsHandler).Record(int64(totalCount - completedCallbacks))
	}()

	for {
		select {
		case <-completionChannel:
			completedCallbacks += 1
			if completedCallbacks == totalCount {
				f.logger.Info("finalizer completed",
					tag.NewInt("completed", completedCallbacks))
				return completedCallbacks
			}

		case <-ctx.Done():
			f.logger.Error("finalizer timed out",
				tag.NewInt("completed", completedCallbacks),
				tag.NewInt("unfinished", totalCount-completedCallbacks))
			return completedCallbacks
		}
	}
}
