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

package common

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var (
	AlreadyDoneErr = errors.New("cannot register callback, already cleaned up")
)

type Finalizer struct {
	logger    log.Logger
	mu        sync.Mutex
	wg        sync.WaitGroup
	callbacks map[string]func(context.Context)
}

func NewFinalizer(logger log.Logger) *Finalizer {
	return &Finalizer{
		logger:    logger,
		callbacks: make(map[string]func(context.Context)),
	}
}

func (b *Finalizer) Register(
	id string,
	callback func(context.Context),
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.callbacks == nil {
		return AlreadyDoneErr
	}

	b.wg.Add(1)
	b.callbacks[id] = callback
	return nil
}

func (b *Finalizer) Deregister(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.callbacks, id)
	b.wg.Done()
}

func (b *Finalizer) Run(timeout time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.logger.Info("running Finalizer",
		tag.NewInt("items", len(b.callbacks)))

	// TODO: use batches instead?
	var wg sync.WaitGroup
	for _, cb := range b.callbacks {
		wg.Add(1)
		go func(callback func(context.Context)) {
			defer wg.Done()
			callback(context.Background())
		}(cb)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		b.logger.Info("Finalizer completed")
	case <-time.NewTimer(timeout).C:
		b.logger.Error("Finalizer timed out")
	}
}
