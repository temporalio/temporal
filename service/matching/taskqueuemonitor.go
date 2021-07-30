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

package matching

import (
	"context"
	"fmt"
	"math"
	"time"
)

type (
	tqmonStop    struct{}
	tqmonGC      struct{}
	tqmonUsurped struct{ id *taskQueueID }

	// tqmonAccess is the interface by which a taskQueueMonitor observes and
	// updates a set of taskQueueManager instances.
	tqmonAccess interface {
		getTaskQueues(int) []taskQueueManager
		unloadTaskQueue(*taskQueueID)
	}

	// taskQueueMonitor performs garbage collection and unloads instances of
	// taskQueueManager accessed via a tqmonAccess.
	taskQueueMonitor struct {
		sigs chan interface{}
	}
)

// newTaskQueueMonitor constructs a new instance of taskQueueMonitor.
func newTaskQueueMonitor() *taskQueueMonitor {
	return &taskQueueMonitor{sigs: make(chan interface{})}
}

// Run runs the taskQueueMonitor event loop, performing GC of a set of
// taskQueueManager instances and unloading the same when signaled or when GC
// deems it appropriate. The implementaion of tqmonAccess MUST NOT re-enter this
// taskQueueMonitor instance.
func (m *taskQueueMonitor) Run(access tqmonAccess, config *Config) {
	defer close(m.sigs)
	t := time.NewTimer(config.IdleTaskqueueCheckInterval())
	defer t.Stop()

	for {
		select {
		case i := <-m.sigs:
			switch sig := i.(type) {
			case tqmonUsurped:
				access.unloadTaskQueue(sig.id)
			case tqmonGC:
				m.runGC(access)
				t.Reset(config.IdleTaskqueueCheckInterval())
			case tqmonStop:
				return
			default:
				panic(fmt.Sprintf(
					"Unsupported signal type sent to taskQueueMonitor %T", sig))
			}
		case <-t.C:
			m.runGC(access)
			t.Reset(config.IdleTaskqueueCheckInterval())
		}
	}
}

// Stop synchronously stops a running taskQueueMonitor. Once stopped a
// taskQueueMonitor must not be restarted. The only time this function returns
// an error is if the provided context ends while the shutdown is in process, in
// which case the ctx.Err() is returned and the internal state of the
// taskQueueMonitor instance is undefined.
func (m *taskQueueMonitor) Stop(ctx context.Context) error {
	select {
	case m.sigs <- tqmonStop{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-m.sigs:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// Signal sends the supplied message to the taskQueueMonitor. Supported message
// types are tqmonStop, tqmonUsurped, and tqmonGC
func (m *taskQueueMonitor) Signal(i interface{}) {
	m.sigs <- i
}

func (m *taskQueueMonitor) runGC(access tqmonAccess) {
	for _, tqm := range access.getTaskQueues(math.MaxInt32) {
		if live, id := tqm.Liveness(); !live {
			access.unloadTaskQueue(id)
		}
	}
}
