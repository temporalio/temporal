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

package goro_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/internal/goro"
)

func block()   { <-make(chan struct{}) }
func nothing() {}

func TestAdaptivePool_CallsF(t *testing.T) {
	t.Parallel()
	ts := clock.NewEventTimeSource()

	p := goro.NewAdaptivePool(ts, 1, 10, 10*time.Millisecond, 10)
	defer p.Stop()

	var wg sync.WaitGroup
	wg.Add(1)
	p.Do(wg.Done)
	wg.Wait()
}

func TestAdaptivePool_Grows(t *testing.T) {
	t.Parallel()
	ts := clock.NewEventTimeSource()

	p := goro.NewAdaptivePool(ts, 5, 10, 10*time.Millisecond, 10)
	defer p.Stop()

	// occupy five workers
	p.Do(block)
	p.Do(block)
	p.Do(block)
	p.Do(block)
	p.Do(block)

	// sixth call will still start new worker after delay
	doneCh := make(chan struct{})
	go func() {
		p.Do(block)
		doneCh <- struct{}{}
	}()

	// wait for goroutine to block in Do
	// there should be one timer
	assert.Eventually(t, func() bool { return ts.NumTimers() == 1 }, time.Second, time.Millisecond)

	select {
	case <-doneCh:
		t.Error("should be blocked")
		return
	default:
	}

	assert.Equal(t, 5, p.NumWorkers()) // still 5 here

	ts.Advance(15 * time.Millisecond)
	<-doneCh

	assert.Equal(t, 6, p.NumWorkers()) // now 6 here
}

func TestAdaptivePool_DoesntGrowPastMax(t *testing.T) {
	t.Parallel()
	ts := clock.NewEventTimeSource()

	p := goro.NewAdaptivePool(ts, 5, 5, 10*time.Millisecond, 10)
	defer p.Stop()

	// occupy five workers, one is interruptible
	p.Do(block)
	p.Do(block)
	interruptCh := make(chan struct{})
	p.Do(func() { interruptCh <- struct{}{} })
	p.Do(block)
	p.Do(block)

	// sixth call will block
	doneCh := make(chan struct{})
	go func() {
		p.Do(block)
		doneCh <- struct{}{}
	}()

	// wait for goroutine to block in Do
	// we can't use NumTimers since it doesn't create a timer
	time.Sleep(10 * time.Millisecond)

	select {
	case <-doneCh:
		t.Error("should be blocked")
		return
	default:
	}

	// unblock fifth, which will allow sixth to run immediately
	<-interruptCh
	// wait for sixth
	<-doneCh

	assert.Equal(t, 5, p.NumWorkers()) // still 5
}

func TestAdaptivePool_ShrinksAgain(t *testing.T) {
	t.Parallel()
	ts := clock.NewEventTimeSource()

	p := goro.NewAdaptivePool(ts, 1, 5, 10*time.Millisecond, 1)
	defer p.Stop()

	// make 3 calls to force it to grow to 3 workers
	p.Do(block)

	syncCh := make(chan struct{}, 10)
	go p.Do(func() { syncCh <- struct{}{}; block() })
	// wait for goroutine to block in Do
	assert.Eventually(t, func() bool { return ts.NumTimers() == 1 }, time.Second, time.Millisecond)
	ts.Advance(10 * time.Millisecond) // allow it to start another
	<-syncCh                          // wait for it to call the function

	go p.Do(func() { syncCh <- struct{}{} })
	assert.Eventually(t, func() bool { return ts.NumTimers() == 1 }, time.Second, time.Millisecond)
	ts.Advance(10 * time.Millisecond) // allow it to start another
	<-syncCh                          // wait for it to call the function

	assert.Equal(t, 3, p.NumWorkers())

	// now there are 3 workers with one free, another call or three should start immediately
	p.Do(nothing)
	p.Do(nothing)
	p.Do(nothing)

	// after no more than 10ms, the free worker should exit
	// wait until worker is blocked on timer
	assert.Eventually(t, func() bool { return ts.NumTimers() == 1 }, time.Second, time.Millisecond)
	ts.Advance(20 * time.Millisecond) // let timer fire
	// wait for worker to exit
	assert.Eventually(t, func() bool { return p.NumWorkers() == 2 }, time.Second, time.Millisecond)
}
