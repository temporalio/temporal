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

	"go.temporal.io/server/internal/goro"
)

func TestAdaptivePool_CallsF(t *testing.T) {
	p := goro.NewAdaptivePool(1, 10, time.Millisecond, 10)
	defer p.Stop()

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(1)
	p.Do(wg.Done)
	wg.Wait()
	assert.Less(t, time.Since(start), time.Second)
}

func TestAdaptivePool_Grows(t *testing.T) {
	p := goro.NewAdaptivePool(1, 10, time.Millisecond, 10)
	defer p.Stop()

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(1)
	// occupy five workers for 1s
	p.Do(func() { time.Sleep(time.Second) })
	p.Do(func() { time.Sleep(time.Second) })
	p.Do(func() { time.Sleep(time.Second) })
	p.Do(func() { time.Sleep(time.Second) })
	p.Do(func() { time.Sleep(time.Second) })
	// sixth call will start new worker and complete in < 1s
	p.Do(wg.Done)
	wg.Wait()

	assert.Less(t, time.Since(start), time.Second)
}

func TestAdaptivePool_DoesntGrowPastMax(t *testing.T) {
	p := goro.NewAdaptivePool(1, 5, time.Millisecond, 10)
	defer p.Stop()

	start := time.Now()
	interruptCh := make(chan struct{})
	doneCh := make(chan struct{})
	// occupy five workers, one is interruptible
	p.Do(func() { time.Sleep(time.Second) })
	p.Do(func() { time.Sleep(time.Second) })
	p.Do(func() { interruptCh <- struct{}{} })
	p.Do(func() { time.Sleep(time.Second) })
	p.Do(func() { time.Sleep(time.Second) })
	// sixth call will block
	go func() {
		p.Do(func() { time.Sleep(time.Second) })
		doneCh <- struct{}{}
	}()

	// wait for done
	select {
	case <-doneCh:
		t.Error("should not be done yet")
		return
	case <-time.After(10 * time.Millisecond):
	}
	// unblock fifth, allow sixth to run
	<-interruptCh
	// wait for sixth
	<-doneCh

	assert.Less(t, time.Since(start), time.Second)
}

func TestAdaptivePool_ShrinksAgain(t *testing.T) {
	p := goro.NewAdaptivePool(1, 5, 10*time.Millisecond, 1)
	defer p.Stop()

	// make 3 calls to force it to grow to 3 workers
	p.Do(func() { time.Sleep(time.Second) })
	p.Do(func() { time.Sleep(time.Second) })
	p.Do(func() {})

	// now there are 3 workers with one free, another call or two should start immediately
	start := time.Now()
	p.Do(func() {})
	p.Do(func() {})
	assert.Less(t, time.Since(start), 10*time.Millisecond)

	// after 10ms, it should shrink back to 2
	time.Sleep(20 * time.Millisecond)

	// next call will wait for targetDelay
	start = time.Now()
	p.Do(func() {})
	assert.Greater(t, time.Since(start), 10*time.Millisecond)
}
