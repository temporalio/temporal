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

package aggregate

import (
	"sync/atomic"
	"time"
)

type (
	MovingWindowAvgChanImpl struct {
		windowSize    time.Duration
		buffer        chan timestampedData
		forceExpireCh chan interface{}
		sum           atomic.Int64
		count         atomic.Int64
	}
)

func NewMovingWindowAvgChanImpl(
	windowSize time.Duration,
	maxBufferSize int,
) *MovingWindowAvgChanImpl {
	ret := &MovingWindowAvgChanImpl{
		windowSize:    windowSize,
		buffer:        make(chan timestampedData, maxBufferSize),
		forceExpireCh: make(chan interface{}),
	}
	go ret.expireLoop() // TODO: need a mechanism to cleanup this goroutine
	return ret
}

func (a *MovingWindowAvgChanImpl) expireLoop() {
	for {
		select {
		case toExpire := <-a.buffer:
			if time.Since(toExpire.timestamp) > a.windowSize {
				// element already outside of window, remove from average
				a.sum.Add(-toExpire.value)
				a.count.Add(-1)
			} else {
				// first element out of the buffer should be the oldest so wait until
				//it moves out of the window before trying to remove more elements
				timer := time.NewTimer(a.windowSize - time.Since(toExpire.timestamp))
				select {
				case <-timer.C:
				case <-a.forceExpireCh: // if the buffer is full, remove one item so new adds don't get blocked
					timer.Stop()
					a.sum.Add(-toExpire.value)
					a.count.Add(-1)
				}
			}
		}
	}
}

func (a *MovingWindowAvgChanImpl) Record(val int64) {
	if len(a.buffer) == cap(a.buffer) {
		// blocks until there is room in the buffer to add more data
		a.forceExpireCh <- struct{}{}
	}

	a.sum.Add(val)
	a.count.Add(1)
	a.buffer <- timestampedData{value: val, timestamp: time.Now()}
}

func (a *MovingWindowAvgChanImpl) Average() float64 {
	if a.count.Load() == 0 {
		return 0
	}
	return float64(a.sum.Load() / a.count.Load())
}
