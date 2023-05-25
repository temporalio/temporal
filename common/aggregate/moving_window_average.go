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
	"sync"
	"time"
)

type (
	MovingWindowAverage interface {
		Record(val int64)
		Average() float64
	}

	timestampedData struct {
		value     int64
		timestamp time.Time
	}

	MovingWindowAvgImpl struct {
		sync.Mutex
		windowSize    time.Duration
		maxBufferSize int
		buffer        []timestampedData
		headIdx       int
		tailIdx       int
		sum           int64
		count         int64
	}
)

func NewMovingWindowAvgImpl(
	windowSize time.Duration,
	maxBufferSize int,
) *MovingWindowAvgImpl {
	return &MovingWindowAvgImpl{
		windowSize:    windowSize,
		maxBufferSize: maxBufferSize,
		buffer:        make([]timestampedData, maxBufferSize),
	}
}

func (a *MovingWindowAvgImpl) Record(val int64) {
	a.Lock()
	defer a.Unlock()

	a.buffer[a.tailIdx] = timestampedData{timestamp: time.Now(), value: val}
	a.tailIdx = (a.tailIdx + 1) % a.maxBufferSize

	a.sum += val
	a.count++

	if a.tailIdx == a.headIdx {
		// buffer full, expire oldest element
		a.sum -= a.buffer[a.headIdx].value
		a.count--
		a.headIdx = (a.headIdx + 1) % a.maxBufferSize
	}
}

func (a *MovingWindowAvgImpl) Average() float64 {
	a.Lock()
	defer a.Unlock()

	a.expireOldValuesLocked()
	if a.count == 0 {
		return 0
	}
	return float64(a.sum) / float64(a.count)
}

func (a *MovingWindowAvgImpl) expireOldValuesLocked() {
	for ; a.headIdx != a.tailIdx; a.headIdx = (a.headIdx + 1) % a.maxBufferSize {
		if time.Since(a.buffer[a.headIdx].timestamp) < a.windowSize {
			break
		}
		a.sum -= a.buffer[a.headIdx].value
		a.count--
	}
}
